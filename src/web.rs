//! Web UI: axum HTTP server, password-gated session auth, and routing.
//!
//! This module owns routing, auth, and data-fetching; presentation lives
//! in [`crate::templates`] (askama templates + their view models) so this
//! file stays about *what data* each route needs, not how it's marked up.

use async_zip::tokio::write::ZipFileWriter;
use async_zip::{Compression, ZipEntryBuilder};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::{Form, Router};
use axum_extra::extract::cookie::{Cookie, Key, PrivateCookieJar, SameSite};
use serde::Deserialize;
use time::OffsetDateTime;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tokio_util::io::ReaderStream;

use crate::health::{HealthSnapshot, Status};
use crate::state::SharedAppState;
use crate::storage::{ArchiveStore, Category, MediaSummary, PostSummary, StorageError};
use crate::templates;

/// Total export size (in bytes) over which the gallery shows a soft
/// "this may take a while" warning. 1 GiB. Deliberately a warning only —
/// the download stays enabled, since category is the only filter and a hard
/// cap would lock out anyone whose single category exceeds it.
const EXPORT_WARN_THRESHOLD_BYTES: u64 = 1_073_741_824;

const SESSION_COOKIE: &str = "bsky_archiver_session";
const SESSION_VALUE: &str = "authenticated";
const DEFAULT_PAGE_SIZE: u32 = 20;
const MAX_PAGE_SIZE: u32 = 100;

/// Router state: the shared application state plus the cookie-signing key
/// derived from `UI_SESSION_SECRET`. Kept separate from
/// [`crate::state::AppState`] so this ticket doesn't have to change what
/// AR-9 already owns.
#[derive(Clone)]
struct WebState {
    app: SharedAppState,
    key: Key,
}

impl axum::extract::FromRef<WebState> for Key {
    fn from_ref(state: &WebState) -> Self {
        state.key.clone()
    }
}

/// Builds the full `axum::Router` for the web UI: public routes
/// (`/login`, `/healthz`) plus every other route behind the session-auth
/// middleware.
pub fn router(app: SharedAppState) -> Router {
    let key = Key::derive_from(app.config.ui_session_secret.expose_secret().as_bytes());
    let state = WebState { app, key };

    let protected = Router::new()
        .route("/", get(dashboard))
        .route("/posts", get(list_posts))
        .route("/posts/:id", get(post_detail))
        .route("/gallery", get(gallery))
        .route("/gallery/export", get(gallery_export))
        .route("/config", get(config_view))
        .route("/media/:category/:id/:filename", get(media_file))
        .route("/logout", axum::routing::post(logout))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_auth));

    let public = Router::new()
        .route("/login", get(login_form).post(login_submit))
        .route("/healthz", get(healthz))
        .route("/static/pico.min.css", get(pico_css))
        .route("/static/app.css", get(app_css))
        .route("/static/htmx.min.js", get(htmx_js));

    public.merge(protected).with_state(state)
}

// ---------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------

async fn require_auth(
    jar: PrivateCookieJar,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    match jar.get(SESSION_COOKIE) {
        Some(cookie) if cookie.value() == SESSION_VALUE => next.run(request).await,
        _ => Redirect::to("/login").into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct LoginForm {
    password: String,
}

async fn login_form() -> Response {
    askama_axum::into_response(&templates::LoginTemplate { error: None })
}

async fn login_submit(
    State(state): State<WebState>,
    jar: PrivateCookieJar,
    Form(form): Form<LoginForm>,
) -> Response {
    let expected = state.app.config.ui_password.expose_secret();
    if constant_time_eq(expected.as_bytes(), form.password.as_bytes()) {
        let cookie = Cookie::build((SESSION_COOKIE, SESSION_VALUE))
            .path("/")
            .http_only(true)
            .same_site(SameSite::Lax)
            .build();
        let jar = jar.add(cookie);
        (jar, Redirect::to("/")).into_response()
    } else {
        let body = askama_axum::into_response(&templates::LoginTemplate {
            error: Some("Incorrect password"),
        });
        (StatusCode::UNAUTHORIZED, body).into_response()
    }
}

async fn logout(jar: PrivateCookieJar) -> Response {
    let jar = jar.remove(Cookie::from(SESSION_COOKIE));
    (jar, Redirect::to("/login")).into_response()
}

/// Compares two byte strings in constant time (with respect to the length
/// of `expected`), so a failed login attempt can't be used to infer how
/// many leading characters of a guess matched the real password.
fn constant_time_eq(expected: &[u8], candidate: &[u8]) -> bool {
    // Always walk the same number of bytes (the length of `expected`)
    // regardless of whether the lengths match, so a length mismatch
    // doesn't return measurably faster than a same-length mismatch.
    let mut diff: u8 = (expected.len() != candidate.len()) as u8;
    for (i, &byte) in expected.iter().enumerate() {
        diff |= byte ^ candidate.get(i).copied().unwrap_or(0);
    }
    diff == 0
}

// ---------------------------------------------------------------------
// Static assets
// ---------------------------------------------------------------------
//
// Vendored (no build step, no CDN) and compiled straight into the
// binary so the single Docker image doesn't need to ship a separate
// static file tree.

const PICO_CSS: &[u8] = include_bytes!("../static/pico.min.css");
const APP_CSS: &[u8] = include_bytes!("../static/app.css");
const HTMX_JS: &[u8] = include_bytes!("../static/htmx.min.js");

async fn pico_css() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        PICO_CSS,
    )
}

async fn app_css() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], APP_CSS)
}

async fn htmx_js() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        HTMX_JS,
    )
}

// ---------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------

async fn healthz(State(state): State<WebState>) -> Response {
    let snapshot: HealthSnapshot = state.app.health.borrow().clone();
    let any_error = [
        &snapshot.firehose,
        &snapshot.rest_fallback,
        &snapshot.likes_bookmarks,
        &snapshot.media_downloader,
    ]
    .iter()
    .any(|s| s.status == Status::Error);

    let status = if any_error {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };

    (status, format!("{snapshot:?}")).into_response()
}

// ---------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------

async fn dashboard(State(state): State<WebState>) -> Result<Response, WebError> {
    let store = &state.app.store;
    let recent = store.list_posts(None, 1, 10).await?;
    let posts_count = store
        .list_posts(Some(Category::Post), 1, 1)
        .await?
        .total_items;
    let likes_count = store
        .list_posts(Some(Category::Like), 1, 1)
        .await?
        .total_items;
    let bookmarks_count = store
        .list_posts(Some(Category::Bookmark), 1, 1)
        .await?
        .total_items;

    let texts = fetch_excerpts(store, &recent.items).await;
    let rows = recent
        .items
        .iter()
        .zip(texts)
        .map(|(summary, text)| templates::post_row(summary, text.as_deref()))
        .collect();

    let snapshot = state.app.health.borrow().clone();
    let health = vec![
        templates::subsystem_row("Firehose", &snapshot.firehose),
        templates::subsystem_row("REST fallback", &snapshot.rest_fallback),
        templates::subsystem_row("Likes & bookmarks", &snapshot.likes_bookmarks),
        templates::subsystem_row("Media downloader", &snapshot.media_downloader),
    ];

    let template = templates::DashboardTemplate {
        posts_count,
        likes_count,
        bookmarks_count,
        health,
        recent: rows,
    };
    Ok(askama_axum::into_response(&template))
}

/// Best-effort fetch of each summary's post text, for building list-view
/// excerpts. The index deliberately doesn't store full record bodies
/// ([`crate::storage`]'s `PostSummary` is index-only), so this reads each
/// item's `record.json` straight from disk, concurrently. A read failure
/// for one item just means that item renders without an excerpt — it
/// never fails the whole page.
async fn fetch_excerpts(store: &ArchiveStore, items: &[PostSummary]) -> Vec<Option<String>> {
    let fetches = items.iter().map(|item| async move {
        match store.get_post(item.category, &item.at_uri).await {
            Ok(Some(record)) => templates::record_text(&record.record).map(str::to_string),
            Ok(None) | Err(_) => None,
        }
    });
    futures_util::future::join_all(fetches).await
}

// ---------------------------------------------------------------------
// Posts list + detail
// ---------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct PostsQuery {
    category: Option<String>,
    page: Option<u32>,
    page_size: Option<u32>,
}

fn clamp_page_size(requested: Option<u32>) -> u32 {
    requested
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE)
}

async fn list_posts(
    State(state): State<WebState>,
    Query(query): Query<PostsQuery>,
    headers: HeaderMap,
) -> Result<Response, WebError> {
    let category = match query.category.as_deref() {
        Some(raw) => Some(raw.parse::<Category>().map_err(|_| WebError::BadRequest {
            message: format!("unknown category {raw:?}"),
        })?),
        None => None,
    };
    let page = query.page.unwrap_or(1).max(1);
    let page_size = clamp_page_size(query.page_size);

    let result = state
        .app
        .store
        .list_posts(category, page, page_size)
        .await?;
    let texts = fetch_excerpts(&state.app.store, &result.items).await;
    let rows = result
        .items
        .iter()
        .zip(texts)
        .map(|(summary, text)| templates::post_row(summary, text.as_deref()))
        .collect();

    let category_param = query.category.clone();
    let pagination =
        templates::build_pagination(result.page, result.total_pages, result.total_items, |n| {
            posts_href(category_param.as_deref(), n, page_size)
        });

    if is_htmx_request(&headers) {
        let fragment = templates::PostsListTemplate { rows, pagination };
        Ok(askama_axum::into_response(&fragment))
    } else {
        let category_options = build_category_options(query.category.as_deref());
        let template = templates::PostsTemplate {
            rows,
            pagination,
            category_options,
        };
        Ok(askama_axum::into_response(&template))
    }
}

fn posts_href(category: Option<&str>, page: u32, page_size: u32) -> String {
    match category {
        Some(category) => format!("/posts?category={category}&page={page}&page_size={page_size}"),
        None => format!("/posts?page={page}&page_size={page_size}"),
    }
}

fn build_category_options(selected: Option<&str>) -> Vec<templates::CategoryOption> {
    let mut options = vec![templates::CategoryOption {
        label: "All",
        href: "/posts".to_string(),
        selected: selected.is_none(),
    }];
    for (value, label) in [
        ("posts", "Posts"),
        ("likes", "Likes"),
        ("bookmarks", "Bookmarks"),
    ] {
        options.push(templates::CategoryOption {
            label,
            href: format!("/posts?category={value}"),
            selected: selected == Some(value),
        });
    }
    options
}

/// Whether a request came from an `htmx` boosted link/form, in which case
/// handlers respond with just the swapped fragment instead of the full
/// page. Requests without this header (a plain link click, a browser
/// reload, or JS disabled entirely) always get the full page, which is
/// what makes pagination work with no JS at all.
fn is_htmx_request(headers: &HeaderMap) -> bool {
    headers.contains_key("HX-Request")
}

/// Encodes an `at_uri` for use as a `/posts/:id` or `/media/.../:id/...`
/// path segment. Only non-alphanumeric characters need escaping to keep
/// the whole `at_uri` inside a single path segment (axum matches routes
/// on the raw, undecoded path, then percent-decodes each segment before
/// handing it to the handler).
pub(crate) fn encode_post_id(at_uri: &str) -> String {
    percent_encoding::utf8_percent_encode(at_uri, percent_encoding::NON_ALPHANUMERIC).to_string()
}

async fn post_detail(
    State(state): State<WebState>,
    Path(id): Path<String>,
) -> Result<Response, WebError> {
    let at_uri = id;
    for category in [Category::Post, Category::Like, Category::Bookmark] {
        if let Some(record) = state.app.store.get_post(category, &at_uri).await? {
            let text = templates::record_text(&record.record).map(str::to_string);
            let media = record
                .media
                .iter()
                .map(|m| templates::PostMedia {
                    url: templates::media_url(category, &at_uri, &m.filename),
                    is_video: templates::is_video_content_type(m.content_type.as_deref()),
                    content_type: m
                        .content_type
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string()),
                    alt: format!("{} media", templates::category_label(category)),
                })
                .collect();
            let raw_json = serde_json::to_string_pretty(&record.record)
                .unwrap_or_else(|_| "<invalid json>".to_string());

            let template = templates::PostDetailTemplate {
                category_label: templates::category_label(category),
                category_badge_class: templates::category_badge_class(category),
                author: templates::author_did_from_at_uri(&at_uri).to_string(),
                bluesky_url: templates::bluesky_post_url(&at_uri),
                text,
                indexed_at: record.indexed_at.clone(),
                media,
                raw_json,
            };
            return Ok(askama_axum::into_response(&template));
        }
    }
    Err(WebError::NotFound)
}

// ---------------------------------------------------------------------
// Gallery
// ---------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct GalleryQuery {
    category: Option<String>,
    page: Option<u32>,
    page_size: Option<u32>,
}

/// Parses a gallery/export `category` query value into a [`Category`].
/// Accepts both the singular tokens the gallery links itself use
/// (`post`/`like`/`bookmark`) and the plural forms `/posts` uses
/// (`posts`/`likes`/`bookmarks`); anything else is a `400`, matching the
/// shape `/posts` returns for an unknown category.
fn parse_gallery_category(raw: Option<&str>) -> Result<Option<Category>, WebError> {
    let category = match raw {
        None => None,
        Some(raw) => Some(match raw {
            "post" | "posts" => Category::Post,
            "like" | "likes" => Category::Like,
            "bookmark" | "bookmarks" => Category::Bookmark,
            other => {
                return Err(WebError::BadRequest {
                    message: format!("unknown category {other:?}"),
                });
            }
        }),
    };
    Ok(category)
}

/// The singular query token the gallery uses for a category in its own
/// links (`/gallery?category=like`, `/gallery/export?category=like`).
fn category_token(category: Category) -> &'static str {
    match category {
        Category::Post => "post",
        Category::Like => "like",
        Category::Bookmark => "bookmark",
    }
}

fn build_gallery_category_options(selected: Option<Category>) -> Vec<templates::CategoryOption> {
    let mut options = vec![templates::CategoryOption {
        label: "All",
        href: "/gallery".to_string(),
        selected: selected.is_none(),
    }];
    for (category, label) in [
        (Category::Post, "Posts"),
        (Category::Like, "Likes"),
        (Category::Bookmark, "Bookmarks"),
    ] {
        options.push(templates::CategoryOption {
            label,
            href: format!("/gallery?category={}", category_token(category)),
            selected: selected == Some(category),
        });
    }
    options
}

async fn gallery(
    State(state): State<WebState>,
    Query(query): Query<GalleryQuery>,
    headers: HeaderMap,
) -> Result<Response, WebError> {
    let category = parse_gallery_category(query.category.as_deref())?;
    let page = query.page.unwrap_or(1).max(1);
    let page_size = clamp_page_size(query.page_size);

    let result = state
        .app
        .store
        .list_media(category, page, page_size)
        .await?;
    let items: Vec<_> = result.items.iter().map(templates::gallery_item).collect();

    let cat_token = category.map(category_token);
    let pagination =
        templates::build_pagination(result.page, result.total_pages, result.total_items, |n| {
            match cat_token {
                Some(token) => {
                    format!("/gallery?category={token}&page={n}&page_size={page_size}")
                }
                None => format!("/gallery?page={n}&page_size={page_size}"),
            }
        });

    if is_htmx_request(&headers) {
        let fragment = templates::GalleryGridTemplate { items, pagination };
        Ok(askama_axum::into_response(&fragment))
    } else {
        let estimate = state.app.store.export_estimate(category).await?;
        let export = build_gallery_export(category, estimate);
        let category_options = build_gallery_category_options(category);
        let template = templates::GalleryTemplate {
            items,
            pagination,
            category_options,
            export,
        };
        Ok(askama_axum::into_response(&template))
    }
}

fn build_gallery_export(
    category: Option<Category>,
    estimate: crate::storage::ExportEstimate,
) -> templates::GalleryExport {
    let href = match category {
        Some(category) => format!("/gallery/export?category={}", category_token(category)),
        None => "/gallery/export".to_string(),
    };
    let size_label = templates::format_bytes(estimate.total_bytes);
    let warning = (estimate.total_bytes > EXPORT_WARN_THRESHOLD_BYTES).then(|| {
        format!(
            "This export is about {size_label}. It may take a while — keep this tab open \
             until the download finishes. If the connection drops you'll need to start over."
        )
    });
    templates::GalleryExport {
        image_count: estimate.image_count,
        size_label,
        href,
        warning,
    }
}

// ---------------------------------------------------------------------
// Gallery export (store-only ZIP64 stream of every image in the selection)
// ---------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ExportQuery {
    category: Option<String>,
}

async fn gallery_export(
    State(state): State<WebState>,
    Query(query): Query<ExportQuery>,
) -> Result<Response, WebError> {
    let category = parse_gallery_category(query.category.as_deref())?;
    let items = state.app.store.list_export_media(category).await?;

    // An empty selection is a 404 rather than a valid zip of nothing, so a
    // hand-typed export URL for a category with no images fails loudly.
    if items.is_empty() {
        return Err(WebError::NotFound);
    }

    let label = category.map(category_token).unwrap_or("all");
    let filename = format!("bsky-archive-{label}-{}.zip", utc_date());

    // Stream the archive: a background task writes the zip into one half of
    // an in-memory pipe while the response body reads the other half, so
    // memory stays bounded (one file buffer at a time) no matter how large
    // the export is, and the first bytes go out before the last file is read.
    let store = state.app.store.clone();
    let (writer, reader) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        if let Err(err) = stream_zip(writer, store, items).await {
            // Headers are already sent, so a late error can't be signalled
            // over HTTP; the client just gets a truncated zip. A client
            // disconnect also surfaces here as a broken-pipe write error.
            tracing::warn!(error = %err, "gallery export stream ended early");
        }
    });

    let mut response = Response::new(Body::from_stream(ReaderStream::new(reader)));
    let response_headers = response.headers_mut();
    response_headers.insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/zip"),
    );
    let disposition = format!("attachment; filename=\"{filename}\"");
    response_headers.insert(
        header::CONTENT_DISPOSITION,
        header::HeaderValue::from_str(&disposition)
            .unwrap_or_else(|_| header::HeaderValue::from_static("attachment")),
    );
    Ok(response)
}

/// Current UTC date as `YYYY-MM-DD`, for the export filename.
fn utc_date() -> String {
    let now = OffsetDateTime::now_utc();
    format!(
        "{:04}-{:02}-{:02}",
        now.year(),
        u8::from(now.month()),
        now.day()
    )
}

/// Writes a store-only ZIP64 archive of `items` into `writer`. Each entry is
/// streamed straight from disk (never buffered whole in memory) with its
/// CRC32 computed on the fly and emitted via a data descriptor; `async-zip`'s
/// streaming writer always emits ZIP64 structures, so archives over 4 GiB or
/// 65,535 entries stay valid. A media row whose file is missing on disk is
/// logged and skipped rather than aborting the whole export.
async fn stream_zip(
    writer: tokio::io::DuplexStream,
    store: ArchiveStore,
    items: Vec<MediaSummary>,
) -> Result<(), async_zip::error::ZipError> {
    let mut zip = ZipFileWriter::with_tokio(writer);
    for item in items {
        let file = match store
            .open_media(item.category, &item.post_at_uri, &item.filename)
            .await
        {
            Ok(Some(file)) => file,
            Ok(None) => {
                tracing::warn!(
                    category = %item.category,
                    post = %item.post_at_uri,
                    filename = %item.filename,
                    "export skipping media row with no file on disk"
                );
                continue;
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    category = %item.category,
                    post = %item.post_at_uri,
                    filename = %item.filename,
                    "export skipping media row that failed to open"
                );
                continue;
            }
        };

        // `<category>/<encoded-post-id>/<filename>`: the per-post prefix is
        // required, not cosmetic — `filename_for` numbers files per post
        // (`000.jpg`, ...), so a flat layout would collide every post's
        // `000.jpg`. The encoded (percent-encoded) AT-URI is ugly but unique
        // and filesystem-safe on every platform.
        let entry_path = format!(
            "{}/{}/{}",
            item.category,
            encode_post_id(&item.post_at_uri),
            item.filename
        );
        let builder = ZipEntryBuilder::new(entry_path.into(), Compression::Stored);
        let mut entry_writer = zip.write_entry_stream(builder).await?;
        let mut reader = file.compat();
        futures_util::io::copy(&mut reader, &mut entry_writer).await?;
        entry_writer.close().await?;
    }
    zip.close().await?;
    Ok(())
}

// ---------------------------------------------------------------------
// Media file bytes
// ---------------------------------------------------------------------

async fn media_file(
    State(state): State<WebState>,
    Path((category, id, filename)): Path<(String, String, String)>,
) -> Result<Response, WebError> {
    let category: Category = category.parse().map_err(|_| WebError::NotFound)?;
    let bytes = state
        .app
        .store
        .read_media(category, &id, &filename)
        .await?
        .ok_or(WebError::NotFound)?;

    let mime = mime_guess::from_path(&filename).first_or_octet_stream();
    let content_type = header::HeaderValue::from_str(mime.as_ref())
        .unwrap_or_else(|_| header::HeaderValue::from_static("application/octet-stream"));

    let mut response = bytes.into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, content_type);
    Ok(response)
}

// ---------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------

async fn config_view(State(state): State<WebState>) -> Response {
    let config = &state.app.config;
    let redacted = "<redacted>".to_string();
    let rows = vec![
        templates::ConfigRow {
            key: "BSKY_IDENTIFIER",
            value: config.bsky_identifier.clone(),
            redacted: false,
        },
        templates::ConfigRow {
            key: "BSKY_APP_PASSWORD",
            value: redacted.clone(),
            redacted: true,
        },
        templates::ConfigRow {
            key: "BSKY_WATCH_HANDLES",
            value: config.bsky_watch_handles.join(", "),
            redacted: false,
        },
        templates::ConfigRow {
            key: "ARCHIVE_DIR",
            value: config.archive_dir.display().to_string(),
            redacted: false,
        },
        templates::ConfigRow {
            key: "DATABASE_PATH",
            value: config.database_path.display().to_string(),
            redacted: false,
        },
        templates::ConfigRow {
            key: "UI_PASSWORD",
            value: redacted.clone(),
            redacted: true,
        },
        templates::ConfigRow {
            key: "UI_SESSION_SECRET",
            value: redacted,
            redacted: true,
        },
        templates::ConfigRow {
            key: "UI_PORT",
            value: config.ui_port.to_string(),
            redacted: false,
        },
        templates::ConfigRow {
            key: "POLL_INTERVAL_SECONDS",
            value: config.poll_interval_seconds.to_string(),
            redacted: false,
        },
        templates::ConfigRow {
            key: "JETSTREAM_URL",
            value: config.jetstream_url.to_string(),
            redacted: false,
        },
        templates::ConfigRow {
            key: "MEDIA_MAX_CONCURRENT_DOWNLOADS",
            value: config.media_max_concurrent_downloads.to_string(),
            redacted: false,
        },
        templates::ConfigRow {
            key: "MEDIA_MAX_BYTES",
            value: config.media_max_bytes.to_string(),
            redacted: false,
        },
    ];
    askama_axum::into_response(&templates::ConfigTemplate { rows })
}

// ---------------------------------------------------------------------
// Errors + helpers
// ---------------------------------------------------------------------

#[derive(Debug)]
enum WebError {
    NotFound,
    BadRequest { message: String },
    Storage(StorageError),
}

impl From<StorageError> for WebError {
    fn from(err: StorageError) -> Self {
        WebError::Storage(err)
    }
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        match self {
            WebError::NotFound => (StatusCode::NOT_FOUND, "not found").into_response(),
            WebError::BadRequest { message } => (StatusCode::BAD_REQUEST, message).into_response(),
            WebError::Storage(err) => {
                tracing::error!(error = %err, "storage error serving web request");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AppConfig, Secret};
    use crate::health::health_channel;
    use crate::state::AppState;
    use crate::storage::{ArchiveStore, Category as StorageCategory};
    use axum::body::Body;
    use axum::http::{Request, header};
    use http_body_util::BodyExt;
    use serde_json::json;
    use std::sync::Arc;
    use tower::ServiceExt;

    async fn test_state() -> (tempfile::TempDir, SharedAppState) {
        let dir = tempfile::tempdir().expect("tempdir");
        let archive_dir = dir.path().join("archive");
        let database_path = dir.path().join("index.sqlite3");
        let store = ArchiveStore::open(archive_dir.clone(), database_path)
            .await
            .expect("open store");

        let config = AppConfig {
            bsky_identifier: "alice.bsky.social".to_string(),
            bsky_app_password: Secret::from("bsky-app-password-secret".to_string()),
            bsky_watch_handles: vec!["alice.bsky.social".to_string()],
            archive_dir,
            database_path: dir.path().join("index.sqlite3"),
            ui_password: Secret::from("correct horse battery staple".to_string()),
            ui_session_secret: Secret::from("a".repeat(64)),
            ui_port: 8080,
            poll_interval_seconds: 120,
            jetstream_url: url::Url::parse("wss://jetstream.example.com/subscribe").unwrap(),
            media_max_concurrent_downloads: 4,
            media_max_bytes: 104_857_600,
        };

        let (_health_tx, health_rx) = health_channel();
        let state: SharedAppState = Arc::new(AppState {
            config,
            store,
            health: health_rx,
        });
        (dir, state)
    }

    async fn body_string(response: Response) -> String {
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn unauthenticated_request_redirects_to_login() {
        let (_dir, state) = test_state().await;
        let app = router(state);

        let response = app
            .oneshot(Request::get("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(response.headers().get(header::LOCATION).unwrap(), "/login");
    }

    #[tokio::test]
    async fn healthz_is_reachable_without_auth() {
        let (_dir, state) = test_state().await;
        let app = router(state);

        let response = app
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn wrong_password_is_rejected() {
        let (_dir, state) = test_state().await;
        let app = router(state);

        let response = app
            .oneshot(
                Request::post("/login")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from("password=nope"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = body_string(response).await;
        assert!(body.contains("Incorrect password"));
    }

    #[tokio::test]
    async fn correct_password_logs_in_and_grants_access() {
        let (_dir, state) = test_state().await;
        let app = router(state);

        let login_response = app
            .clone()
            .oneshot(
                Request::post("/login")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from("password=correct%20horse%20battery%20staple"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(login_response.status(), StatusCode::SEE_OTHER);
        let cookie = login_response
            .headers()
            .get(header::SET_COOKIE)
            .expect("login sets a session cookie")
            .to_str()
            .unwrap()
            .to_string();
        let cookie_pair = cookie.split(';').next().unwrap().to_string();

        let protected_response = app
            .oneshot(
                Request::get("/")
                    .header(header::COOKIE, cookie_pair)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(protected_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn posts_pagination_behaves_at_boundaries() {
        let (_dir, state) = test_state().await;
        for i in 0..25 {
            state
                .store
                .save_post(
                    StorageCategory::Post,
                    &format!("at://did:plc:alice/app.bsky.feed.post/{i:03}"),
                    &format!("cid-{i}"),
                    json!({"i": i}),
                )
                .await
                .unwrap();
        }

        let key = Key::derive_from(state.config.ui_session_secret.expose_secret().as_bytes());
        let app_state = WebState {
            app: Arc::clone(&state),
            key,
        };

        let page1 = list_posts(
            State(app_state.clone()),
            Query(PostsQuery {
                category: None,
                page: Some(1),
                page_size: Some(10),
            }),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        assert!(
            body_string(page1.into_response())
                .await
                .contains("Page 1 of 3")
        );

        let page3 = list_posts(
            State(app_state.clone()),
            Query(PostsQuery {
                category: None,
                page: Some(3),
                page_size: Some(10),
            }),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        assert!(
            body_string(page3.into_response())
                .await
                .contains("Page 3 of 3")
        );

        let page4 = list_posts(
            State(app_state.clone()),
            Query(PostsQuery {
                category: None,
                page: Some(4),
                page_size: Some(10),
            }),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        let page4_body = body_string(page4.into_response()).await;
        assert!(page4_body.contains("Page 4 of 3"));
        assert!(!page4_body.contains("class=\"item-card\""));

        let huge_page_size = list_posts(
            State(app_state),
            Query(PostsQuery {
                category: None,
                page: Some(1),
                page_size: Some(u32::MAX),
            }),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        let body = body_string(huge_page_size.into_response()).await;
        assert!(body.contains(&format!("Page 1 of {}", 25u32.div_ceil(MAX_PAGE_SIZE))));
    }

    #[tokio::test]
    async fn gallery_pagination_behaves_at_boundaries() {
        let (_dir, state) = test_state().await;
        for i in 0..15 {
            let at_uri = format!("at://did:plc:alice/app.bsky.feed.post/{i:03}");
            state
                .store
                .save_post(
                    StorageCategory::Post,
                    &at_uri,
                    &format!("cid-{i}"),
                    json!({}),
                )
                .await
                .unwrap();
            state
                .store
                .save_media(
                    StorageCategory::Post,
                    &at_uri,
                    "img.jpg",
                    Some("image/jpeg".to_string()),
                    vec![0u8; 4],
                )
                .await
                .unwrap();
        }

        let key = Key::derive_from(state.config.ui_session_secret.expose_secret().as_bytes());
        let app_state = WebState {
            app: Arc::clone(&state),
            key,
        };

        let page1 = gallery(
            State(app_state.clone()),
            Query(GalleryQuery {
                category: None,
                page: Some(1),
                page_size: Some(10),
            }),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        assert!(
            body_string(page1.into_response())
                .await
                .contains("Page 1 of 2")
        );

        let page2 = gallery(
            State(app_state),
            Query(GalleryQuery {
                category: None,
                page: Some(2),
                page_size: Some(10),
            }),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        let body = body_string(page2.into_response()).await;
        assert!(body.contains("Page 2 of 2"));
        assert!(body.contains("(15 total)"));
    }

    #[tokio::test]
    async fn config_response_never_leaks_secrets() {
        let (_dir, state) = test_state().await;
        let app_password = state.config.bsky_app_password.expose_secret().to_string();
        let ui_password = state.config.ui_password.expose_secret().to_string();
        let session_secret = state.config.ui_session_secret.expose_secret().to_string();

        let key = Key::derive_from(session_secret.as_bytes());
        let app_state = WebState { app: state, key };

        let response = config_view(State(app_state)).await;
        let body = body_string(response.into_response()).await;

        assert!(!body.contains(&app_password));
        assert!(!body.contains(&ui_password));
        assert!(!body.contains(&session_secret));
        assert!(body.contains("&lt;redacted&gt;"));
    }

    #[tokio::test]
    async fn post_detail_round_trips_and_missing_id_is_404() {
        let (_dir, state) = test_state().await;
        let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
        state
            .store
            .save_post(
                StorageCategory::Post,
                at_uri,
                "cid-1",
                json!({"text": "hi"}),
            )
            .await
            .unwrap();

        let app = router(Arc::clone(&state));
        let login_response = app
            .clone()
            .oneshot(
                Request::post("/login")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from("password=correct%20horse%20battery%20staple"))
                    .unwrap(),
            )
            .await
            .unwrap();
        let cookie = login_response
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_string();

        let found = app
            .clone()
            .oneshot(
                Request::get(format!("/posts/{}", encode_post_id(at_uri)))
                    .header(header::COOKIE, cookie.clone())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(found.status(), StatusCode::OK);
        let body = body_string(found).await;
        assert!(body.contains("hi"));

        let missing = app
            .oneshot(
                Request::get(format!("/posts/{}", encode_post_id("at://missing")))
                    .header(header::COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    /// Logs in against `app` and returns the `Cookie` header value to reuse
    /// on subsequent requests.
    async fn login(app: &Router) -> String {
        let response = app
            .clone()
            .oneshot(
                Request::post("/login")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from("password=correct%20horse%20battery%20staple"))
                    .unwrap(),
            )
            .await
            .unwrap();
        response
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_string()
    }

    /// Seeds one image post and one video-bearing like, covering both
    /// media branches (`<img>` and `<video>`) every template needs to
    /// handle.
    async fn seed_fixture_media(store: &ArchiveStore) -> (String, String) {
        let image_post = "at://did:plc:alice/app.bsky.feed.post/img1";
        store
            .save_post(
                StorageCategory::Post,
                image_post,
                "cid-img1",
                json!({"text": "a photo post", "createdAt": "2024-01-01T00:00:00Z"}),
            )
            .await
            .unwrap();
        store
            .save_media(
                StorageCategory::Post,
                image_post,
                "img1.jpg",
                Some("image/jpeg".to_string()),
                vec![0xFFu8; 8],
            )
            .await
            .unwrap();

        let video_like = "at://did:plc:bob/app.bsky.feed.post/vid1";
        store
            .save_post(
                StorageCategory::Like,
                video_like,
                "cid-vid1",
                json!({"text": "a liked video"}),
            )
            .await
            .unwrap();
        store
            .save_media(
                StorageCategory::Like,
                video_like,
                "vid1.mp4",
                Some("video/mp4".to_string()),
                vec![0x00u8; 8],
            )
            .await
            .unwrap();

        (image_post.to_string(), video_like.to_string())
    }

    #[tokio::test]
    async fn dashboard_renders_counts_health_and_recent_activity() {
        let (_dir, state) = test_state().await;
        seed_fixture_media(&state.store).await;
        let app = router(Arc::clone(&state));
        let cookie = login(&app).await;

        let response = app
            .oneshot(
                Request::get("/")
                    .header(header::COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response).await;

        assert!(body.contains("a photo post"), "post excerpt missing");
        assert!(
            body.contains("Connected") || body.contains("Degraded"),
            "health status missing"
        );
        assert!(body.contains("badge-post"), "category badge missing");
        assert!(
            body.contains("<img") && body.contains("alt="),
            "image thumbnail missing alt"
        );
    }

    #[tokio::test]
    async fn posts_list_renders_cards_badges_thumbnails_and_pagination() {
        let (_dir, state) = test_state().await;
        seed_fixture_media(&state.store).await;
        let app = router(Arc::clone(&state));
        let cookie = login(&app).await;

        let response = app
            .oneshot(
                Request::get("/posts")
                    .header(header::COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response).await;

        assert!(body.contains("a photo post"));
        assert!(body.contains("a liked video"));
        assert!(body.contains("badge-post") && body.contains("badge-like"));
        assert!(body.contains("<img") && body.contains("alt="));
        assert!(body.contains("<video"));
        assert!(body.contains("class=\"pagination\""));
        assert!(body.contains("Page 1 of 1"));
    }

    #[tokio::test]
    async fn posts_list_htmx_request_returns_only_the_fragment() {
        let (_dir, state) = test_state().await;
        seed_fixture_media(&state.store).await;
        let app = router(Arc::clone(&state));
        let cookie = login(&app).await;

        let response = app
            .oneshot(
                Request::get("/posts")
                    .header(header::COOKIE, cookie)
                    .header("HX-Request", "true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response).await;

        assert!(body.contains("id=\"posts-list\""));
        assert!(
            !body.contains("<nav>\n<ul>"),
            "fragment must not include the base layout nav"
        );
        assert!(
            !body.contains("Dashboard</a>"),
            "fragment must not include full-page chrome"
        );
    }

    #[tokio::test]
    async fn post_detail_renders_image_and_video_with_raw_json_and_bluesky_link() {
        let (_dir, state) = test_state().await;
        let (image_post, video_like) = seed_fixture_media(&state.store).await;
        let app = router(Arc::clone(&state));
        let cookie = login(&app).await;

        let image_response = app
            .clone()
            .oneshot(
                Request::get(format!("/posts/{}", encode_post_id(&image_post)))
                    .header(header::COOKIE, cookie.clone())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let image_body = body_string(image_response).await;
        assert!(image_body.contains("a photo post"));
        assert!(image_body.contains("<img") && image_body.contains("alt="));
        assert!(image_body.contains("<details>") && image_body.contains("Raw JSON"));
        assert!(image_body.contains("View on Bluesky"));
        assert!(image_body.contains("did:plc:alice"));

        let video_response = app
            .oneshot(
                Request::get(format!("/posts/{}", encode_post_id(&video_like)))
                    .header(header::COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let video_body = body_string(video_response).await;
        assert!(video_body.contains("a liked video"));
        assert!(video_body.contains("<video") && video_body.contains("controls"));
        assert!(video_body.contains("badge-like"));
    }

    #[tokio::test]
    async fn gallery_renders_media_grid_with_alt_text_and_pagination() {
        let (_dir, state) = test_state().await;
        seed_fixture_media(&state.store).await;
        let app = router(Arc::clone(&state));
        let cookie = login(&app).await;

        let response = app
            .oneshot(
                Request::get("/gallery")
                    .header(header::COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = body_string(response).await;

        assert!(body.contains("<img") && body.contains("alt="));
        assert!(body.contains("<video"));
        assert!(body.contains("data-lightbox"));
        assert!(body.contains("id=\"lightbox\""));
        assert!(body.contains("class=\"pagination\""));
    }

    #[tokio::test]
    async fn media_route_serves_bytes_with_guessed_content_type() {
        let (_dir, state) = test_state().await;
        let (image_post, _) = seed_fixture_media(&state.store).await;
        let app = router(Arc::clone(&state));
        let cookie = login(&app).await;

        let url = format!(
            "/media/{}/{}/img1.jpg",
            StorageCategory::Post,
            encode_post_id(&image_post)
        );
        let response = app
            .oneshot(
                Request::get(url)
                    .header(header::COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "image/jpeg"
        );
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(bytes.as_ref(), [0xFFu8; 8]);
    }

    #[tokio::test]
    async fn media_route_404s_for_unknown_file() {
        let (_dir, state) = test_state().await;
        let (image_post, _) = seed_fixture_media(&state.store).await;
        let app = router(Arc::clone(&state));
        let cookie = login(&app).await;

        let url = format!(
            "/media/{}/{}/does-not-exist.jpg",
            StorageCategory::Post,
            encode_post_id(&image_post)
        );
        let response = app
            .oneshot(
                Request::get(url)
                    .header(header::COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn config_page_marks_secrets_redacted_but_shows_plain_values() {
        let (_dir, state) = test_state().await;
        let app = router(Arc::clone(&state));
        let cookie = login(&app).await;

        let response = app
            .oneshot(
                Request::get("/config")
                    .header(header::COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = body_string(response).await;

        assert!(body.contains("UI_PASSWORD"));
        assert!(body.contains("BSKY_APP_PASSWORD"));
        assert!(body.contains("UI_SESSION_SECRET"));
        assert!(body.matches("&lt;redacted&gt;").count() >= 3);
        assert!(body.contains("alice.bsky.social"));
        assert!(body.contains("class=\"redacted\""));
    }

    #[tokio::test]
    async fn login_page_renders_form_and_error_state() {
        let (_dir, state) = test_state().await;
        let app = router(state);

        let blank = app
            .clone()
            .oneshot(Request::get("/login").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let blank_body = body_string(blank).await;
        assert!(blank_body.contains("type=\"password\""));
        assert!(!blank_body.contains("Incorrect password"));

        let rejected = app
            .oneshot(
                Request::post("/login")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from("password=nope"))
                    .unwrap(),
            )
            .await
            .unwrap();
        let error_body = body_string(rejected).await;
        assert!(error_body.contains("Incorrect password"));
        assert!(error_body.contains("role=\"alert\""));
    }

    /// Seeds one image under `Post` and one video under `Like`, for the
    /// gallery filter / export predicate tests.
    async fn seed_image_and_video(store: &ArchiveStore) -> (String, String) {
        let image_post = "at://did:plc:alice/app.bsky.feed.post/img";
        store
            .save_post(StorageCategory::Post, image_post, "cid-img", json!({}))
            .await
            .unwrap();
        store
            .save_media(
                StorageCategory::Post,
                image_post,
                "000.jpg",
                Some("image/jpeg".to_string()),
                vec![0xFFu8; 8],
            )
            .await
            .unwrap();

        let video_like = "at://did:plc:bob/app.bsky.feed.post/vid";
        store
            .save_post(StorageCategory::Like, video_like, "cid-vid", json!({}))
            .await
            .unwrap();
        store
            .save_media(
                StorageCategory::Like,
                video_like,
                "000.mp4",
                Some("video/mp4".to_string()),
                vec![0x00u8; 8],
            )
            .await
            .unwrap();

        (image_post.to_string(), video_like.to_string())
    }

    #[tokio::test]
    async fn gallery_category_filter_isolates_media() {
        let (_dir, state) = test_state().await;
        let (image_post, video_like) = seed_image_and_video(&state.store).await;
        let key = Key::derive_from(state.config.ui_session_secret.expose_secret().as_bytes());
        let app_state = WebState {
            app: Arc::clone(&state),
            key,
        };

        // Filtering to likes shows only the like's media, not the post's.
        let likes = gallery(
            State(app_state.clone()),
            Query(GalleryQuery {
                category: Some("like".to_string()),
                page: Some(1),
                page_size: Some(20),
            }),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        let likes_body = body_string(likes.into_response()).await;
        assert!(likes_body.contains(&encode_post_id(&video_like)));
        assert!(!likes_body.contains(&encode_post_id(&image_post)));
        // The category nav marks the current selection.
        assert!(likes_body.contains("<span class=\"current\">Likes</span>"));
    }

    #[tokio::test]
    async fn gallery_export_estimate_counts_images_only_including_null_content_type() {
        let (_dir, state) = test_state().await;
        // A null-content-type row whose filename extension marks it an image
        // (the case a bare `content_type LIKE 'image/%'` predicate would drop).
        let null_ct = "at://did:plc:alice/app.bsky.feed.post/pngnull";
        state
            .store
            .save_post(StorageCategory::Post, null_ct, "cid-png", json!({}))
            .await
            .unwrap();
        state
            .store
            .save_media(
                StorageCategory::Post,
                null_ct,
                "000.png",
                None,
                vec![0u8; 4],
            )
            .await
            .unwrap();
        // A video under the same category — excluded from the count.
        seed_image_and_video(&state.store).await;

        let key = Key::derive_from(state.config.ui_session_secret.expose_secret().as_bytes());
        let app_state = WebState {
            app: Arc::clone(&state),
            key,
        };

        // Posts category: the jpg image + the null-content-type png = 2, no video.
        let posts = gallery(
            State(app_state.clone()),
            Query(GalleryQuery {
                category: Some("post".to_string()),
                page: Some(1),
                page_size: Some(20),
            }),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        let posts_body = body_string(posts.into_response()).await;
        assert!(posts_body.contains("2 images"));
        assert!(posts_body.contains("Download zip"));

        // Likes category: only a video → empty export selection.
        let likes = gallery(
            State(app_state),
            Query(GalleryQuery {
                category: Some("like".to_string()),
                page: Some(1),
                page_size: Some(20),
            }),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        let likes_body = body_string(likes.into_response()).await;
        assert!(likes_body.contains("No images in this selection to export."));
        assert!(!likes_body.contains("Download zip"));
    }

    #[tokio::test]
    async fn gallery_and_export_reject_unknown_category_with_400() {
        let (_dir, state) = test_state().await;
        let app = router(Arc::clone(&state));
        let cookie = login(&app).await;

        for url in ["/gallery?category=bogus", "/gallery/export?category=bogus"] {
            let response = app
                .clone()
                .oneshot(
                    Request::get(url)
                        .header(header::COOKIE, cookie.clone())
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "unknown category on {url} should be 400"
            );
        }
    }

    #[tokio::test]
    async fn gallery_export_empty_selection_is_404() {
        let (_dir, state) = test_state().await;
        // Only a video is archived, so the image export selection is empty.
        seed_image_and_video(&state.store).await;
        let app = router(Arc::clone(&state));
        let cookie = login(&app).await;

        let response = app
            .oneshot(
                Request::get("/gallery/export?category=like")
                    .header(header::COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn gallery_export_streams_store_only_zip_of_images() {
        use std::io::Read as _;

        let (_dir, state) = test_state().await;
        let (image_post, _video_like) = seed_image_and_video(&state.store).await;
        let app = router(Arc::clone(&state));
        let cookie = login(&app).await;

        let response = app
            .oneshot(
                Request::get("/gallery/export")
                    .header(header::COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/zip"
        );

        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let mut archive =
            zip::ZipArchive::new(std::io::Cursor::new(bytes.to_vec())).expect("valid zip");
        assert_eq!(archive.len(), 1, "only the image, not the video");
        let mut entry = archive.by_index(0).unwrap();
        assert_eq!(
            entry.name(),
            format!("posts/{}/000.jpg", encode_post_id(&image_post))
        );
        // Store-only: compression method 0.
        assert_eq!(entry.compression(), zip::CompressionMethod::Stored);
        let mut buf = Vec::new();
        entry.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, vec![0xFFu8; 8]);
    }
}
