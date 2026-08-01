//! Web UI: axum HTTP server, password-gated session auth, and the routes
//! the frontend ticket (AR-11) will replace with real `askama` templates.
//!
//! This ticket owns routing, auth, and data-fetching; rendering here is
//! deliberately minimal (plain HTML strings marked `-- placeholder --`) so
//! AR-11 can swap it for real templates without touching this module's
//! handlers or tests.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::{Form, Router};
use axum_extra::extract::cookie::{Cookie, Key, PrivateCookieJar, SameSite};
use serde::Deserialize;

use crate::health::{HealthSnapshot, Status};
use crate::state::SharedAppState;
use crate::storage::{Category, Page, StorageError};

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
        .route("/config", get(config_view))
        .route("/logout", axum::routing::post(logout))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_auth));

    let public = Router::new()
        .route("/login", get(login_form).post(login_submit))
        .route("/healthz", get(healthz));

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

async fn login_form() -> Html<String> {
    Html(login_page_html(None))
}

fn login_page_html(error: Option<&str>) -> String {
    let error_html = match error {
        Some(msg) => format!("<p style=\"color:red\">{}</p>", html_escape(msg)),
        None => String::new(),
    };
    format!(
        "<!-- placeholder: AR-11 will replace with a real template -->\n\
         <h1>bsky-archiver login</h1>\n\
         {error_html}\n\
         <form method=\"post\" action=\"/login\">\n\
           <label>Password <input type=\"password\" name=\"password\"></label>\n\
           <button type=\"submit\">Log in</button>\n\
         </form>"
    )
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
        (
            StatusCode::UNAUTHORIZED,
            Html(login_page_html(Some("Incorrect password"))),
        )
            .into_response()
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

async fn dashboard(State(state): State<WebState>) -> Result<Html<String>, WebError> {
    let recent = state.app.store.list_posts(None, 1, 10).await?;
    let mut body = String::from(
        "<!-- placeholder: AR-11 will replace with a real template -->\n<h1>Dashboard</h1>\n<ul>\n",
    );
    for item in &recent.items {
        body.push_str(&format!(
            "<li>{} {} at {}</li>\n",
            html_escape(&item.category.to_string()),
            html_escape(&item.at_uri),
            html_escape(&item.indexed_at)
        ));
    }
    body.push_str("</ul>\n");
    body.push_str(&format!(
        "<p>Total archived (this category filter): {}</p>",
        recent.total_items
    ));
    Ok(Html(body))
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
) -> Result<Html<String>, WebError> {
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
    Ok(Html(render_posts_page(&result)))
}

fn render_posts_page(page: &Page<crate::storage::PostSummary>) -> String {
    let mut body = String::from(
        "<!-- placeholder: AR-11 will replace with a real template -->\n<h1>Posts</h1>\n<ul>\n",
    );
    for item in &page.items {
        body.push_str(&format!(
            "<li><a href=\"/posts/{}\">{} {}</a> ({} media)</li>\n",
            encode_post_id(&item.at_uri),
            html_escape(&item.category.to_string()),
            html_escape(&item.at_uri),
            item.media_count
        ));
    }
    body.push_str("</ul>\n");
    body.push_str(&format!(
        "<p>Page {} of {} ({} total)</p>",
        page.page, page.total_pages, page.total_items
    ));
    body
}

/// Encodes an `at_uri` for use as the `/posts/:id` path segment. Only `/`
/// needs escaping to keep the whole `at_uri` inside a single path segment
/// (axum matches routes on the raw, undecoded path, then percent-decodes
/// each segment before handing it to the handler).
fn encode_post_id(at_uri: &str) -> String {
    percent_encoding::utf8_percent_encode(at_uri, percent_encoding::NON_ALPHANUMERIC).to_string()
}

async fn post_detail(
    State(state): State<WebState>,
    Path(id): Path<String>,
) -> Result<Html<String>, WebError> {
    let at_uri = id;
    for category in [Category::Post, Category::Like, Category::Bookmark] {
        if let Some(record) = state.app.store.get_post(category, &at_uri).await? {
            let pretty = serde_json::to_string_pretty(&record.record)
                .unwrap_or_else(|_| "<invalid json>".to_string());
            let mut body = format!(
                "<!-- placeholder: AR-11 will replace with a real template -->\n\
                 <h1>{}</h1>\n<p>{}</p>\n<pre>{}</pre>\n<h2>Media</h2>\n<ul>\n",
                html_escape(&category.to_string()),
                html_escape(&record.at_uri),
                html_escape(&pretty)
            );
            for media in &record.media {
                body.push_str(&format!("<li>{}</li>\n", html_escape(&media.filename)));
            }
            body.push_str("</ul>");
            return Ok(Html(body));
        }
    }
    Err(WebError::NotFound)
}

// ---------------------------------------------------------------------
// Gallery
// ---------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct GalleryQuery {
    page: Option<u32>,
    page_size: Option<u32>,
}

async fn gallery(
    State(state): State<WebState>,
    Query(query): Query<GalleryQuery>,
) -> Result<Html<String>, WebError> {
    let page = query.page.unwrap_or(1).max(1);
    let page_size = clamp_page_size(query.page_size);

    let result = state.app.store.list_media(page, page_size).await?;
    let mut body = String::from(
        "<!-- placeholder: AR-11 will replace with a real template -->\n<h1>Gallery</h1>\n<div>\n",
    );
    for item in &result.items {
        body.push_str(&format!(
            "<figure>{} / {}</figure>\n",
            html_escape(&item.post_at_uri),
            html_escape(&item.filename)
        ));
    }
    body.push_str("</div>\n");
    body.push_str(&format!(
        "<p>Page {} of {} ({} total)</p>",
        result.page, result.total_pages, result.total_items
    ));
    Ok(Html(body))
}

// ---------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------

async fn config_view(State(state): State<WebState>) -> Html<String> {
    let redacted = format!("{:#?}", state.app.config);
    Html(format!(
        "<!-- placeholder: AR-11 will replace with a real template -->\n\
         <h1>Active configuration</h1>\n<pre>{}</pre>",
        html_escape(&redacted)
    ))
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

fn html_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
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
        )
        .await
        .unwrap();
        let page4_body = body_string(page4.into_response()).await;
        assert!(page4_body.contains("Page 4 of 3"));
        assert!(!page4_body.contains("<li>"));

        let huge_page_size = list_posts(
            State(app_state),
            Query(PostsQuery {
                category: None,
                page: Some(1),
                page_size: Some(u32::MAX),
            }),
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
                page: Some(1),
                page_size: Some(10),
            }),
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
                page: Some(2),
                page_size: Some(10),
            }),
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
}
