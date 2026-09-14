//! Gallery: the media grid (`/gallery`) and the store-only zip export
//! (`/gallery/export`).

use async_zip::tokio::write::ZipFileWriter;
use async_zip::{Compression, ZipEntryBuilder};
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, header};
use axum::response::Response;
use serde::Deserialize;
use time::OffsetDateTime;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tokio_util::io::ReaderStream;

use super::{WebError, WebState, clamp_page_size, encode_post_id, is_htmx_request};
use crate::storage::{ArchiveStore, Category, MediaSort, MediaSummary};
use crate::templates;

/// Total export size (in bytes) over which the gallery shows a soft
/// "this may take a while" warning. 1 GiB. Deliberately a warning only —
/// the download stays enabled, since category is the only filter and a hard
/// cap would lock out anyone whose single category exceeds it.
const EXPORT_WARN_THRESHOLD_BYTES: u64 = 1_073_741_824;

// ---------------------------------------------------------------------
// Gallery
// ---------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub(super) struct GalleryQuery {
    pub(super) category: Option<String>,
    pub(super) page: Option<u32>,
    pub(super) page_size: Option<u32>,
    pub(super) sort: Option<String>,
}

/// Parses a gallery `sort` query value into a [`MediaSort`]: `newest`
/// (default, archive time) / `oldest` / `created-newest` / `created-oldest`
/// / `bookmarked-newest` / `bookmarked-oldest` (the bookmark action time,
/// the order Bluesky's own bookmarks list uses); anything else is a `400`,
/// matching how an unknown category is handled.
fn parse_gallery_sort(raw: Option<&str>) -> Result<MediaSort, WebError> {
    let sort = match raw {
        None | Some("newest") => MediaSort::NewestArchived,
        Some("oldest") => MediaSort::OldestArchived,
        Some("created-newest") => MediaSort::NewestCreated,
        Some("created-oldest") => MediaSort::OldestCreated,
        Some("bookmarked-newest") => MediaSort::NewestAction,
        Some("bookmarked-oldest") => MediaSort::OldestAction,
        Some(other) => {
            return Err(WebError::BadRequest {
                message: format!("unknown sort {other:?}"),
            });
        }
    };
    Ok(sort)
}

/// The query token for a [`MediaSort`] in gallery links, the token that
/// [`parse_gallery_sort`] accepts.
fn sort_token(sort: MediaSort) -> &'static str {
    match sort {
        MediaSort::NewestArchived => "newest",
        MediaSort::OldestArchived => "oldest",
        MediaSort::NewestCreated => "created-newest",
        MediaSort::OldestCreated => "created-oldest",
        MediaSort::NewestAction => "bookmarked-newest",
        MediaSort::OldestAction => "bookmarked-oldest",
    }
}

/// Builds `/gallery` hrefs carrying every active filter, so switching one
/// (category, sort, page) keeps the others.
fn gallery_href(category: Option<Category>, sort: MediaSort, page: u32, page_size: u32) -> String {
    let category_token = category
        .map(category_token)
        .map(|token| format!("category={token}&"))
        .unwrap_or_default();
    format!(
        "/gallery?{category_token}sort={}&page={page}&page_size={page_size}",
        sort_token(sort)
    )
}

fn build_gallery_sort_options(
    active_category: Option<Category>,
    selected: MediaSort,
    page_size: u32,
) -> Vec<templates::SortOption> {
    [
        (MediaSort::NewestArchived, "Newest archived"),
        (MediaSort::OldestArchived, "Oldest archived"),
        (MediaSort::NewestCreated, "Newest created"),
        (MediaSort::OldestCreated, "Oldest created"),
        (MediaSort::NewestAction, "Newest bookmarked"),
        (MediaSort::OldestAction, "Oldest bookmarked"),
    ]
    .into_iter()
    .map(|(sort, label)| templates::SortOption {
        label,
        href: gallery_href(active_category, sort, 1, page_size),
        selected: sort == selected,
    })
    .collect()
}

/// Parses a gallery/export `category` query value into a [`Category`].
/// Accepts both the singular tokens the gallery links itself use
/// (`post`/`like`/`bookmark`/`tumblr-like`) and the plural forms `/posts`
/// uses (`posts`/`likes`/`bookmarks`/`tumblr_likes`); anything else is a
/// `400`, matching the shape `/posts` returns for an unknown category.
fn parse_gallery_category(raw: Option<&str>) -> Result<Option<Category>, WebError> {
    let category = match raw {
        None => None,
        Some(raw) => Some(match raw {
            "post" | "posts" => Category::Post,
            "like" | "likes" => Category::Like,
            "bookmark" | "bookmarks" => Category::Bookmark,
            "tumblr-like" | "tumblr_likes" => Category::TumblrLike,
            "telegram" | "telegram_channels" => Category::TelegramChannel,
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
        Category::TumblrLike => "tumblr-like",
        Category::TelegramChannel => "telegram",
    }
}

fn build_gallery_category_options(
    selected: Option<Category>,
    sort: MediaSort,
    page_size: u32,
) -> Vec<templates::CategoryOption> {
    let mut options = vec![templates::CategoryOption {
        label: "All",
        href: gallery_href(None, sort, 1, page_size),
        selected: selected.is_none(),
    }];
    for (category, label) in [
        (Category::Post, "Posts"),
        (Category::Like, "Likes"),
        (Category::Bookmark, "Bookmarks"),
        (Category::TumblrLike, "Tumblr Likes"),
    ] {
        options.push(templates::CategoryOption {
            label,
            href: gallery_href(Some(category), sort, 1, page_size),
            selected: selected == Some(category),
        });
    }
    options
}

pub(super) async fn gallery(
    State(state): State<WebState>,
    Query(query): Query<GalleryQuery>,
    headers: HeaderMap,
) -> Result<Response, WebError> {
    let category = parse_gallery_category(query.category.as_deref())?;
    let sort = parse_gallery_sort(query.sort.as_deref())?;
    let page = query.page.unwrap_or(1).max(1);
    let page_size = clamp_page_size(query.page_size);

    let result = state
        .app
        .store
        .list_media(category, page, page_size, sort)
        .await?;
    let items: Vec<_> = result.items.iter().map(templates::gallery_item).collect();

    let pagination =
        templates::build_pagination(result.page, result.total_pages, result.total_items, |n| {
            gallery_href(category, sort, n, page_size)
        });

    if is_htmx_request(&headers) {
        let fragment = templates::GalleryGridTemplate { items, pagination };
        Ok(askama_axum::into_response(&fragment))
    } else {
        let estimate = state.app.store.export_estimate(category).await?;
        let export = build_gallery_export(category, estimate);
        let category_options = build_gallery_category_options(category, sort, page_size);
        let sort_options = build_gallery_sort_options(category, sort, page_size);
        let template = templates::GalleryTemplate {
            version: templates::APP_VERSION,
            git_revision: templates::GIT_REVISION,
            build_date: templates::BUILD_DATE,
            items,
            pagination,
            category_options,
            sort_options,
            export,
        };
        Ok(askama_axum::into_response(&template))
    }
}

// ---------------------------------------------------------------------
// Gallery export (store-only ZIP64 stream of every image in the selection)
// ---------------------------------------------------------------------

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

#[derive(Debug, Deserialize)]
pub(super) struct ExportQuery {
    category: Option<String>,
}

pub(super) async fn gallery_export(
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
