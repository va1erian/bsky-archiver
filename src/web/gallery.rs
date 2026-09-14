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
// Account viewer (live browse of one account's pictures)
// ---------------------------------------------------------------------

/// How many feed items to request per API page in the account viewer.
/// With the `posts_with_media` filter every item carries media, so one
/// page fills a grid comfortably without chaining requests per view.
const ACCOUNT_GALLERY_PAGE_LIMIT: u32 = 50;

#[derive(Debug, Deserialize)]
pub(super) struct AccountGalleryQuery {
    pub(super) actor: Option<String>,
    pub(super) cursor: Option<String>,
    /// `on`/`1`/`true` (the checkbox's value) skips images from reposts.
    pub(super) skip_reposts: Option<String>,
}

/// Builds `/gallery/account` hrefs carrying every active filter (actor,
/// repost preference, cursor) so the "older" link continues the same
/// browse.
fn account_gallery_href(actor: &str, skip_reposts: bool, cursor: Option<&str>) -> String {
    let mut href = format!(
        "/gallery/account?actor={}",
        percent_encoding::utf8_percent_encode(actor, percent_encoding::NON_ALPHANUMERIC)
    );
    if skip_reposts {
        href.push_str("&skip_reposts=on");
    }
    if let Some(cursor) = cursor {
        href.push_str("&cursor=");
        href.push_str(
            &percent_encoding::utf8_percent_encode(cursor, percent_encoding::NON_ALPHANUMERIC)
                .to_string(),
        );
    }
    href
}

/// One image (fullsize/thumb/alt) hydrated out of an embed *view*, as
/// returned by `getAuthorFeed`. Videos are skipped: the viewer is
/// pictures-only, and Bluesky's video embeds are HLS playlists that a
/// plain `<video>` element can't play. Recognizes the same shapes the
/// archive pipeline does, recursing into `recordWithMedia`'s nested media.
fn embed_images(embed: &serde_json::Value, out: &mut Vec<(String, String, String)>) {
    let Some(embed_type) = embed.get("$type").and_then(|v| v.as_str()) else {
        return;
    };

    let collect = |key: &str, out: &mut Vec<(String, String, String)>| {
        if let Some(images) = embed.get(key).and_then(|v| v.as_array()) {
            for image in images {
                let Some(fullsize) = image.get("fullsize").and_then(|v| v.as_str()) else {
                    continue;
                };
                let thumb = image
                    .get("thumb")
                    .and_then(|v| v.as_str())
                    .unwrap_or(fullsize);
                let alt = image.get("alt").and_then(|v| v.as_str()).unwrap_or("");
                out.push((fullsize.to_string(), thumb.to_string(), alt.to_string()));
            }
        }
    };

    match embed_type {
        "app.bsky.embed.images#view" => collect("images", out),
        "app.bsky.embed.gallery#view" => collect("items", out),
        "app.bsky.embed.recordWithMedia#view" => {
            if let Some(media) = embed.get("media") {
                embed_images(media, out);
            }
        }
        _ => {}
    }
}

/// Flattens one page of feed items into gallery items: reposts optionally
/// skipped, each post's images expanded into a thumbnail/fullsize pair
/// linking back to the post on bsky.app.
fn account_gallery_items(
    feed: &[serde_json::Value],
    actor: &str,
    skip_reposts: bool,
) -> Vec<templates::GalleryItem> {
    let mut items = Vec::new();
    for entry in feed {
        if skip_reposts && crate::poller::is_repost_item(entry) {
            continue;
        }
        let Some(post) = entry.get("post") else {
            continue;
        };
        let Some(at_uri) = post.get("uri").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(embed) = post.get("embed") else {
            continue;
        };

        let mut images = Vec::new();
        embed_images(embed, &mut images);
        if images.is_empty() {
            continue;
        }

        let post_href = templates::bluesky_post_url(at_uri).unwrap_or_default();
        for (fullsize, thumb, alt) in images {
            let alt = if alt.is_empty() {
                format!("Picture from {actor}")
            } else {
                alt
            };
            items.push(templates::GalleryItem {
                thumb_url: thumb,
                full_url: fullsize,
                is_video: false,
                post_href: post_href.clone(),
                alt,
            });
        }
    }
    items
}

pub(super) async fn gallery_account(
    State(state): State<WebState>,
    Query(query): Query<AccountGalleryQuery>,
    headers: HeaderMap,
) -> Result<Response, WebError> {
    let actor = query
        .actor
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let skip_reposts = matches!(
        query.skip_reposts.as_deref(),
        Some("on") | Some("1") | Some("true")
    );

    let (items, pagination, error) = match actor.as_deref() {
        None => (Vec::new(), None, None),
        Some(actor) => match state
            .app
            .bluesky
            .get_author_feed_media(actor, query.cursor.as_deref(), ACCOUNT_GALLERY_PAGE_LIMIT)
            .await
        {
            Ok(page) => {
                let items = account_gallery_items(&page.feed, actor, skip_reposts);
                // The cursor link only when the API offers one AND the page
                // wasn't filtered down to nothing — otherwise "older" would
                // dead-end through empty page after empty page.
                let next = page
                    .cursor
                    .filter(|_| !items.is_empty())
                    .map(|cursor| account_gallery_href(actor, skip_reposts, Some(&cursor)));
                let start = query
                    .cursor
                    .as_deref()
                    .map(|_| account_gallery_href(actor, skip_reposts, None));
                (
                    items,
                    Some(templates::CursorPagination {
                        start_href: start,
                        next_href: next,
                    }),
                    None,
                )
            }
            Err(err) => {
                tracing::warn!(actor = %actor, error = %err, "account viewer feed fetch failed");
                (
                    Vec::new(),
                    None,
                    Some(format!(
                        "could not load {actor}'s feed — check the handle and try again"
                    )),
                )
            }
        },
    };

    let pagination = pagination.unwrap_or(templates::CursorPagination {
        start_href: None,
        next_href: None,
    });

    if actor.is_some() && is_htmx_request(&headers) {
        let fragment = templates::AccountGalleryGridTemplate {
            items,
            pagination,
            error,
        };
        Ok(askama_axum::into_response(&fragment))
    } else {
        let template = templates::AccountGalleryTemplate {
            version: templates::APP_VERSION,
            git_revision: templates::GIT_REVISION,
            build_date: templates::BUILD_DATE,
            actor: actor.unwrap_or_default(),
            skip_reposts,
            items,
            pagination,
            error,
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
