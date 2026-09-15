//! Browser (`/browser`): live browsing of one Bluesky account's pictures,
//! plus the saved-accounts (favorites) panel for one-click access.
//!
//! This is the "browse an account" viewer that used to live on the gallery
//! page, moved to its own page and given persistent favorites. It behaves
//! identically to the gallery: same grid markup, same lightbox contract
//! (`data-lightbox` links + `rel="next"` pagination the base lightbox walks
//! across), same htmx fragment swaps on pagination.

use axum::Form;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use serde::Deserialize;

use super::{WebError, WebState, is_htmx_request};
use crate::templates;

/// How many feed items to request per API page by default in the browser.
/// With the `posts_with_media` filter every item carries media, so one
/// page fills a grid comfortably without chaining requests per view.
const ACCOUNT_GALLERY_PAGE_LIMIT: u32 = 50;

#[derive(Debug, Deserialize)]
pub(super) struct BrowserQuery {
    pub(super) actor: Option<String>,
    pub(super) cursor: Option<String>,
    /// `on`/`1`/`true` (the checkbox's value) skips images from reposts.
    pub(super) skip_reposts: Option<String>,
    /// `newest` (default, the API's own order) / `oldest` (the page
    /// reversed). Anything else falls back to `newest`.
    pub(super) sort: Option<String>,
}

/// One offer of the browser's sort picker: the label, and whether it
/// reverses the page (hrefs and selection are built from these two).
#[derive(Debug, Clone)]
struct BrowserSort {
    label: &'static str,
    oldest: bool,
}

const BROWSER_SORTS: [BrowserSort; 2] = [
    BrowserSort {
        label: "Newest first",
        oldest: false,
    },
    BrowserSort {
        label: "Oldest first",
        oldest: true,
    },
];

/// Parses a browser `sort` query value; unknown tokens behave as `newest`.
fn parse_browser_sort(raw: Option<&str>) -> bool {
    raw == Some("oldest")
}

/// Builds `/browser` hrefs carrying every active filter (actor, repost
/// preference, sort, cursor) so the "older" link continues the same browse.
fn browser_href(actor: &str, skip_reposts: bool, oldest: bool, cursor: Option<&str>) -> String {
    let mut href = format!(
        "/browser?actor={}",
        percent_encoding::utf8_percent_encode(actor, percent_encoding::NON_ALPHANUMERIC)
    );
    if skip_reposts {
        href.push_str("&skip_reposts=on");
    }
    // `sort=newest` is the default, so only the non-default sort is
    // emitted, mirroring how the archive gallery builds its links.
    if oldest {
        href.push_str("&sort=oldest");
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
/// linking back to the post on bsky.app. Every image carries its post's
/// strong ref so the lightbox can like/bookmark the post it came from.
/// `oldest` reverses the page's order (Bluesky feeds are newest-first
/// cursors; there is no server-side ascending traversal without walking
/// every page, so oldest-first reverses each fetched page).
fn account_gallery_items(
    feed: &[serde_json::Value],
    actor: &str,
    skip_reposts: bool,
    oldest: bool,
) -> Vec<templates::GalleryItem> {
    // Only image-bearing *posts of this page* take part in the reversal —
    // repost-filtering and embed-shapes decide membership first; ordering
    // is applied to the flattened result.
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
        let post_cid = post
            .get("cid")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
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
                post_uri: at_uri.to_string(),
                post_cid: post_cid.clone(),
                alt,
            });
        }
    }
    if oldest {
        items.reverse();
    }
    items
}

pub(super) async fn browser(
    State(state): State<WebState>,
    Query(query): Query<BrowserQuery>,
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
    let oldest = parse_browser_sort(query.sort.as_deref());

    let (items, pagination, error) = match actor.as_deref() {
        None => (Vec::new(), None, None),
        Some(actor) => match state
            .app
            .bluesky
            .get_author_feed_media(actor, query.cursor.as_deref(), ACCOUNT_GALLERY_PAGE_LIMIT)
            .await
        {
            Ok(page) => {
                let items = account_gallery_items(&page.feed, actor, skip_reposts, oldest);
                // The cursor link only when the API offers one AND the page
                // wasn't filtered down to nothing — otherwise "older" would
                // dead-end through empty page after empty page.
                let next = page
                    .cursor
                    .filter(|_| !items.is_empty())
                    .map(|cursor| browser_href(actor, skip_reposts, oldest, Some(&cursor)));
                let start = query
                    .cursor
                    .as_deref()
                    .map(|_| browser_href(actor, skip_reposts, oldest, None));
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
        let saved = saved_rows(&state).await;
        let sort_options = BROWSER_SORTS
            .iter()
            .map(|sort| templates::SortOption {
                label: sort.label,
                href: actor
                    .as_deref()
                    .map(|actor| browser_href(actor, skip_reposts, sort.oldest, None))
                    .unwrap_or_else(|| "/browser".to_string()),
                selected: sort.oldest == oldest,
            })
            .collect();
        let template = templates::BrowserTemplate {
            version: templates::APP_VERSION,
            git_revision: templates::GIT_REVISION,
            build_date: templates::BUILD_DATE,
            actor: actor.unwrap_or_default(),
            skip_reposts,
            items,
            pagination,
            error,
            saved,
            sort_options,
        };
        Ok(askama_axum::into_response(&template))
    }
}

async fn saved_rows(state: &WebState) -> Vec<templates::SavedAccountRow> {
    match state.app.store.list_saved_accounts().await {
        Ok(list) => list.iter().map(templates::saved_account_row).collect(),
        Err(err) => {
            tracing::error!(error = %err, "failed to load saved accounts");
            Vec::new()
        }
    }
}

/// The shared response for a saved-account mutation: an htmx request gets
/// the favorites panel (with any error rendered inline inside it) swapped
/// over `#saved-accounts-panel` via outerHTML; a plain no-JS form post
/// redirects back to the browser page.
async fn saved_accounts_mutation_response(
    state: WebState,
    headers: HeaderMap,
    outcome: Result<(), String>,
) -> Response {
    if is_htmx_request(&headers) {
        saved_accounts_panel(state, outcome.err()).await
    } else {
        Redirect::to("/browser").into_response()
    }
}

/// Renders the favorites panel from the current database roster plus an
/// optional inline error. This is the response body for both saved-account
/// mutations and the markup the browser page embeds.
async fn saved_accounts_panel(state: WebState, error: Option<String>) -> Response {
    let saved = saved_rows(&state).await;
    askama_axum::into_response(&templates::SavedAccountsPanelTemplate { saved, error })
}

#[derive(Debug, Deserialize)]
pub(super) struct AddSavedAccountForm {
    handle: String,
}

/// Saves a favorite account. The handle is trimmed; unlike the watched
/// sources it is NOT resolved or validated against the API first — a
/// favorite is a bookmark for convenience, and a typo can be removed just
/// as easily as it was added.
pub(super) async fn add_saved_account(
    State(state): State<WebState>,
    headers: HeaderMap,
    Form(form): Form<AddSavedAccountForm>,
) -> Response {
    let handle = form.handle.trim().trim_start_matches('@').to_string();
    let outcome = if handle.is_empty() {
        Err("enter a handle to save".to_string())
    } else {
        match state.app.store.add_saved_account(&handle).await {
            Ok(_) => Ok(()),
            Err(err) => {
                tracing::error!(error = %err, "failed to persist saved account");
                Err("failed to save account".to_string())
            }
        }
    };
    saved_accounts_mutation_response(state, headers, outcome).await
}

pub(super) async fn remove_saved_account(
    State(state): State<WebState>,
    Path(id): Path<i64>,
    headers: HeaderMap,
) -> Response {
    let outcome = match state.app.store.remove_saved_account(id).await {
        Ok(_) => Ok(()),
        Err(err) => {
            tracing::error!(error = %err, "failed to remove saved account");
            Err("failed to remove account".to_string())
        }
    };
    saved_accounts_mutation_response(state, headers, outcome).await
}

/// The old `/gallery/account` URL, kept working as a redirect: existing
/// bookmarks/links land on the browser page with the same browse state.
pub(super) async fn gallery_account_redirect(Query(query): Query<BrowserQuery>) -> Response {
    let actor = query
        .actor
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let target = match actor {
        Some(actor) => browser_href(
            actor,
            query.skip_reposts.as_deref() == Some("on"),
            parse_browser_sort(query.sort.as_deref()),
            query.cursor.as_deref(),
        ),
        None => "/browser".to_string(),
    };
    Redirect::to(&target).into_response()
}

// ---------------------------------------------------------------------
// Like / bookmark actions (the lightbox buttons)
// ---------------------------------------------------------------------

/// Request body of the lightbox like/bookmark actions: the strong ref of
/// the post to act on, taken straight off the grid item's
/// `data-post-uri`/`data-post-cid` attributes.
#[derive(Debug, Deserialize)]
pub(super) struct PostActionForm {
    uri: String,
    cid: String,
}

/// Likes a post with the archiver account itself — the same account the
/// likes/bookmarks poller archives from, so a like made here shows up in
/// the next poll pass like any other.
pub(super) async fn like_post(
    State(state): State<WebState>,
    Json(form): Json<PostActionForm>,
) -> Response {
    let Some(post_ref) = strong_ref(&form) else {
        return action_error("missing post uri/cid");
    };
    action_response(state.app.bluesky.like_post(&post_ref).await, "Liked")
}

/// Bookmarks a post (`app.bsky.bookmark.createBookmark`, the same private
/// bookmark store `getBookmarks` reads, so a bookmark made here is
/// archived by the poller too).
pub(super) async fn bookmark_post(
    State(state): State<WebState>,
    Json(form): Json<PostActionForm>,
) -> Response {
    let Some(post_ref) = strong_ref(&form) else {
        return action_error("missing post uri/cid");
    };
    action_response(
        state.app.bluesky.bookmark_post(&post_ref).await,
        "Bookmarked",
    )
}

/// Rejects forms whose fields are blank bodies before an API call is made.
fn strong_ref(form: &PostActionForm) -> Option<crate::bluesky::StrongRef> {
    let uri = form.uri.trim();
    let cid = form.cid.trim();
    (uri.starts_with("at://") && !cid.is_empty()).then(|| crate::bluesky::StrongRef {
        uri: uri.to_string(),
        cid: cid.to_string(),
    })
}

fn action_error(message: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({ "ok": false, "message": message })),
    )
        .into_response()
}

/// Shared response shape for the write actions: the lightbox JS `fetch`es
/// them and flips the button into its done state on success; an upstream
/// failure is a 502 with a short message the client surfaces.
fn action_response(
    result: Result<(), crate::bluesky::BlueskyError>,
    done: &'static str,
) -> Response {
    match result {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({ "ok": true, "state": done })),
        )
            .into_response(),
        Err(err) => {
            tracing::warn!(error = %err, "bluesky write action failed");
            (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "ok": false, "message": err.to_string() })),
            )
                .into_response()
        }
    }
}
