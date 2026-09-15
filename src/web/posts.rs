//! Posts list (`/posts`), the deferred excerpt fill (`/posts/excerpts`),
//! and post detail (`/posts/:id`) handlers.

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::Response;
use serde::Deserialize;

use super::dashboard::fetch_excerpts;
use super::{WebError, WebState, clamp_page_size, is_htmx_request};
use crate::storage::{Category, PostSort};
use crate::templates;

// ---------------------------------------------------------------------
// Posts list + detail
// ---------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub(super) struct PostsQuery {
    pub(super) category: Option<String>,
    pub(super) page: Option<u32>,
    pub(super) page_size: Option<u32>,
    pub(super) sort: Option<String>,
}

/// Shared query parsing/execution for `/posts` and `/posts/excerpts` so both
/// address exactly the same page of index rows.
struct PagedQuery {
    category: Option<Category>,
    page: u32,
    page_size: u32,
    sort: PostSort,
}

fn paged_query(query: PostsQuery) -> Result<PagedQuery, WebError> {
    let category = match query.category.as_deref() {
        Some(raw) => Some(raw.parse::<Category>().map_err(|_| WebError::BadRequest {
            message: format!("unknown category {raw:?}"),
        })?),
        None => None,
    };
    Ok(PagedQuery {
        category,
        page: query.page.unwrap_or(1).max(1),
        page_size: clamp_page_size(query.page_size),
        sort: parse_post_sort(query.sort.as_deref())?,
    })
}

/// Parses a `/posts` `sort` query value into a [`PostSort`]: `newest`
/// (default, archive time) / `oldest` / `created-newest` / `created-oldest`
/// / `bookmarked-newest` / `bookmarked-oldest` (the like/bookmark action
/// position, the order Bluesky's own lists use); anything else is a `400`,
/// matching how an unknown category is handled and the tokens/shape the
/// gallery's sort picker uses.
fn parse_post_sort(raw: Option<&str>) -> Result<PostSort, WebError> {
    let sort = match raw {
        None | Some("newest") => PostSort::NewestArchived,
        Some("oldest") => PostSort::OldestArchived,
        Some("created-newest") => PostSort::NewestCreated,
        Some("created-oldest") => PostSort::OldestCreated,
        Some("bookmarked-newest") => PostSort::NewestAction,
        Some("bookmarked-oldest") => PostSort::OldestAction,
        Some(other) => {
            return Err(WebError::BadRequest {
                message: format!("unknown sort {other:?}"),
            });
        }
    };
    Ok(sort)
}

/// The query token for a [`PostSort`] in `/posts` links, the token that
/// [`parse_post_sort`] accepts.
fn sort_token(sort: PostSort) -> &'static str {
    match sort {
        PostSort::NewestArchived => "newest",
        PostSort::OldestArchived => "oldest",
        PostSort::NewestCreated => "created-newest",
        PostSort::OldestCreated => "created-oldest",
        PostSort::NewestAction => "bookmarked-newest",
        PostSort::OldestAction => "bookmarked-oldest",
    }
}

/// Fast path for both the full page and the pagination fragment: rows come
/// straight from the index, excerpt-less. Every response carries the
/// out-of-band excerpt loader on `#posts-list`, so the list reads text via
/// `/posts/excerpts` after render (and again after each pagination swap).
pub(super) async fn list_posts(
    State(state): State<WebState>,
    Query(query): Query<PostsQuery>,
    headers: HeaderMap,
) -> Result<Response, WebError> {
    let category_param = query.category.clone();
    let parsed = paged_query(query)?;
    let result = state
        .app
        .store
        .list_posts(parsed.category, parsed.page, parsed.page_size, parsed.sort)
        .await?;
    let excerpts_href = excerpts_href(
        category_param.as_deref(),
        parsed.sort,
        parsed.page,
        parsed.page_size,
    );

    let rows = result
        .items
        .iter()
        .map(|summary| templates::post_row(summary, None))
        .collect();

    let pagination =
        templates::build_pagination(result.page, result.total_pages, result.total_items, |n| {
            posts_href(category_param.as_deref(), parsed.sort, n, parsed.page_size)
        });

    if is_htmx_request(&headers) {
        let fragment = templates::PostsListTemplate {
            rows,
            pagination,
            excerpts_href: Some(excerpts_href),
            category: category_param.clone().unwrap_or_default(),
            page_size: parsed.page_size,
        };
        Ok(askama_axum::into_response(&fragment))
    } else {
        let category_options =
            build_category_options(category_param.as_deref(), parsed.sort, parsed.page_size);
        let sort_options =
            build_post_sort_options(category_param.as_deref(), parsed.sort, parsed.page_size);
        let template = templates::PostsTemplate {
            version: templates::APP_VERSION,
            git_revision: templates::GIT_REVISION,
            build_date: templates::BUILD_DATE,
            rows,
            pagination,
            category_options,
            sort_options,
            category: category_param.clone().unwrap_or_default(),
            page_size: parsed.page_size,
            excerpts_href: Some(excerpts_href),
        };
        Ok(askama_axum::into_response(&template))
    }
}

/// The deferred excerpt fill: same page selection as the list, batched
/// record reads, response is a set of `hx-swap-oob` paragraphs that patch
/// the excerpt slots in place. Slots whose read failed are simply not
/// emitted, leaving their placeholder empty.
pub(super) async fn excerpts(
    State(state): State<WebState>,
    Query(query): Query<PostsQuery>,
) -> Result<Response, WebError> {
    let parsed = paged_query(query)?;
    let result = state
        .app
        .store
        .list_posts(parsed.category, parsed.page, parsed.page_size, parsed.sort)
        .await?;
    let texts = fetch_excerpts(&state.app.store, &result.items).await;
    let excerpts: Vec<templates::ExcerptSlot> = result
        .items
        .iter()
        .zip(texts)
        .filter_map(|(summary, text)| {
            text.map(|t| templates::ExcerptSlot {
                id: templates::excerpt_id(&summary.at_uri),
                text: Some(t),
            })
        })
        .collect();
    Ok(askama_axum::into_response(&templates::ExcerptsTemplate {
        excerpts,
    }))
}

/// Builds `/posts` hrefs carrying every active filter, so moving pages
/// keeps the category, sort, and page size.
fn posts_path(category: Option<&str>, sort: PostSort) -> String {
    let category_part = match category {
        Some(category) => format!("category={category}&"),
        None => String::new(),
    };
    format!("/posts?{category_part}sort={}", sort_token(sort))
}

fn posts_href(category: Option<&str>, sort: PostSort, page: u32, page_size: u32) -> String {
    format!(
        "{}&page={page}&page_size={page_size}",
        posts_path(category, sort)
    )
}

/// Like [`posts_href`] but pointing at the deferred excerpt fill.
fn excerpts_href(category: Option<&str>, sort: PostSort, page: u32, page_size: u32) -> String {
    let path = posts_path(category, sort).replace("/posts?", "/posts/excerpts?");
    format!("{path}&page={page}&page_size={page_size}")
}

fn build_category_options(
    selected: Option<&str>,
    sort: PostSort,
    page_size: u32,
) -> Vec<templates::CategoryOption> {
    // Switching category resets to page 1 but keeps the active sort, like
    // the gallery's category nav.
    let href = |token: Option<&str>| posts_href(token, sort, 1, page_size);
    let mut options = vec![templates::CategoryOption {
        label: "All",
        href: href(None),
        selected: selected.is_none(),
    }];
    for (value, label) in [
        ("posts", "Posts"),
        ("likes", "Likes"),
        ("bookmarks", "Bookmarks"),
        ("tumblr_likes", "Tumblr Likes"),
    ] {
        options.push(templates::CategoryOption {
            label,
            href: href(Some(value)),
            selected: selected == Some(value),
        });
    }
    options
}

/// Builds the sort dropdown's options: each resets to page 1 while keeping
/// the active category. Same labels and tokens as the gallery's picker.
fn build_post_sort_options(
    active_category: Option<&str>,
    selected: PostSort,
    page_size: u32,
) -> Vec<templates::SortOption> {
    [
        (PostSort::NewestArchived, "Newest archived"),
        (PostSort::OldestArchived, "Oldest archived"),
        (PostSort::NewestCreated, "Newest created"),
        (PostSort::OldestCreated, "Oldest created"),
        (PostSort::NewestAction, "Newest bookmarked"),
        (PostSort::OldestAction, "Oldest bookmarked"),
    ]
    .into_iter()
    .map(|(sort, label)| templates::SortOption {
        label,
        href: posts_href(active_category, sort, 1, page_size),
        selected: sort == selected,
    })
    .collect()
}

pub(super) async fn post_detail(
    State(state): State<WebState>,
    Path(id): Path<String>,
) -> Result<Response, WebError> {
    let at_uri = id;
    // Single batched probe across the categories (one blocking task, one
    // filesystem hit, one index lookup) instead of the old sequential
    // get_post loop, which paid a spawn_blocking hop plus a filesystem probe
    // and a database round-trip per category.
    let Some((category, record)) = state.app.store.get_post_any(&at_uri).await? else {
        return Err(WebError::NotFound);
    };
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
        version: templates::APP_VERSION,
        git_revision: templates::GIT_REVISION,
        build_date: templates::BUILD_DATE,
        category_label: templates::category_label(category),
        category_badge_class: templates::category_badge_class(category),
        author: templates::author_did_from_at_uri(&at_uri).to_string(),
        bluesky_url: templates::bluesky_post_url(&at_uri),
        text,
        indexed_at: templates::display_time(&record.indexed_at),
        action_at: record.action_at.as_deref().map(templates::display_time),
        deleted_at: record.deleted_at.as_deref().map(templates::display_time),
        media,
        raw_json,
    };
    Ok(askama_axum::into_response(&template))
}
