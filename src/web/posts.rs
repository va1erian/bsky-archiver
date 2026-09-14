//! Posts list (`/posts`) and post detail (`/posts/:id`) handlers.

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::Response;
use serde::Deserialize;

use super::dashboard::fetch_excerpts;
use super::{WebError, WebState, clamp_page_size, is_htmx_request};
use crate::storage::Category;
use crate::templates;

// ---------------------------------------------------------------------
// Posts list + detail
// ---------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub(super) struct PostsQuery {
    pub(super) category: Option<String>,
    pub(super) page: Option<u32>,
    pub(super) page_size: Option<u32>,
}

pub(super) async fn list_posts(
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
    let category_param = query.category.clone();
    let self_href = posts_href(query.category.as_deref(), result.page, page_size);

    let pagination =
        templates::build_pagination(result.page, result.total_pages, result.total_items, |n| {
            posts_href(category_param.as_deref(), n, page_size)
        });

    if is_htmx_request(&headers) {
        // Fragment swap: the excerpt reads (one batched record-file scan
        // across the page's items) happen here, after the fast full page
        // has already shown skeleton rows.
        let texts = fetch_excerpts(&state.app.store, &result.items).await;
        let rows = result
            .items
            .iter()
            .zip(texts)
            .map(|(summary, text)| templates::post_row(summary, text.as_deref()))
            .collect();
        let fragment = templates::PostsListTemplate {
            rows,
            pagination,
            self_refresh: None,
        };
        Ok(askama_axum::into_response(&fragment))
    } else {
        // Full page fast path: rows straight from the index, no excerpt
        // reads — the skeleton list re-fetches itself via htmx after load.
        let rows = result
            .items
            .iter()
            .map(|summary| templates::post_row(summary, None))
            .collect();
        let category_options = build_category_options(query.category.as_deref());
        let template = templates::PostsTemplate {
            version: templates::APP_VERSION,
            git_revision: templates::GIT_REVISION,
            build_date: templates::BUILD_DATE,
            rows,
            pagination,
            category_options,
            self_refresh: Some(self_href),
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
        ("tumblr_likes", "Tumblr Likes"),
    ] {
        options.push(templates::CategoryOption {
            label,
            href: format!("/posts?category={value}"),
            selected: selected == Some(value),
        });
    }
    options
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
