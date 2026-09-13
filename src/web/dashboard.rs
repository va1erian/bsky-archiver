//! Dashboard: the `/` overview with archive counts, subsystem health, and
//! recent activity.

use axum::extract::State;
use axum::response::Response;

use super::{WebError, WebState};
use crate::storage::{ArchiveStore, Category, PostSummary};
use crate::templates;

pub(super) async fn dashboard(State(state): State<WebState>) -> Result<Response, WebError> {
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
    let tumblr_likes_count = store
        .list_posts(Some(Category::TumblrLike), 1, 1)
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
        templates::subsystem_row("Feed poller", &snapshot.feed_poller),
        templates::subsystem_row("Likes & bookmarks", &snapshot.likes_bookmarks),
        templates::subsystem_row("Tumblr likes", &snapshot.tumblr_likes),
        templates::subsystem_row("Nightly sweep", &snapshot.nightly_sweep),
        templates::subsystem_row("Media downloader", &snapshot.media_downloader),
    ];

    let template = templates::DashboardTemplate {
        version: templates::APP_VERSION,
        git_revision: templates::GIT_REVISION,
        build_date: templates::BUILD_DATE,
        posts_count,
        likes_count,
        bookmarks_count,
        tumblr_likes_count,
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
pub(super) async fn fetch_excerpts(
    store: &ArchiveStore,
    items: &[PostSummary],
) -> Vec<Option<String>> {
    let fetches = items.iter().map(|item| async move {
        match store.get_post(item.category, &item.at_uri).await {
            Ok(Some(record)) => templates::record_text(&record.record).map(str::to_string),
            Ok(None) | Err(_) => None,
        }
    });
    futures_util::future::join_all(fetches).await
}
