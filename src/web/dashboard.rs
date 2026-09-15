//! Dashboard: the `/` overview with archive counts, subsystem health, and
//! recent activity.
//!
//! Reactivity: the page renders with only the cheap index counts and the
//! health snapshot, so boosted navigation lands instantly; the recent-
//! activity grid renders excerpt-less and re-fetches itself from `/recent`
//! via htmx after load — that's where the slow per-item `record.json` reads
//! live.

use axum::extract::State;
use axum::response::Response;

use super::{WebError, WebState};
use crate::storage::{ArchiveStore, Category, PostSort, PostSummary};
use crate::templates;

pub(super) async fn dashboard(State(state): State<WebState>) -> Result<Response, WebError> {
    let store = &state.app.store;
    let counts_future = futures_util::future::join_all([
        store.list_posts(Some(Category::Post), 1, 1, PostSort::default()),
        store.list_posts(Some(Category::Like), 1, 1, PostSort::default()),
        store.list_posts(Some(Category::Bookmark), 1, 1, PostSort::default()),
        store.list_posts(Some(Category::TumblrLike), 1, 1, PostSort::default()),
    ]);
    let (counts, recent) = futures_util::future::join(
        counts_future,
        store.list_posts(None, 1, 10, PostSort::default()),
    )
    .await;
    let recent = recent?;
    let counts: Vec<u64> = counts
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|page| page.total_items)
        .collect();
    let [
        posts_count,
        likes_count,
        bookmarks_count,
        tumblr_likes_count,
    ] = counts.try_into().expect("exactly four counters");

    // Fast path: newest rows straight from the index, excerpt-less. The
    // grid re-fetches itself as an excerpt-capable fragment after load.
    let rows = recent
        .items
        .iter()
        .map(|summary| templates::post_row(summary, None))
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

/// The deferred recent-activity excerpt fill the dashboard's grid pulls in
/// after load. It re-runs the newest-posts query and reads each item's
/// `record.json` in a single batched blocking task; the response is a set of
/// `hx-swap-oob` paragraphs that patch the grid's excerpt slots in place,
/// leaving the thumbnails and layout (and any playing videos) untouched.
pub(super) async fn recent(State(state): State<WebState>) -> Result<Response, WebError> {
    let recent = state
        .app
        .store
        .list_posts(None, 1, 10, PostSort::default())
        .await?;
    let texts = fetch_excerpts(&state.app.store, &recent.items).await;
    let excerpts: Vec<templates::ExcerptSlot> = recent
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

/// Best-effort fetch of each summary's post text, for building list-view
/// excerpts. The index deliberately doesn't store full record bodies
/// ([`crate::storage`]'s `PostSummary` is index-only), so this reads each
/// item's `record.json` from disk — in one batched blocking task via
/// [`ArchiveStore::get_records_batch`] — not in one task per item like
/// before. A read failure for one item just means that item renders without
/// an excerpt — it never fails the whole page.
pub(super) async fn fetch_excerpts(
    store: &ArchiveStore,
    items: &[PostSummary],
) -> Vec<Option<String>> {
    let entries = items
        .iter()
        .map(|item| (item.category, item.at_uri.clone()))
        .collect();
    let records = store.get_records_batch(entries).await.unwrap_or_default();
    records
        .into_iter()
        .map(|record| {
            record
                .as_ref()
                .and_then(|record| templates::record_text(&record.record))
                .map(str::to_string)
        })
        .collect()
}
