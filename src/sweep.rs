//! Nightly deletion sweep for the likes/bookmarks archive.
//!
//! Likes and bookmarks are never on the firehose, and the REST pollers only
//! ever see the *current* view of those lists: a liked or bookmarked post
//! whose author deleted it (or whose account was deactivated) stops
//! hydrating, and nothing in the steady-state polling path notices. The
//! Jetstream delete-op path (which marks `posts.deleted_at`) only covers
//! watched accounts' own posts. This module closes that gap: once per
//! night, at a fixed local hour ([`NightlySweeper`]), it
//!
//! 1. walks the account's entire bookmark list — every page, no dedup
//!    boundary — archiving anything the periodic poller missed and marking
//!    bookmarked posts the API reports as `notFound` (deleted upstream)
//!    with a `deleted_at` timestamp;
//! 2. walks the account's entire like list the same way;
//! 3. batch-verifies (`app.bsky.feed.getPosts`, up to
//!    [`VERIFY_BATCH_SIZE`] URIs per call) every URI already archived under
//!    the like/bookmark categories — catching deletions that happened
//!    while the service was down, or of posts no longer in either list —
//!    and marks the missing ones deleted.
//!
//! Like the firehose delete-op path, marking only touches the index: the
//! on-disk JSON record and its media are kept (the archive is the durable
//! copy), and the UI badges anything with a `deleted_at` timestamp.
//! `blockedPosts` responses are logged but never marked — a blocked post
//! still exists upstream, it is just not viewable by the authenticated
//! account.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use time::OffsetDateTime;
use tracing::{debug, info, warn};

use crate::bluesky::BlueskyClient;
use crate::pipeline::{CandidatePostSender, PostCategory};
use crate::poller::{PAGE_SIZE, PollError, archive_like_bookmark_post};
use crate::storage::{ArchiveStore, Category};

/// How many URIs to verify per `app.bsky.feed.getPosts` call — the
/// endpoint's own maximum.
pub const VERIFY_BATCH_SIZE: usize = 25;

/// How long to wait before retrying a failed sweep. A sweep that errors
/// partway (e.g. a rate-limit window or a transient 5xx) retries until it
/// completes, so one bad night can't push the next full pass a whole day
/// out.
pub const RETRY_DELAY: Duration = Duration::from_secs(15 * 60);

/// Errors from one full sweep pass.
#[derive(Debug, thiserror::Error)]
pub enum SweepError {
    #[error(transparent)]
    Bluesky(#[from] crate::bluesky::BlueskyError),
    #[error(transparent)]
    Storage(#[from] crate::storage::StorageError),
}

impl From<PollError> for SweepError {
    fn from(err: PollError) -> Self {
        match err {
            PollError::Bluesky(err) => SweepError::Bluesky(err),
            PollError::Storage(err) => SweepError::Storage(err),
        }
    }
}

/// The outcome of one full sweep pass, for logging and tests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SweepSummary {
    /// Posts newly archived by the full likes/bookmarks walks.
    pub newly_archived: usize,
    /// Archived posts newly marked deleted (first `deleted_at` write).
    pub newly_marked_deleted: usize,
}

/// Runs the nightly likes/bookmarks deletion sweep forever, at a fixed
/// local hour. Meant to be spawned as a supervised background task.
pub struct NightlySweeper {
    client: Arc<BlueskyClient>,
    store: ArchiveStore,
    sender: CandidatePostSender,
    actor: String,
    /// Local hour of day (0-23, config-validated) the sweep runs at.
    local_hour: u32,
}

impl NightlySweeper {
    pub fn new(
        client: Arc<BlueskyClient>,
        store: ArchiveStore,
        sender: CandidatePostSender,
        actor: String,
        local_hour: u32,
    ) -> Self {
        Self {
            client,
            store,
            sender,
            actor,
            local_hour,
        }
    }

    /// Runs forever: sleep until the next occurrence of the configured
    /// local hour, then sweep. A failed sweep retries every [`RETRY_DELAY`]
    /// until it completes, then resumes the nightly schedule.
    pub async fn run(&self) {
        loop {
            let delay = duration_until_next_sweep(self.local_hour, local_now());
            info!(
                hour = self.local_hour,
                delay_secs = delay.as_secs(),
                "nightly likes/bookmarks sweep scheduled"
            );
            tokio::time::sleep(delay).await;

            loop {
                match self.sweep_once().await {
                    Ok(summary) => {
                        info!(
                            newly_archived = summary.newly_archived,
                            newly_marked_deleted = summary.newly_marked_deleted,
                            "nightly sweep complete"
                        );
                        break;
                    }
                    Err(err) => {
                        warn!(
                            error = %err,
                            retry_secs = RETRY_DELAY.as_secs(),
                            "nightly sweep failed; retrying"
                        );
                        tokio::time::sleep(RETRY_DELAY).await;
                    }
                }
            }
        }
    }

    /// One full sweep: walk the bookmark list, walk the like list, then
    /// batch-verify every archived like/bookmark URI. Each step commits as
    /// it goes; an `Ok` return means every step ran to completion.
    pub async fn sweep_once(&self) -> Result<SweepSummary, SweepError> {
        let mut summary = SweepSummary::default();

        let (bookmarks_archived, bookmarks_marked) = self.walk_bookmarks().await?;
        summary.newly_archived += bookmarks_archived;
        summary.newly_marked_deleted += bookmarks_marked;

        summary.newly_archived += self.walk_likes().await?;
        summary.newly_marked_deleted += self.verify_archived().await?;

        Ok(summary)
    }

    /// Walks the account's entire bookmark list (every page — no dedup
    /// boundary), archiving anything the periodic poller missed and
    /// marking bookmarked posts the API reports as `notFound` deleted.
    async fn walk_bookmarks(&self) -> Result<(usize, usize), SweepError> {
        let mut cursor: Option<String> = None;
        let mut newly_archived = 0usize;
        let mut marked_deleted = 0usize;

        loop {
            let page = self
                .client
                .get_bookmarks(cursor.as_deref(), PAGE_SIZE)
                .await?;
            if page.bookmarks.is_empty() {
                break;
            }

            for entry in &page.bookmarks {
                let Some(item) = entry.item.as_ref() else {
                    debug!(subject = %entry.subject.uri, "sweep: bookmark item missing; skipping");
                    continue;
                };
                let Some(post) = item.post() else {
                    if item.is_not_found() {
                        if let Some(created_at) = entry.created_at.as_deref() {
                            self.store
                                .set_action_at(Category::Bookmark, &entry.subject.uri, created_at)
                                .await?;
                        }
                        if self.store.mark_post_deleted(&entry.subject.uri).await? {
                            marked_deleted += 1;
                            info!(subject = %entry.subject.uri, "sweep: bookmarked post deleted upstream; marked");
                        }
                    } else {
                        debug!(subject = %entry.subject.uri, "sweep: bookmark not resolvable (blocked?); skipping");
                    }
                    continue;
                };

                let already_archived = archive_like_bookmark_post(
                    &self.store,
                    &self.sender,
                    Category::Bookmark,
                    PostCategory::Bookmark,
                    post,
                    entry.created_at.as_deref(),
                )
                .await?;
                if !already_archived {
                    newly_archived += 1;
                }
            }

            match page.cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        Ok((newly_archived, marked_deleted))
    }

    /// Walks the account's entire like list (every page — no dedup
    /// boundary), archiving anything the periodic poller missed.
    async fn walk_likes(&self) -> Result<usize, SweepError> {
        let mut cursor: Option<String> = None;
        let mut newly_archived = 0usize;

        loop {
            let page = self
                .client
                .get_actor_likes(&self.actor, cursor.as_deref(), PAGE_SIZE)
                .await?;
            if page.feed.is_empty() {
                break;
            }

            for entry in &page.feed {
                let already_archived = archive_like_bookmark_post(
                    &self.store,
                    &self.sender,
                    Category::Like,
                    PostCategory::Like,
                    &entry.post,
                    None,
                )
                .await?;
                if !already_archived {
                    newly_archived += 1;
                }
            }

            match page.cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        Ok(newly_archived)
    }

    /// Batch-verifies every URI archived under the like/bookmark
    /// categories and marks the ones the API reports as not found —
    /// deleted upstream, or the author's account was deactivated — with a
    /// `deleted_at` timestamp. This is what catches deletions of posts no
    /// longer visible in either list (and deletions that happened while
    /// the service was down), including for authors not on the watch list,
    /// whom the firehose delete-op path never sees.
    async fn verify_archived(&self) -> Result<usize, SweepError> {
        let mut uris: Vec<String> = Vec::new();
        let mut seen = HashSet::new();
        for category in [Category::Bookmark, Category::Like] {
            for uri in self.store.list_archived_uris(category).await? {
                if seen.insert(uri.clone()) {
                    uris.push(uri);
                }
            }
        }

        let mut marked_deleted = 0usize;
        for chunk in uris.chunks(VERIFY_BATCH_SIZE) {
            let page = self.client.get_posts(chunk).await?;
            for missing in &page.not_found_posts {
                if self.store.mark_post_deleted(&missing.uri).await? {
                    marked_deleted += 1;
                    info!(subject = %missing.uri, "sweep: archived post deleted upstream; marked");
                }
            }
            for blocked in &page.blocked_posts {
                debug!(subject = %blocked.uri, "sweep: post not viewable (blocked?); left unmarked");
            }
        }

        Ok(marked_deleted)
    }
}

/// The wall-clock time the sweeper treats as "now". Falls back to UTC when
/// the platform can't determine the local offset (rare, multithreaded);
/// containers usually run UTC anyway, which is also why the schedule is
/// configurable rather than hardcoded.
fn local_now() -> OffsetDateTime {
    OffsetDateTime::now_local().unwrap_or_else(|_| OffsetDateTime::now_utc())
}

/// Duration from `now` until the next occurrence of `local_hour`:00 local
/// time. If `now` is exactly on the hour, the next occurrence is tomorrow
/// (the sweep that is about to run already is).
fn duration_until_next_sweep(local_hour: u32, now: OffsetDateTime) -> Duration {
    let hour = u8::try_from(local_hour).expect("config validates the sweep hour is 0-23");
    let sweep_time = time::Time::from_hms(hour, 0, 0).expect("config validates the sweep hour");
    let today = now.replace_time(sweep_time);
    let next = if today > now {
        today
    } else {
        today + time::Duration::days(1)
    };
    (next - now).unsigned_abs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bluesky::BookmarkView;
    use crate::config::Secret;
    use crate::pipeline::candidate_post_channel;
    use serde_json::json;
    use time::macros::datetime;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn open_store() -> (tempfile::TempDir, ArchiveStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store =
            ArchiveStore::open(dir.path().join("archive"), dir.path().join("index.sqlite3"))
                .await
                .expect("open store");
        (dir, store)
    }

    fn make_client(server: &MockServer) -> Arc<BlueskyClient> {
        Arc::new(BlueskyClient::new(
            url::Url::parse(&server.uri()).unwrap(),
            "alice.bsky.social".to_string(),
            Secret::from("app-password".to_string()),
        ))
    }

    async fn mount_login(server: &MockServer) {
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "accessJwt": "token",
                "refreshJwt": "refresh",
                "did": "did:plc:alice",
                "handle": "alice.bsky.social",
            })))
            .mount(server)
            .await;
    }

    /// A resolvable bookmarked post with the given uri/cid and text.
    fn bookmark_entry(uri: &str, cid: &str, text: &str) -> serde_json::Value {
        json!({
            "subject": {"uri": uri, "cid": cid},
            "item": {
                "uri": uri,
                "cid": cid,
                "author": {"did": "did:plc:carol"},
                "record": {"text": text},
            },
        })
    }

    fn not_found_entry(uri: &str, cid: &str) -> serde_json::Value {
        json!({
            "subject": {"uri": uri, "cid": cid},
            "item": {"uri": uri, "notFound": true},
        })
    }

    fn blocked_entry(uri: &str, cid: &str) -> serde_json::Value {
        json!({
            "subject": {"uri": uri, "cid": cid},
            "item": {
                "uri": uri,
                "cid": cid,
                "author": {"did": "did:plc:y"},
                "blocked": true,
            },
        })
    }

    async fn is_marked_deleted(store: &ArchiveStore, at_uri: &str) -> bool {
        store
            .get_post(Category::Bookmark, at_uri)
            .await
            .expect("get_post")
            .expect("post archived")
            .deleted_at
            .is_some()
    }

    #[test]
    fn sweep_delay_targets_configured_local_hour() {
        let now = datetime!(2026-09-07 15:00:00 UTC);
        assert_eq!(
            duration_until_next_sweep(3, now),
            Duration::from_secs(12 * 3600)
        );

        let now = datetime!(2026-09-07 01:30:00 UTC);
        assert_eq!(
            duration_until_next_sweep(3, now),
            Duration::from_secs(90 * 60)
        );

        // Exactly on the hour: the next occurrence is tomorrow.
        let now = datetime!(2026-09-07 03:00:00 UTC);
        assert_eq!(
            duration_until_next_sweep(3, now),
            Duration::from_secs(24 * 3600)
        );

        // Hour 23: 22:30 is tonight, 23:30 has to be tomorrow night.
        let now = datetime!(2026-09-07 22:30:00 UTC);
        assert_eq!(
            duration_until_next_sweep(23, now),
            Duration::from_secs(30 * 60)
        );
        let now = datetime!(2026-09-07 23:30:00 UTC);
        assert_eq!(
            duration_until_next_sweep(23, now),
            Duration::from_secs(23 * 3600 + 30 * 60)
        );
    }

    #[tokio::test]
    async fn sweep_archives_missed_items_and_marks_deleted_upstream_posts() {
        let server = MockServer::start().await;
        mount_login(&server).await;
        let (_dir, store) = open_store().await;

        // Post 1 is already archived (the poller got it before its author
        // deleted it); post 2 was bookmarked while the service was down.
        store
            .save_post(
                Category::Bookmark,
                "at://did:plc:carol/app.bsky.feed.post/1",
                "cid-1",
                json!({"text": "deleted later"}),
            )
            .await
            .unwrap();

        // Page 1: a new resolvable bookmark, the deleted post (notFound),
        // and a blocked one. Page 2: another new bookmark, then exhausted.
        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
            .and(query_param("cursor", "page-2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "bookmarks": [bookmark_entry(
                    "at://did:plc:carol/app.bsky.feed.post/4",
                    "cid-4",
                    "second page",
                )],
                "cursor": null,
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "bookmarks": [
                    bookmark_entry(
                        "at://did:plc:carol/app.bsky.feed.post/2",
                        "cid-2",
                        "missed by poller",
                    ),
                    not_found_entry("at://did:plc:carol/app.bsky.feed.post/1", "cid-1"),
                    blocked_entry("at://did:plc:y/app.bsky.feed.post/3", "cid-3"),
                ],
                "cursor": "page-2",
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getActorLikes"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "feed": [],
                "cursor": null,
            })))
            .mount(&server)
            .await;

        // The verify pass batches every archived bookmark/like URI; report
        // post 2 as still present and post 4 as deleted-while-down (e.g.
        // the author deleted it right after it was bookmarked).
        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getPosts"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "posts": [],
                "notFoundPosts": [
                    {"uri": "at://did:plc:carol/app.bsky.feed.post/4"},
                ],
                "blockedPosts": [],
            })))
            .mount(&server)
            .await;

        let (tx, _rx) = candidate_post_channel(8);
        let sweeper = NightlySweeper::new(
            make_client(&server),
            store.clone(),
            tx,
            "did:plc:alice".to_string(),
            3,
        );
        let summary = sweeper.sweep_once().await.expect("sweep succeeds");

        // Posts 2 and 4 were archived by the walk; post 1 was already there.
        assert_eq!(summary.newly_archived, 2);
        // Post 1 was marked by the walk's notFound signal; post 4 by the
        // verify pass. The blocked post (3) must never be marked.
        assert_eq!(summary.newly_marked_deleted, 2);
        assert!(
            is_marked_deleted(&store, "at://did:plc:carol/app.bsky.feed.post/1").await,
            "deleted bookmarked post should be marked"
        );
        assert!(
            is_marked_deleted(&store, "at://did:plc:carol/app.bsky.feed.post/4").await,
            "post deleted while the service was down should be marked"
        );
        assert!(
            store
                .get_post(
                    Category::Bookmark,
                    "at://did:plc:carol/app.bsky.feed.post/2"
                )
                .await
                .expect("get_post")
                .expect("post 2 archived")
                .deleted_at
                .is_none(),
            "resolvable post must not be marked deleted"
        );
        assert!(
            store
                .get_post(Category::Bookmark, "at://did:plc:y/app.bsky.feed.post/3")
                .await
                .expect("get_post")
                .is_none(),
            "blocked post was never archived (it did not resolve)"
        );
    }

    #[tokio::test]
    async fn sweep_verifies_archived_likes_and_marks_them_deleted() {
        let server = MockServer::start().await;
        mount_login(&server).await;
        let (_dir, store) = open_store().await;

        // A liked post whose author deleted it while the service was down:
        // it no longer appears in getActorLikes at all, so only the
        // getPosts verify pass can catch it.
        store
            .save_post(
                Category::Like,
                "at://did:plc:dave/app.bsky.feed.post/9",
                "cid-9",
                json!({"text": "liked, then deleted"}),
            )
            .await
            .unwrap();

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getActorLikes"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "feed": [],
                "cursor": null,
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "bookmarks": [],
                "cursor": null,
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getPosts"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "posts": [],
                "notFoundPosts": [
                    {"uri": "at://did:plc:dave/app.bsky.feed.post/9"},
                ],
                "blockedPosts": [],
            })))
            .mount(&server)
            .await;

        let (tx, _rx) = candidate_post_channel(8);
        let sweeper = NightlySweeper::new(
            make_client(&server),
            store.clone(),
            tx,
            "did:plc:alice".to_string(),
            3,
        );
        let summary = sweeper.sweep_once().await.expect("sweep succeeds");

        assert_eq!(summary.newly_archived, 0);
        assert_eq!(summary.newly_marked_deleted, 1);
        let record = store
            .get_post(Category::Like, "at://did:plc:dave/app.bsky.feed.post/9")
            .await
            .expect("get_post")
            .expect("like still archived");
        assert!(
            record.deleted_at.is_some(),
            "deleted liked post must be kept and marked"
        );
    }

    #[tokio::test]
    async fn sweep_counts_remarks_and_missing_items_without_error() {
        let server = MockServer::start().await;
        mount_login(&server).await;
        let (_dir, store) = open_store().await;

        // A bookmark that is already marked deleted: neither the walk nor
        // the verify pass may double-count it, and neither may fail.
        store
            .save_post(
                Category::Bookmark,
                "at://did:plc:carol/app.bsky.feed.post/1",
                "cid-1",
                json!({"text": "gone"}),
            )
            .await
            .unwrap();
        store
            .mark_post_deleted("at://did:plc:carol/app.bsky.feed.post/1")
            .await
            .unwrap();

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "bookmarks": [
                    not_found_entry("at://did:plc:carol/app.bsky.feed.post/1", "cid-1"),
                ],
                "cursor": null,
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getActorLikes"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "feed": [],
                "cursor": null,
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getPosts"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "posts": [],
                "notFoundPosts": [
                    {"uri": "at://did:plc:carol/app.bsky.feed.post/1"},
                ],
                "blockedPosts": [],
            })))
            .mount(&server)
            .await;

        let (tx, _rx) = candidate_post_channel(8);
        let sweeper = NightlySweeper::new(
            make_client(&server),
            store.clone(),
            tx,
            "did:plc:alice".to_string(),
            3,
        );
        let summary = sweeper.sweep_once().await.expect("sweep succeeds");

        assert_eq!(summary.newly_archived, 0);
        assert_eq!(
            summary.newly_marked_deleted, 0,
            "an already-marked post must not be re-counted"
        );
    }

    /// The lexicon allows a bookmark entry with no hydrated item at all;
    /// the sweeper must skip it rather than treat it as a deletion.
    #[tokio::test]
    async fn sweep_skips_bookmark_entries_with_no_item() {
        let server = MockServer::start().await;
        mount_login(&server).await;
        let (_dir, store) = open_store().await;

        let entry: BookmarkView = serde_json::from_value(json!({
            "subject": {
                "uri": "at://did:plc:carol/app.bsky.feed.post/1",
                "cid": "cid-1",
            },
        }))
        .unwrap();
        assert!(entry.item.is_none());

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "bookmarks": [{
                    "subject": {
                        "uri": "at://did:plc:carol/app.bsky.feed.post/1",
                        "cid": "cid-1",
                    },
                }],
                "cursor": null,
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getActorLikes"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "feed": [],
                "cursor": null,
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getPosts"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "posts": [],
                "notFoundPosts": [],
                "blockedPosts": [],
            })))
            .mount(&server)
            .await;

        let (tx, _rx) = candidate_post_channel(8);
        let sweeper = NightlySweeper::new(
            make_client(&server),
            store.clone(),
            tx,
            "did:plc:alice".to_string(),
            3,
        );
        let summary = sweeper.sweep_once().await.expect("sweep succeeds");
        assert_eq!(summary.newly_marked_deleted, 0);
    }

    /// The sweep's full bookmark walk backfills the bookmark action time
    /// for rows archived before it was captured (the periodic poller stops
    /// at its dedup boundary and never revisits them).
    #[tokio::test]
    async fn sweep_backfills_action_time_for_already_archived_bookmarks() {
        let server = MockServer::start().await;
        mount_login(&server).await;
        let (_dir, store) = open_store().await;

        let uri = "at://did:plc:carol/app.bsky.feed.post/1";
        store
            .save_post(Category::Bookmark, uri, "cid-1", json!({"text": "old row"}))
            .await
            .unwrap();

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "bookmarks": [{
                    "subject": {"uri": uri, "cid": "cid-1"},
                    "createdAt": "2026-09-04T09:00:00.000Z",
                    "item": {
                        "uri": uri,
                        "cid": "cid-1",
                        "author": {"did": "did:plc:carol"},
                        "record": {"text": "old row"},
                    },
                }],
                "cursor": null,
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getActorLikes"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "feed": [],
                "cursor": null,
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getPosts"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "posts": [],
                "notFoundPosts": [],
                "blockedPosts": [],
            })))
            .mount(&server)
            .await;

        let (tx, _rx) = candidate_post_channel(8);
        let sweeper = NightlySweeper::new(
            make_client(&server),
            store.clone(),
            tx,
            "did:plc:alice".to_string(),
            3,
        );
        sweeper.sweep_once().await.expect("sweep succeeds");

        let record = store
            .get_post(Category::Bookmark, uri)
            .await
            .expect("get_post")
            .expect("still archived");
        assert_eq!(
            record.action_at.as_deref(),
            Some("2026-09-04T09:00:00.000Z"),
            "the sweep should backfill the bookmark action time"
        );
    }
}
