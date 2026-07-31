//! Periodic REST polling for likes and bookmarks (not available on the
//! firehose), and the REST-polling fallback path for authored posts when the
//! firehose connection is unavailable. Uses adaptive intervals with
//! exponential backoff and jitter.
//!
//! Only the likes/bookmarks half (AR-7) is implemented here today. The
//! authored-post REST fallback (AR-6) is expected to land alongside this in
//! its own branch and get merged in later.

// Not wired into `main` yet: the ticket that assembles the full producer
// set (firehose + REST fallback + this poller) into a running application
// lands after all of them merge.
#![allow(dead_code)]

use std::time::Duration;

use rand::Rng;
use tracing::{debug, info, warn};

use crate::bluesky::{BlueskyClient, BlueskyError, PostView};
use crate::pipeline::{
    CandidatePost, CandidatePostSender, MediaRef, PostCategory, has_archivable_media,
};
use crate::storage::{ArchiveStore, Category, SaveOutcome, StorageError};

/// Page size requested per `getActorLikes` / `getBookmarks` call.
const PAGE_SIZE: u32 = 50;

/// Cap on exponential backoff multiplier (2^N baseline intervals).
const MAX_BACKOFF_EXPONENT: u32 = 5;

/// Errors that can end a single poll pass for one category.
#[derive(Debug, thiserror::Error)]
pub enum PollError {
    #[error(transparent)]
    Bluesky(#[from] BlueskyError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// Polls the watched account's likes and bookmarks on a timer, archiving
/// each new item's JSON record and — for items with media — handing a
/// [`CandidatePost`] off to the shared downloader channel.
pub struct LikesBookmarksPoller {
    client: std::sync::Arc<BlueskyClient>,
    store: ArchiveStore,
    sender: CandidatePostSender,
    actor: String,
    base_interval: Duration,
}

impl LikesBookmarksPoller {
    pub fn new(
        client: std::sync::Arc<BlueskyClient>,
        store: ArchiveStore,
        sender: CandidatePostSender,
        actor: String,
        base_interval: Duration,
    ) -> Self {
        Self {
            client,
            store,
            sender,
            actor,
            base_interval,
        }
    }

    /// Runs the poll loop forever: alternates likes and bookmarks passes on
    /// `base_interval`, backing off (with jitter) after consecutive
    /// failures and resetting to `base_interval` on success.
    pub async fn run(&self) {
        let mut consecutive_errors: u32 = 0;
        loop {
            let likes_result = self.poll_likes().await;
            if let Err(err) = &likes_result {
                warn!(error = %err, "likes poll pass failed");
            }
            let bookmarks_result = self.poll_bookmarks().await;
            if let Err(err) = &bookmarks_result {
                warn!(error = %err, "bookmarks poll pass failed");
            }

            if likes_result.is_err() || bookmarks_result.is_err() {
                consecutive_errors = consecutive_errors.saturating_add(1);
            } else {
                consecutive_errors = 0;
            }

            let delay = backoff_delay(self.base_interval, consecutive_errors);
            tokio::time::sleep(delay).await;
        }
    }

    /// One pagination pass over likes, newest-first, stopping as soon as an
    /// already-archived item is hit (or pages run out).
    pub async fn poll_likes(&self) -> Result<(), PollError> {
        let mut cursor: Option<String> = None;
        loop {
            let page = self
                .client
                .get_actor_likes(&self.actor, cursor.as_deref(), PAGE_SIZE)
                .await?;
            if page.feed.is_empty() {
                return Ok(());
            }

            for entry in &page.feed {
                if self
                    .archive_one(Category::Like, PostCategory::Like, &entry.post)
                    .await?
                {
                    // Dedup boundary: this item (and everything older) is
                    // already archived, so pagination can stop here.
                    return Ok(());
                }
            }

            match page.cursor {
                Some(next) => cursor = Some(next),
                None => return Ok(()),
            }
        }
    }

    /// One pagination pass over bookmarks, newest-first, stopping as soon
    /// as an already-archived item is hit (or pages run out).
    pub async fn poll_bookmarks(&self) -> Result<(), PollError> {
        let mut cursor: Option<String> = None;
        loop {
            let page = self
                .client
                .get_bookmarks(cursor.as_deref(), PAGE_SIZE)
                .await?;
            if page.bookmarks.is_empty() {
                return Ok(());
            }

            for entry in &page.bookmarks {
                if self
                    .archive_one(Category::Bookmark, PostCategory::Bookmark, &entry.subject)
                    .await?
                {
                    return Ok(());
                }
            }

            match page.cursor {
                Some(next) => cursor = Some(next),
                None => return Ok(()),
            }
        }
    }

    /// Archives one post (JSON record, plus a [`CandidatePost`] if it has
    /// media) under `category`, deduping against the archive. Returns
    /// `true` if this item was already archived (i.e. the dedup boundary
    /// for this pagination pass has been reached).
    async fn archive_one(
        &self,
        category: Category,
        post_category: PostCategory,
        post: &PostView,
    ) -> Result<bool, PollError> {
        if self.store.is_archived(category, &post.uri).await? {
            debug!(at_uri = %post.uri, %category, "reached dedup boundary");
            return Ok(true);
        }

        let outcome = self
            .store
            .save_post(category, &post.uri, &post.cid, post.record.clone())
            .await?;

        if outcome == SaveOutcome::Inserted && has_archivable_media(&post.record) {
            let media = post
                .embed
                .as_ref()
                .map(extract_media_refs)
                .unwrap_or_default();

            if !media.is_empty() {
                let candidate = CandidatePost {
                    at_uri: post.uri.clone(),
                    cid: post.cid.clone(),
                    author_did: post.author.did.clone(),
                    category: post_category,
                    record: post.record.clone(),
                    media,
                };
                if self.sender.send(candidate).await.is_err() {
                    warn!(
                        at_uri = %post.uri,
                        "candidate post channel closed; media downloader not receiving"
                    );
                }
            }
        }

        if outcome == SaveOutcome::Inserted {
            info!(at_uri = %post.uri, %category, "archived new item");
        }

        Ok(false)
    }
}

/// Computes the delay before the next poll pass: `base` doubled per
/// consecutive error (capped at [`MAX_BACKOFF_EXPONENT`]), plus up to 20%
/// jitter so multiple deployments don't all retry in lockstep.
fn backoff_delay(base: Duration, consecutive_errors: u32) -> Duration {
    let exponent = consecutive_errors.min(MAX_BACKOFF_EXPONENT);
    let multiplier = 1u32 << exponent;
    let backed_off = base.saturating_mul(multiplier);

    let jitter_fraction = rand::thread_rng().gen_range(0.0..0.2);
    let jitter = backed_off.mul_f64(jitter_fraction);
    backed_off + jitter
}

/// Extracts downloadable media references from a hydrated post-view embed
/// (CDN URLs, not raw blob refs). Mirrors the embed shapes
/// [`has_archivable_media`] recognizes.
fn extract_media_refs(embed: &serde_json::Value) -> Vec<MediaRef> {
    let Some(embed_type) = embed.get("$type").and_then(|v| v.as_str()) else {
        return Vec::new();
    };

    match embed_type {
        "app.bsky.embed.images#view" | "app.bsky.embed.images" => embed
            .get("images")
            .and_then(|v| v.as_array())
            .map(|images| {
                images
                    .iter()
                    .filter_map(|image| {
                        let cdn_url = image.get("fullsize").and_then(|v| v.as_str())?;
                        Some(MediaRef {
                            cdn_url: cdn_url.to_string(),
                            declared_mime_type: Some("image/jpeg".to_string()),
                            declared_size_bytes: None,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default(),
        "app.bsky.embed.video#view" | "app.bsky.embed.video" => embed
            .get("playlist")
            .and_then(|v| v.as_str())
            .map(|cdn_url| {
                vec![MediaRef {
                    cdn_url: cdn_url.to_string(),
                    declared_mime_type: Some("application/vnd.apple.mpegurl".to_string()),
                    declared_size_bytes: None,
                }]
            })
            .unwrap_or_default(),
        "app.bsky.embed.recordWithMedia#view" | "app.bsky.embed.recordWithMedia" => embed
            .get("media")
            .map(extract_media_refs)
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Secret;
    use crate::pipeline::candidate_post_channel;
    use serde_json::json;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn mock_session(server: &MockServer) {
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "accessJwt": "token-1",
                "refreshJwt": "refresh-1",
                "did": "did:plc:alice",
                "handle": "alice.bsky.social",
            })))
            .mount(server)
            .await;
    }

    fn make_client(server: &MockServer) -> std::sync::Arc<BlueskyClient> {
        std::sync::Arc::new(BlueskyClient::new(
            url::Url::parse(&server.uri()).unwrap(),
            "alice.bsky.social".to_string(),
            Secret::from("app-password".to_string()),
        ))
    }

    async fn open_store() -> (tempfile::TempDir, ArchiveStore) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let archive_dir = dir.path().join("archive");
        let database_path = dir.path().join("index.sqlite3");
        let store = ArchiveStore::open(archive_dir, database_path)
            .await
            .expect("open store");
        (dir, store)
    }

    fn text_only_post(uri: &str, cid: &str) -> serde_json::Value {
        json!({
            "post": {
                "uri": uri,
                "cid": cid,
                "author": {"did": "did:plc:bob"},
                "record": {"text": "hello"},
            }
        })
    }

    fn image_post(uri: &str, cid: &str) -> serde_json::Value {
        json!({
            "post": {
                "uri": uri,
                "cid": cid,
                "author": {"did": "did:plc:bob"},
                "record": {
                    "text": "look",
                    "embed": {
                        "$type": "app.bsky.embed.images",
                        "images": [{"alt": "a cat"}],
                    }
                },
                "embed": {
                    "$type": "app.bsky.embed.images#view",
                    "images": [{
                        "alt": "a cat",
                        "fullsize": "https://cdn.example.com/img1.jpg",
                        "thumb": "https://cdn.example.com/img1-thumb.jpg",
                    }],
                },
            }
        })
    }

    fn bookmark_wrap(post: serde_json::Value) -> serde_json::Value {
        json!({ "subject": post["post"] })
    }

    #[tokio::test]
    async fn likes_and_bookmarks_are_archived_under_distinct_categories() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getActorLikes"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "feed": [text_only_post("at://did:plc:bob/app.bsky.feed.post/1", "cid-1")],
                "cursor": null,
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "bookmarks": [bookmark_wrap(text_only_post(
                    "at://did:plc:carol/app.bsky.feed.post/2",
                    "cid-2",
                ))],
                "cursor": null,
            })))
            .mount(&server)
            .await;

        let (_dir, store) = open_store().await;
        let (tx, _rx) = candidate_post_channel(8);
        let client = make_client(&server);
        let poller = LikesBookmarksPoller::new(
            client,
            store.clone(),
            tx,
            "did:plc:alice".to_string(),
            Duration::from_secs(60),
        );

        poller.poll_likes().await.expect("poll likes");
        poller.poll_bookmarks().await.expect("poll bookmarks");

        assert!(
            store
                .is_archived(Category::Like, "at://did:plc:bob/app.bsky.feed.post/1")
                .await
                .unwrap()
        );
        assert!(
            !store
                .is_archived(Category::Bookmark, "at://did:plc:bob/app.bsky.feed.post/1")
                .await
                .unwrap()
        );
        assert!(
            store
                .is_archived(
                    Category::Bookmark,
                    "at://did:plc:carol/app.bsky.feed.post/2"
                )
                .await
                .unwrap()
        );
        assert!(
            !store
                .is_archived(Category::Like, "at://did:plc:carol/app.bsky.feed.post/2")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn likes_pagination_stops_at_dedup_boundary() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        let (_dir, store) = open_store().await;
        // Pre-archive the item that will appear as the last one on page 1.
        store
            .save_post(
                Category::Like,
                "at://did:plc:bob/app.bsky.feed.post/already",
                "cid-already",
                json!({"text": "old"}),
            )
            .await
            .unwrap();

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getActorLikes"))
            .and(query_param("cursor", "page-2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "feed": [text_only_post("at://did:plc:bob/app.bsky.feed.post/should-not-fetch", "cid-x")],
                "cursor": null,
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getActorLikes"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "feed": [
                    text_only_post("at://did:plc:bob/app.bsky.feed.post/new", "cid-new"),
                    text_only_post("at://did:plc:bob/app.bsky.feed.post/already", "cid-already"),
                ],
                "cursor": "page-2",
            })))
            .mount(&server)
            .await;

        let (tx, _rx) = candidate_post_channel(8);
        let client = make_client(&server);
        let poller = LikesBookmarksPoller::new(
            client,
            store.clone(),
            tx,
            "did:plc:alice".to_string(),
            Duration::from_secs(60),
        );

        poller.poll_likes().await.expect("poll likes");

        assert!(
            store
                .is_archived(Category::Like, "at://did:plc:bob/app.bsky.feed.post/new")
                .await
                .unwrap()
        );
        assert!(
            !store
                .is_archived(
                    Category::Like,
                    "at://did:plc:bob/app.bsky.feed.post/should-not-fetch"
                )
                .await
                .unwrap(),
            "pagination must not have continued past the dedup boundary"
        );
    }

    #[tokio::test]
    async fn bookmarks_pagination_stops_at_dedup_boundary_independently_of_likes() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        let (_dir, store) = open_store().await;
        // The same at_uri is already archived as a *like*, but not as a
        // bookmark: bookmarks pagination must not treat this as a boundary.
        store
            .save_post(
                Category::Like,
                "at://did:plc:bob/app.bsky.feed.post/shared",
                "cid-shared",
                json!({"text": "shared"}),
            )
            .await
            .unwrap();

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "bookmarks": [bookmark_wrap(text_only_post(
                    "at://did:plc:bob/app.bsky.feed.post/shared",
                    "cid-shared",
                ))],
                "cursor": null,
            })))
            .mount(&server)
            .await;

        let (tx, _rx) = candidate_post_channel(8);
        let client = make_client(&server);
        let poller = LikesBookmarksPoller::new(
            client,
            store.clone(),
            tx,
            "did:plc:alice".to_string(),
            Duration::from_secs(60),
        );

        poller.poll_bookmarks().await.expect("poll bookmarks");

        assert!(
            store
                .is_archived(
                    Category::Bookmark,
                    "at://did:plc:bob/app.bsky.feed.post/shared"
                )
                .await
                .unwrap(),
            "bookmarks dedup must be independent of the likes archive"
        );
    }

    #[tokio::test]
    async fn liked_post_with_media_produces_candidate_post_with_correct_category() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getActorLikes"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "feed": [image_post("at://did:plc:bob/app.bsky.feed.post/img", "cid-img")],
                "cursor": null,
            })))
            .mount(&server)
            .await;

        let (_dir, store) = open_store().await;
        let (tx, mut rx) = candidate_post_channel(8);
        let client = make_client(&server);
        let poller = LikesBookmarksPoller::new(
            client,
            store,
            tx,
            "did:plc:alice".to_string(),
            Duration::from_secs(60),
        );

        poller.poll_likes().await.expect("poll likes");

        let candidate = rx.try_recv().expect("candidate post should be sent");
        assert_eq!(candidate.at_uri, "at://did:plc:bob/app.bsky.feed.post/img");
        assert_eq!(candidate.category, PostCategory::Like);
        assert_eq!(candidate.media.len(), 1);
        assert_eq!(
            candidate.media[0].cdn_url,
            "https://cdn.example.com/img1.jpg"
        );
        assert!(rx.try_recv().is_err(), "only one candidate expected");
    }

    #[tokio::test]
    async fn bookmarked_post_with_media_produces_candidate_post_with_correct_category() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "bookmarks": [bookmark_wrap(image_post(
                    "at://did:plc:bob/app.bsky.feed.post/img2",
                    "cid-img2",
                ))],
                "cursor": null,
            })))
            .mount(&server)
            .await;

        let (_dir, store) = open_store().await;
        let (tx, mut rx) = candidate_post_channel(8);
        let client = make_client(&server);
        let poller = LikesBookmarksPoller::new(
            client,
            store,
            tx,
            "did:plc:alice".to_string(),
            Duration::from_secs(60),
        );

        poller.poll_bookmarks().await.expect("poll bookmarks");

        let candidate = rx.try_recv().expect("candidate post should be sent");
        assert_eq!(candidate.at_uri, "at://did:plc:bob/app.bsky.feed.post/img2");
        assert_eq!(candidate.category, PostCategory::Bookmark);
        assert_eq!(candidate.media.len(), 1);
    }

    #[tokio::test]
    async fn text_only_liked_post_produces_no_candidate() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getActorLikes"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "feed": [text_only_post("at://did:plc:bob/app.bsky.feed.post/text", "cid-text")],
                "cursor": null,
            })))
            .mount(&server)
            .await;

        let (_dir, store) = open_store().await;
        let (tx, mut rx) = candidate_post_channel(8);
        let client = make_client(&server);
        let poller = LikesBookmarksPoller::new(
            client,
            store,
            tx,
            "did:plc:alice".to_string(),
            Duration::from_secs(60),
        );

        poller.poll_likes().await.expect("poll likes");
        assert!(rx.try_recv().is_err(), "text-only post has no media");
    }

    #[test]
    fn backoff_delay_grows_and_caps() {
        let base = Duration::from_secs(10);
        let d0 = backoff_delay(base, 0);
        let d1 = backoff_delay(base, 1);
        let d_capped = backoff_delay(base, 100);

        assert!(d0 >= base && d0 < base.mul_f64(1.2) + Duration::from_millis(1));
        assert!(d1 >= base.mul_f64(2.0));
        assert!(d_capped <= base.mul_f64((1u32 << MAX_BACKOFF_EXPONENT) as f64 * 1.2 + 1.0));
    }

    #[test]
    fn extract_media_refs_handles_record_with_media() {
        let embed = json!({
            "$type": "app.bsky.embed.recordWithMedia#view",
            "record": {"record": {"uri": "at://...", "cid": "..."}},
            "media": {
                "$type": "app.bsky.embed.video#view",
                "playlist": "https://cdn.example.com/video.m3u8",
            }
        });
        let refs = extract_media_refs(&embed);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].cdn_url, "https://cdn.example.com/video.m3u8");
    }

    #[test]
    fn extract_media_refs_ignores_unknown_embed() {
        let embed = json!({"$type": "app.bsky.embed.external#view"});
        assert!(extract_media_refs(&embed).is_empty());
    }
}
