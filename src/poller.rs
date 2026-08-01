//! Periodic REST polling for likes and bookmarks (not available on the
//! firehose), and the REST-polling fallback path for authored posts when the
//! firehose connection is unavailable. Uses adaptive intervals with
//! exponential backoff and jitter.
//!
//! Feeds the same [`crate::pipeline::CandidatePost`] channel the firehose
//! consumer feeds, so the media downloader (AR-8) doesn't need to know
//! which producer a given post came from. Dedups against
//! [`crate::storage::ArchiveStore`] so a post already captured by another
//! producer is never reprocessed here, and vice versa.

use std::time::{Duration, Instant};

use rand::Rng;
use tracing::{debug, info, warn};

use crate::bluesky::{BlueskyClient, BlueskyError, PostView};
use crate::pipeline::{
    CandidatePost, CandidatePostSender, ConnectionHealth, ConnectionHealthReceiver, MediaRef,
    PostCategory, has_archivable_media,
};
use crate::storage::{ArchiveStore, Category, SaveOutcome, StorageError};

/// How many feed items to request per `getAuthorFeed` page.
const DEFAULT_PAGE_LIMIT: u32 = 50;

/// Tunables for the adaptive polling interval and the firehose-health
/// grace period. All fields have sane production defaults
/// ([`PollerConfig::new`]); tests override them to use short durations so
/// suites run fast.
#[derive(Debug, Clone)]
pub struct PollerConfig {
    /// Baseline polling interval, tightened back to after new content is
    /// found. Corresponds to `POLL_INTERVAL_SECONDS`.
    pub baseline_interval: Duration,
    /// Upper bound the adaptive interval backs off to under repeated empty
    /// results or errors.
    pub max_interval: Duration,
    /// How long Jetstream must report `Reconnecting` before this fallback
    /// considers itself active.
    pub disconnected_grace_period: Duration,
    /// How often to re-check whether the fallback should activate while
    /// idle and no `ConnectionHealth` change has arrived (handles the case
    /// where `Reconnecting` crosses the grace period purely due to elapsed
    /// time, with no new health event).
    pub health_recheck_interval: Duration,
    /// Feed items requested per `getAuthorFeed` page.
    pub page_limit: u32,
}

impl PollerConfig {
    /// Builds a production configuration from `POLL_INTERVAL_SECONDS`.
    pub fn new(baseline_interval: Duration) -> Self {
        PollerConfig {
            baseline_interval,
            max_interval: baseline_interval.saturating_mul(8).max(baseline_interval),
            disconnected_grace_period: Duration::from_secs(30),
            health_recheck_interval: Duration::from_secs(5),
            page_limit: DEFAULT_PAGE_LIMIT,
        }
    }
}

/// Adaptive polling interval: grows (capped) on empty results or errors,
/// resets to baseline as soon as new content is found again. Jitter is
/// applied separately by [`AdaptiveInterval::jittered`] so the stored
/// `current` value stays deterministic and easy to reason about/test.
#[derive(Debug, Clone)]
struct AdaptiveInterval {
    baseline: Duration,
    max: Duration,
    current: Duration,
}

impl AdaptiveInterval {
    fn new(baseline: Duration, max: Duration) -> Self {
        let max = max.max(baseline);
        AdaptiveInterval {
            baseline,
            max,
            current: baseline,
        }
    }

    /// New content was found this cycle: tighten straight back to
    /// baseline.
    fn on_content_found(&mut self) {
        self.current = self.baseline;
    }

    /// The feed came back empty (nothing new, no error): back off
    /// gradually.
    fn on_empty(&mut self) {
        self.current = double_capped(self.current, self.max);
    }

    /// The poll attempt errored: back off (same growth curve as empty
    /// results; a later ticket, AR-13, may want a steeper curve shared
    /// across pollers, but that's out of scope here).
    fn on_error(&mut self) {
        self.current = double_capped(self.current, self.max);
    }

    /// The current interval with symmetric jitter applied, so multiple
    /// deployments polling the same account wouldn't all land in lockstep.
    fn jittered(&self) -> Duration {
        jitter(self.current)
    }
}

fn double_capped(current: Duration, max: Duration) -> Duration {
    current.saturating_mul(2).min(max)
}

/// Applies +/-20% jitter to `base`.
fn jitter(base: Duration) -> Duration {
    let millis = base.as_millis().max(1) as f64;
    let factor = rand::thread_rng().gen_range(0.8..=1.2);
    Duration::from_millis((millis * factor).round() as u64)
}

/// Whether the REST-polling fallback should currently be active, given the
/// firehose's reported [`ConnectionHealth`]: active once Jetstream has been
/// disconnected for longer than `grace_period`, or is disabled outright;
/// idle while Jetstream is healthy, so the fallback doesn't double-fetch
/// everything the firehose is already delivering.
fn is_active(health: ConnectionHealth, now: Instant, grace_period: Duration) -> bool {
    match health {
        ConnectionHealth::Connected => false,
        ConnectionHealth::Disabled => true,
        ConnectionHealth::Reconnecting { since } => {
            now.saturating_duration_since(since) >= grace_period
        }
    }
}

/// The outcome of polling all watched handles once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CycleOutcome {
    NewContent,
    Empty,
    Error,
}

/// Runs the REST-polling fallback until `sender` is dropped/closed. Meant
/// to be spawned as a long-lived background task (`tokio::spawn`).
pub struct RestFallbackPoller {
    client: std::sync::Arc<BlueskyClient>,
    archive: ArchiveStore,
    sender: CandidatePostSender,
    health_rx: ConnectionHealthReceiver,
    watch_handles: Vec<String>,
    config: PollerConfig,
}

impl RestFallbackPoller {
    pub fn new(
        client: std::sync::Arc<BlueskyClient>,
        archive: ArchiveStore,
        sender: CandidatePostSender,
        health_rx: ConnectionHealthReceiver,
        watch_handles: Vec<String>,
        config: PollerConfig,
    ) -> Self {
        RestFallbackPoller {
            client,
            archive,
            sender,
            health_rx,
            watch_handles,
            config,
        }
    }

    /// Runs forever, alternating between waiting for the fallback to be
    /// "active" (per [`is_active`]) and polling all watched handles on an
    /// adaptive interval. Returns only if the candidate-post channel is
    /// closed (the downstream consumer shut down).
    pub async fn run(mut self) {
        let mut interval =
            AdaptiveInterval::new(self.config.baseline_interval, self.config.max_interval);

        loop {
            if !self.wait_until_active().await {
                return;
            }

            match self.poll_all_handles().await {
                CycleOutcome::NewContent => interval.on_content_found(),
                CycleOutcome::Empty => interval.on_empty(),
                CycleOutcome::Error => interval.on_error(),
            }

            let delay = interval.jittered();
            debug!(delay_ms = delay.as_millis() as u64, "rest poller sleeping");
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                changed = self.health_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
        }
    }

    /// Blocks until the fallback should be active. Returns `false` if the
    /// health channel closed (firehose task gone for good) while waiting.
    async fn wait_until_active(&mut self) -> bool {
        loop {
            let health = *self.health_rx.borrow();
            if is_active(
                health,
                Instant::now(),
                self.config.disconnected_grace_period,
            ) {
                return true;
            }
            tokio::select! {
                changed = self.health_rx.changed() => {
                    if changed.is_err() {
                        return false;
                    }
                }
                _ = tokio::time::sleep(self.config.health_recheck_interval) => {}
            }
        }
    }

    async fn poll_all_handles(&self) -> CycleOutcome {
        let mut any_new = false;
        let mut any_error = false;

        for handle in &self.watch_handles {
            match poll_handle_once(
                &self.client,
                &self.archive,
                &self.sender,
                handle,
                self.config.page_limit,
            )
            .await
            {
                Ok(new_count) => {
                    if new_count > 0 {
                        info!(handle = %handle, new_count, "rest poller found new authored posts");
                        any_new = true;
                    }
                }
                Err(err) => {
                    warn!(handle = %handle, error = %err, "rest poller failed to poll handle");
                    any_error = true;
                }
            }
        }

        if any_new {
            CycleOutcome::NewContent
        } else if any_error {
            CycleOutcome::Error
        } else {
            CycleOutcome::Empty
        }
    }
}

/// Errors from a single poll-and-drain pass over one handle's feed.
#[derive(Debug, thiserror::Error)]
enum PollHandleError {
    #[error(transparent)]
    Bluesky(#[from] BlueskyError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("candidate post channel closed")]
    ChannelClosed,
}

/// Walks `handle`'s authored feed newest-first, one page at a time, until
/// either the feed is exhausted or a post already present in `archive` is
/// reached (the dedup boundary — everything older is assumed already
/// archived). Archive-worthy posts found before that boundary are sent to
/// `sender`. Returns how many new candidates were sent.
async fn poll_handle_once(
    client: &BlueskyClient,
    archive: &ArchiveStore,
    sender: &CandidatePostSender,
    handle: &str,
    page_limit: u32,
) -> Result<usize, PollHandleError> {
    let mut cursor: Option<String> = None;
    let mut new_count = 0usize;

    loop {
        let page = client
            .get_author_feed(handle, cursor.as_deref(), page_limit)
            .await?;

        if page.feed.is_empty() {
            break;
        }

        for item in &page.feed {
            let Some(post) = item.get("post") else {
                continue;
            };
            let (Some(at_uri), Some(cid), Some(author_did)) = (
                post.get("uri").and_then(|v| v.as_str()),
                post.get("cid").and_then(|v| v.as_str()),
                post.get("author")
                    .and_then(|a| a.get("did"))
                    .and_then(|v| v.as_str()),
            ) else {
                warn!("rest poller skipping feed item missing uri/cid/author.did");
                continue;
            };

            if archive.is_archived(Category::Post, at_uri).await? {
                debug!(at_uri, "rest poller reached dedup boundary");
                return Ok(new_count);
            }

            let Some(record) = post.get("record") else {
                continue;
            };
            if !has_archivable_media(record) {
                continue;
            }

            let media = post
                .get("embed")
                .map(extract_media_from_view)
                .unwrap_or_default();

            let candidate = CandidatePost {
                at_uri: at_uri.to_string(),
                cid: cid.to_string(),
                author_did: author_did.to_string(),
                category: PostCategory::Authored,
                record: record.clone(),
                media,
            };

            sender
                .send(candidate)
                .await
                .map_err(|_| PollHandleError::ChannelClosed)?;
            new_count += 1;
        }

        match page.cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    Ok(new_count)
}

/// Extracts downloadable media from a hydrated embed *view* (as returned
/// alongside `post.record` by `getAuthorFeed`, distinct from the raw
/// record's blob-reference embed that [`has_archivable_media`] checks).
/// Recognizes the same three shapes `has_archivable_media` does, walking
/// into `recordWithMedia#view`'s nested `media`.
fn extract_media_from_view(embed: &serde_json::Value) -> Vec<MediaRef> {
    let Some(embed_type) = embed.get("$type").and_then(|v| v.as_str()) else {
        return Vec::new();
    };

    match embed_type {
        "app.bsky.embed.images#view" => embed
            .get("images")
            .and_then(|v| v.as_array())
            .map(|images| {
                images
                    .iter()
                    .filter_map(|image| {
                        let cdn_url = image.get("fullsize").and_then(|v| v.as_str())?;
                        Some(MediaRef {
                            cdn_url: cdn_url.to_string(),
                            declared_mime_type: None,
                            declared_size_bytes: None,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default(),
        "app.bsky.embed.video#view" => embed
            .get("playlist")
            .and_then(|v| v.as_str())
            .map(|url| {
                vec![MediaRef {
                    cdn_url: url.to_string(),
                    declared_mime_type: Some("application/vnd.apple.mpegurl".to_string()),
                    declared_size_bytes: None,
                }]
            })
            .unwrap_or_default(),
        "app.bsky.embed.recordWithMedia#view" => embed
            .get("media")
            .map(extract_media_from_view)
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

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
    use crate::pipeline::{candidate_post_channel, connection_health_channel};
    use serde_json::json;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn feed_view_post(n: u32, with_media: bool) -> serde_json::Value {
        let record = if with_media {
            json!({
                "text": format!("post {n}"),
                "embed": {
                    "$type": "app.bsky.embed.images",
                    "images": [{"alt": "", "image": {"ref": "bafy"}}],
                }
            })
        } else {
            json!({"text": format!("post {n}")})
        };
        let embed = if with_media {
            json!({
                "$type": "app.bsky.embed.images#view",
                "images": [{"fullsize": format!("https://cdn.example.com/{n}.jpg"), "thumb": "", "alt": ""}],
            })
        } else {
            serde_json::Value::Null
        };

        let mut post = json!({
            "uri": format!("at://did:plc:alice/app.bsky.feed.post/{n}"),
            "cid": format!("cid-{n}"),
            "author": {"did": "did:plc:alice", "handle": "alice.bsky.social"},
            "record": record,
        });
        if with_media {
            post["embed"] = embed;
        }
        json!({"post": post})
    }

    async fn open_store() -> (tempfile::TempDir, ArchiveStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store =
            ArchiveStore::open(dir.path().join("archive"), dir.path().join("index.sqlite3"))
                .await
                .expect("open store");
        (dir, store)
    }

    fn test_client(server: &MockServer) -> BlueskyClient {
        BlueskyClient::new(
            url::Url::parse(&server.uri()).unwrap(),
            "alice.bsky.social".to_string(),
            Secret::from("app-password".to_string()),
        )
    }

    fn make_client(server: &MockServer) -> std::sync::Arc<BlueskyClient> {
        std::sync::Arc::new(BlueskyClient::new(
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
    async fn pagination_walks_to_dedup_boundary_and_stops() {
        let server = MockServer::start().await;
        mount_login(&server).await;
        let (_dir, store) = open_store().await;

        // Post 1 (oldest of the three) is already archived; it should be
        // hit on page 2 and stop pagination immediately, so a would-be
        // page 3 is never requested.
        store
            .save_post(
                Category::Post,
                "at://did:plc:alice/app.bsky.feed.post/1",
                "cid-1",
                json!({}),
            )
            .await
            .unwrap();

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getAuthorFeed"))
            .and(query_param("cursor", "page-2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "feed": [feed_view_post(1, true)],
                "cursor": "page-3-should-never-be-requested",
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getAuthorFeed"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "feed": [feed_view_post(3, true), feed_view_post(2, false)],
                "cursor": "page-2",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (tx, mut rx) = candidate_post_channel(8);
        let client = test_client(&server);
        let new_count = poll_handle_once(&client, &store, &tx, "alice.bsky.social", 50)
            .await
            .expect("poll succeeds");

        // Post 3 has media (sent), post 2 has none (skipped), post 1 is
        // the dedup boundary (stops pagination, never sent).
        assert_eq!(new_count, 1);
        drop(tx);
        let sent = rx.recv().await.expect("candidate sent");
        assert_eq!(sent.at_uri, "at://did:plc:alice/app.bsky.feed.post/3");
        assert_eq!(sent.media.len(), 1);
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn empty_feed_is_not_treated_as_new_content() {
        let server = MockServer::start().await;
        mount_login(&server).await;
        let (_dir, store) = open_store().await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getAuthorFeed"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"feed": [], "cursor": null})),
            )
            .mount(&server)
            .await;

        let (tx, _rx) = candidate_post_channel(8);
        let client = test_client(&server);
        let new_count = poll_handle_once(&client, &store, &tx, "alice.bsky.social", 50)
            .await
            .expect("poll succeeds");
        assert_eq!(new_count, 0);
    }

    #[test]
    fn adaptive_interval_grows_on_empty_and_error_then_resets_on_content() {
        let baseline = Duration::from_secs(10);
        let max = Duration::from_secs(80);
        let mut interval = AdaptiveInterval::new(baseline, max);
        assert_eq!(interval.current, baseline);

        interval.on_empty();
        assert_eq!(interval.current, Duration::from_secs(20));
        interval.on_empty();
        assert_eq!(interval.current, Duration::from_secs(40));
        interval.on_error();
        assert_eq!(interval.current, Duration::from_secs(80));
        // Capped at max, does not keep growing past it.
        interval.on_error();
        assert_eq!(interval.current, Duration::from_secs(80));

        interval.on_content_found();
        assert_eq!(interval.current, baseline);
    }

    #[test]
    fn jitter_stays_within_plus_minus_twenty_percent() {
        let base = Duration::from_secs(100);
        for _ in 0..200 {
            let jittered = jitter(base);
            assert!(jittered >= Duration::from_millis(79_000));
            assert!(jittered <= Duration::from_millis(121_000));
        }
    }

    #[test]
    fn is_active_reflects_connection_health() {
        let now = Instant::now();
        let grace = Duration::from_secs(30);

        assert!(!is_active(ConnectionHealth::Connected, now, grace));
        assert!(is_active(ConnectionHealth::Disabled, now, grace));

        let just_started = ConnectionHealth::Reconnecting { since: now };
        assert!(!is_active(just_started, now, grace));

        let long_ago = now - Duration::from_secs(60);
        let stale = ConnectionHealth::Reconnecting { since: long_ago };
        assert!(is_active(stale, now, grace));
    }

    #[tokio::test]
    async fn stays_idle_while_connection_healthy_then_polls_once_disconnected() {
        let server = MockServer::start().await;
        mount_login(&server).await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getAuthorFeed"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"feed": [], "cursor": null})),
            )
            .mount(&server)
            .await;

        let (_dir, store) = open_store().await;
        let (tx, _rx) = candidate_post_channel(8);
        let (health_tx, health_rx) = connection_health_channel(ConnectionHealth::Connected);
        let client = test_client(&server);

        // Real (small) durations rather than paused virtual time: the
        // wiremock server does real loopback network I/O, which paused
        // Tokio time doesn't reliably drive forward.
        let mut config = PollerConfig::new(Duration::from_millis(20));
        config.disconnected_grace_period = Duration::from_millis(10);
        config.health_recheck_interval = Duration::from_millis(10);

        let poller = RestFallbackPoller::new(
            std::sync::Arc::new(client),
            store,
            tx,
            health_rx,
            vec!["alice.bsky.social".to_string()],
            config,
        );
        tokio::spawn(poller.run());

        // While healthy, waiting well past several health-recheck/baseline
        // intervals must never trigger a poll.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            0,
            "must not poll while firehose is connected"
        );

        // Once the firehose reports disabled, the fallback should become
        // active and eventually poll.
        health_tx.send(ConnectionHealth::Disabled).unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        let requests = server.received_requests().await.unwrap();
        assert!(
            requests
                .iter()
                .any(|r| r.url.path() == "/xrpc/app.bsky.feed.getAuthorFeed"),
            "expected a poll once disconnected, got: {requests:?}"
        );
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
