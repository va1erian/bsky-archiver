//! REST-polling fallback for authored-posts-with-media capture, used
//! whenever the Jetstream firehose connection (AR-5) has been down longer
//! than a short grace period, or is disabled entirely by configuration.
//!
//! Feeds the same [`crate::pipeline::CandidatePost`] channel the firehose
//! consumer feeds, so the media downloader (AR-8) doesn't need to know
//! which producer a given post came from. Dedups against
//! [`crate::storage::ArchiveStore`] so a post already captured by the
//! firehose is never reprocessed here, and vice versa.

// Not yet wired into `main`: AR-9 (service orchestration) spawns the
// `RestFallbackPoller`. Silence dead-code lints on this module's public
// surface until then.
#![allow(dead_code)]

use std::time::{Duration, Instant};

use rand::Rng;
use tracing::{debug, info, warn};

use crate::bluesky::{BskyClient, BskyError};
use crate::pipeline::{
    CandidatePost, CandidatePostSender, ConnectionHealth, ConnectionHealthReceiver, MediaRef,
    PostCategory, has_archivable_media,
};
use crate::storage::{ArchiveStore, Category, StorageError};

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
    client: BskyClient,
    archive: ArchiveStore,
    sender: CandidatePostSender,
    health_rx: ConnectionHealthReceiver,
    watch_handles: Vec<String>,
    config: PollerConfig,
}

impl RestFallbackPoller {
    pub fn new(
        client: BskyClient,
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
    Bsky(#[from] BskyError),
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
    client: &BskyClient,
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

    fn test_client(server: &MockServer) -> BskyClient {
        BskyClient::new(
            url::Url::parse(&server.uri()).unwrap(),
            "alice.bsky.social".to_string(),
            Secret::from("app-password".to_string()),
        )
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
            client,
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
}
