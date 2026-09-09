//! Periodic REST polling for likes and bookmarks (not available on the
//! firehose), the REST-polling fallback path for authored posts when the
//! firehose connection is unavailable, and the feed poller (which pulls any
//! watched algorithm/custom feed on the same cadence, since feeds are never
//! subscribed on the firehose). Uses adaptive intervals with exponential
//! backoff and jitter.
//!
//! Feeds the same [`crate::pipeline::CandidatePost`] channel the firehose
//! consumer feeds, so the media downloader (AR-8) doesn't need to know
//! which producer a given post came from. Dedups against
//! [`crate::storage::ArchiveStore`] so a post already captured by another
//! producer is never reprocessed here, and vice versa.
//!
//! Both the account fallback and the feed poller take their watch targets
//! from a live [`watch::Receiver`] over [`crate::watchlist::Watchlist`]'s
//! roster, reading the current set per tick rather than a startup-captured
//! list, so a UI add/remove takes effect live without a restart.

use std::time::{Duration, Instant};

use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::bluesky::{BlueskyClient, BlueskyError, PostView};
use crate::pipeline::{
    CandidatePost, CandidatePostSender, ConnectionHealth, ConnectionHealthReceiver, MediaRef,
    PostCategory, has_archivable_media,
};
use crate::ratelimit::{Backoff, BackoffConfig};
use crate::storage::{
    ArchiveStore, Category, SaveOutcome, SourceKind, StorageError, WatchedSource,
};

/// How many feed items to request per `getAuthorFeed`/`getFeed` page.
pub(crate) const DEFAULT_PAGE_LIMIT: u32 = 50;

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
    /// results). Consecutive errors also feed [`RestFallbackPoller`]'s
    /// shared circuit breaker (AR-13), which kicks in with a much longer
    /// pause if the endpoint keeps failing well past what this interval
    /// alone backs off to.
    fn on_error(&mut self) {
        self.current = double_capped(self.current, self.max);
    }

    /// The current interval with symmetric jitter applied, so multiple
    /// deployments polling the same account wouldn't all land in lockstep.
    fn jittered(&self) -> Duration {
        jitter(self.current)
    }
}

/// Doubles `current`, capped at `max`. Delegates to the shared backoff
/// growth curve ([`crate::ratelimit`]) so this and every other retry loop in
/// the app grow at the same rate.
fn double_capped(current: Duration, max: Duration) -> Duration {
    crate::ratelimit::grow_capped(current, 2.0, max)
}

/// Applies +/-20% jitter to `base`. Delegates to the shared jitter formula
/// ([`crate::ratelimit::jittered`]) so this and every other retry loop in
/// the app apply jitter the same way.
fn jitter(base: Duration) -> Duration {
    crate::ratelimit::jittered(base, 0.2)
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

/// The outcome of polling all watched sources once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CycleOutcome {
    NewContent,
    Empty,
    Error,
}

/// Runs the REST-polling fallback until `sender` is dropped/closed. Meant
/// to be spawned as a long-lived background task (`tokio::spawn`).
///
/// The set of accounts it polls is read live from the roster
/// ([`crate::watchlist::Watchlist`]) on every tick rather than captured at
/// construction, so a UI add/remove takes effect without a restart; a roster
/// reload also wakes the loop's sleep so a new account is backfilled (via
/// the fallback) promptly.
pub struct RestFallbackPoller {
    client: std::sync::Arc<BlueskyClient>,
    archive: ArchiveStore,
    sender: CandidatePostSender,
    health_rx: ConnectionHealthReceiver,
    /// Live view of the watched-sources roster; the watched account set is
    /// re-derived from it on every poll cycle, so UI add/remove operations
    /// take effect without a restart.
    watchlist_rx: watch::Receiver<Vec<WatchedSource>>,
    /// Whether `watchlist_rx` can still deliver reloads; set `false` once the
    /// channel closes so a dead roster can't spin the loop's sleep select.
    roster_alive: bool,
    config: PollerConfig,
}

impl RestFallbackPoller {
    /// `watchlist_rx` is the live watch-list receiver (see
    /// [`crate::watchlist::Watchlist::subscribe`]); the poller re-reads it
    /// every tick.
    pub fn new(
        client: std::sync::Arc<BlueskyClient>,
        archive: ArchiveStore,
        sender: CandidatePostSender,
        health_rx: ConnectionHealthReceiver,
        watchlist_rx: watch::Receiver<Vec<WatchedSource>>,
        config: PollerConfig,
    ) -> Self {
        RestFallbackPoller {
            client,
            archive,
            sender,
            health_rx,
            watchlist_rx,
            roster_alive: true,
            config,
        }
    }

    /// The currently watched account values (handles or DIDs) from the live
    /// roster. Only accounts are polled here — feeds are handled by
    /// [`FeedPoller`].
    fn account_values(&self) -> Vec<String> {
        self.watchlist_rx
            .borrow()
            .iter()
            .filter(|source| source.kind == SourceKind::Account)
            .map(|source| source.value.clone())
            .collect()
    }

    /// Runs forever, alternating between waiting for the fallback to be
    /// "active" (per [`is_active`]) and polling all watched handles on an
    /// adaptive interval. Returns only if the candidate-post channel is
    /// closed (the downstream consumer shut down).
    ///
    /// Alongside the adaptive interval (which governs the normal empty/
    /// content-found cadence), a shared [`Backoff`] circuit breaker (AR-13)
    /// tracks consecutive `Error` outcomes: once it trips, its cooldown
    /// floors the sleep so a persistently failing endpoint gets a much
    /// longer rest instead of being hammered at the interval's own capped
    /// backoff rate.
    pub async fn run(mut self) {
        let mut interval =
            AdaptiveInterval::new(self.config.baseline_interval, self.config.max_interval);
        let mut breaker = Backoff::new(BackoffConfig::new(
            self.config.baseline_interval,
            self.config.max_interval,
        ));

        loop {
            if !self.wait_until_active().await {
                return;
            }

            let delay = match self.poll_all_handles().await {
                CycleOutcome::NewContent => {
                    interval.on_content_found();
                    breaker.on_success();
                    interval.jittered()
                }
                CycleOutcome::Empty => {
                    interval.on_empty();
                    breaker.on_success();
                    interval.jittered()
                }
                CycleOutcome::Error => {
                    interval.on_error();
                    let breaker_delay = breaker.on_failure(None);
                    if breaker.is_open() {
                        warn!(
                            cooldown_ms = breaker_delay.as_millis() as u64,
                            "rest poller circuit breaker open; pausing well past the normal backoff"
                        );
                    }
                    interval.jittered().max(breaker_delay)
                }
            };

            debug!(delay_ms = delay.as_millis() as u64, "rest poller sleeping");
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                changed = self.health_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                changed = self.watchlist_rx.changed(), if self.roster_alive => {
                    // A live watch-list reload: falling through to the next
                    // loop iteration re-reads the roster (and polls it) right
                    // away instead of sleeping out the rest of the interval.
                    match changed {
                        Err(_) => self.roster_alive = false,
                        Ok(()) => {
                            let _ = self.watchlist_rx.borrow_and_update();
                        }
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

    /// Polls every account in the current roster once, snapshotting the
    /// values up front so the roster guard is never held across an await.
    async fn poll_all_handles(&self) -> CycleOutcome {
        let mut any_new = false;
        let mut any_error = false;

        for handle in self.account_values() {
            match poll_handle_once(
                &self.client,
                &self.archive,
                &self.sender,
                &handle,
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

/// Polls every watched feed (`app.bsky.feed.getFeed`) on an adaptive
/// interval, in addition to — and completely independent of — the
/// firehose/REST-fallback account path. Feeds have no connection-health
/// gating: they're fetched over REST unconditionally, paginated per feed
/// with its own cursor, and their posts flow into the same
/// [`CandidatePost`] pipeline as everything else.
///
/// The watched feed URIs are read live from the roster (via `watchlist_rx`)
/// on every tick, and a roster reload (add/remove) wakes the loop's sleep so
/// a UI change is acted on immediately.
pub struct FeedPoller {
    client: std::sync::Arc<BlueskyClient>,
    archive: ArchiveStore,
    sender: CandidatePostSender,
    watchlist_rx: watch::Receiver<Vec<WatchedSource>>,
    /// Whether `watchlist_rx` can still deliver reloads; set `false` once the
    /// channel closes so a dead roster can't spin the loop's sleep select.
    roster_alive: bool,
    config: PollerConfig,
}

impl FeedPoller {
    /// `watchlist_rx` is the live watch-list receiver (see
    /// [`crate::watchlist::Watchlist::subscribe`]).
    pub fn new(
        client: std::sync::Arc<BlueskyClient>,
        archive: ArchiveStore,
        sender: CandidatePostSender,
        watchlist_rx: watch::Receiver<Vec<WatchedSource>>,
        config: PollerConfig,
    ) -> Self {
        FeedPoller {
            client,
            archive,
            sender,
            watchlist_rx,
            roster_alive: true,
            config,
        }
    }

    /// The currently watched `at://` feed URIs, from the live roster.
    fn feed_values(&self) -> Vec<String> {
        self.watchlist_rx
            .borrow()
            .iter()
            .filter(|source| source.kind == SourceKind::Feed)
            .map(|source| source.value.clone())
            .collect()
    }

    /// Polls every watched feed once. Each feed is walked newest-first to
    /// its own dedup boundary; archive-worthy posts become candidates.
    async fn poll_all_feeds(&self) -> CycleOutcome {
        let mut any_new = false;
        let mut any_error = false;

        for feed in self.feed_values() {
            match poll_feed_once(
                &self.client,
                &self.archive,
                &self.sender,
                &feed,
                self.config.page_limit,
            )
            .await
            {
                Ok(new_count) => {
                    if new_count > 0 {
                        info!(feed = %feed, new_count, "feed poller found new posts");
                        any_new = true;
                    }
                }
                Err(err) => {
                    warn!(feed = %feed, error = %err, "feed poller failed to poll feed");
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

    /// Runs forever: poll every watched feed, then sleep an adaptive delay
    /// (tightening on new content, backing off on empties/errors, with a
    /// circuit breaker on prolonged failure). A roster reload wakes the
    /// sleep so a newly added feed is picked up promptly and a removed one
    /// stops being polled.
    pub async fn run(mut self) {
        let mut interval =
            AdaptiveInterval::new(self.config.baseline_interval, self.config.max_interval);
        let mut breaker = Backoff::new(BackoffConfig::new(
            self.config.baseline_interval,
            self.config.max_interval,
        ));

        loop {
            let outcome = self.poll_all_feeds().await;
            let delay = match outcome {
                CycleOutcome::NewContent => {
                    interval.on_content_found();
                    breaker.on_success();
                    interval.jittered()
                }
                CycleOutcome::Empty => {
                    interval.on_empty();
                    breaker.on_success();
                    interval.jittered()
                }
                CycleOutcome::Error => {
                    interval.on_error();
                    let breaker_delay = breaker.on_failure(None);
                    if breaker.is_open() {
                        warn!(
                            cooldown_ms = breaker_delay.as_millis() as u64,
                            "feed poller circuit breaker open; pausing well past the normal backoff"
                        );
                    }
                    interval.jittered().max(breaker_delay)
                }
            };

            debug!(delay_ms = delay.as_millis() as u64, "feed poller sleeping");
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                changed = self.watchlist_rx.changed(), if self.roster_alive => {
                    // A live roster reload: falling out of this select
                    // re-polls the (possibly updated) feed set on the next
                    // loop iteration. On a closed roster channel, disable the
                    // arm so the loop can't spin on a permanently-resolvable
                    // `changed()`.
                    match changed {
                        Err(_) => self.roster_alive = false,
                        Ok(()) => {
                            let _ = self.watchlist_rx.borrow_and_update();
                        }
                    }
                }
            }
        }
    }
}

/// Page size used by the web UI's one-shot backfill passes.
pub const BACKFILL_PAGE_LIMIT: u32 = 50;

/// Errors from a single poll-and-drain pass over one account's authored feed
/// or one watched feed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum PollHandleError {
    #[error(transparent)]
    Bluesky(#[from] BlueskyError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("candidate post channel closed")]
    ChannelClosed,
}

/// Errors from a single one-shot backfill pass ([`backfill_account_once`] /
/// [`backfill_feed_once`]).
#[derive(Debug, thiserror::Error)]
pub enum BackfillError {
    #[error(transparent)]
    Bluesky(#[from] BlueskyError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("candidate post channel closed")]
    ChannelClosed,
}

impl From<PollHandleError> for BackfillError {
    fn from(err: PollHandleError) -> Self {
        match err {
            PollHandleError::Bluesky(err) => BackfillError::Bluesky(err),
            PollHandleError::Storage(err) => BackfillError::Storage(err),
            PollHandleError::ChannelClosed => BackfillError::ChannelClosed,
        }
    }
}

/// Polls one account's authored feed in a single pass (newest-first, to the
/// dedup boundary), handing archive-worthy posts to `sender`. The web UI
/// calls this from a spawned task immediately after the account is added so
/// its existing media posts are backfilled right away instead of waiting up
/// to a full poll interval. Returns how many new candidates were sent.
pub async fn backfill_account_once(
    client: &BlueskyClient,
    archive: &ArchiveStore,
    sender: &CandidatePostSender,
    account: &str,
) -> Result<usize, BackfillError> {
    poll_handle_once(client, archive, sender, account, BACKFILL_PAGE_LIMIT)
        .await
        .map_err(BackfillError::from)
}

/// Polls one feed generator in a single pass (newest-first, to the dedup
/// boundary), handing archive-worthy posts to `sender`. The web UI calls
/// this from a spawned task immediately after the feed is added. Returns how
/// many new candidates were sent.
pub async fn backfill_feed_once(
    client: &BlueskyClient,
    archive: &ArchiveStore,
    sender: &CandidatePostSender,
    feed_uri: &str,
) -> Result<usize, BackfillError> {
    poll_feed_once(client, archive, sender, feed_uri, BACKFILL_PAGE_LIMIT)
        .await
        .map_err(BackfillError::from)
}

/// Walks `handle`'s authored feed newest-first, one page at a time, until
/// either the feed is exhausted or a post already present in `archive` is
/// reached (the dedup boundary — everything older is assumed already
/// archived). Archive-worthy posts found before that boundary are sent to
/// `sender`. Returns how many new candidates were sent.
pub(crate) async fn poll_handle_once(
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

        match drain_feed_items(archive, sender, &page.feed, new_count).await? {
            DrainOutcome::DedupBoundary => break,
            DrainOutcome::Continue(count) => new_count = count,
        }

        match page.cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    Ok(new_count)
}

/// Walks a watched feed (an `at://` `app.bsky.feed.generator` URI) newest-
/// first via `app.bsky.feed.getFeed`, one page at a time, until either the
/// feed is exhausted or a post already present in `archive` is reached (the
/// dedup boundary). Archive-worthy posts found before that boundary are sent
/// to `sender`. Returns how many new candidates were sent.
async fn poll_feed_once(
    client: &BlueskyClient,
    archive: &ArchiveStore,
    sender: &CandidatePostSender,
    feed_uri: &str,
    page_limit: u32,
) -> Result<usize, PollHandleError> {
    let mut cursor: Option<String> = None;
    let mut new_count = 0usize;

    loop {
        let page = client
            .get_feed(feed_uri, cursor.as_deref(), page_limit)
            .await?;

        if page.feed.is_empty() {
            break;
        }

        match drain_feed_items(archive, sender, &page.feed, new_count).await? {
            DrainOutcome::DedupBoundary => break,
            DrainOutcome::Continue(count) => new_count = count,
        }

        match page.cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    Ok(new_count)
}

/// The outcome of draining one page of feed items.
enum DrainOutcome {
    /// An already-archived post was hit: defer to the caller, which must stop
    /// paginating.
    DedupBoundary,
    /// The whole page was consumed; `count` is the running new-candidate
    /// count.
    Continue(usize),
}

/// Drains one page of `feed` items (as returned by `getAuthorFeed`/
/// `getFeed`), sending archive-worthy posts to `sender` and deferring to the
/// caller once the first already-archived post (the dedup boundary) is
/// reached.
async fn drain_feed_items(
    archive: &ArchiveStore,
    sender: &CandidatePostSender,
    feed: &[serde_json::Value],
    mut new_count: usize,
) -> Result<DrainOutcome, PollHandleError> {
    for item in feed {
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
            warn!("poller skipping feed item missing uri/cid/author.did");
            continue;
        };

        if archive.is_archived(Category::Post, at_uri).await? {
            debug!(at_uri, "poller reached dedup boundary");
            return Ok(DrainOutcome::DedupBoundary);
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

    Ok(DrainOutcome::Continue(new_count))
}

/// Extracts downloadable media from a hydrated embed *view* (as returned
/// alongside `post.record` by `getAuthorFeed`/`getFeed`, distinct from the raw
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

/// Page size requested per `getActorLikes` / `getBookmarks` call. Also used
/// by the nightly sweeper's full walks of the same lists.
pub(crate) const PAGE_SIZE: u32 = 50;

/// Errors that can end a single poll pass for one category.
#[derive(Debug, thiserror::Error)]
pub enum PollError {
    #[error(transparent)]
    Bluesky(#[from] BlueskyError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl PollError {
    /// The server-provided retry hint carried by the underlying error, if
    /// any (see [`BlueskyError::retry_after`]).
    fn retry_after(&self) -> Option<Duration> {
        match self {
            PollError::Bluesky(err) => err.retry_after(),
            PollError::Storage(_) => None,
        }
    }
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
    /// `base_interval`, backing off (with jitter, via the shared
    /// [`Backoff`] policy) after consecutive failures and resetting to
    /// `base_interval` on success. Honors a server-provided `Retry-After`/
    /// `ratelimit-reset` hint over the computed backoff when either poll
    /// pass surfaces one, and opens a circuit breaker (pausing much longer)
    /// after too many consecutive failed cycles.
    pub async fn run(&self) {
        let mut backoff = Backoff::new(BackoffConfig::new(
            self.base_interval,
            self.base_interval.saturating_mul(32),
        ));
        loop {
            let likes_result = self.poll_likes().await;
            if let Err(err) = &likes_result {
                warn!(error = %err, "likes poll pass failed");
            }
            let bookmarks_result = self.poll_bookmarks().await;
            if let Err(err) = &bookmarks_result {
                warn!(error = %err, "bookmarks poll pass failed");
            }

            let delay = match (&likes_result, &bookmarks_result) {
                (Ok(_), Ok(_)) => {
                    backoff.on_success();
                    crate::ratelimit::jittered(self.base_interval, 0.2)
                }
                _ => {
                    let hint = likes_result
                        .as_ref()
                        .err()
                        .and_then(PollError::retry_after)
                        .or_else(|| {
                            bookmarks_result
                                .as_ref()
                                .err()
                                .and_then(PollError::retry_after)
                        });
                    let delay = backoff.on_failure(hint);
                    if backoff.is_open() {
                        warn!(
                            cooldown_ms = delay.as_millis() as u64,
                            "likes/bookmarks poller circuit breaker open; pausing well past the normal backoff"
                        );
                    }
                    delay
                }
            };

            debug!(
                delay_ms = delay.as_millis() as u64,
                next_backoff_ms = backoff.current_delay().as_millis() as u64,
                "likes/bookmarks poller sleeping"
            );
            tokio::time::sleep(delay).await;
        }
    }

    /// One pagination pass over likes, newest-first, stopping as soon as an
    /// already-archived item is hit (or pages run out). Items are ranked
    /// with their position in the API's list (0 = newest), mirroring
    /// [`Self::poll_bookmarks`].
    pub async fn poll_likes(&self) -> Result<(), PollError> {
        let mut cursor: Option<String> = None;
        let mut rank: i64 = 0;
        loop {
            let page = self
                .client
                .get_actor_likes(&self.actor, cursor.as_deref(), PAGE_SIZE)
                .await?;
            if page.feed.is_empty() {
                return Ok(());
            }

            for entry in &page.feed {
                let position = rank;
                rank += 1;
                if self
                    .archive_one(
                        Category::Like,
                        PostCategory::Like,
                        &entry.post,
                        None,
                        position,
                    )
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
    /// as an already-archived item is hit (or pages run out). A bookmark
    /// whose post no longer resolves upstream is not a dedup boundary (the
    /// pagination order is unaffected by deletion); if it specifically came
    /// back as `notFound` — the post was deleted — the archived copy is
    /// marked with a `deleted_at` timestamp so the UI can badge it.
    ///
    /// Every entry the walk visits is ranked with its position in the
    /// API's list (0 = newest): that order is what the Bluesky app
    /// displays, so the gallery's bookmarked sorts key off it (see
    /// `MediaSort::NewestAction`).
    pub async fn poll_bookmarks(&self) -> Result<(), PollError> {
        let mut cursor: Option<String> = None;
        let mut rank: i64 = 0;
        loop {
            let page = self
                .client
                .get_bookmarks(cursor.as_deref(), PAGE_SIZE)
                .await?;
            if page.bookmarks.is_empty() {
                return Ok(());
            }

            for entry in &page.bookmarks {
                let position = rank;
                rank += 1;
                let Some(item) = entry.item.as_ref() else {
                    debug!(subject = %entry.subject.uri, "bookmark item missing; skipping");
                    continue;
                };
                let Some(post) = item.post() else {
                    if item.is_not_found() {
                        if let Some(created_at) = entry.created_at.as_deref() {
                            self.store
                                .set_action_at(Category::Bookmark, &entry.subject.uri, created_at)
                                .await?;
                        }
                        self.store
                            .set_action_seq(Category::Bookmark, &entry.subject.uri, position)
                            .await?;
                        mark_bookmark_deleted(&self.store, &entry.subject.uri).await?;
                    } else {
                        debug!(subject = %entry.subject.uri, "bookmark not resolvable (blocked?); skipping");
                    }
                    continue;
                };
                if self
                    .archive_one(
                        Category::Bookmark,
                        PostCategory::Bookmark,
                        post,
                        entry.created_at.as_deref(),
                        position,
                    )
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
    /// media) under `category`, deduping against the archive. `position`
    /// is the item's rank in the API list this walk is traversing (0 =
    /// newest). Returns `true` if this item was already archived (i.e. the
    /// dedup boundary for this pagination pass has been reached).
    async fn archive_one(
        &self,
        category: Category,
        post_category: PostCategory,
        post: &PostView,
        action_at: Option<&str>,
        position: i64,
    ) -> Result<bool, PollError> {
        archive_like_bookmark_post(
            &self.store,
            &self.sender,
            category,
            post_category,
            post,
            action_at,
            position,
        )
        .await
    }
}

/// Archives one like/bookmark post (JSON record, plus a [`CandidatePost`]
/// if it has media), deduping against the archive. Shared by
/// [`LikesBookmarksPoller`] and the nightly sweeper ([`crate::sweep`]).
/// `action_at` is the bookmark action time (`bookmarkView.createdAt`) for
/// bookmarks; `position` is the item's rank in the API list being walked
/// (0 = newest) — the ordering Bluesky's own UI displays, which the
/// gallery's bookmarked/liked sorts reproduce. Both are recorded in the
/// index + envelope whether the post is newly saved or was already
/// archived (the sweeper's full walks backfill older rows that way).
/// Returns `true` if the post was already archived (i.e. the dedup
/// boundary for a pagination pass has been reached).
pub(crate) async fn archive_like_bookmark_post(
    store: &ArchiveStore,
    sender: &CandidatePostSender,
    category: Category,
    post_category: PostCategory,
    post: &PostView,
    action_at: Option<&str>,
    position: i64,
) -> Result<bool, PollError> {
    if store.is_archived(category, &post.uri).await? {
        if let Some(action_at) = action_at {
            store.set_action_at(category, &post.uri, action_at).await?;
        }
        store.set_action_seq(category, &post.uri, position).await?;
        repair_corrupt_media(store, sender, category, post_category, post).await?;
        debug!(at_uri = %post.uri, %category, "reached dedup boundary");
        return Ok(true);
    }

    let outcome = store
        .save_post(category, &post.uri, &post.cid, post.record.clone())
        .await?;

    if outcome == SaveOutcome::Inserted && has_archivable_media(&post.record) {
        enqueue_media(sender, post_category, post).await?;
    }

    if let Some(action_at) = action_at {
        store.set_action_at(category, &post.uri, action_at).await?;
    }
    store.set_action_seq(category, &post.uri, position).await?;

    if outcome == SaveOutcome::Inserted {
        debug!(at_uri = %post.uri, %category, "archived new item");
    }

    Ok(false)
}

/// Sends one post's media refs to the downloader. Shared by the fresh
/// archive path and the corruption-repair path.
async fn enqueue_media(
    sender: &CandidatePostSender,
    post_category: PostCategory,
    post: &PostView,
) -> Result<(), PollError> {
    let media = post
        .embed
        .as_ref()
        .map(extract_media_refs)
        .unwrap_or_default();
    if media.is_empty() {
        return Ok(());
    }
    let candidate = CandidatePost {
        at_uri: post.uri.clone(),
        cid: post.cid.clone(),
        author_did: post.author.did.clone(),
        category: post_category,
        record: post.record.clone(),
        media,
    };
    if sender.send(candidate).await.is_err() {
        warn!(
            at_uri = %post.uri,
            "candidate post channel closed; media downloader not receiving"
        );
    }
    Ok(())
}

/// Re-downloads an already-archived post's media when the stored files
/// carry a corruption signature from a pre-fix archive version (raw HLS
/// playlists, TS bytes mislabeled as MP4). The post's media rows and files
/// are removed and the post is re-queued with its hydrated CDN URLs, so
/// the current downloader logic replaces everything with fresh, valid
/// files.
///
/// Best effort: any failure is logged and swallowed — the stale files stay
/// until the next walk tries again.
async fn repair_corrupt_media(
    store: &ArchiveStore,
    sender: &CandidatePostSender,
    category: Category,
    post_category: PostCategory,
    post: &PostView,
) -> Result<(), PollError> {
    let rows = store.list_post_media(category, &post.uri).await?;
    if rows.is_empty() {
        // Nothing stored (a previous download failed permanently, or an
        // interrupted repair): re-queue from the still-hydrated embed —
        // but only when the record actually carries media, so text-only
        // posts don't re-queue (and log) on every walk.
        if has_archivable_media(&post.record) {
            return self_heal_media(store, sender, category, post_category, post).await;
        }
        return Ok(());
    }
    for (filename, content_type) in rows {
        let head = store
            .read_media_head(category, &post.uri, &filename, 3 * 188)
            .await?
            .unwrap_or_default();
        if crate::storage::stored_media_looks_corrupt(&filename, content_type.as_deref(), &head) {
            return self_heal_media(store, sender, category, post_category, post).await;
        }
    }
    Ok(())
}

/// Clears an archived post's media and re-queues the download. Used when
/// the stored files carry a pre-fix corruption signature, or when the post
/// should have media but none is stored.
async fn self_heal_media(
    store: &ArchiveStore,
    sender: &CandidatePostSender,
    category: Category,
    post_category: PostCategory,
    post: &PostView,
) -> Result<(), PollError> {
    match store.delete_post_media(category, &post.uri).await {
        Ok(removed) => {
            info!(
                at_uri = %post.uri,
                %category,
                removed,
                "archived media missing or corrupt from a pre-fix version; re-downloading"
            );
        }
        Err(err) => {
            warn!(at_uri = %post.uri, error = %err, "failed to clear corrupt media rows");
            return Ok(());
        }
    }
    // `has_archivable_media` was true when this post was first queued, so
    // re-derive the refs from the still-hydrated embed unconditionally.
    enqueue_media(sender, post_category, post).await
}

/// Marks an archived post as deleted in the index after the API reported
/// its bookmark as `notFound` (the post was deleted upstream). Storage
/// failures are logged and swallowed: one unmarkable row must not abort
/// the whole pagination pass, and the nightly sweep re-checks every
/// archived URI anyway.
async fn mark_bookmark_deleted(store: &ArchiveStore, subject_uri: &str) -> Result<(), PollError> {
    match store.mark_post_deleted(subject_uri).await {
        Ok(true) => {
            info!(subject = %subject_uri, "bookmarked post deleted upstream; marked in index");
            Ok(())
        }
        Ok(false) => Ok(()),
        Err(err) => {
            warn!(subject = %subject_uri, error = %err, "failed to mark deleted bookmarked post");
            Ok(())
        }
    }
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
mod tests;
