//! Service orchestration: the startup sequence, the supervised background
//! tasks (firehose consumer, REST fallback poller, likes/bookmarks poller,
//! media downloader), and graceful shutdown.
//!
//! Split into [`init`] (build everything, fail fast on misconfiguration —
//! but tolerate a transient Bluesky rate limit and authenticate lazily) and
//! [`serve`] (run forever until a shutdown signal arrives) so startup
//! validation failure paths can be exercised directly in tests without
//! spawning a real process or running the service loop.
//!
//! ## Manual verification of process-level graceful shutdown
//!
//! A true SIGTERM/SIGINT test isn't practical in `cargo test` (it would
//! need to spawn and signal a real child process reliably across
//! platforms). To verify by hand instead:
//!
//! 1. Run the binary with real config against a test Bluesky account.
//! 2. While it's archiving something, send it SIGTERM (`kill -TERM <pid>`
//!    on Linux/macOS) or Ctrl+C.
//! 3. Confirm the logs show `shutdown signal received`, then
//!    `shutdown complete`, then the process exits with status 0.
//! 4. Confirm `ARCHIVE_DIR` contains no leftover `.tmp-*` files, and that
//!    every `record.json` present is valid JSON (a torn write would leave
//!    either an orphaned temp file or fail to parse).

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt;
use tokio::sync::watch;
use url::Url;

use crate::bluesky::{BlueskyClient, BlueskyError};
use crate::config::{AppConfig, ConfigError};
use crate::firehose::FirehoseConsumer;
use crate::health::{self, HealthSender, HealthSnapshot, SubsystemHealth};
use crate::media::MediaDownloader;
use crate::pipeline::{
    CandidatePostReceiver, CandidatePostSender, ConnectionHealth, ConnectionHealthReceiver,
    ConnectionHealthSender, candidate_post_channel, connection_health_channel, weak_from_sender,
};
use crate::poller::{FeedPoller, LikesBookmarksPoller, PollerConfig, RestFallbackPoller};
use crate::ratelimit::{Backoff, BackoffConfig, RequestLimiter};
use crate::state::{AppState, SharedAppState};
use crate::storage::{ArchiveStore, SourceKind, StorageError};
use crate::sweep::NightlySweeper;
use crate::tumblr::{DEFAULT_TUMBLR_BASE_URL, TumblrClient, TumblrLikesPoller};
use crate::watchlist::Watchlist;

/// The production Bluesky XRPC entryway. Not part of the canonical env var
/// schema (there is no `BSKY_BASE_URL`); overridable only for tests, which
/// point [`init`] at a `wiremock` server instead.
pub const DEFAULT_BLUESKY_BASE_URL: &str = "https://bsky.social";

/// The production Tumblr API v2 entryway, parsed once for the Tumblr likes
/// poller. Only used when Tumblr credentials are configured.
fn tumblr_base_url() -> Url {
    Url::parse(DEFAULT_TUMBLR_BASE_URL).expect("default tumblr base url is valid")
}

/// Process-wide soft cap on outbound Bluesky-related HTTP requests in
/// flight at once (REST polling + media downloads combined). Not part of
/// the canonical env var schema — a burst-smoothing safety net, not a
/// user-facing tunable.
const GLOBAL_INFLIGHT_REQUEST_CAP: usize = 16;

/// Everything that can go wrong during startup, each wrapping the
/// underlying error with enough context to log clearly and exit non-zero.
#[derive(Debug, thiserror::Error)]
pub enum StartupError {
    #[error("configuration error: {0}")]
    Config(#[from] ConfigError),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("bluesky startup error: {0}")]
    Bluesky(#[from] BlueskyError),
}

/// Everything [`serve`] needs to spawn the supervised background tasks,
/// produced by [`init`].
pub struct Started {
    pub state: SharedAppState,
    health_tx: HealthSender,
    bluesky_client: Arc<BlueskyClient>,
    /// The authenticated account's DID, when startup managed to log in.
    /// `None` when Bluesky rate-limited the startup login: the service still
    /// starts, its own-account queries fall back to the configured handle,
    /// and the session is established lazily on the first API call.
    self_did: Option<String>,
    candidate_tx: CandidatePostSender,
    candidate_rx: CandidatePostReceiver,
    conn_health_tx: ConnectionHealthSender,
    conn_health_rx: ConnectionHealthReceiver,
    request_limiter: Arc<RequestLimiter>,
}

/// Runs the whole service: loads config from the environment, starts up
/// (failing fast on misconfiguration, but not on a transient rate limit),
/// then serves until a shutdown signal arrives.
pub async fn run() -> Result<(), StartupError> {
    let config = AppConfig::from_env()?;
    let base_url = Url::parse(DEFAULT_BLUESKY_BASE_URL).expect("default bluesky base url is valid");
    let started = init(config, base_url).await?;
    serve(started).await;
    Ok(())
}

/// The startup sequence: open storage (self-healing the SQLite index if
/// it's missing), authenticate to Bluesky (failing fast on bad
/// credentials, but starting anyway when rate-limited), and load/seed the
/// `watched_sources` watch list into the in-memory roster. Returns
/// everything [`serve`] needs to spawn the background tasks.
pub async fn init(config: AppConfig, bluesky_base_url: Url) -> Result<Started, StartupError> {
    tracing::info!(config = ?config, "starting up");

    let db_existed = tokio::fs::metadata(&config.database_path).await.is_ok();
    let store = ArchiveStore::open(config.archive_dir.clone(), config.database_path.clone())
        .await
        .map_err(StartupError::from)?;
    if !db_existed {
        tracing::info!("sqlite index missing; rebuilding it from the on-disk archive");
        store.reindex().await.map_err(StartupError::from)?;
    }

    let request_limiter = Arc::new(RequestLimiter::new(GLOBAL_INFLIGHT_REQUEST_CAP));
    let bluesky_client = Arc::new(
        BlueskyClient::new(
            bluesky_base_url,
            config.bsky_identifier.clone(),
            config.bsky_app_password.clone(),
        )
        .with_request_limiter(Arc::clone(&request_limiter)),
    );
    // Bad credentials are a misconfiguration and still abort startup, but a
    // rate limit is transient: start up anyway (the web UI comes up, the
    // supervised pollers retry with backoff) and let the client log in
    // lazily on the first real API call.
    let self_did = match bluesky_client.authenticate().await {
        Ok(did) => {
            tracing::info!(did = %did, "authenticated with bluesky");
            Some(did)
        }
        Err(err) if err.is_rate_limited() => {
            tracing::warn!(
                error = %err,
                "bluesky rate-limited startup authentication; starting without an initial session"
            );
            None
        }
        Err(err) => return Err(StartupError::from(err)),
    };

    seed_empty_watchlist(&config, &store, self_did.as_deref()).await?;
    let watchlist = Watchlist::new(store.list_watched_sources().await?);
    tracing::info!(
        sources = ?watchlist.snapshot(),
        "loaded watch list"
    );

    let (health_tx, health_rx) = health::health_channel();
    let (conn_health_tx, conn_health_rx) = connection_health_channel(ConnectionHealth::Disabled);
    let (candidate_tx, candidate_rx) = candidate_post_channel(64);

    let state: SharedAppState = Arc::new(AppState {
        config,
        store,
        health: health_rx,
        watchlist: watchlist.clone(),
        bluesky: Arc::clone(&bluesky_client),
        candidate_weak: weak_from_sender(&candidate_tx),
    });

    Ok(Started {
        state,
        health_tx,
        bluesky_client,
        self_did,
        candidate_tx,
        candidate_rx,
        conn_health_tx,
        conn_health_rx,
        request_limiter,
    })
}

/// Guarantees a fresh install (an empty `watched_sources` table) watches the
/// authenticated account by default, preserving the pre-v2 behavior where
/// `BSKY_IDENTIFIER` alone meant "archive your own posts". Only seeds when
/// the table is entirely empty; a non-empty list is left untouched.
async fn seed_empty_watchlist(
    config: &AppConfig,
    store: &ArchiveStore,
    self_did: Option<&str>,
) -> Result<(), StorageError> {
    if !store.list_watched_sources().await?.is_empty() {
        return Ok(());
    }
    store
        .add_watched_source(SourceKind::Account, &config.bsky_identifier, self_did)
        .await?;
    tracing::info!(
        identifier = %config.bsky_identifier,
        did = ?self_did,
        "watch list was empty; seeded with the authenticated account"
    );
    Ok(())
}

/// Resolves and persists the DID for watched account sources that don't have
/// one yet, then reloads the live roster so the firehose filters on real
/// DIDs.
///
/// This is the repair for an install whose account was seeded while Bluesky
/// was rate-limiting the startup login: the account is watched by handle, but
/// Jetstream matches on DIDs, so until this runs the firehose would filter on
/// a handle and deliver nothing for it. Runs in the background (a rate limit
/// must not block startup) and retries with backoff, honoring any
/// server-provided hint, until every account resolves or `shutdown` fires.
/// Returns once none lack a DID.
async fn backfill_missing_dids(
    client: Arc<BlueskyClient>,
    store: ArchiveStore,
    watchlist: Watchlist,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut backoff = Backoff::new(BackoffConfig::new(
        Duration::from_secs(5),
        Duration::from_secs(300),
    ));
    loop {
        if *shutdown.borrow() {
            return;
        }

        let missing: Vec<String> = match store.list_watched_sources().await {
            Ok(sources) => sources
                .into_iter()
                .filter(|source| {
                    source.kind == SourceKind::Account
                        && source.did.is_none()
                        && !source.value.starts_with("did:")
                })
                .map(|source| source.value)
                .collect(),
            Err(err) => {
                tracing::warn!(error = %err, "failed to list watched sources for DID backfill");
                return;
            }
        };
        if missing.is_empty() {
            return;
        }

        // Resolve this pass's accounts, reloading the roster for whichever
        // ones succeeded even if a later one defers, so a single
        // unresolvable handle can't keep every repair out of the live roster.
        let mut resolved_any = false;
        let mut retry_in: Option<Duration> = None;
        for value in missing {
            match client.resolve_handle(&value).await {
                Ok(did) => match store.set_watched_source_did(&value, &did).await {
                    Ok(true) => {
                        tracing::info!(handle = %value, %did, "backfilled watched account DID");
                        resolved_any = true;
                    }
                    Ok(false) => {
                        tracing::debug!(
                            handle = %value,
                            "watched account removed during DID backfill; skipping"
                        );
                    }
                    Err(err) => {
                        let delay = backoff.on_failure(None);
                        tracing::warn!(
                            handle = %value,
                            error = %err,
                            retry_secs = delay.as_secs(),
                            "failed to persist resolved watch DID; retrying"
                        );
                        retry_in = Some(delay);
                        break;
                    }
                },
                Err(err) => {
                    let delay = backoff.on_failure(err.retry_after());
                    tracing::warn!(
                        handle = %value,
                        error = %err,
                        retry_secs = delay.as_secs(),
                        "could not resolve watched account DID; retrying"
                    );
                    retry_in = Some(delay);
                    break;
                }
            }
        }

        if resolved_any && let Err(err) = watchlist.reload_from_store(&store).await {
            tracing::warn!(error = %err, "failed to reload watch list after DID backfill");
        }

        let Some(delay) = retry_in else {
            // Nothing deferred this pass; reset the backoff and loop once
            // more to confirm none remain, then return.
            backoff.on_success();
            continue;
        };
        // A deferred handle keeps its accumulated backoff across passes (the
        // reset above only runs on a fully clean pass).
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

/// Spawns the supervised background tasks and runs until SIGINT/SIGTERM
/// (or Ctrl+C on platforms without SIGTERM), then shuts everything down
/// gracefully: the firehose consumer stops on its own internal shutdown
/// signal, the REST fallback and likes/bookmarks pollers (which have no
/// such signal) are cancelled, and the media downloader is left to finish
/// draining whatever's already queued once every producer's sender has
/// dropped.
pub async fn serve(started: Started) {
    let Started {
        state,
        health_tx,
        bluesky_client,
        self_did,
        candidate_tx,
        candidate_rx,
        conn_health_tx,
        conn_health_rx,
        request_limiter,
    } = started;

    let config = &state.config;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let cursor_path = config.archive_dir.join("jetstream_cursor");
    // The account's own queries (likes/bookmarks poller, nightly sweep)
    // identify it by handle or DID. Prefer the DID resolved at startup,
    // falling back to the configured handle when startup was rate-limited
    // and no session had been established yet.
    let self_actor = self_did.unwrap_or_else(|| config.bsky_identifier.clone());
    // One shared roster subscription drives every producer, so a UI add or
    // remove (reload) is observed by all of them.
    let roster_rx = state.watchlist.subscribe();

    // Repair any watched account seeded without a DID (see
    // `backfill_missing_dids`); a no-op when every account already has one.
    tokio::spawn(backfill_missing_dids(
        Arc::clone(&bluesky_client),
        state.store.clone(),
        state.watchlist.clone(),
        shutdown_rx.clone(),
    ));

    let firehose_handle = spawn_firehose(
        config.jetstream_url.clone(),
        roster_rx.clone(),
        cursor_path,
        candidate_tx.clone(),
        conn_health_tx,
        health_tx.clone(),
        shutdown_rx.clone(),
        state.store.clone(),
    );

    let rest_fallback_handle = spawn_rest_fallback(
        Arc::clone(&bluesky_client),
        state.store.clone(),
        candidate_tx.clone(),
        conn_health_rx,
        roster_rx.clone(),
        PollerConfig::new(Duration::from_secs(config.poll_interval_seconds)),
        health_tx.clone(),
        shutdown_rx.clone(),
    );

    let feed_poller_handle = spawn_feed_poller(
        Arc::clone(&bluesky_client),
        state.store.clone(),
        candidate_tx.clone(),
        roster_rx,
        PollerConfig::new(Duration::from_secs(config.poll_interval_seconds)),
        health_tx.clone(),
        shutdown_rx.clone(),
    );

    let likes_bookmarks_handle = spawn_likes_bookmarks(
        Arc::clone(&bluesky_client),
        state.store.clone(),
        candidate_tx.clone(),
        self_actor.clone(),
        Duration::from_secs(config.poll_interval_seconds),
        health_tx.clone(),
        shutdown_rx.clone(),
    );

    // The Tumblr likes poller only runs when Tumblr credentials are
    // configured; otherwise its health entry is marked Disabled so the
    // dashboard reports the absence honestly instead of a stuck "starting
    // up".
    let tumblr_likes_handle = match state.config.tumblr.clone() {
        Some(tumblr_config) => {
            let tumblr_client = Arc::new(
                TumblrClient::new(tumblr_base_url(), tumblr_config)
                    .with_request_limiter(Arc::clone(&request_limiter)),
            );
            Some(spawn_tumblr_likes(
                tumblr_client,
                state.store.clone(),
                candidate_tx.clone(),
                Duration::from_secs(config.tumblr_poll_interval_seconds),
                health_tx.clone(),
                shutdown_rx.clone(),
            ))
        }
        None => {
            health_tx.send_modify(|s| s.tumblr_likes = SubsystemHealth::disabled());
            None
        }
    };

    let nightly_sweep_handle = spawn_nightly_sweep(
        Arc::clone(&bluesky_client),
        state.store.clone(),
        candidate_tx.clone(),
        self_actor,
        config.nightly_sweep_local_hour,
        health_tx.clone(),
        shutdown_rx.clone(),
    );

    // Every producer above holds its own clone of `candidate_tx`; dropping
    // this one is what lets the channel close (and the media downloader
    // drain and stop) once all the producer tasks have actually ended.
    //
    // Before that, one startup catch-up: walk every watched account's feed
    // once, regardless of firehose health. The firehose resumes from its
    // persisted cursor, but Jetstream only replays a limited window —
    // anything posted during a longer outage would be silently skipped
    // forever, because the REST fallback only runs while the firehose is
    // *down*, not after a clean reconnect. The walk stops at each account's
    // dedup boundary, so a healthy archive costs one page fetch per account.
    let catch_up_accounts: Vec<String> = state
        .watchlist
        .snapshot()
        .iter()
        .filter(|source| source.kind == SourceKind::Account)
        .map(|source| source.value.clone())
        .collect();
    if !catch_up_accounts.is_empty() {
        let client = Arc::clone(&bluesky_client);
        let store = state.store.clone();
        let sender = candidate_tx.clone();
        tokio::spawn(async move {
            for account in catch_up_accounts {
                match crate::poller::backfill_account_once(&client, &store, &sender, &account).await
                {
                    Ok(new_count) => {
                        tracing::info!(
                            handle = %account,
                            new_count,
                            "startup catch-up walk complete"
                        );
                    }
                    Err(err) => {
                        tracing::warn!(
                            handle = %account,
                            error = %err,
                            "startup catch-up walk failed"
                        );
                    }
                }
            }
        });
    }

    drop(candidate_tx);

    let media_handle = tokio::spawn(run_media_downloader_supervised(
        candidate_rx,
        state.store.clone(),
        config.media_max_concurrent_downloads,
        config.media_max_bytes,
        Arc::clone(&request_limiter),
        health_tx,
        shutdown_rx.clone(),
    ));

    let web_handle = spawn_web_server(Arc::clone(&state), config.ui_port, shutdown_rx.clone());

    shutdown_signal().await;
    tracing::info!("shutdown signal received; stopping background tasks");
    let _ = shutdown_tx.send(true);

    let _ = tokio::join!(
        firehose_handle,
        rest_fallback_handle,
        feed_poller_handle,
        likes_bookmarks_handle,
        nightly_sweep_handle,
        web_handle
    );
    if let Some(tumblr_likes_handle) = tumblr_likes_handle {
        let _ = tumblr_likes_handle.await;
    }

    match tokio::time::timeout(Duration::from_secs(30), media_handle).await {
        Ok(_) => tracing::info!("media downloader drained cleanly"),
        Err(_) => {
            tracing::warn!("media downloader did not drain within the shutdown timeout")
        }
    }

    tracing::info!("shutdown complete");
}

#[allow(clippy::too_many_arguments)]
fn spawn_firehose(
    jetstream_url: Url,
    roster_rx: watch::Receiver<Vec<crate::storage::WatchedSource>>,
    cursor_path: PathBuf,
    candidate_tx: CandidatePostSender,
    conn_health_tx: ConnectionHealthSender,
    health_tx: HealthSender,
    shutdown_rx: watch::Receiver<bool>,
    store: ArchiveStore,
) -> tokio::task::JoinHandle<()> {
    // `FirehoseConsumer::run` needs its own clone of the shutdown receiver
    // on every (re)attempt; `supervise` below takes ownership of
    // `shutdown_rx` itself for its own restart loop, so `consumer_shutdown_rx`
    // is a separate clone captured by the `make` closure.
    let consumer_shutdown_rx = shutdown_rx.clone();
    tokio::spawn(supervise(
        "firehose",
        shutdown_rx,
        health_tx,
        |snapshot, health| snapshot.firehose = health,
        move || {
            let mut consumer = FirehoseConsumer::new(
                jetstream_url.clone(),
                cursor_path.clone(),
                candidate_tx.clone(),
                conn_health_tx.clone(),
                roster_rx.clone(),
                store.clone(),
            );
            let shutdown_rx = consumer_shutdown_rx.clone();
            async move { consumer.run(shutdown_rx).await }
        },
    ))
}

#[allow(clippy::too_many_arguments)]
fn spawn_rest_fallback(
    client: Arc<BlueskyClient>,
    store: ArchiveStore,
    candidate_tx: CandidatePostSender,
    conn_health_rx: ConnectionHealthReceiver,
    roster_rx: watch::Receiver<Vec<crate::storage::WatchedSource>>,
    poller_config: PollerConfig,
    health_tx: HealthSender,
    shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(supervise(
        "rest_fallback",
        shutdown_rx,
        health_tx,
        |snapshot, health| snapshot.rest_fallback = health,
        move || {
            let poller = RestFallbackPoller::new(
                Arc::clone(&client),
                store.clone(),
                candidate_tx.clone(),
                conn_health_rx.clone(),
                roster_rx.clone(),
                poller_config.clone(),
            );
            async move { poller.run().await }
        },
    ))
}

#[allow(clippy::too_many_arguments)]
fn spawn_feed_poller(
    client: Arc<BlueskyClient>,
    store: ArchiveStore,
    candidate_tx: CandidatePostSender,
    roster_rx: watch::Receiver<Vec<crate::storage::WatchedSource>>,
    poller_config: PollerConfig,
    health_tx: HealthSender,
    shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(supervise(
        "feed_poller",
        shutdown_rx,
        health_tx,
        |snapshot, health| snapshot.feed_poller = health,
        move || {
            let poller = FeedPoller::new(
                Arc::clone(&client),
                store.clone(),
                candidate_tx.clone(),
                roster_rx.clone(),
                poller_config.clone(),
            );
            async move { poller.run().await }
        },
    ))
}

fn spawn_likes_bookmarks(
    client: Arc<BlueskyClient>,
    store: ArchiveStore,
    candidate_tx: CandidatePostSender,
    actor: String,
    base_interval: Duration,
    health_tx: HealthSender,
    shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(supervise(
        "likes_bookmarks",
        shutdown_rx,
        health_tx,
        |snapshot, health| snapshot.likes_bookmarks = health,
        move || {
            let poller = LikesBookmarksPoller::new(
                Arc::clone(&client),
                store.clone(),
                candidate_tx.clone(),
                actor.clone(),
                base_interval,
            );
            async move { poller.run().await }
        },
    ))
}

/// Spawns the Tumblr likes poller under the same supervisor as every other
/// background task. Only called when Tumblr credentials are configured.
#[allow(clippy::too_many_arguments)]
fn spawn_tumblr_likes(
    client: Arc<TumblrClient>,
    store: ArchiveStore,
    candidate_tx: CandidatePostSender,
    base_interval: Duration,
    health_tx: HealthSender,
    shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(supervise(
        "tumblr_likes",
        shutdown_rx,
        health_tx,
        |snapshot, health| snapshot.tumblr_likes = health,
        move || {
            let poller = TumblrLikesPoller::new(
                Arc::clone(&client),
                store.clone(),
                candidate_tx.clone(),
                base_interval,
            );
            async move { poller.run().await }
        },
    ))
}

/// Spawns the nightly likes/bookmarks deletion sweeper under the same
/// supervisor as every other background task. Unlike the pollers it is
/// mostly idle (sleeping until its configured local hour), so its health
/// entry means "task alive and scheduled", not "actively sweeping".
fn spawn_nightly_sweep(
    client: Arc<BlueskyClient>,
    store: ArchiveStore,
    candidate_tx: CandidatePostSender,
    actor: String,
    local_hour: u32,
    health_tx: HealthSender,
    shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(supervise(
        "nightly_sweep",
        shutdown_rx,
        health_tx,
        |snapshot, health| snapshot.nightly_sweep = health,
        move || {
            let sweeper = NightlySweeper::new(
                Arc::clone(&client),
                store.clone(),
                candidate_tx.clone(),
                actor.clone(),
                local_hour,
            );
            async move { sweeper.run().await }
        },
    ))
}

/// Generic supervisor for a background task built from stateless (cheaply
/// re-constructible) inputs: spawns `make()`, and if it ever exits
/// (normally, which none of these are meant to do on their own, or via
/// panic) logs the fact and restarts it with exponential backoff, unless
/// `shutdown` has fired — in which case the current attempt is aborted and
/// awaited before returning.
async fn supervise<F, Fut>(
    label: &'static str,
    mut shutdown: watch::Receiver<bool>,
    health_tx: HealthSender,
    set_field: impl Fn(&mut HealthSnapshot, SubsystemHealth),
    mut make: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut attempt: u32 = 0;
    loop {
        if *shutdown.borrow() {
            return;
        }

        health_tx.send_modify(|s| set_field(s, SubsystemHealth::connected()));
        let mut task = tokio::spawn(make());

        tokio::select! {
            result = &mut task => {
                match result {
                    Ok(()) => {
                        tracing::warn!(subsystem = label, "task exited unexpectedly; restarting");
                    }
                    Err(err) => {
                        tracing::error!(subsystem = label, error = %err, "task panicked; restarting");
                    }
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    task.abort();
                    let _ = task.await;
                    return;
                }
            }
        }

        if *shutdown.borrow() {
            return;
        }

        attempt = attempt.saturating_add(1);
        health_tx
            .send_modify(|s| set_field(s, SubsystemHealth::degraded("restarting after failure")));
        let delay = restart_backoff(attempt);
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

/// Exponential backoff for subsystem restarts: 1s, 2s, 4s, ... capped at 60s.
fn restart_backoff(attempt: u32) -> Duration {
    let secs = 1u64 << attempt.min(6);
    Duration::from_secs(secs.min(60))
}

/// Drives the media downloader, owning the sole [`CandidatePostReceiver`]
/// for its entire lifetime so a panic inside one `MediaDownloader::run`
/// attempt doesn't lose the channel (and everything already queued on it)
/// the way a `tokio::spawn`-per-attempt restart would. Panics are caught
/// in-process via `catch_unwind` instead. Returns once the channel closes
/// (every producer's sender has dropped), which is how graceful shutdown
/// stops this task: no in-flight download or write is aborted, they're
/// simply allowed to finish.
async fn run_media_downloader_supervised(
    mut candidates: CandidatePostReceiver,
    store: ArchiveStore,
    max_concurrent_downloads: usize,
    max_bytes: u64,
    request_limiter: Arc<RequestLimiter>,
    health_tx: HealthSender,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut attempt: u32 = 0;
    loop {
        if *shutdown.borrow() {
            return;
        }

        health_tx.send_modify(|s| s.media_downloader = SubsystemHealth::connected());
        let downloader = MediaDownloader::new(store.clone(), max_concurrent_downloads, max_bytes)
            .with_request_limiter(Arc::clone(&request_limiter));
        let outcome = AssertUnwindSafe(downloader.run(&mut candidates))
            .catch_unwind()
            .await;

        match outcome {
            Ok(()) => return,
            Err(_panic) => {
                tracing::error!(subsystem = "media_downloader", "task panicked; restarting");
                health_tx.send_modify(|s| {
                    s.media_downloader = SubsystemHealth::degraded("restarting after panic")
                });
                attempt = attempt.saturating_add(1);
                let delay = restart_backoff(attempt);
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// Spawns the web UI's HTTP server on `port`, serving until `shutdown_rx`
/// fires. A bind failure (e.g. the port is already in use) is logged and
/// ends this task alone rather than the whole process, matching how the
/// other supervised subsystems degrade independently.
fn spawn_web_server(
    state: SharedAppState,
    port: u16,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => listener,
            Err(err) => {
                tracing::error!(error = %err, %port, "failed to bind web UI listener");
                return;
            }
        };
        tracing::info!(%addr, "web UI listening");

        let router = crate::web::router(state);
        let result = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.wait_for(|shutdown| *shutdown).await;
            })
            .await;
        if let Err(err) = result {
            tracing::error!(error = %err, "web UI server exited with error");
        }
    })
}

/// Resolves once SIGINT (Ctrl+C, all platforms) or SIGTERM (Unix only —
/// there is no such signal on Windows) is received.
///
/// If installing either OS signal handler itself fails (an unusual runtime
/// condition, not a startup-validation failure — e.g. another part of the
/// process already registered a conflicting handler), that path is logged
/// and treated as never resolving rather than panicking: the process stays
/// up and can still be stopped via the other signal (or a hard kill)
/// instead of crashing outright over a failure in the shutdown path itself.
async fn shutdown_signal() {
    let ctrl_c = async {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {}
            Err(err) => {
                tracing::error!(error = %err, "failed to install ctrl_c handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => {
                tracing::error!(error = %err, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Secret;
    use crate::pipeline::{CandidatePost, PostCategory, candidate_post_channel};
    use std::path::Path;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_config(archive_dir: PathBuf, database_path: PathBuf) -> AppConfig {
        AppConfig {
            bsky_identifier: "alice.bsky.social".to_string(),
            bsky_app_password: Secret::from("app-password".to_string()),
            tumblr: None,
            archive_dir,
            database_path,
            ui_password: Secret::from("ui-password".to_string()),
            ui_session_secret: Secret::from("session-secret".to_string()),
            ui_port: 8080,
            poll_interval_seconds: 120,
            jetstream_url: Url::parse("wss://jetstream.example.com/subscribe").unwrap(),
            media_max_concurrent_downloads: 4,
            media_max_bytes: 104_857_600,
            nightly_sweep_local_hour: 3,
            tumblr_poll_interval_seconds: 300,
        }
    }

    async fn mount_login(server: &MockServer, did: &str) {
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "accessJwt": "token-1",
                "refreshJwt": "refresh-1",
                "did": did,
                "handle": "alice.bsky.social",
            })))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn init_fails_fast_when_database_path_is_unwritable() {
        let archive_dir = tempfile::tempdir().expect("tempdir");
        let blocking_file = archive_dir.path().join("blocker");
        std::fs::write(&blocking_file, b"not a directory").unwrap();
        let database_path = blocking_file.join("index.sqlite3");

        let config = test_config(archive_dir.path().to_path_buf(), database_path);
        let base_url = Url::parse("http://127.0.0.1:1").unwrap();

        let err = match init(config, base_url).await {
            Err(err) => err,
            Ok(_) => panic!("unwritable database path should fail fast"),
        };
        assert!(matches!(err, StartupError::Storage(_)));
    }

    #[tokio::test]
    async fn init_fails_fast_on_bad_bluesky_credentials() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "error": "AuthenticationRequired",
                "message": "invalid credentials",
            })))
            .mount(&server)
            .await;

        let archive_dir = tempfile::tempdir().expect("tempdir");
        let database_path = archive_dir.path().join("index.sqlite3");
        let config = test_config(archive_dir.path().to_path_buf(), database_path);
        let base_url = Url::parse(&server.uri()).unwrap();

        let err = match init(config, base_url).await {
            Err(err) => err,
            Ok(_) => panic!("bad credentials should fail startup"),
        };
        assert!(matches!(err, StartupError::Bluesky(_)));
    }

    #[tokio::test]
    async fn init_starts_up_when_bluesky_rate_limits_authentication() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "30")
                    .set_body_json(serde_json::json!({
                        "error": "RateLimitExceeded",
                        "message": "Rate Limit Exceeded",
                    })),
            )
            .mount(&server)
            .await;

        let archive_dir = tempfile::tempdir().expect("tempdir");
        let database_path = archive_dir.path().join("index.sqlite3");
        let config = test_config(archive_dir.path().to_path_buf(), database_path);
        let base_url = Url::parse(&server.uri()).unwrap();

        let started = init(config, base_url)
            .await
            .expect("a rate-limited startup must still come up");
        assert!(
            started.self_did.is_none(),
            "no DID is known until the client authenticates lazily"
        );

        // The watch list is still seeded with the authenticated account; the
        // DID is simply left unresolved for now.
        let sources = started.state.watchlist.snapshot();
        assert_eq!(sources.len(), 1, "fresh install seeds exactly one account");
        assert_eq!(sources[0].kind, SourceKind::Account);
        assert_eq!(sources[0].value, "alice.bsky.social");
        assert_eq!(sources[0].did, None);
    }

    #[tokio::test]
    async fn backfill_missing_dids_resolves_and_reloads_the_roster() {
        let server = MockServer::start().await;
        // `resolveHandle` is authenticated, so a login is needed first.
        mount_login(&server, "did:plc:alice").await;
        Mock::given(method("GET"))
            .and(path("/xrpc/com.atproto.identity.resolveHandle"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "did": "did:plc:bob",
            })))
            .mount(&server)
            .await;

        let archive_dir = tempfile::tempdir().expect("tempdir");
        let database_path = archive_dir.path().join("index.sqlite3");
        let store = ArchiveStore::open(archive_dir.path().to_path_buf(), database_path)
            .await
            .expect("open store");
        store
            .add_watched_source(SourceKind::Account, "bob.bsky.social", None)
            .await
            .expect("add source");
        // A feed source (no DID by design) must be left alone.
        store
            .add_watched_source(
                SourceKind::Feed,
                "at://did:plc:alice/app.bsky.feed.generator/x",
                None,
            )
            .await
            .expect("add feed");
        let watchlist = Watchlist::new(store.list_watched_sources().await.unwrap());

        let client = Arc::new(BlueskyClient::new(
            Url::parse(&server.uri()).unwrap(),
            "alice.bsky.social".to_string(),
            Secret::from("app-password".to_string()),
        ));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        backfill_missing_dids(
            Arc::clone(&client),
            store.clone(),
            watchlist.clone(),
            shutdown_rx,
        )
        .await;

        let sources = store.list_watched_sources().await.unwrap();
        let account = sources
            .iter()
            .find(|s| s.kind == SourceKind::Account)
            .expect("account source");
        assert_eq!(account.did.as_deref(), Some("did:plc:bob"));
        // The feed source is untouched: it has no DID by design.
        let feed = sources
            .iter()
            .find(|s| s.kind == SourceKind::Feed)
            .expect("feed source");
        assert_eq!(feed.did, None);
        // The live roster was reloaded, so the firehose will see the DID.
        let restored = watchlist
            .snapshot()
            .into_iter()
            .find(|s| s.kind == SourceKind::Account)
            .expect("account source");
        assert_eq!(restored.did.as_deref(), Some("did:plc:bob"));
    }

    #[tokio::test]
    async fn backfill_missing_dids_gives_up_when_shutdown_fires() {
        let server = MockServer::start().await;
        mount_login(&server, "did:plc:alice").await;
        Mock::given(method("GET"))
            .and(path("/xrpc/com.atproto.identity.resolveHandle"))
            .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "600"))
            .mount(&server)
            .await;

        let archive_dir = tempfile::tempdir().expect("tempdir");
        let database_path = archive_dir.path().join("index.sqlite3");
        let store = ArchiveStore::open(archive_dir.path().to_path_buf(), database_path)
            .await
            .expect("open store");
        store
            .add_watched_source(SourceKind::Account, "bob.bsky.social", None)
            .await
            .expect("add source");
        let watchlist = Watchlist::new(store.list_watched_sources().await.unwrap());

        let client = Arc::new(BlueskyClient::new(
            Url::parse(&server.uri()).unwrap(),
            "alice.bsky.social".to_string(),
            Secret::from("app-password".to_string()),
        ));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // The resolve is rate-limited with a long hint, so the task parks in
        // its retry sleep; a shutdown must break out of it (rather than the
        // task hanging until the hint elapses) and leave the DID unresolved.
        let task = tokio::spawn(backfill_missing_dids(
            Arc::clone(&client),
            store.clone(),
            watchlist,
            shutdown_rx,
        ));
        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("backfill must stop promptly on shutdown")
            .expect("backfill task should not panic");

        let account = store
            .list_watched_sources()
            .await
            .unwrap()
            .into_iter()
            .find(|s| s.kind == SourceKind::Account)
            .expect("account source");
        assert_eq!(account.did, None);
    }

    #[tokio::test]
    async fn init_reindexes_from_disk_when_database_is_missing() {
        let server = MockServer::start().await;
        mount_login(&server, "did:plc:alice").await;

        let archive_dir = tempfile::tempdir().expect("tempdir");
        // Seed an archived post directly on disk, bypassing `ArchiveStore`,
        // simulating a prior run whose SQLite index was lost (deleted)
        // while the archive itself survived.
        let post_dir = archive_dir.path().join("posts").join("ab").join("cd1234");
        std::fs::create_dir_all(&post_dir).unwrap();
        std::fs::write(
            post_dir.join("record.json"),
            serde_json::to_vec(&serde_json::json!({
                "at_uri": "at://did:plc:alice/app.bsky.feed.post/1",
                "cid": "cid-1",
                "indexed_at": "2024-01-01T00:00:00Z",
                "media": [],
                "record": {"text": "hello"},
            }))
            .unwrap(),
        )
        .unwrap();

        let database_path = archive_dir.path().join("index.sqlite3");
        let config = test_config(archive_dir.path().to_path_buf(), database_path);
        let base_url = Url::parse(&server.uri()).unwrap();

        let started = init(config, base_url).await.expect("init should succeed");
        let page = started
            .state
            .store
            .list_posts(None, 1, 10, crate::storage::PostSort::default())
            .await
            .expect("list posts");
        assert_eq!(page.items.len(), 1);
        assert_eq!(
            page.items[0].at_uri,
            "at://did:plc:alice/app.bsky.feed.post/1"
        );
    }

    #[tokio::test]
    async fn init_seeds_the_watchlist_with_the_authenticated_account_when_empty() {
        let server = MockServer::start().await;
        mount_login(&server, "did:plc:alice").await;
        // No `resolveHandle` mock is registered: seeding must reuse the DID
        // from login rather than resolving the identifier over the API.

        let archive_dir = tempfile::tempdir().expect("tempdir");
        let database_path = archive_dir.path().join("index.sqlite3");
        let config = test_config(archive_dir.path().to_path_buf(), database_path);
        let base_url = Url::parse(&server.uri()).unwrap();

        let started = init(config, base_url).await.expect("init should succeed");
        assert_eq!(started.self_did.as_deref(), Some("did:plc:alice"));

        let sources = started.state.watchlist.snapshot();
        assert_eq!(sources.len(), 1, "fresh install seeds exactly one account");
        assert_eq!(sources[0].kind, SourceKind::Account);
        assert_eq!(sources[0].value, "alice.bsky.social");
        assert_eq!(sources[0].did.as_deref(), Some("did:plc:alice"));

        // The seed is durable, in the same table the web UI manages.
        let from_db = started
            .state
            .store
            .list_watched_sources()
            .await
            .expect("list watched sources");
        assert_eq!(from_db, sources);
    }

    #[tokio::test]
    async fn init_reloads_an_existing_watchlist_without_reseeding() {
        let server = MockServer::start().await;
        mount_login(&server, "did:plc:alice").await;

        let archive_dir = tempfile::tempdir().expect("tempdir");
        let database_path = archive_dir.path().join("index.sqlite3");
        let config = test_config(archive_dir.path().to_path_buf(), database_path);

        // Pre-populate the watch list (as a previous run would have, via the
        // web UI): an account that isn't the authenticated identity.
        let store = ArchiveStore::open(config.archive_dir.clone(), config.database_path.clone())
            .await
            .expect("open store");
        store
            .add_watched_source(SourceKind::Account, "bob.bsky.social", Some("did:plc:bob"))
            .await
            .expect("add source");
        drop(store);

        let base_url = Url::parse(&server.uri()).unwrap();
        let started = init(config, base_url).await.expect("init should succeed");
        let sources = started.state.watchlist.snapshot();
        assert_eq!(sources.len(), 1, "existing list must not be reseeded");
        assert_eq!(sources[0].value, "bob.bsky.social");
        assert_eq!(sources[0].did.as_deref(), Some("did:plc:bob"));
    }

    fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_files(&path, out);
            } else {
                out.push(path);
            }
        }
    }

    /// Demonstrates that the graceful-shutdown path for the media
    /// downloader — every producer's `CandidatePostSender` dropping, which
    /// closes the channel and lets `MediaDownloader::run` return on its
    /// own rather than being aborted mid-write — never leaves a torn or
    /// orphaned-temp-file write behind. `ArchiveStore::save_post` writes
    /// via write-temp-then-rename ([`crate::storage`]); this test checks
    /// that guarantee holds all the way through the shutdown path built
    /// here, not just in isolation.
    #[tokio::test]
    async fn media_downloader_drains_cleanly_and_writes_atomically_on_shutdown() {
        let archive_dir = tempfile::tempdir().expect("tempdir");
        let database_path = archive_dir.path().join("index.sqlite3");
        let store = ArchiveStore::open(archive_dir.path().to_path_buf(), database_path)
            .await
            .expect("open store");

        let (tx, rx) = candidate_post_channel(4);
        let (health_tx, _health_rx) = health::health_channel();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let candidate = CandidatePost {
            at_uri: "at://did:plc:alice/app.bsky.feed.post/1".to_string(),
            cid: "cid-1".to_string(),
            author_did: "did:plc:alice".to_string(),
            category: PostCategory::Authored,
            record: serde_json::json!({"text": "hello"}),
            media: vec![],
        };
        tx.send(candidate).await.expect("send candidate");
        // This is exactly what `serve`'s shutdown path does: once every
        // producer has stopped, its `candidate_tx` clone drops, and once
        // they're all gone the channel closes.
        drop(tx);

        run_media_downloader_supervised(
            rx,
            store.clone(),
            4,
            104_857_600,
            Arc::new(RequestLimiter::new(16)),
            health_tx,
            shutdown_rx,
        )
        .await;

        let page = store
            .list_posts(None, 1, 10, crate::storage::PostSort::default())
            .await
            .expect("list posts");
        assert_eq!(page.items.len(), 1);
        assert_eq!(
            page.items[0].at_uri,
            "at://did:plc:alice/app.bsky.feed.post/1"
        );

        let mut files = Vec::new();
        collect_files(archive_dir.path(), &mut files);
        let stray_tmp_files: Vec<_> = files
            .iter()
            .filter(|f| {
                f.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(".tmp-"))
            })
            .collect();
        assert!(
            stray_tmp_files.is_empty(),
            "found stray temp files: {stray_tmp_files:?}"
        );

        let record_files: Vec<_> = files
            .iter()
            .filter(|f| f.file_name().and_then(|n| n.to_str()) == Some("record.json"))
            .collect();
        assert_eq!(record_files.len(), 1);
        let bytes = std::fs::read(record_files[0]).unwrap();
        let _: serde_json::Value =
            serde_json::from_slice(&bytes).expect("record.json must be valid, complete JSON");
    }

    #[test]
    fn restart_backoff_grows_and_caps() {
        assert_eq!(restart_backoff(0), Duration::from_secs(1));
        assert_eq!(restart_backoff(1), Duration::from_secs(2));
        assert_eq!(restart_backoff(2), Duration::from_secs(4));
        assert_eq!(restart_backoff(10), Duration::from_secs(60));
    }
}
