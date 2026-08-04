//! Service orchestration: the startup sequence, the supervised background
//! tasks (firehose consumer, REST fallback poller, likes/bookmarks poller,
//! media downloader), and graceful shutdown.
//!
//! Split into [`init`] (build everything, fail fast on any error) and
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
use crate::config::{AppConfig, ConfigError, ResolvedFeed, feed_at_uri, feed_slug};
use crate::firehose::FirehoseConsumer;
use crate::health::{self, HealthSender, HealthSnapshot, SubsystemHealth};
use crate::media::MediaDownloader;
use crate::pipeline::{
    CandidatePostReceiver, CandidatePostSender, ConnectionHealth, ConnectionHealthReceiver,
    ConnectionHealthSender, candidate_post_channel, connection_health_channel,
};
use crate::poller::{FeedPoller, LikesBookmarksPoller, PollerConfig, RestFallbackPoller};
use crate::ratelimit::RequestLimiter;
use crate::state::{AppState, SharedAppState};
use crate::storage::{ArchiveStore, StorageError};

/// The production Bluesky XRPC entryway. Not part of the canonical env var
/// schema (there is no `BSKY_BASE_URL`); overridable only for tests, which
/// point [`init`] at a `wiremock` server instead.
pub const DEFAULT_BLUESKY_BASE_URL: &str = "https://bsky.social";

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
    #[error("feed configuration error: {0}")]
    Feed(String),
}

/// Everything [`serve`] needs to spawn the supervised background tasks,
/// produced by [`init`].
pub struct Started {
    pub state: SharedAppState,
    health_tx: HealthSender,
    bluesky_client: Arc<BlueskyClient>,
    self_did: String,
    watched_dids: Vec<String>,
    candidate_tx: CandidatePostSender,
    candidate_rx: CandidatePostReceiver,
    conn_health_tx: ConnectionHealthSender,
    conn_health_rx: ConnectionHealthReceiver,
    request_limiter: Arc<RequestLimiter>,
}

/// Runs the whole service: loads config from the environment, starts up
/// (failing fast on any error), then serves until a shutdown signal
/// arrives.
pub async fn run() -> Result<(), StartupError> {
    let config = AppConfig::from_env()?;
    let base_url = Url::parse(DEFAULT_BLUESKY_BASE_URL).expect("default bluesky base url is valid");
    let started = init(config, base_url).await?;
    serve(started).await;
    Ok(())
}

/// The startup sequence: open storage (self-healing the SQLite index if
/// it's missing), authenticate to Bluesky (failing fast on bad
/// credentials), and resolve every watched handle to a DID. Returns
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
    store
        .backfill_fts_index()
        .await
        .map_err(StartupError::from)?;

    let request_limiter = Arc::new(RequestLimiter::new(GLOBAL_INFLIGHT_REQUEST_CAP));
    let bluesky_client = Arc::new(
        BlueskyClient::new(
            bluesky_base_url,
            config.bsky_identifier.clone(),
            config.bsky_app_password.clone(),
        )
        .with_request_limiter(Arc::clone(&request_limiter)),
    );
    let self_did = bluesky_client
        .authenticate()
        .await
        .map_err(StartupError::from)?;
    tracing::info!(did = %self_did, "authenticated with bluesky");

    let mut watched_dids = Vec::with_capacity(config.bsky_watch_handles.len());
    for handle in &config.bsky_watch_handles {
        let did =
            resolve_watched_handle(&bluesky_client, &config.bsky_identifier, &self_did, handle)
                .await
                .map_err(StartupError::from)?;
        watched_dids.push(did);
    }
    tracing::info!(watched_dids = ?watched_dids, "resolved watched handles");

    let feeds = resolve_feeds(&bluesky_client, &config, &self_did).await?;
    if !feeds.is_empty() {
        tracing::info!(
            feeds = ?feeds.iter().map(|f| &f.at_uri).collect::<Vec<_>>(),
            "resolved custom feeds"
        );
    }

    let (health_tx, health_rx) = health::health_channel();
    let (conn_health_tx, conn_health_rx) = connection_health_channel(ConnectionHealth::Disabled);
    let (candidate_tx, candidate_rx) = candidate_post_channel(64);

    let state: SharedAppState = Arc::new(AppState {
        config,
        store,
        health: health_rx,
        feeds,
    });

    Ok(Started {
        state,
        health_tx,
        bluesky_client,
        self_did,
        watched_dids,
        candidate_tx,
        candidate_rx,
        conn_health_tx,
        conn_health_rx,
        request_limiter,
    })
}

/// Resolves one watched handle to a DID: reuses `self_did` when the handle
/// is the identity we just authenticated as, passes an already-DID value
/// straight through, and otherwise resolves it via the Bluesky API.
async fn resolve_watched_handle(
    client: &BlueskyClient,
    identifier: &str,
    self_did: &str,
    handle: &str,
) -> Result<String, BlueskyError> {
    if handle == identifier {
        Ok(self_did.to_string())
    } else if handle.starts_with("did:") {
        Ok(handle.to_string())
    } else {
        client.resolve_handle(handle).await
    }
}

/// Resolves every configured feed's handle segment to a DID, derives its
/// stable slug + canonical `at://` URI, and fetches its display name once
/// (best-effort). Fails fast — naming the offending entry — on an
/// unresolvable handle or on two entries resolving to the same feed.
async fn resolve_feeds(
    client: &BlueskyClient,
    config: &AppConfig,
    self_did: &str,
) -> Result<Vec<ResolvedFeed>, StartupError> {
    let mut feeds = Vec::with_capacity(config.watch_feeds.len());
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    for feed in &config.watch_feeds {
        let did = if feed.actor == config.bsky_identifier {
            self_did.to_string()
        } else if feed.actor.starts_with("did:") {
            feed.actor.clone()
        } else {
            client.resolve_handle(&feed.actor).await.map_err(|err| {
                StartupError::Feed(format!(
                    "could not resolve handle {:?} in feed entry {:?}: {err}",
                    feed.actor, feed.input
                ))
            })?
        };

        let at_uri = feed_at_uri(&did, &feed.rkey);
        let slug = feed_slug(&did, &feed.rkey);
        if !seen.insert(slug.clone()) {
            return Err(StartupError::Feed(format!(
                "feed entry {:?} resolves to the same feed ({at_uri}) as an earlier entry",
                feed.input
            )));
        }

        // Display name is cosmetic: a failure here must not fail startup.
        let display_name = match client.get_feed_generator(&at_uri).await {
            Ok(name) => name,
            Err(err) => {
                tracing::warn!(
                    feed = %at_uri,
                    error = %err,
                    "could not fetch feed generator display name; falling back to slug"
                );
                None
            }
        };

        feeds.push(ResolvedFeed {
            slug,
            at_uri,
            input: feed.input.clone(),
            display_name,
        });
    }

    Ok(feeds)
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
        watched_dids,
        candidate_tx,
        candidate_rx,
        conn_health_tx,
        conn_health_rx,
        request_limiter,
    } = started;

    let config = &state.config;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let cursor_path = config.archive_dir.join("jetstream_cursor");

    let firehose_handle = spawn_firehose(
        config.jetstream_url.clone(),
        watched_dids,
        cursor_path,
        candidate_tx.clone(),
        conn_health_tx,
        health_tx.clone(),
        shutdown_rx.clone(),
    );

    let rest_fallback_handle = spawn_rest_fallback(
        Arc::clone(&bluesky_client),
        state.store.clone(),
        candidate_tx.clone(),
        conn_health_rx,
        config.bsky_watch_handles.clone(),
        PollerConfig::new(Duration::from_secs(config.poll_interval_seconds)),
        health_tx.clone(),
        shutdown_rx.clone(),
    );

    let likes_bookmarks_handle = spawn_likes_bookmarks(
        Arc::clone(&bluesky_client),
        state.store.clone(),
        candidate_tx.clone(),
        self_did,
        Duration::from_secs(config.poll_interval_seconds),
        health_tx.clone(),
        shutdown_rx.clone(),
    );

    // The feed poller has no producer/channel role (it downloads inline, to
    // enforce each feed's byte cap), so it doesn't hold `candidate_tx`. It is
    // only spawned when feeds are configured; otherwise the map stays empty.
    let feeds_handle = if state.feeds.is_empty() {
        None
    } else {
        // Seed each feed's health entry so the dashboard/`/healthz` list them
        // as starting up before the first poll completes.
        let starting = SubsystemHealth::degraded("starting up");
        health_tx.send_modify(|snapshot| {
            for feed in &state.feeds {
                snapshot.feeds.insert(feed.slug.clone(), starting.clone());
            }
        });
        Some(spawn_feeds(
            Arc::clone(&bluesky_client),
            state.store.clone(),
            Arc::clone(&request_limiter),
            state.feeds.clone(),
            config.feed_max_bytes,
            config.media_max_bytes,
            PollerConfig::new(Duration::from_secs(config.poll_interval_seconds)),
            health_tx.clone(),
            shutdown_rx.clone(),
        ))
    };

    // Every producer above holds its own clone of `candidate_tx`; dropping
    // this one is what lets the channel close (and the media downloader
    // drain and stop) once all three producer tasks have actually ended.
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
        likes_bookmarks_handle,
        web_handle
    );
    if let Some(feeds_handle) = feeds_handle {
        let _ = feeds_handle.await;
    }

    match tokio::time::timeout(Duration::from_secs(30), media_handle).await {
        Ok(_) => tracing::info!("media downloader drained cleanly"),
        Err(_) => {
            tracing::warn!("media downloader did not drain within the shutdown timeout")
        }
    }

    tracing::info!("shutdown complete");
}

fn spawn_firehose(
    jetstream_url: Url,
    watched_dids: Vec<String>,
    cursor_path: PathBuf,
    candidate_tx: CandidatePostSender,
    conn_health_tx: ConnectionHealthSender,
    health_tx: HealthSender,
    shutdown_rx: watch::Receiver<bool>,
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
                watched_dids.clone(),
                cursor_path.clone(),
                candidate_tx.clone(),
                conn_health_tx.clone(),
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
    watch_handles: Vec<String>,
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
                watch_handles.clone(),
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

/// Spawns the supervised custom-feed poller. Uses the same generic
/// [`supervise`] restart loop as the other pollers, but with a no-op
/// `set_field`: the feed poller owns a *map* of per-feed health entries (one
/// per slug) that it updates itself, rather than a single fixed
/// [`HealthSnapshot`] field. Only ever called when at least one feed is
/// configured, so the inner task never returns on its own.
#[allow(clippy::too_many_arguments)]
fn spawn_feeds(
    client: Arc<BlueskyClient>,
    store: ArchiveStore,
    request_limiter: Arc<RequestLimiter>,
    feeds: Vec<ResolvedFeed>,
    feed_max_bytes: u64,
    media_max_bytes: u64,
    poller_config: PollerConfig,
    health_tx: HealthSender,
    shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(supervise(
        "feeds",
        shutdown_rx,
        health_tx.clone(),
        |_snapshot, _health| {},
        move || {
            let poller = FeedPoller::new(
                Arc::clone(&client),
                store.clone(),
                Some(Arc::clone(&request_limiter)),
                feeds.clone(),
                feed_max_bytes,
                media_max_bytes,
                health_tx.clone(),
                poller_config.clone(),
            );
            async move { poller.run().await }
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
            bsky_watch_handles: vec!["alice.bsky.social".to_string()],
            watch_feeds: Vec::new(),
            archive_dir,
            database_path,
            ui_password: Secret::from("ui-password".to_string()),
            ui_session_secret: Secret::from("session-secret".to_string()),
            ui_port: 8080,
            poll_interval_seconds: 120,
            jetstream_url: Url::parse("wss://jetstream.example.com/subscribe").unwrap(),
            media_max_concurrent_downloads: 4,
            media_max_bytes: 104_857_600,
            feed_max_bytes: 2_147_483_648,
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
    async fn init_fails_fast_when_watch_handle_cannot_be_resolved() {
        let server = MockServer::start().await;
        mount_login(&server, "did:plc:alice").await;
        Mock::given(method("GET"))
            .and(path("/xrpc/com.atproto.identity.resolveHandle"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "InvalidRequest",
                "message": "unable to resolve handle",
            })))
            .mount(&server)
            .await;

        let archive_dir = tempfile::tempdir().expect("tempdir");
        let database_path = archive_dir.path().join("index.sqlite3");
        let mut config = test_config(archive_dir.path().to_path_buf(), database_path);
        config
            .bsky_watch_handles
            .push("bob.bsky.social".to_string());
        let base_url = Url::parse(&server.uri()).unwrap();

        let err = match init(config, base_url).await {
            Err(err) => err,
            Ok(_) => panic!("unresolvable watch handle should fail startup"),
        };
        assert!(matches!(err, StartupError::Bluesky(_)));
    }

    #[tokio::test]
    async fn init_fails_fast_when_a_feed_handle_cannot_be_resolved() {
        let server = MockServer::start().await;
        mount_login(&server, "did:plc:alice").await;
        Mock::given(method("GET"))
            .and(path("/xrpc/com.atproto.identity.resolveHandle"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "InvalidRequest",
                "message": "unable to resolve handle",
            })))
            .mount(&server)
            .await;

        let archive_dir = tempfile::tempdir().expect("tempdir");
        let database_path = archive_dir.path().join("index.sqlite3");
        let mut config = test_config(archive_dir.path().to_path_buf(), database_path);
        config.watch_feeds = vec![crate::config::FeedConfig {
            input: "https://bsky.app/profile/nobody.bsky.social/feed/x".to_string(),
            actor: "nobody.bsky.social".to_string(),
            rkey: "x".to_string(),
        }];
        let base_url = Url::parse(&server.uri()).unwrap();

        let err = match init(config, base_url).await {
            Err(err) => err,
            Ok(_) => panic!("unresolvable feed handle should fail startup"),
        };
        match err {
            StartupError::Feed(message) => assert!(
                message.contains("nobody.bsky.social"),
                "error should name the offending entry: {message}"
            ),
            other => panic!("expected StartupError::Feed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn init_fails_fast_when_two_feeds_resolve_to_the_same_feed() {
        let server = MockServer::start().await;
        mount_login(&server, "did:plc:alice").await;
        // `getFeedGenerator` is best-effort (display name only); leaving it
        // unmocked (404) exercises the non-fatal fallback for the first entry.

        let archive_dir = tempfile::tempdir().expect("tempdir");
        let database_path = archive_dir.path().join("index.sqlite3");
        let mut config = test_config(archive_dir.path().to_path_buf(), database_path);
        // Two entries with the same DID + rkey (one as an at:// URI, one as a
        // bsky.app URL for the same DID) resolve to the same slug.
        config.watch_feeds = vec![
            crate::config::FeedConfig {
                input: "at://did:plc:dup/app.bsky.feed.generator/samerkey".to_string(),
                actor: "did:plc:dup".to_string(),
                rkey: "samerkey".to_string(),
            },
            crate::config::FeedConfig {
                input: "https://bsky.app/profile/did:plc:dup/feed/samerkey".to_string(),
                actor: "did:plc:dup".to_string(),
                rkey: "samerkey".to_string(),
            },
        ];
        let base_url = Url::parse(&server.uri()).unwrap();

        let err = match init(config, base_url).await {
            Err(err) => err,
            Ok(_) => panic!("duplicate feeds should fail startup"),
        };
        match err {
            StartupError::Feed(message) => assert!(
                message.contains("same feed"),
                "error should explain the duplicate: {message}"
            ),
            other => panic!("expected StartupError::Feed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn init_resolves_a_feed_with_display_name_and_stable_slug() {
        let server = MockServer::start().await;
        mount_login(&server, "did:plc:alice").await;
        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getFeedGenerator"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "view": {"displayName": "Cool Cats"},
            })))
            .mount(&server)
            .await;

        let archive_dir = tempfile::tempdir().expect("tempdir");
        let database_path = archive_dir.path().join("index.sqlite3");
        let mut config = test_config(archive_dir.path().to_path_buf(), database_path);
        config.watch_feeds = vec![crate::config::FeedConfig {
            input: "at://did:plc:feedgen/app.bsky.feed.generator/cats".to_string(),
            actor: "did:plc:feedgen".to_string(),
            rkey: "cats".to_string(),
        }];
        let base_url = Url::parse(&server.uri()).unwrap();

        let started = init(config, base_url).await.expect("init should succeed");
        assert_eq!(started.state.feeds.len(), 1);
        let feed = &started.state.feeds[0];
        assert_eq!(feed.display_name.as_deref(), Some("Cool Cats"));
        assert_eq!(
            feed.at_uri,
            "at://did:plc:feedgen/app.bsky.feed.generator/cats"
        );
        assert_eq!(feed.slug, feed_slug("did:plc:feedgen", "cats"));
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
            .list_posts(None, 1, 10)
            .await
            .expect("list posts");
        assert_eq!(page.items.len(), 1);
        assert_eq!(
            page.items[0].at_uri,
            "at://did:plc:alice/app.bsky.feed.post/1"
        );
    }

    #[tokio::test]
    async fn init_reuses_self_did_for_default_watch_handle() {
        let server = MockServer::start().await;
        mount_login(&server, "did:plc:alice").await;
        // No `resolveHandle` mock is registered: if `init` tried to resolve
        // `alice.bsky.social` (the default watch handle, equal to the
        // identifier) via the API instead of reusing the DID it already
        // has from login, this test would fail with a 404 from wiremock.

        let archive_dir = tempfile::tempdir().expect("tempdir");
        let database_path = archive_dir.path().join("index.sqlite3");
        let config = test_config(archive_dir.path().to_path_buf(), database_path);
        let base_url = Url::parse(&server.uri()).unwrap();

        let started = init(config, base_url).await.expect("init should succeed");
        assert_eq!(started.watched_dids, vec!["did:plc:alice".to_string()]);
        assert_eq!(started.self_did, "did:plc:alice");
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

        let page = store.list_posts(None, 1, 10).await.expect("list posts");
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
