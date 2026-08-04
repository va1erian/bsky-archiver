//! Jetstream websocket consumer: real-time capture of the watched accounts'
//! authored posts with media, filtered by DID.
//!
//! [`FirehoseConsumer`] connects to a Jetstream endpoint (filtered AT
//! Protocol firehose), subscribed to `app.bsky.feed.post` commits for the
//! watched accounts' DIDs, and turns qualifying create-commits into
//! [`crate::pipeline::CandidatePost`]s handed off through
//! [`crate::pipeline::candidate_post_channel`]. It is the sole producer of
//! [`ConnectionHealth`], which [`crate::poller`] (AR-6) reads to decide
//! whether the REST-polling fallback needs to be active.
//!
//! The watched-DID set comes from the live roster
//! ([`crate::watchlist::Watchlist`]) rather than a startup snapshot: the
//! consumer holds a `watch::Receiver` over the roster and, when a change
//! alters the watched accounts, closes its session and reconnects with the
//! new `wantedDids` set (cursor continuity across the reconnect is
//! acceptable). Feeds are never subscribed on the firehose.

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use serde::Deserialize;
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use url::Url;

use crate::pipeline::{
    CandidatePost, CandidatePostSender, ConnectionHealth, ConnectionHealthSender, MediaRef,
    PostCategory, has_archivable_media,
};
use crate::storage::{SourceKind, WatchedSource};

/// Errors that can end one connection attempt/session. Every variant is
/// recoverable by reconnecting; there is no fatal variant because a
/// misbehaving Jetstream instance should never stop the consumer for good.
#[derive(Debug, thiserror::Error)]
pub enum FirehoseError {
    #[error("websocket connect failed: {0}")]
    Connect(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("connection closed by peer")]
    ClosedByPeer,
}

/// Exponential-backoff-with-jitter parameters for reconnect attempts.
/// Configurable so tests can use short delays; production uses the
/// [`Default`] values.
#[derive(Debug, Clone, Copy)]
pub struct BackoffConfig {
    pub initial: Duration,
    pub max: Duration,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        BackoffConfig {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(60),
        }
    }
}

impl BackoffConfig {
    /// The jittered delay before reconnect attempt number `attempt` (0-based
    /// counting from the first failure). Doubles `initial` per attempt, capped
    /// at `max`, then jitters uniformly within 50%-100% of that value so many
    /// disconnected clients don't reconnect in lockstep.
    fn delay_for(&self, attempt: u32) -> Duration {
        let doublings = attempt.min(32);
        let scaled = self
            .initial
            .saturating_mul(1u32.checked_shl(doublings).unwrap_or(u32::MAX));
        let capped = scaled.min(self.max);
        let jitter_floor = capped / 2;
        let extra = capped.saturating_sub(jitter_floor);
        if extra.is_zero() {
            return capped;
        }
        let extra_nanos = extra.as_nanos().min(u64::MAX as u128) as u64;
        let jitter_nanos = rand::thread_rng().gen_range(0..=extra_nanos);
        jitter_floor + Duration::from_nanos(jitter_nanos)
    }
}

/// Durable storage for the last-processed Jetstream cursor (a microsecond
/// unix timestamp), so a restart resumes from roughly where it left off
/// instead of silently missing posts made while the process was down.
/// Backed by a single small file rather than the SQLite index, since it's
/// pure last-write-wins scalar state with no query needs.
#[derive(Debug, Clone)]
pub struct CursorStore {
    path: PathBuf,
}

impl CursorStore {
    pub fn new(path: PathBuf) -> Self {
        CursorStore { path }
    }

    /// Reads the last-persisted cursor, if any. Returns `None` (rather than
    /// erroring) when the file is absent or unparsable, since "no cursor
    /// yet" and "cursor file corrupted" both just mean "start from Jetstream's
    /// own default replay window".
    pub fn load(&self) -> Option<i64> {
        std::fs::read_to_string(&self.path)
            .ok()
            .and_then(|s| s.trim().parse().ok())
    }

    /// Persists `cursor`, atomically (write-to-temp then rename) so a crash
    /// mid-write never leaves a corrupt cursor file behind.
    pub fn save(&self, cursor: i64) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp_path = self.path.with_extension("tmp");
        std::fs::write(&tmp_path, cursor.to_string())?;
        std::fs::rename(&tmp_path, &self.path)
    }
}

/// The subset of a Jetstream event frame this consumer cares about. Unknown
/// fields and unknown `kind`/`collection` values are ignored, not errors:
/// Jetstream may add event kinds we don't handle yet.
#[derive(Debug, Deserialize)]
struct JetstreamEvent {
    did: String,
    time_us: i64,
    kind: String,
    #[serde(default)]
    commit: Option<JetstreamCommit>,
}

#[derive(Debug, Deserialize)]
struct JetstreamCommit {
    rev: String,
    operation: String,
    collection: String,
    rkey: String,
    #[serde(default)]
    record: Option<Value>,
    #[serde(default)]
    cid: Option<String>,
}

const WANTED_COLLECTION: &str = "app.bsky.feed.post";

fn build_subscribe_url(base: &Url, watched_dids: &[String], cursor: Option<i64>) -> Url {
    let mut url = base.clone();
    {
        let mut pairs = url.query_pairs_mut();
        pairs.clear();
        pairs.append_pair("wantedCollections", WANTED_COLLECTION);
        for did in watched_dids {
            pairs.append_pair("wantedDids", did);
        }
        if let Some(cursor) = cursor {
            pairs.append_pair("cursor", &cursor.to_string());
        }
    }
    url
}

/// Connects to `JETSTREAM_URL`, filtered to `app.bsky.feed.post` commits for
/// the given (already DID-resolved) watched accounts, and forwards
/// archive-worthy posts as [`CandidatePost`]s. Reconnects forever (until
/// `shutdown` fires) with jittered exponential backoff, and is the sole
/// producer of the [`ConnectionHealth`] signal AR-6 reads.
pub struct FirehoseConsumer {
    jetstream_url: Url,
    /// Live view of the watch-list roster, from which the account DIDs are
    /// (re)computed. The consumer holds a `watch::Receiver` so a roster
    /// reload both updates the wanted-DID set and wakes the read loop.
    roster_rx: watch::Receiver<Vec<WatchedSource>>,
    /// The account DID set the most recent session subscribed to, so a
    /// roster change that doesn't actually alter the watched accounts (e.g.
    /// a feed being added) can be ignored instead of forcing a pointless
    /// reconnect.
    connected_dids: Option<HashSet<String>>,
    /// Whether `roster_rx` can still deliver changes. Guarded separately so
    /// a closed channel (the watchlist is gone) can't spin read_loop's
    /// select into an infinite immediate-reconnect loop.
    roster_alive: bool,
    cursor_store: CursorStore,
    candidate_tx: CandidatePostSender,
    health_tx: ConnectionHealthSender,
    backoff: BackoffConfig,
}

impl FirehoseConsumer {
    /// Builds a consumer whose wanted-DID set is derived live from
    /// `roster_rx` (see [`crate::watchlist::Watchlist::subscribe`]).
    pub fn new(
        jetstream_url: Url,
        cursor_path: PathBuf,
        candidate_tx: CandidatePostSender,
        health_tx: ConnectionHealthSender,
        roster_rx: watch::Receiver<Vec<WatchedSource>>,
    ) -> Self {
        FirehoseConsumer {
            jetstream_url,
            roster_rx,
            connected_dids: None,
            roster_alive: true,
            cursor_store: CursorStore::new(cursor_path),
            candidate_tx,
            health_tx,
            backoff: BackoffConfig::default(),
        }
    }

    /// Overrides the reconnect backoff parameters. Exposed for tests, which
    /// need reconnects to happen fast rather than waiting out production
    /// delays.
    #[allow(dead_code)]
    pub fn with_backoff(mut self, backoff: BackoffConfig) -> Self {
        self.backoff = backoff;
        self
    }

    /// Runs the connect/consume/reconnect loop until `shutdown` is set to
    /// `true`. Never returns an error: every failure is logged and answered
    /// with a backoff-then-reconnect rather than propagated, since a
    /// disconnect is an expected, routine event for this consumer.
    pub async fn run(&mut self, mut shutdown: watch::Receiver<bool>) {
        let mut attempt: u32 = 0;

        loop {
            if *shutdown.borrow() {
                return;
            }

            match self.connect_and_process(&mut shutdown).await {
                Ok(()) => {
                    if *shutdown.borrow() {
                        return;
                    }
                    attempt = 0;
                }
                Err(err) => {
                    tracing::warn!(error = %err, "firehose connection lost");
                }
            }

            let since = Instant::now();
            let _ = self
                .health_tx
                .send(ConnectionHealth::Reconnecting { since });

            let delay = self.backoff.delay_for(attempt);
            attempt = attempt.saturating_add(1);
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

    async fn connect_and_process(
        &mut self,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<(), FirehoseError> {
        let watched_dids = self.watched_dids();
        self.connected_dids = Some(watched_dids.iter().cloned().collect());
        let cursor = self.cursor_store.load();
        let subscribe_url = build_subscribe_url(&self.jetstream_url, &watched_dids, cursor);

        tracing::info!(url = %subscribe_url, cursor = ?cursor, "connecting to jetstream");
        let (ws_stream, _response) = connect_async(subscribe_url.as_str()).await?;
        let _ = self.health_tx.send(ConnectionHealth::Connected);
        tracing::info!("jetstream connected");

        self.read_loop(ws_stream, shutdown).await
    }

    /// The currently watched account DIDs, derived live from the roster.
    /// Feeds are deliberately excluded: the firehose only subscribes to
    /// accounts.
    fn watched_dids(&self) -> Vec<String> {
        self.roster_rx
            .borrow()
            .iter()
            .filter(|source| source.kind == SourceKind::Account)
            .map(|source| source.did.clone().unwrap_or_else(|| source.value.clone()))
            .collect()
    }

    /// Whether the current roster's account DID set differs from the set the
    /// last session subscribed to (i.e. a live reload actually changed the
    /// firehose filter).
    fn dids_changed(&self) -> bool {
        let current: HashSet<String> = self.watched_dids().into_iter().collect();
        match &self.connected_dids {
            Some(connected) => connected != &current,
            None => !current.is_empty(),
        }
    }

    async fn read_loop(
        &mut self,
        mut ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<(), FirehoseError> {
        loop {
            tokio::select! {
                message = ws_stream.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) => {
                            self.handle_frame(&text).await;
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            let _ = ws_stream.send(Message::Pong(payload)).await;
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            return Err(FirehoseError::ClosedByPeer);
                        }
                        Some(Ok(_)) => {
                            // Binary/Pong/Frame: Jetstream only sends text
                            // frames for events; anything else is ignored.
                        }
                        Some(Err(err)) => {
                            return Err(FirehoseError::Connect(err));
                        }
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        let _ = ws_stream.close(None).await;
                        return Ok(());
                    }
                }
                changed = self.roster_rx.changed(), if self.roster_alive => {
                    match changed {
                        Err(_) => {
                            // The roster watchlist is gone; stop watching for
                            // further changes rather than spinning on a closed
                            // channel.
                            self.roster_alive = false;
                            continue;
                        }
                        Ok(()) => {
                            // Mark the change as seen so `changed()` only
                            // fires for genuinely new reloads.
                            let _ = self.roster_rx.borrow_and_update();
                            if self.dids_changed() {
                                tracing::info!("watched accounts changed; reconnecting with the new wantedDids");
                                let _ = ws_stream.close(None).await;
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
    }

    async fn handle_frame(&mut self, text: &str) {
        let event: JetstreamEvent = match serde_json::from_str(text) {
            Ok(event) => event,
            Err(err) => {
                tracing::warn!(error = %err, "failed to parse jetstream frame, skipping");
                return;
            }
        };

        if event.kind != "commit" {
            return;
        }
        let Some(connected_dids) = &self.connected_dids else {
            return;
        };
        if !connected_dids.contains(&event.did) {
            return;
        }
        let Some(commit) = event.commit else {
            return;
        };
        if commit.operation != "create" || commit.collection != WANTED_COLLECTION {
            return;
        }
        let Some(record) = commit.record else {
            return;
        };

        if has_archivable_media(&record) {
            let media = extract_media(&record, &event.did);
            let candidate = CandidatePost {
                at_uri: format!("at://{}/{}/{}", event.did, commit.collection, commit.rkey),
                cid: commit.cid.unwrap_or_default(),
                author_did: event.did.clone(),
                category: PostCategory::Authored,
                record,
                media,
            };
            if self.candidate_tx.send(candidate).await.is_err() {
                tracing::warn!("candidate post channel closed; dropping event");
            }
        }

        let _ = commit.rev;
        if let Err(err) = self.cursor_store.save(event.time_us) {
            tracing::warn!(error = %err, "failed to persist jetstream cursor");
        }
    }
}

/// Extracts [`MediaRef`]s from a post record's `embed`, mirroring
/// [`has_archivable_media`]'s recognized shapes (direct image/video embeds,
/// and media nested in `recordWithMedia`).
fn extract_media(record: &Value, author_did: &str) -> Vec<MediaRef> {
    record
        .get("embed")
        .map(|embed| media_from_embed(embed, author_did))
        .unwrap_or_default()
}

fn media_from_embed(embed: &Value, author_did: &str) -> Vec<MediaRef> {
    let Some(embed_type) = embed.get("$type").and_then(|v| v.as_str()) else {
        return Vec::new();
    };

    match embed_type {
        "app.bsky.embed.images" => embed
            .get("images")
            .and_then(|v| v.as_array())
            .map(|images| {
                images
                    .iter()
                    .filter_map(|image| image.get("image"))
                    .filter_map(|blob| image_media_ref(blob, author_did))
                    .collect()
            })
            .unwrap_or_default(),
        "app.bsky.embed.video" => embed
            .get("video")
            .and_then(|blob| video_media_ref(blob, author_did))
            .into_iter()
            .collect(),
        "app.bsky.embed.recordWithMedia" => embed
            .get("media")
            .map(|media| media_from_embed(media, author_did))
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn blob_cid(blob: &Value) -> Option<String> {
    blob.get("ref")
        .and_then(|r| r.get("$link"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

fn image_media_ref(blob: &Value, author_did: &str) -> Option<MediaRef> {
    let cid = blob_cid(blob)?;
    let mime = blob
        .get("mimeType")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let size = blob.get("size").and_then(|v| v.as_u64());
    let ext = mime
        .as_deref()
        .and_then(image_extension_for_mime)
        .unwrap_or("jpeg");
    Some(MediaRef {
        cdn_url: format!("https://cdn.bsky.app/img/feed_fullsize/plain/{author_did}/{cid}@{ext}"),
        declared_mime_type: mime,
        declared_size_bytes: size,
    })
}

fn video_media_ref(blob: &Value, author_did: &str) -> Option<MediaRef> {
    let cid = blob_cid(blob)?;
    let mime = blob
        .get("mimeType")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let size = blob.get("size").and_then(|v| v.as_u64());
    Some(MediaRef {
        cdn_url: format!("https://video.bsky.app/watch/{author_did}/{cid}/playlist.m3u8"),
        declared_mime_type: mime,
        declared_size_bytes: size,
    })
}

fn image_extension_for_mime(mime: &str) -> Option<&'static str> {
    match mime {
        "image/jpeg" => Some("jpeg"),
        "image/png" => Some("png"),
        "image/webp" => Some("webp"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::candidate_post_channel;
    use crate::pipeline::connection_health_channel;
    use serde_json::json;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::sync::Mutex as AsyncMutex;

    fn image_post(did: &str) -> Value {
        json!({
            "did": did,
            "time_us": 1_700_000_000_000_000i64,
            "kind": "commit",
            "commit": {
                "rev": "rev1",
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": "abc123",
                "cid": "cid-1",
                "record": {
                    "text": "look at this",
                    "embed": {
                        "$type": "app.bsky.embed.images",
                        "images": [{
                            "alt": "a cat",
                            "image": {
                                "$type": "blob",
                                "ref": {"$link": "bafyimg1"},
                                "mimeType": "image/jpeg",
                                "size": 1234
                            }
                        }]
                    }
                }
            }
        })
    }

    fn video_post(did: &str) -> Value {
        json!({
            "did": did,
            "time_us": 1_700_000_000_100_000i64,
            "kind": "commit",
            "commit": {
                "rev": "rev2",
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": "vid123",
                "cid": "cid-2",
                "record": {
                    "text": "watch this",
                    "embed": {
                        "$type": "app.bsky.embed.video",
                        "video": {
                            "$type": "blob",
                            "ref": {"$link": "bafyvid1"},
                            "mimeType": "video/mp4",
                            "size": 99999
                        }
                    }
                }
            }
        })
    }

    fn text_only_post(did: &str) -> Value {
        json!({
            "did": did,
            "time_us": 1_700_000_000_200_000i64,
            "kind": "commit",
            "commit": {
                "rev": "rev3",
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": "txt123",
                "cid": "cid-3",
                "record": {"text": "just words"}
            }
        })
    }

    /// A minimal mock Jetstream server. `scripts` is a queue of per-connection
    /// event sequences: each accepted connection sends the next entry's
    /// frames in order, then closes.
    struct MockServer {
        addr: SocketAddr,
    }

    /// Records every subscribe URL (path + query string) the client used to
    /// connect, in connection order.
    async fn spawn_mock_server(
        scripts: Vec<Vec<Value>>,
    ) -> (MockServer, Arc<AsyncMutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let seen_urls: Arc<AsyncMutex<Vec<String>>> = Arc::new(AsyncMutex::new(Vec::new()));
        let seen_urls_task = Arc::clone(&seen_urls);

        tokio::spawn(async move {
            let mut scripts = scripts.into_iter();
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let Some(script) = scripts.next() else {
                    // No more scripted sessions: accept and immediately
                    // drop so the client's connect doesn't hang forever.
                    drop(stream);
                    continue;
                };
                let seen_urls_conn = Arc::clone(&seen_urls_task);
                tokio::spawn(async move {
                    handle_connection(stream, script, seen_urls_conn).await;
                });
            }
        });

        (MockServer { addr }, seen_urls)
    }

    #[allow(clippy::result_large_err)]
    async fn handle_connection(
        stream: TcpStream,
        script: Vec<Value>,
        seen_urls: Arc<AsyncMutex<Vec<String>>>,
    ) {
        let mut request_path = String::new();
        let callback =
            |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
             response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                request_path = req.uri().to_string();
                Ok(response)
            };
        let ws_stream = match tokio_tungstenite::accept_hdr_async(stream, callback).await {
            Ok(ws) => ws,
            Err(_) => return,
        };
        seen_urls.lock().await.push(request_path);

        let (mut write, _read) = ws_stream.split();
        // An empty script means "connect and stay idle": used to hold a
        // session open across an externally-triggered roster change/reconnect
        // without any event traffic.
        if script.is_empty() {
            std::future::pending::<()>().await;
            return;
        }
        for event in script {
            let text = serde_json::to_string(&event).expect("serialize event");
            if write.send(Message::Text(text)).await.is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = write.close().await;
    }

    fn watched_source(id: i64, did: &str) -> WatchedSource {
        WatchedSource {
            id,
            kind: SourceKind::Account,
            value: did.to_string(),
            did: Some(did.to_string()),
            added_at: "2024-01-01T00:00:00Z".to_string(),
        }
    }

    /// A live roster receiver seeded with one account per DID; the matching
    /// `watch::Sender` is dropped, which is exactly what tests that never
    /// change the roster want (the closed channel is handled gracefully).
    fn account_roster(dids: &[&str]) -> watch::Receiver<Vec<WatchedSource>> {
        let sources = dids
            .iter()
            .enumerate()
            .map(|(i, did)| watched_source(i as i64 + 1, did))
            .collect();
        let (_tx, rx) = watch::channel(sources);
        rx
    }

    fn test_backoff() -> BackoffConfig {
        BackoffConfig {
            initial: Duration::from_millis(5),
            max: Duration::from_millis(50),
        }
    }

    #[tokio::test]
    async fn matching_media_post_is_forwarded_as_candidate() {
        let did = "did:plc:alice";
        let (_server, _seen) = spawn_mock_server(vec![vec![image_post(did)]]).await;
        let addr = _server.addr;

        let url = Url::parse(&format!("ws://{addr}/subscribe")).unwrap();
        let (candidate_tx, mut candidate_rx) = candidate_post_channel(8);
        let (health_tx, _health_rx) = connection_health_channel(ConnectionHealth::Disabled);
        let cursor_path = std::env::temp_dir().join(format!("bsky-firehose-cursor-{}", uuid()));

        let mut consumer = FirehoseConsumer::new(
            url,
            cursor_path.clone(),
            candidate_tx,
            health_tx,
            account_roster(&[did]),
        )
        .with_backoff(test_backoff());

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            consumer.run(shutdown_rx).await;
        });

        let candidate = tokio::time::timeout(Duration::from_secs(2), candidate_rx.recv())
            .await
            .expect("candidate received in time")
            .expect("channel open");
        assert_eq!(candidate.author_did, did);
        assert_eq!(candidate.media.len(), 1);
        assert!(candidate.media[0].cdn_url.contains("bafyimg1"));

        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        let _ = std::fs::remove_file(&cursor_path);
    }

    #[tokio::test]
    async fn video_post_is_detected_as_media() {
        let did = "did:plc:bob";
        let (_server, _seen) = spawn_mock_server(vec![vec![video_post(did)]]).await;
        let addr = _server.addr;

        let url = Url::parse(&format!("ws://{addr}/subscribe")).unwrap();
        let (candidate_tx, mut candidate_rx) = candidate_post_channel(8);
        let (health_tx, _health_rx) = connection_health_channel(ConnectionHealth::Disabled);
        let cursor_path = std::env::temp_dir().join(format!("bsky-firehose-cursor-{}", uuid()));

        let mut consumer = FirehoseConsumer::new(
            url,
            cursor_path.clone(),
            candidate_tx,
            health_tx,
            account_roster(&[did]),
        )
        .with_backoff(test_backoff());

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            consumer.run(shutdown_rx).await;
        });

        let candidate = tokio::time::timeout(Duration::from_secs(2), candidate_rx.recv())
            .await
            .expect("candidate received in time")
            .expect("channel open");
        assert!(candidate.media[0].cdn_url.contains("playlist.m3u8"));

        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        let _ = std::fs::remove_file(&cursor_path);
    }

    #[tokio::test]
    async fn non_matching_did_and_text_only_posts_are_ignored() {
        let did = "did:plc:alice";
        let other_did = "did:plc:mallory";
        let (_server, _seen) = spawn_mock_server(vec![vec![
            image_post(other_did),
            text_only_post(did),
            image_post(did),
        ]])
        .await;
        let addr = _server.addr;

        let url = Url::parse(&format!("ws://{addr}/subscribe")).unwrap();
        let (candidate_tx, mut candidate_rx) = candidate_post_channel(8);
        let (health_tx, _health_rx) = connection_health_channel(ConnectionHealth::Disabled);
        let cursor_path = std::env::temp_dir().join(format!("bsky-firehose-cursor-{}", uuid()));

        let mut consumer = FirehoseConsumer::new(
            url,
            cursor_path.clone(),
            candidate_tx,
            health_tx,
            account_roster(&[did]),
        )
        .with_backoff(test_backoff());

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            consumer.run(shutdown_rx).await;
        });

        let candidate = tokio::time::timeout(Duration::from_secs(2), candidate_rx.recv())
            .await
            .expect("candidate received in time")
            .expect("channel open");
        // Only the third event (matching DID + media) should ever surface.
        assert_eq!(candidate.author_did, did);
        assert_eq!(candidate.cid, "cid-1");

        // Give a little time for any spurious extra sends; there should be
        // none, since the other two events don't qualify.
        let extra = tokio::time::timeout(Duration::from_millis(200), candidate_rx.recv()).await;
        assert!(extra.is_err(), "no further candidates should be forwarded");

        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        let _ = std::fs::remove_file(&cursor_path);
    }

    #[tokio::test]
    async fn reconnects_after_drop_and_resumes_from_last_cursor() {
        let did = "did:plc:alice";
        let (_server, seen_urls) =
            spawn_mock_server(vec![vec![image_post(did)], vec![image_post(did)]]).await;
        let addr = _server.addr;

        let url = Url::parse(&format!("ws://{addr}/subscribe")).unwrap();
        let (candidate_tx, mut candidate_rx) = candidate_post_channel(8);
        let (health_tx, mut health_rx) = connection_health_channel(ConnectionHealth::Disabled);
        let cursor_path = std::env::temp_dir().join(format!("bsky-firehose-cursor-{}", uuid()));

        let mut consumer = FirehoseConsumer::new(
            url,
            cursor_path.clone(),
            candidate_tx,
            health_tx,
            account_roster(&[did]),
        )
        .with_backoff(test_backoff());

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            consumer.run(shutdown_rx).await;
        });

        // First connection: Connected, then delivers one candidate, then the
        // mock server closes it.
        health_rx.changed().await.unwrap();
        assert_eq!(*health_rx.borrow(), ConnectionHealth::Connected);
        let _first = tokio::time::timeout(Duration::from_secs(2), candidate_rx.recv())
            .await
            .expect("first candidate")
            .expect("channel open");

        // Health should flip to Reconnecting once the server drops us.
        loop {
            health_rx.changed().await.unwrap();
            if matches!(*health_rx.borrow(), ConnectionHealth::Reconnecting { .. }) {
                break;
            }
        }

        // Then back to Connected on the second (reconnected) session, which
        // delivers the second scripted candidate.
        loop {
            health_rx.changed().await.unwrap();
            if matches!(*health_rx.borrow(), ConnectionHealth::Connected) {
                break;
            }
        }
        let _second = tokio::time::timeout(Duration::from_secs(2), candidate_rx.recv())
            .await
            .expect("second candidate")
            .expect("channel open");

        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;

        let urls = seen_urls.lock().await.clone();
        assert_eq!(urls.len(), 2, "expected two connection attempts: {urls:?}");
        assert!(
            !urls[0].contains("cursor="),
            "first connection should have no persisted cursor yet: {}",
            urls[0]
        );
        assert!(
            urls[1].contains("cursor=1700000000000000"),
            "reconnect should resume from the last-seen cursor: {}",
            urls[1]
        );

        let _ = std::fs::remove_file(&cursor_path);
    }

    #[tokio::test]
    async fn roster_change_that_alters_watched_dids_reconnects_with_the_new_set() {
        let did = "did:plc:alice";
        let added_did = "did:plc:bob";
        let (_server, seen_urls) = spawn_mock_server(vec![vec![], vec![]]).await;
        let addr = _server.addr;

        let url = Url::parse(&format!("ws://{addr}/subscribe")).unwrap();
        let (candidate_tx, _candidate_rx) = candidate_post_channel(8);
        let (health_tx, _health_rx) = connection_health_channel(ConnectionHealth::Disabled);
        let cursor_path = std::env::temp_dir().join(format!("bsky-firehose-cursor-{}", uuid()));

        let (roster_tx, roster_rx) = watch::channel(vec![watched_source(1, did)]);
        let mut consumer =
            FirehoseConsumer::new(url, cursor_path.clone(), candidate_tx, health_tx, roster_rx)
                .with_backoff(test_backoff());

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            consumer.run(shutdown_rx).await;
        });

        // First session: stays idle (empty script) until we act.
        tokio::time::timeout(Duration::from_secs(2), async {
            while seen_urls.lock().await.is_empty() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("first connection made");

        // The UI adds an account: the roster reload must close the idle
        // session and reconnect with both DIDs in wantedDids.
        // `send_replace` returns the previous value; the replacement is the
        // only thing that matters here.
        roster_tx.send_replace(vec![watched_source(1, did), watched_source(2, added_did)]);

        tokio::time::timeout(Duration::from_secs(2), async {
            while seen_urls.lock().await.len() < 2 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("second connection made");
        let urls = seen_urls.lock().await.clone();
        assert!(
            urls[1].contains("wantedDids=did%3Aplc%3Aalice"),
            "{}",
            urls[1]
        );
        assert!(
            urls[1].contains("wantedDids=did%3Aplc%3Abob"),
            "{}",
            urls[1]
        );

        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        let _ = std::fs::remove_file(&cursor_path);
    }

    #[tokio::test]
    async fn roster_change_that_leaves_watched_dids_unchanged_does_not_reconnect() {
        let did = "did:plc:alice";
        let (_server, seen_urls) = spawn_mock_server(vec![vec![]]).await;
        let addr = _server.addr;

        let url = Url::parse(&format!("ws://{addr}/subscribe")).unwrap();
        let (candidate_tx, _candidate_rx) = candidate_post_channel(8);
        let (health_tx, _health_rx) = connection_health_channel(ConnectionHealth::Disabled);
        let cursor_path = std::env::temp_dir().join(format!("bsky-firehose-cursor-{}", uuid()));

        let (roster_tx, roster_rx) = watch::channel(vec![watched_source(1, did)]);
        let mut consumer =
            FirehoseConsumer::new(url, cursor_path.clone(), candidate_tx, health_tx, roster_rx)
                .with_backoff(test_backoff());

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            consumer.run(shutdown_rx).await;
        });

        tokio::time::timeout(Duration::from_secs(2), async {
            while seen_urls.lock().await.is_empty() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("first connection made");

        // Adding a *feed* reloads the roster but leaves the account DIDs
        // untouched: the firehose must keep its session rather than
        // reconnecting (note the mock has only one scripted connection).
        roster_tx.send_replace(vec![
            watched_source(1, did),
            WatchedSource {
                id: 2,
                kind: SourceKind::Feed,
                value: "at://did:plc:alice/app.bsky.feed.generator/whats-hot".to_string(),
                did: None,
                added_at: "2024-01-01T00:00:00Z".to_string(),
            },
        ]);

        // Give any (buggy) reconnect a real chance to happen; only one
        // connection may ever be made.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            seen_urls.lock().await.len(),
            1,
            "feed-only roster change must not reconnect the firehose"
        );

        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        let _ = std::fs::remove_file(&cursor_path);
    }

    #[tokio::test]
    async fn cursor_store_round_trips() {
        let path = std::env::temp_dir().join(format!("bsky-firehose-cursor-store-{}", uuid()));
        let store = CursorStore::new(path.clone());
        assert_eq!(store.load(), None);
        store.save(1_700_000_000_000_000).unwrap();
        assert_eq!(store.load(), Some(1_700_000_000_000_000));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn subscribe_url_includes_collection_dids_and_cursor() {
        let base = Url::parse("wss://jetstream.example/subscribe").unwrap();
        let url = build_subscribe_url(
            &base,
            &["did:plc:alice".to_string(), "did:plc:bob".to_string()],
            Some(42),
        );
        let query = url.query().unwrap_or_default();
        assert!(query.contains("wantedCollections=app.bsky.feed.post"));
        assert!(query.contains("wantedDids=did%3Aplc%3Aalice"));
        assert!(query.contains("wantedDids=did%3Aplc%3Abob"));
        assert!(query.contains("cursor=42"));
    }

    #[test]
    fn subscribe_url_omits_cursor_when_none() {
        let base = Url::parse("wss://jetstream.example/subscribe").unwrap();
        let url = build_subscribe_url(&base, &["did:plc:alice".to_string()], None);
        assert!(!url.query().unwrap_or_default().contains("cursor="));
    }

    #[test]
    fn backoff_never_exceeds_max_and_never_hangs() {
        let backoff = BackoffConfig {
            initial: Duration::from_millis(100),
            max: Duration::from_millis(500),
        };
        for attempt in 0..20 {
            let delay = backoff.delay_for(attempt);
            assert!(
                delay <= Duration::from_millis(500),
                "attempt {attempt}: {delay:?}"
            );
        }
    }

    fn uuid() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }
}
