//! Telegram channel media archiving: an MTProto (user-account) client
//! built on [`grammers_client`] and a poller ([`TelegramArchiver`]) that
//! walks each watched channel's message history, archiving the JSON record
//! of every message that carries an image or video and downloading that
//! media straight over MTProto into the shared archive.
//!
//! ## Why a poller, and why not the shared candidate pipeline
//!
//! Telegram's update stream only carries messages the server pushes at a
//! live connection — everything posted while offline is silently skipped,
//! which makes polling with a dedup boundary (exactly how
//! [`crate::tumblr`] works) the simpler and self-healing capture model.
//! The poll walks each channel newest-first and stops once a full page
//! window of consecutively already-archived messages has been seen, so
//! steady-state cost is one history request per channel per cycle.
//!
//! The shared media downloader ([`crate::media`]) consumes [`MediaRef`]s
//! built from plain HTTPS CDN URLs. Telegram media is not served from a
//! CDN: files come from Telegram's datacenters over MTProto, chunk by
//! chunk, using access hashes no URL could carry. So this module downloads
//! media itself (via `Client::iter_download`, bounded by `MEDIA_MAX_BYTES`
//! and the shared download-concurrency cap) and saves it with
//! [`ArchiveStore::save_media`] directly. Messages without a recognized
//! image/video attachment are never archived at all — only media posts
//! belong in the gallery.
//!
//! ## Authorization and the session file
//!
//! MTProto runs as a *user account*, not a bot. Authorization is a
//! one-time interactive login (phone → login code → optional 2FA
//! password), performed by the `bsky-archiver telegram-login` subcommand,
//! which persists the session as a SQLite file at
//! `<ARCHIVE_DIR>/telegram.session` — the same path the service reads, so
//! the login and the daemon share one account session. The service itself
//! never prompts: if the session is missing or unauthorized, the archiver
//! reports [`Status::Error`][crate::health::Status::Error] health with
//! instructions instead of hanging.
//!
//! ## Channel addressing
//!
//! Watched channels are configured as `@username`s (see
//! `TELEGRAM_CHANNELS`), resolved at startup via
//! `contacts.resolveUsername`. A public username covers every public
//! channel. Private channels cannot be resolved without the session
//! already knowing the peer, so they are out of scope; archive a public
//! channel (or join-and-make-public) instead.
//!
//! ## Manual verification
//!
//! The MTProto layer cannot be meaningfully mocked (`grammers` speaks a
//! binary protocol to Telegram's datacenters; there is no wiremock
//! equivalent), so the network paths are verified by hand:
//!
//! 1. Run `bsky-archiver telegram-login` with real `TELEGRAM_*` env vars
//!    and confirm it completes and writes `telegram.session`.
//! 2. Start the service and confirm the dashboard's Telegram entry reaches
//!    `Connected` while the first backfill archives messages from a real
//!    test channel with photos and videos under `telegram_channels/`.
//! 3. Post a photo in the channel; within one poll interval confirm it is
//!    archived and visible in the gallery.
//! 4. Restart: confirm the poll immediately re-reaches the dedup boundary
//!    without re-downloading anything.

use std::sync::Arc;
use std::time::Duration;

use chrono::SecondsFormat;
use grammers_client::sender::{SenderPool, SenderPoolRunner};
use grammers_client::session::storages::SqliteSession;
use grammers_client::{Client, InvocationError, SignInError};
use grammers_session::types::PeerRef;
use grammers_session::updates::UpdatesLike;
use serde::Serialize;
use tokio::sync::Semaphore;

use crate::config::TelegramConfig;
use crate::storage::{ArchiveStore, Category, StorageError};

/// Messages fetched per `messages.getHistory` page — the API's own maximum.
/// Doubles as the "full window" of consecutively already-archived messages
/// that ends a steady-state poll walk.
pub const PAGE_LIMIT: usize = 100;

/// Delay between consecutive history pages within one walk. Backfills are
/// paced well below flood thresholds; the grammers client additionally
/// auto-sleeps on small `FLOOD_WAIT`s on its own.
const PAGE_DELAY: Duration = Duration::from_millis(1200);

/// The broad kinds of attachment we archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaKind {
    Image,
    Video,
}

impl MediaKind {
    fn as_str(self) -> &'static str {
        match self {
            MediaKind::Image => "image",
            MediaKind::Video => "video",
        }
    }
}

/// The parts of one message attachment that matter to the archive: which
/// broad kind it is, its MIME type (Telegram always declares one for
/// documents; photos are JPEG) and its declared byte size (documents
/// declare one; photos don't). Plain data, deliberately free of grammers
/// types, so record-building and tests work without a connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MediaDescriptor {
    pub kind: MediaKind,
    pub mime_type: Option<String>,
    pub declared_size_bytes: Option<u64>,
}

/// Serializes one message into the JSON stored as `record.json`:
/// identity (`channel`/`messageId`), timestamp (`createdAt` — the field
/// the index's `record_created_at` mirror reads), caption text, and the
/// media descriptors exactly as the API declared them. `empty` records
/// (no `media` array entries) are never written: only media messages get
/// archived in the first place.
pub fn message_record(
    channel: &str,
    message_id: i32,
    created_at: &str,
    text: &str,
    media: &[MediaDescriptor],
) -> serde_json::Value {
    serde_json::json!({
        "createdAt": created_at,
        "channel": channel,
        "messageId": message_id,
        "text": text,
        "media": media,
    })
}

/// The dedup key of one message: `telegram:{channel}/{message_id}`. The
/// storage layer hashes it into the on-disk item id, exactly like the
/// Tumblr analogue. `channel` is the normalized (no `@`) username each
/// watched channel is registered under.
pub fn message_key(channel: &str, message_id: i32) -> String {
    format!("telegram:{channel}/{message_id}")
}

/// The file name for a message's `index`-th (0-based) attachment: a
/// zero-padded index prefix (so files never collide) plus an extension
/// from the MIME type, falling back to the kind's canonical extension.
pub fn media_filename(index: usize, mime_type: Option<&str>, kind: MediaKind) -> String {
    let ext = mime_type
        .and_then(extension_for_mime)
        .unwrap_or_else(|| extension_for_kind(kind));
    format!("{index:03}.{ext}")
}

fn extension_for_kind(kind: MediaKind) -> &'static str {
    match kind {
        MediaKind::Image => "jpg",
        MediaKind::Video => "mp4",
    }
}

/// Whether a MIME type is an archive-worthy image or video, and which
/// broad kind it is. Media whose type is absent or unrecognized
/// (e.g. `application/octet-stream`) is skipped: a wrong guess would put
/// non-image junk into a gallery of pictures, and "unknown" is precisely
/// when guessing is least trustworthy.
pub fn mime_kind(mime_type: &str) -> Option<MediaKind> {
    let base = mime_type.split(';').next().unwrap_or(mime_type).trim();
    match base {
        "image/jpeg" | "image/png" | "image/gif" | "image/webp" | "image/tiff" | "image/bmp"
        | "image/heic" | "image/heif" | "image/avif" => Some(MediaKind::Image),
        "video/mp4" | "video/quicktime" | "video/x-matroska" | "video/webm" | "video/x-msvideo"
        | "video/mpeg" | "video/3gpp" | "video/x-flv" | "video/x-ms-wmv" => Some(MediaKind::Video),
        _ => None,
    }
}

fn extension_for_mime(mime_type: &str) -> Option<&'static str> {
    match mime_type.split(';').next().unwrap_or(mime_type).trim() {
        "image/jpeg" | "image/jpg" => Some("jpg"),
        "image/png" => Some("png"),
        "image/gif" => Some("gif"),
        "image/webp" => Some("webp"),
        "image/tiff" => Some("tiff"),
        "image/bmp" => Some("bmp"),
        "image/heic" => Some("heic"),
        "image/heif" => Some("heif"),
        "image/avif" => Some("avif"),
        "video/mp4" => Some("mp4"),
        "video/quicktime" => Some("mov"),
        "video/x-matroska" => Some("mkv"),
        "video/webm" => Some("webm"),
        "video/x-msvideo" => Some("avi"),
        "video/mpeg" => Some("mpeg"),
        "video/3gpp" => Some("3gp"),
        "video/x-flv" => Some("flv"),
        "video/x-ms-wmv" => Some("wmv"),
        _ => None,
    }
}

/// Classifies the single attachment of one message into a
/// [`MediaDescriptor`], or `None` when the attachment is a kind we skip
/// (stickers, voice notes, polls, contacts, web previews, unsaved
/// round-trip files, …). Photos are always archive-worthy compressed
/// images (JPEG in practice); documents pass through the MIME check.
///
/// Albums (a photo group posted at once) arrive as *separate messages*
/// sharing a `grouped_id`, each with its own attachment, so every message
/// carries at most one attachment by construction — one descriptor max.
pub fn describe_media(media: &grammers_client::media::Media) -> Option<MediaDescriptor> {
    use grammers_client::media::Media;

    match media {
        Media::Photo(_) => Some(MediaDescriptor {
            kind: MediaKind::Image,
            mime_type: Some("image/jpeg".to_string()),
            declared_size_bytes: None,
        }),
        Media::Document(document) => describe_document(document),
        // Stickers, polls, voice/audio notes, contacts, geo, dice, venues,
        // live location, and web pages are skipped wholesale.
        _ => None,
    }
}

/// The document half of [`describe_media`]: only documents whose declared
/// MIME type identifies an image or a video are archived.
fn describe_document(document: &GrammersDocument) -> Option<MediaDescriptor> {
    let mime_type = document.mime_type()?;
    let kind = mime_kind(mime_type)?;
    Some(MediaDescriptor {
        kind,
        mime_type: Some(mime_type.to_string()),
        declared_size_bytes: document.size().map(|size| size as u64),
    })
}

use grammers_client::media::Document as GrammersDocument;
use grammers_client::message::Message as GrammersMessage;

/// Extracts the single attachment descriptor of one message (or `None`).
pub fn message_media(message: &GrammersMessage) -> Option<MediaDescriptor> {
    message.media().and_then(|media| describe_media(&media))
}

/// The RFC 3339 UTC timestamp of a message — the record's `createdAt`,
/// which the index mirrors into `record_created_at` for created-time
/// sorting in the gallery.
pub fn message_created_at(message: &GrammersMessage) -> String {
    message.date().to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Connection bundle handed over by [`connect`]: the connected `Client`,
/// cheaply cloneable. The background tasks keeping the MTProto session
/// alive (pool runner and update drain) are detached on spawn on purpose:
/// they run for as long as the process does, matching how every other
/// supervised-but-permanent piece of the service behaves.
#[derive(Clone)]
pub struct TelegramConnection {
    pub client: Client,
}

/// Errors from the Telegram client layer. `SignIn` and `Invoke` results
/// are boxed (they are the largest of grammers' error types), so the enum
/// stays a reasonable size.
#[derive(Debug, thiserror::Error)]
pub enum TelegramError {
    #[error("opening the session store failed: {0}")]
    Session(Box<dyn std::error::Error + Send + Sync>),
    #[error("authorization check failed: {0}")]
    AuthCheck(Box<dyn std::error::Error + Send + Sync>),
    #[error("sign-in failed: {0}")]
    SignIn(Box<SignInError>),
    #[error("telegram request failed: {0}")]
    Invoke(Box<InvocationError>),
    #[error(
        "channel {0:?} could not be resolved: not a public channel username, \
             or not visible to the logged-in account"
    )]
    UnresolvableChannel(String),
    #[error("io failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("storage layer failed: {0}")]
    Storage(#[from] StorageError),
}

impl From<SignInError> for TelegramError {
    fn from(err: SignInError) -> Self {
        TelegramError::SignIn(Box::new(err))
    }
}

impl From<InvocationError> for TelegramError {
    fn from(err: InvocationError) -> Self {
        TelegramError::Invoke(Box::new(err))
    }
}

/// Opens (creating if needed) the SQLite session store, builds the
/// connection pool (`SenderPool` + runner) and the client, and spawns the
/// runner task that owns all network I/O — Telegram connections are
/// created lazily on first use, so there is no separate "connect" step.
///
/// The `updates` receiver pushed by the runner is drained by a dedicated
/// task: this archiver captures media by polling history, so the update
/// pump only needs to not back up. Telegram pushes updates at every
/// connected session whether the app wants them or not.
pub async fn connect(
    session_path: &std::path::Path,
    config: &TelegramConfig,
) -> Result<TelegramConnection, TelegramError> {
    let session = SqliteSession::open(session_path)
        .await
        .map_err(|err| TelegramError::Session(Box::new(err)))?;

    let SenderPool {
        runner,
        handle,
        updates,
        ..
    } = SenderPool::new(Arc::new(session), config.api_id);

    let client = Client::new(handle);
    // The runner keeps all socket I/O moving; the update drain keeps the
    // unbounded channel from buffering updates nobody consumes. Both end
    // when the runtime (or supervisor) drops them, matching the service.
    let _runner_task = tokio::spawn(runner_run(runner));
    let _updates_task = tokio::spawn(drain_updates(updates));
    Ok(TelegramConnection { client })
}

/// Glue over [`SenderPoolRunner::run`], which takes `self` and returns
/// `()` when it ends.
async fn runner_run(runner: SenderPoolRunner) {
    runner.run().await
}

async fn drain_updates(mut updates: tokio::sync::mpsc::UnboundedReceiver<UpdatesLike>) {
    while updates.recv().await.is_some() {}
}

/// Whether the account persisted into the session store is authorized. The
/// service uses this to decide between running the archive loop and
/// reporting an actionable health error.
pub async fn is_authorized(client: &Client) -> Result<bool, TelegramError> {
    client
        .is_authorized()
        .await
        .map_err(|err| TelegramError::AuthCheck(Box::new(err)))
}

/// Connects, checks authorization, resolves the watched channels, and
/// runs the poll loop until a cycle fails. Meant as the one entry point
/// the app supervisor retried from; the login subcommand replaces this
/// phase (it stops at a successful interactive sign-in).
///
/// Returns a typed error the supervisor understands:
/// - [`TelegramError::Session`] / [`TelegramError::AuthCheck`] mean the
///   session file is missing or unauthorized, which only a human can fix
///   (via `telegram-login`), so the supervisor reports `Error` and stops.
/// - [`TelegramError::UnresolvableChannel`] means configuration is wrong
///   (or the channel vanished), likewise non-retryable in-thread.
/// - Every other error is a transient failure worth reconnecting for.
pub async fn run_archiver(
    session_path: &std::path::Path,
    config: &TelegramConfig,
    store: &ArchiveStore,
    max_bytes: u64,
    download_concurrency: usize,
) -> Result<(), TelegramError> {
    let connection = connect(session_path, config).await?;
    if !is_authorized(&connection.client).await? {
        return Err(TelegramError::AuthCheck(
            "the session file holds no authorized account".into(),
        ));
    }
    let archiver = TelegramArchiver::new(
        connection.client,
        config,
        store.clone(),
        max_bytes,
        download_concurrency,
    )
    .await?;
    archiver.run().await
}

/// Interactive first-time login: prompts for the phone number, the login
/// code Telegram sends, and (when the account has 2FA) the account
/// password, then leaves an authorized session at `session_path` for the
/// service to pick up. Run only by the `telegram-login` subcommand
/// ([`crate::app::run_telegram_login`]) — never inside the service.
pub async fn interactive_login(
    session_path: &std::path::Path,
    config: &TelegramConfig,
) -> Result<(), TelegramError> {
    let connection = connect(session_path, config).await?;

    if is_authorized(&connection.client).await? {
        tracing::info!(
            "this session is already authorized; nothing to do (delete the session file to re-login)"
        );
        return Ok(());
    }

    let phone = prompt("Your phone number (international format, e.g. +15551234567): ")?;
    let token = connection
        .client
        .request_login_code(&phone, config.api_hash.expose_secret())
        .await?;
    let code = prompt("The login code Telegram sent you: ")?;
    match connection.client.sign_in(&token, &code).await {
        Err(SignInError::PasswordRequired(password_token)) => {
            let hint = password_token.hint().unwrap_or("");
            let prompt_text = if hint.is_empty() {
                "Your 2FA password: ".to_string()
            } else {
                format!("Your 2FA password (hint {hint}): ")
            };
            let password = prompt(&prompt_text)?;
            connection
                .client
                .check_password(password_token, password.trim())
                .await?;
        }
        Ok(_) => {}
        Err(err) => return Err(err.into()),
    };

    println!(
        "Signed in; session saved to {}. The archiver service can now run with this session.",
        session_path.display()
    );
    Ok(())
}

fn prompt(message: &str) -> Result<String, TelegramError> {
    use std::io::Write as _;
    print!("{message}");
    std::io::stdout().flush()?;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    Ok(line.trim().to_string())
}

/// One channel resolved against the login account: the normalized username
/// (the watch-list key, reused for the dedup key), its display title, and
/// the peer reference that lets every later history call skip resolution.
pub struct WatchedChannel {
    pub username: String,
    pub title: String,
    pub peer: PeerRef,
}

/// Resolves every configured channel username into a [`WatchedChannel`].
/// Any unresolvable username is a hard error (fail fast at startup of the
/// archiver; the service leaves the rest running and reports the failure).
pub async fn resolve_channels(
    client: &Client,
    usernames: &[String],
) -> Result<Vec<WatchedChannel>, TelegramError> {
    let mut channels = Vec::with_capacity(usernames.len());
    for username in usernames {
        let peer = client
            .resolve_username(username)
            .await?
            .ok_or_else(|| TelegramError::UnresolvableChannel(username.clone()))?;
        let title = peer
            .name()
            .map(str::to_string)
            .unwrap_or_else(|| username.clone());
        let peer_ref = peer
            .to_ref()
            .await
            .map_err(TelegramError::AuthCheck)?
            .ok_or_else(|| TelegramError::UnresolvableChannel(username.clone()))?;
        channels.push(WatchedChannel {
            username: username.clone(),
            title,
            peer: peer_ref,
        });
    }
    Ok(channels)
}

/// The channel poller: every cycle walks each watched channel's history
/// newest-first, archiving every media message until the dedup boundary,
/// then sleeps the poll interval. Media downloads (MTProto, chunked) ride
/// a separate semaphore so they never queue up unbounded.
pub struct TelegramArchiver {
    client: Client,
    store: ArchiveStore,
    channels: Vec<WatchedChannel>,
    base_interval: Duration,
    /// Depth (in messages, newest-first) of the startup backfill beyond
    /// the dedup boundary; `None` means the entire channel history.
    backfill_cap: Option<u64>,
    max_bytes: u64,
    download_concurrency: Arc<Semaphore>,
    /// `true` for the first cycle after startup, when a deep walk resumes
    /// any interrupted backfill; later cycles stop at the dedup boundary.
    first_cycle: bool,
}

impl TelegramArchiver {
    pub async fn new(
        client: Client,
        config: &TelegramConfig,
        store: ArchiveStore,
        max_bytes: u64,
        download_concurrency: usize,
    ) -> Result<Self, TelegramError> {
        let channels = resolve_channels(&client, &config.channels).await?;
        let backfill_cap = (config.backfill_limit > 0).then_some(config.backfill_limit);
        Ok(Self {
            client,
            store,
            channels,
            base_interval: Duration::from_secs(config.poll_interval_seconds),
            backfill_cap,
            max_bytes,
            download_concurrency: Arc::new(Semaphore::new(download_concurrency.max(1))),
            first_cycle: true,
        })
    }

    /// Runs the poll cycle forever. A cycle-ending error aborts the loop
    /// (its supervisor restarts and reconnects); healthy cycles sleep the
    /// base interval between each other.
    pub async fn run(mut self) -> Result<(), TelegramError> {
        loop {
            let full_walk = self.first_cycle;
            self.first_cycle = false;
            if let Err(err) = self.cycle(full_walk).await {
                tracing::error!(error = %err, "telegram archiver cycle failed; reconnecting");
                return Err(err);
            }
            tracing::debug!(
                interval_ms = self.base_interval.as_millis() as u64,
                "telegram archiver sleeping until next cycle"
            );
            tokio::time::sleep(self.base_interval).await;
        }
    }

    /// One poll cycle: walk each channel in turn. Any per-channel failure
    /// aborts the whole cycle so the supervisor can reconnect cleanly.
    async fn cycle(&self, full_walk: bool) -> Result<(), TelegramError> {
        for channel in &self.channels {
            let new_messages = self.walk_channel(channel, full_walk).await?;
            tracing::info!(
                channel = %channel.username,
                full_walk,
                new_messages,
                "telegram channel walk complete"
            );
        }
        Ok(())
    }

    /// Walks one channel's history newest→oldest until:
    ///
    /// - steady state: a full page window (`PAGE_LIMIT`) of consecutively
    ///   already-archived *media* messages, i.e. we re-entered archived
    ///   territory; or
    /// - a full-history/`backfill_cap` bound is hit (first cycle walk); or
    /// - the channel's history is exhausted.
    ///
    /// Messages without a recognized image/video attachment are passed
    /// over *without* counting toward the boundary: a media countdown
    /// would otherwise treat a run of text-only messages as archived
    /// territory and skip the very first photo posted after them.
    async fn walk_channel(
        &self,
        channel: &WatchedChannel,
        full_walk: bool,
    ) -> Result<usize, TelegramError> {
        let mut iter = self.client.iter_messages(channel.peer);
        let mut new_messages = 0usize;
        let mut consecutive_archived = 0usize;
        let mut visited = 0u64;

        while let Some(message) = iter.next().await? {
            visited += 1;

            // The root iterator hands pages out in one go; pace a
            // brief delay roughly once per page (every PAGE_LIMIT
            // messages) so a deep backfill stays polite.
            if visited.is_multiple_of(PAGE_LIMIT as u64) {
                tokio::time::sleep(PAGE_DELAY).await;
            }

            let Some(descriptor) = message_media(&message) else {
                continue;
            };

            let key = message_key(&channel.username, message.id());
            if self
                .store
                .is_archived(Category::TelegramChannel, &key)
                .await?
            {
                consecutive_archived += 1;
                if !full_walk && consecutive_archived >= PAGE_LIMIT {
                    tracing::debug!(
                        channel = %channel.username,
                        visited,
                        "telegram dedup boundary reached (full page window archived)"
                    );
                    break;
                }
                if let Some(cap) = self.backfill_cap
                    && visited >= cap
                {
                    tracing::debug!(
                        channel = %channel.username,
                        visited,
                        "telegram backfill cap reached"
                    );
                    break;
                }
                continue;
            }
            consecutive_archived = 0;

            self.archive_one(channel, &key, &message, &descriptor)
                .await?;
            new_messages += 1;

            if let Some(cap) = self.backfill_cap
                && visited >= cap
            {
                tracing::debug!(
                    channel = %channel.username,
                    visited,
                    "telegram backfill cap reached"
                );
                break;
            }
        }

        Ok(new_messages)
    }

    /// Archives one media message: saves its record (deduped by
    /// [`message_key`]), then downloads the attachment over MTProto under
    /// the download-concurrency cap and the `MEDIA_MAX_BYTES` guard.
    async fn archive_one(
        &self,
        channel: &WatchedChannel,
        key: &str,
        message: &GrammersMessage,
        descriptor: &MediaDescriptor,
    ) -> Result<(), TelegramError> {
        let record = message_record(
            &channel.username,
            message.id(),
            &message_created_at(message),
            message.text(),
            std::slice::from_ref(descriptor),
        );
        // `cid` is a Bluesky-only field; every non-AT-URI category stores
        // an empty string there (same as the Tumblr archiver).
        self.store
            .save_post(Category::TelegramChannel, key, "", record)
            .await?;

        let bytes = match self.download_media_bytes(message, descriptor).await {
            Ok(bytes) => bytes,
            Err(err) => {
                tracing::error!(
                    channel = %channel.username,
                    message_id = message.id(),
                    error = %err,
                    "telegram media download failed; record stays archived without it"
                );
                return Ok(());
            }
        };
        if bytes.is_empty() {
            return Ok(());
        }

        let filename = media_filename(0, descriptor.mime_type.as_deref(), descriptor.kind);
        if let Err(err) = self
            .store
            .save_media(
                Category::TelegramChannel,
                key,
                &filename,
                descriptor.mime_type.clone(),
                bytes,
            )
            .await
        {
            tracing::error!(
                channel = %channel.username,
                message_id = message.id(),
                filename = %filename,
                error = %err,
                "failed to store downloaded telegram media"
            );
        }

        tracing::info!(
            channel = %channel.username,
            message_id = message.id(),
            kind = descriptor.kind.as_str(),
            "archived new telegram channel message"
        );
        Ok(())
    }

    /// One MTProto media download (the grammers client's default retry
    /// policy already sleeps through small `FLOOD_WAIT`s). Enforces the
    /// `MEDIA_MAX_BYTES` cap against both the API's declared size (early
    /// skip) and the actual received bytes.
    async fn download_media_bytes(
        &self,
        message: &GrammersMessage,
        descriptor: &MediaDescriptor,
    ) -> Result<Vec<u8>, TelegramError> {
        let _permit = self
            .download_concurrency
            .acquire()
            .await
            .expect("semaphore is never closed");

        if let Some(declared) = descriptor.declared_size_bytes
            && declared > self.max_bytes
        {
            tracing::warn!(
                channel = %message_message_channel(message),
                message_id = message.id(),
                declared,
                "declared size exceeds MEDIA_MAX_BYTES; skipping telegram media"
            );
            return Ok(Vec::new());
        }

        let Some(media) = message.media() else {
            return Ok(Vec::new());
        };

        let mut download = self.client.iter_download(&media);
        let mut bytes = Vec::new();
        while let Some(chunk) = download.next().await? {
            bytes.extend_from_slice(&chunk);
            if bytes.len() as u64 > self.max_bytes {
                return Err(TelegramError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "telegram media exceeded MEDIA_MAX_BYTES",
                )));
            }
        }
        Ok(bytes)
    }
}

/// Best-effort channel name for log lines from inside the downloader, read
/// off the message's own context.
fn message_message_channel(message: &GrammersMessage) -> String {
    message
        .peer()
        .and_then(|peer| peer.name().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
mod tests;
