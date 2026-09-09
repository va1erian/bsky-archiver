//! Media (image/video) downloading: concurrency limiting, size-capped
//! streaming downloads, and storage alongside archived post JSON.
//!
//! [`MediaDownloader`] is the consumer side of [`crate::pipeline`]'s
//! `CandidatePost` channel: it drains candidates produced by the firehose
//! (AR-5), REST fallback (AR-6), and likes/bookmarks poller (AR-7), archives
//! each post's JSON record, then downloads and stores every attached media
//! file.

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::pipeline::{CandidatePostReceiver, MediaRef, PostCategory};
use crate::ratelimit::{Backoff, BackoffConfig, RequestLimiter};
use crate::remux;
use crate::storage::{ArchiveStore, Category};

/// Maximum number of attempts (including the first) made to download a
/// single media file before giving up on it.
const MAX_ATTEMPTS: u32 = 3;
/// Base delay for exponential backoff between retry attempts; doubled after
/// each failed attempt (via the shared [`Backoff`] policy).
const RETRY_BASE_DELAY: Duration = Duration::from_millis(50);
/// Upper bound the per-download retry backoff is capped at.
const RETRY_MAX_DELAY: Duration = Duration::from_secs(2);

/// Errors that can occur while downloading a single media file.
#[derive(Debug, thiserror::Error)]
enum DownloadError {
    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("declared content-length {0} bytes exceeds the configured cap")]
    DeclaredTooLarge(u64),
    #[error("downloaded body exceeded the configured cap after {0} bytes")]
    BodyTooLarge(u64),
    #[error("server returned status {0}")]
    Status(reqwest::StatusCode, Option<Duration>),
    #[error("media stream not archivable: {0}")]
    Unsupported(&'static str),
}

impl DownloadError {
    /// Whether this failure is transient and worth retrying: connection
    /// errors, timeouts, 5xx responses, and 429 (rate limited). Anything
    /// else (a permanent 4xx, or the size cap being exceeded) is not
    /// retried, since a retry would just fail the same way again.
    fn is_retryable(&self) -> bool {
        match self {
            DownloadError::Http(err) => err.is_timeout() || err.is_connect() || err.is_request(),
            DownloadError::Status(status, _) => {
                status.is_server_error() || *status == reqwest::StatusCode::TOO_MANY_REQUESTS
            }
            DownloadError::DeclaredTooLarge(_)
            | DownloadError::BodyTooLarge(_)
            | DownloadError::Unsupported(_) => false,
        }
    }

    /// A server-provided retry hint (`Retry-After`/`ratelimit-reset`), if
    /// this failure carries one.
    fn retry_after(&self) -> Option<Duration> {
        match self {
            DownloadError::Status(_, retry_after) => *retry_after,
            _ => None,
        }
    }
}

struct Downloaded {
    bytes: Vec<u8>,
    content_type: Option<String>,
}

/// Consumes [`CandidatePost`]s from a [`CandidatePostReceiver`], archiving
/// each post's JSON record and downloading its attached media, bounded by a
/// shared concurrency limit and a per-file size cap.
pub struct MediaDownloader {
    http: reqwest::Client,
    store: ArchiveStore,
    semaphore: Arc<Semaphore>,
    max_bytes: u64,
    /// Process-wide soft cap on requests in flight, shared with
    /// [`crate::bluesky::BlueskyClient`]. `None` in tests/callers that
    /// don't opt in, in which case downloads are never throttled here.
    request_limiter: Option<Arc<RequestLimiter>>,
}

impl MediaDownloader {
    /// Creates a downloader that writes into `store`, allows at most
    /// `max_concurrent_downloads` media downloads in flight at once, and
    /// aborts any single download whose declared or actual size exceeds
    /// `max_bytes`.
    pub fn new(store: ArchiveStore, max_concurrent_downloads: usize, max_bytes: u64) -> Self {
        Self {
            http: reqwest::Client::new(),
            store,
            semaphore: Arc::new(Semaphore::new(max_concurrent_downloads.max(1))),
            max_bytes,
            request_limiter: None,
        }
    }

    /// Attaches a process-wide request limiter: every download this
    /// instance makes will wait for a slot on `limiter` before hitting the
    /// network, alongside REST polling requests sharing the same limiter.
    pub fn with_request_limiter(mut self, limiter: Arc<RequestLimiter>) -> Self {
        self.request_limiter = Some(limiter);
        self
    }

    /// Drains `candidates` until the channel is closed, archiving each
    /// post's record and spawning a bounded-concurrency download task per
    /// attached media file. Returns once every candidate has been received
    /// and every spawned download task has finished. A single post or media
    /// failure is logged and does not stop the pipeline.
    ///
    /// Takes the receiver by reference (rather than owning it) so a
    /// supervisor ([`crate::app`]) can hold onto the same receiver across
    /// restarts if a run panics — the channel and everything already queued
    /// on it survives even though this particular downloader instance
    /// doesn't.
    pub async fn run(self, candidates: &mut CandidatePostReceiver) {
        let downloader = Arc::new(self);
        let mut media_tasks = JoinSet::new();

        while let Some(candidate) = candidates.recv().await {
            let category = map_category(candidate.category);
            match downloader
                .store
                .save_post(
                    category,
                    &candidate.at_uri,
                    &candidate.cid,
                    candidate.record.clone(),
                )
                .await
            {
                Ok(_) => {}
                Err(err) => {
                    tracing::error!(
                        at_uri = %candidate.at_uri,
                        error = %err,
                        "failed to archive post record; skipping its media"
                    );
                    continue;
                }
            }

            for (index, media) in candidate.media.into_iter().enumerate() {
                let downloader = Arc::clone(&downloader);
                let at_uri = candidate.at_uri.clone();
                media_tasks.spawn(async move {
                    downloader
                        .download_and_store(category, at_uri, index, media)
                        .await;
                });
            }
        }

        while media_tasks.join_next().await.is_some() {}
    }

    /// Downloads one media file (retrying transient failures) and, on
    /// success, saves it via [`ArchiveStore::save_media`]. A permanent
    /// failure (retries exhausted, or a non-retryable error) is logged and
    /// swallowed: the post's already-archived JSON record is left in place
    /// without this media file attached.
    async fn download_and_store(
        &self,
        category: Category,
        at_uri: String,
        index: usize,
        media: MediaRef,
    ) {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore is never closed");

        let mut backoff = Backoff::new(BackoffConfig::new(RETRY_BASE_DELAY, RETRY_MAX_DELAY));
        let mut attempt = 0u32;
        let result = loop {
            attempt += 1;
            match self.download_once(&media).await {
                Ok(downloaded) => break Ok(downloaded),
                Err(err) if err.is_retryable() && attempt < MAX_ATTEMPTS => {
                    let delay = backoff.on_failure(err.retry_after());
                    tracing::warn!(
                        at_uri = %at_uri,
                        cdn_url = %media.cdn_url,
                        attempt,
                        error = %err,
                        delay_ms = delay.as_millis() as u64,
                        "media download failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(err) => break Err(err),
            }
        };

        let downloaded = match result {
            Ok(downloaded) => downloaded,
            Err(err) => {
                tracing::error!(
                    at_uri = %at_uri,
                    cdn_url = %media.cdn_url,
                    error = %err,
                    "giving up on media download after retries; post record stays archived without it"
                );
                return;
            }
        };

        // An HLS-backed video legitimately arrives as `video/mp4` while the
        // record declared the playlist type — that reassembly is expected,
        // not a mismatch worth logging.
        let declared_matches = media
            .declared_mime_type
            .as_deref()
            .map(is_hls_playlist_content_type)
            .unwrap_or(false);
        if !declared_matches
            && let (Some(declared), Some(actual)) =
                (&media.declared_mime_type, &downloaded.content_type)
            && base_mime(declared) != base_mime(actual)
        {
            tracing::debug!(
                at_uri = %at_uri,
                cdn_url = %media.cdn_url,
                declared = %declared,
                actual = %actual,
                "downloaded content-type does not match the post record's declared mime type"
            );
        }

        let filename = filename_for(index, &media, downloaded.content_type.as_deref());
        if let Err(err) = self
            .store
            .save_media(
                category,
                &at_uri,
                &filename,
                downloaded.content_type,
                downloaded.bytes,
            )
            .await
        {
            tracing::error!(
                at_uri = %at_uri,
                filename = %filename,
                error = %err,
                "failed to store downloaded media"
            );
        }
    }

    /// Performs a single download attempt. For ordinary media (images,
    /// direct video files) it streams the response body, aborting as soon
    /// as the declared `Content-Length` or the actual bytes received exceed
    /// `max_bytes`, without ever buffering an unbounded body into memory.
    /// For an HLS playlist (what Bluesky's video CDN serves for video
    /// embeds) it instead downloads the playlist and every segment it
    /// lists, returning their concatenation as one playable file.
    async fn download_once(&self, media: &MediaRef) -> Result<Downloaded, DownloadError> {
        let _request_permit = match &self.request_limiter {
            Some(limiter) => Some(limiter.acquire().await),
            None => None,
        };
        let response = self.http.get(&media.cdn_url).send().await?;

        let status = response.status();
        if !status.is_success() {
            let retry_after = crate::bluesky::parse_retry_hint(response.headers());
            return Err(DownloadError::Status(status, retry_after));
        }

        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);

        let playlist_url = response.url().clone();
        let content_type_is_playlist = content_type
            .as_deref()
            .map(is_hls_playlist_content_type)
            .unwrap_or(false);
        if content_type_is_playlist || is_hls_playlist_url(&media.cdn_url) {
            drop(response);
            drop(_request_permit);
            return self.download_hls(playlist_url).await;
        }

        if let Some(declared_len) = response.content_length()
            && declared_len > self.max_bytes
        {
            return Err(DownloadError::DeclaredTooLarge(declared_len));
        }

        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            bytes.extend_from_slice(&chunk);
            if bytes.len() as u64 > self.max_bytes {
                return Err(DownloadError::BodyTooLarge(bytes.len() as u64));
            }
        }

        Ok(Downloaded {
            bytes,
            content_type,
        })
    }

    /// Downloads one HLS stream end to end: fetch the playlist, walk into
    /// the highest-bandwidth variant if it's a master playlist, then fetch
    /// the init segment (fMP4 streams) and every media segment in order.
    /// Bluesky's segments are MPEG-TS, so the concatenation is remuxed
    /// into a moov-first MP4 that plays in a plain `<video>` element — a
    /// self-contained backup playable without an HLS client. If the
    /// remuxer can't handle the stream, the raw TS bytes are stored under
    /// an honest `video/mp2t` label instead. The overall size is capped
    /// at `max_bytes`, mirroring the cap on any other single media file.
    async fn download_hls(&self, playlist_url: url::Url) -> Result<Downloaded, DownloadError> {
        let playlist_text = self.fetch_bytes(&playlist_url).await?;
        let playlist_text = String::from_utf8(playlist_text)
            .map_err(|_| DownloadError::Unsupported("HLS playlist is not valid UTF-8"))?;

        // A master playlist lists bitrate variants instead of segments;
        // walk into the best one (per HLS spec the variant's URI is the
        // line immediately after its `#EXT-X-STREAM-INF` tag).
        let (playlist_url, playlist_text) = match master_playlist_variant(&playlist_text) {
            Some(variant) => {
                let variant_url = playlist_url
                    .join(&variant)
                    .map_err(|_| DownloadError::Unsupported("invalid HLS variant URI"))?;
                let variant_text = self.fetch_bytes(&variant_url).await?;
                let variant_text = String::from_utf8(variant_text)
                    .map_err(|_| DownloadError::Unsupported("HLS playlist is not valid UTF-8"))?;
                (variant_url, variant_text)
            }
            None => (playlist_url, playlist_text),
        };

        let (init_segment, segments) = parse_media_playlist(&playlist_text, &playlist_url)?;
        if segments.is_empty() {
            return Err(DownloadError::Unsupported("HLS playlist lists no segments"));
        }

        let mut bytes = Vec::new();
        if let Some(init) = init_segment {
            self.append_segment(&mut bytes, &init).await?;
        }
        for segment in &segments {
            self.append_segment(&mut bytes, segment).await?;
        }

        // Bluesky's TS segments must be remuxed before a browser can
        // play them; fMP4 streams (init segment, no TS sync pattern)
        // already concatenate into a valid MP4.
        if remux::looks_like_mpeg_ts(&bytes) {
            return match remux::remux_ts_to_mp4(&bytes) {
                Ok(mp4) => Ok(Downloaded {
                    bytes: mp4,
                    content_type: Some("video/mp4".to_string()),
                }),
                Err(err) => {
                    tracing::warn!(
                        playlist = %playlist_url,
                        error = %err,
                        "HLS stream could not be remuxed to MP4; storing the raw MPEG-TS stream"
                    );
                    Ok(Downloaded {
                        bytes,
                        content_type: Some("video/mp2t".to_string()),
                    })
                }
            };
        }

        Ok(Downloaded {
            bytes,
            content_type: Some("video/mp4".to_string()),
        })
    }

    /// Fetches one HLS resource (a playlist or a segment) as bytes. Each
    /// request rides the shared request limiter; non-success statuses
    /// surface as retryable/unretryable `Status` errors like any other
    /// download, so the caller's existing retry loop applies to the whole
    /// stream.
    async fn fetch_bytes(&self, url: &url::Url) -> Result<Vec<u8>, DownloadError> {
        let _request_permit = match &self.request_limiter {
            Some(limiter) => Some(limiter.acquire().await),
            None => None,
        };
        let response = self.http.get(url.clone()).send().await?;
        let status = response.status();
        if !status.is_success() {
            let retry_after = crate::bluesky::parse_retry_hint(response.headers());
            return Err(DownloadError::Status(status, retry_after));
        }
        // Same declared-size guard as the ordinary download path: a segment
        // over the cap is rejected before buffering it into memory.
        if let Some(declared_len) = response.content_length()
            && declared_len > self.max_bytes
        {
            return Err(DownloadError::DeclaredTooLarge(declared_len));
        }
        Ok(response.bytes().await?.to_vec())
    }

    /// Appends one segment to the accumulating video, enforcing the overall
    /// `max_bytes` cap across the whole stream.
    async fn append_segment(
        &self,
        bytes: &mut Vec<u8>,
        url: &url::Url,
    ) -> Result<(), DownloadError> {
        let segment = self.fetch_bytes(url).await?;
        bytes.extend_from_slice(&segment);
        if bytes.len() as u64 > self.max_bytes {
            return Err(DownloadError::BodyTooLarge(bytes.len() as u64));
        }
        Ok(())
    }
}

fn map_category(category: PostCategory) -> Category {
    match category {
        PostCategory::Authored => Category::Post,
        PostCategory::Like => Category::Like,
        PostCategory::Bookmark => Category::Bookmark,
    }
}

/// The mimetype without any `;charset=...`-style parameters, for comparing
/// a declared type against an actually-received one.
fn base_mime(mime: &str) -> &str {
    mime.split(';').next().unwrap_or(mime).trim()
}

/// Whether `content_type` identifies an HLS playlist (master or media) —
/// served by Bluesky's video CDN for video embeds — rather than a
/// directly-downloadable media file.
fn is_hls_playlist_content_type(content_type: &str) -> bool {
    matches!(
        base_mime(content_type).to_ascii_lowercase().as_str(),
        "application/vnd.apple.mpegurl" | "application/x-mpegurl"
    )
}

/// Whether a URL path unambiguously points at an `.m3u8` playlist. Used as
/// a fallback when the CDN sends no recognizable `Content-Type`, and for
/// MediaRefs whose declared mime type is the *original upload's* (`video/mp4`)
/// rather than the playlist's.
fn is_hls_playlist_url(url: &str) -> bool {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    path.rsplit('/').next().unwrap_or(path).ends_with(".m3u8")
}

/// The URI of the highest-bandwidth variant in a master HLS playlist, if
/// this is one (a media playlist has no `#EXT-X-STREAM-INF` tags and
/// returns `None`). Relative URIs are returned as-is; the caller resolves
/// them against the master playlist's own URL.
fn master_playlist_variant(playlist: &str) -> Option<String> {
    let lines: Vec<&str> = playlist.lines().map(str::trim).collect();
    let mut best: Option<(u64, &str)> = None;
    for (index, line) in lines.iter().enumerate() {
        let Some(tag) = line.strip_prefix("#EXT-X-STREAM-INF:") else {
            continue;
        };
        // The variant URI is the line immediately following the tag.
        let uri = lines.get(index + 1)?;
        if uri.is_empty() || uri.starts_with('#') {
            continue;
        }
        let bandwidth = tag
            .split(',')
            .find_map(|attr| attr.trim().strip_prefix("BANDWIDTH=")?.parse::<u64>().ok())
            .unwrap_or(0);
        if best.is_none_or(|(best_bw, _)| bandwidth > best_bw) {
            best = Some((bandwidth, uri));
        }
    }
    best.map(|(_, uri)| uri.to_string())
}

/// Parses an HLS media playlist into its optional init segment (the
/// `#EXT-X-MAP` of an fMP4 stream — always first, never chunked) and its
/// ordered media segments, each URI resolved against the playlist's own
/// URL. Encrypted streams (`#EXT-X-KEY` with any `METHOD` other than
/// `NONE`) are rejected: the archive stores only self-contained playable
/// files, and a key-protected stream can't be reassembled into one.
fn parse_media_playlist(
    playlist: &str,
    base_url: &url::Url,
) -> Result<(Option<url::Url>, Vec<url::Url>), DownloadError> {
    let mut init_segment = None;
    let mut segments = Vec::new();

    for line in playlist.lines() {
        let line = line.trim();
        if let Some(attrs) = line.strip_prefix("#EXT-X-KEY:") {
            let method = attrs
                .split(',')
                .find_map(|attr| attr.trim().strip_prefix("METHOD="))
                .unwrap_or("NONE");
            if method != "NONE" {
                return Err(DownloadError::Unsupported("encrypted HLS stream"));
            }
        } else if let Some(attrs) = line.strip_prefix("#EXT-X-MAP:") {
            let uri = attrs.split("URI=\"").nth(1).unwrap_or_default();
            let uri = uri.split('"').next().unwrap_or_default();
            if !uri.is_empty() {
                init_segment = Some(
                    base_url
                        .join(uri)
                        .map_err(|_| DownloadError::Unsupported("invalid HLS segment URI"))?,
                );
            }
        } else if !line.is_empty() && !line.starts_with('#') {
            segments.push(
                base_url
                    .join(line)
                    .map_err(|_| DownloadError::Unsupported("invalid HLS segment URI"))?,
            );
        }
    }

    Ok((init_segment, segments))
}

/// Picks a filename for the `index`-th media file on a post: an index
/// prefix (so multiple files never collide) plus an extension guessed from
/// the actual/declared content-type, falling back to the CDN URL's own
/// extension, and finally a generic `.bin`.
fn filename_for(index: usize, media: &MediaRef, actual_content_type: Option<&str>) -> String {
    let content_type = actual_content_type.or(media.declared_mime_type.as_deref());
    let ext = extension_for(content_type, &media.cdn_url);
    format!("{index:03}.{ext}")
}

fn extension_for(content_type: Option<&str>, url: &str) -> String {
    if let Some(ext) = content_type.and_then(|ct| extension_from_mime(base_mime(ct))) {
        return ext.to_string();
    }
    if let Some(ext) = extension_from_url(url) {
        return ext;
    }
    "bin".to_string()
}

fn extension_from_mime(mime: &str) -> Option<&'static str> {
    match mime {
        "image/jpeg" => Some("jpg"),
        "image/png" => Some("png"),
        "image/gif" => Some("gif"),
        "image/webp" => Some("webp"),
        "video/mp4" => Some("mp4"),
        "video/webm" => Some("webm"),
        "video/mp2t" => Some("ts"),
        _ => None,
    }
}

fn extension_from_url(url: &str) -> Option<String> {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    let filename = path.rsplit('/').next()?;
    let (_, ext) = filename.rsplit_once('.')?;
    if ext.is_empty() || ext.len() > 5 || !ext.chars().all(|c| c.is_ascii_alphanumeric()) {
        return None;
    }
    Some(ext.to_lowercase())
}

#[cfg(test)]
mod tests;
