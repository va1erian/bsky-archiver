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
    /// The concatenation of init + segments of Bluesky's fMP4 streams is
    /// itself a valid fragmented-MP4 file, so the returned bytes play in a
    /// plain `<video>` element — a self-contained backup playable without
    /// an HLS client. The overall size is capped at `max_bytes`, mirroring
    /// the cap on any other single media file.
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
mod tests {
    use super::*;
    use crate::pipeline::{CandidatePost, candidate_post_channel};
    use serde_json::json;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    async fn open_store() -> (tempfile::TempDir, ArchiveStore) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let archive_dir = dir.path().join("archive");
        let database_path = dir.path().join("index.sqlite3");
        let store = ArchiveStore::open(archive_dir, database_path)
            .await
            .expect("open store");
        (dir, store)
    }

    fn candidate(at_uri: &str, media: Vec<MediaRef>) -> CandidatePost {
        CandidatePost {
            at_uri: at_uri.to_string(),
            cid: "cid-1".to_string(),
            author_did: "did:plc:alice".to_string(),
            category: PostCategory::Authored,
            record: json!({"text": "hello"}),
            media,
        }
    }

    #[tokio::test]
    async fn successful_download_is_stored_alongside_the_post() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/img.jpg"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "image/jpeg")
                    .set_body_bytes(b"fake-jpeg-bytes".to_vec()),
            )
            .mount(&server)
            .await;

        let (_dir, store) = open_store().await;
        let downloader = MediaDownloader::new(store.clone(), 4, 1_000_000);
        let (tx, mut rx) = candidate_post_channel(4);

        let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
        tx.send(candidate(
            at_uri,
            vec![MediaRef {
                cdn_url: format!("{}/img.jpg", server.uri()),
                declared_mime_type: Some("image/jpeg".to_string()),
                declared_size_bytes: None,
            }],
        ))
        .await
        .unwrap();
        drop(tx);

        downloader.run(&mut rx).await;

        let record = store
            .get_post(Category::Post, at_uri)
            .await
            .unwrap()
            .expect("post should be archived");
        assert_eq!(record.media.len(), 1);
        assert_eq!(record.media[0].filename, "000.jpg");
        assert_eq!(record.media[0].content_type.as_deref(), Some("image/jpeg"));
        assert_eq!(record.media[0].size_bytes, "fake-jpeg-bytes".len() as u64);
    }

    #[tokio::test]
    async fn content_type_mismatch_is_logged_but_not_a_hard_failure() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/img.jpg"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "image/png")
                    .set_body_bytes(b"actually-a-png".to_vec()),
            )
            .mount(&server)
            .await;

        let (_dir, store) = open_store().await;
        let downloader = MediaDownloader::new(store.clone(), 4, 1_000_000);
        let (tx, mut rx) = candidate_post_channel(4);

        let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
        tx.send(candidate(
            at_uri,
            vec![MediaRef {
                cdn_url: format!("{}/img.jpg", server.uri()),
                declared_mime_type: Some("image/jpeg".to_string()),
                declared_size_bytes: None,
            }],
        ))
        .await
        .unwrap();
        drop(tx);

        downloader.run(&mut rx).await;

        let record = store
            .get_post(Category::Post, at_uri)
            .await
            .unwrap()
            .expect("post should be archived");
        assert_eq!(record.media.len(), 1, "mismatch should not block the save");
        assert_eq!(record.media[0].filename, "000.png");
    }

    #[tokio::test]
    async fn oversized_response_is_aborted_and_leaves_no_partial_file() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/big.jpg"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "image/jpeg")
                    .set_body_bytes(vec![0u8; 1024]),
            )
            .mount(&server)
            .await;

        let (dir, store) = open_store().await;
        let downloader = MediaDownloader::new(store.clone(), 4, 100);
        let (tx, mut rx) = candidate_post_channel(4);

        let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
        tx.send(candidate(
            at_uri,
            vec![MediaRef {
                cdn_url: format!("{}/big.jpg", server.uri()),
                declared_mime_type: Some("image/jpeg".to_string()),
                declared_size_bytes: None,
            }],
        ))
        .await
        .unwrap();
        drop(tx);

        downloader.run(&mut rx).await;

        let record = store
            .get_post(Category::Post, at_uri)
            .await
            .unwrap()
            .expect("post record should still be archived");
        assert!(
            record.media.is_empty(),
            "oversized download must not be saved"
        );

        let media_dir = dir.path().join("archive");
        let has_any_media_file = walk_files(&media_dir)
            .into_iter()
            .any(|p| p.file_name().is_some_and(|n| n != "record.json"));
        assert!(
            !has_any_media_file,
            "no partial media file should be left on disk"
        );
    }

    fn walk_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut out = Vec::new();
        if let Ok(entries) = std::fs::read_dir(root) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    out.extend(walk_files(&path));
                } else {
                    out.push(path);
                }
            }
        }
        out
    }

    struct FlakyResponder {
        attempts: AtomicUsize,
        fail_times: usize,
    }

    impl Respond for FlakyResponder {
        fn respond(&self, _request: &Request) -> ResponseTemplate {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            if attempt < self.fail_times {
                ResponseTemplate::new(503)
            } else {
                ResponseTemplate::new(200)
                    .insert_header("content-type", "image/jpeg")
                    .set_body_bytes(b"ok-after-retry".to_vec())
            }
        }
    }

    #[tokio::test]
    async fn retries_transient_failures_then_succeeds() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/flaky.jpg"))
            .respond_with(FlakyResponder {
                attempts: AtomicUsize::new(0),
                fail_times: 2,
            })
            .mount(&server)
            .await;

        let (_dir, store) = open_store().await;
        let downloader = MediaDownloader::new(store.clone(), 4, 1_000_000);
        let (tx, mut rx) = candidate_post_channel(4);

        let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
        tx.send(candidate(
            at_uri,
            vec![MediaRef {
                cdn_url: format!("{}/flaky.jpg", server.uri()),
                declared_mime_type: Some("image/jpeg".to_string()),
                declared_size_bytes: None,
            }],
        ))
        .await
        .unwrap();
        drop(tx);

        downloader.run(&mut rx).await;

        let record = store
            .get_post(Category::Post, at_uri)
            .await
            .unwrap()
            .expect("post should be archived");
        assert_eq!(record.media.len(), 1, "should succeed after retries");
    }

    #[tokio::test]
    async fn exhausting_retries_gives_up_without_crashing_but_keeps_the_post() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/always-down.jpg"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let (_dir, store) = open_store().await;
        let downloader = MediaDownloader::new(store.clone(), 4, 1_000_000);
        let (tx, mut rx) = candidate_post_channel(4);

        let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
        tx.send(candidate(
            at_uri,
            vec![MediaRef {
                cdn_url: format!("{}/always-down.jpg", server.uri()),
                declared_mime_type: Some("image/jpeg".to_string()),
                declared_size_bytes: None,
            }],
        ))
        .await
        .unwrap();
        drop(tx);

        // Must return normally (no panic) even though the download never
        // succeeds.
        downloader.run(&mut rx).await;

        let record = store
            .get_post(Category::Post, at_uri)
            .await
            .unwrap()
            .expect("post JSON must remain archived even when media fails permanently");
        assert!(record.media.is_empty());
    }

    struct TrackingResponder {
        arrivals: Mutex<Vec<Instant>>,
        delay: Duration,
    }

    #[derive(Clone)]
    struct SharedTrackingResponder(Arc<TrackingResponder>);

    impl Respond for SharedTrackingResponder {
        fn respond(&self, _request: &Request) -> ResponseTemplate {
            self.0.arrivals.lock().unwrap().push(Instant::now());
            ResponseTemplate::new(200)
                .insert_header("content-type", "image/jpeg")
                .set_body_bytes(b"bytes".to_vec())
                .set_delay(self.0.delay)
        }
    }

    /// Computes the maximum number of `[arrival, arrival + delay)`
    /// intervals that overlap at any single point in time: the peak number
    /// of requests the mock server was handling concurrently.
    fn peak_overlap(arrivals: &[Instant], delay: Duration) -> usize {
        let mut events: Vec<(Instant, i32)> = Vec::new();
        for &start in arrivals {
            events.push((start, 1));
            events.push((start + delay, -1));
        }
        events.sort_by_key(|(t, kind)| (*t, *kind));

        let mut current = 0i32;
        let mut peak = 0i32;
        for (_, kind) in events {
            current += kind;
            peak = peak.max(current);
        }
        peak.max(0) as usize
    }

    #[tokio::test]
    async fn concurrency_cap_is_respected() {
        let server = MockServer::start().await;
        let delay = Duration::from_millis(150);
        let responder = Arc::new(TrackingResponder {
            arrivals: Mutex::new(Vec::new()),
            delay,
        });

        Mock::given(method("GET"))
            .and(path("/media.jpg"))
            .respond_with(SharedTrackingResponder(responder.clone()))
            .mount(&server)
            .await;

        let (_dir, store) = open_store().await;
        const LIMIT: usize = 2;
        let downloader = MediaDownloader::new(store.clone(), LIMIT, 1_000_000);
        let (tx, mut rx) = candidate_post_channel(16);

        for i in 0..6 {
            let at_uri = format!("at://did:plc:alice/app.bsky.feed.post/{i}");
            tx.send(candidate(
                &at_uri,
                vec![MediaRef {
                    cdn_url: format!("{}/media.jpg", server.uri()),
                    declared_mime_type: Some("image/jpeg".to_string()),
                    declared_size_bytes: None,
                }],
            ))
            .await
            .unwrap();
        }
        drop(tx);

        downloader.run(&mut rx).await;

        let arrivals = responder.arrivals.lock().unwrap().clone();
        assert_eq!(arrivals.len(), 6);
        let peak = peak_overlap(&arrivals, delay);
        assert!(
            peak <= LIMIT,
            "observed {peak} concurrent in-flight requests, expected at most {LIMIT}"
        );
        assert!(
            peak >= 2,
            "test is vacuous unless it actually observes concurrency, got {peak}"
        );
    }

    #[test]
    fn extension_from_mime_covers_common_media_types() {
        assert_eq!(extension_from_mime("image/jpeg"), Some("jpg"));
        assert_eq!(extension_from_mime("video/mp4"), Some("mp4"));
        assert_eq!(extension_from_mime("application/octet-stream"), None);
    }

    #[test]
    fn hls_playlist_content_types_are_recognized() {
        assert!(is_hls_playlist_content_type(
            "application/vnd.apple.mpegurl"
        ));
        assert!(is_hls_playlist_content_type("application/x-mpegurl"));
        assert!(is_hls_playlist_content_type(
            "Application/VND.APPLE.MPEGURL; charset=utf-8"
        ));
        assert!(!is_hls_playlist_content_type("video/mp4"));
        assert!(!is_hls_playlist_content_type("image/jpeg"));
    }

    #[test]
    fn hls_playlist_urls_are_recognized() {
        assert!(is_hls_playlist_url(
            "https://video.bsky.app/watch/did/cid/playlist.m3u8"
        ));
        assert!(is_hls_playlist_url(
            "https://cdn.example.com/a.m3u8?token=x"
        ));
        assert!(!is_hls_playlist_url("https://cdn.example.com/a.mp4"));
        assert!(!is_hls_playlist_url("https://cdn.example.com/m3u8"));
    }

    #[test]
    fn master_playlist_variant_picks_highest_bandwidth() {
        let master = "\
#EXTM3U
#EXT-X-VERSION:7
#EXT-X-STREAM-INF:BANDWIDTH=500000
low.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2000000
high.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=1000000
mid.m3u8
";
        assert_eq!(
            master_playlist_variant(master).as_deref(),
            Some("high.m3u8")
        );
    }

    #[test]
    fn master_playlist_detection_on_media_playlist_returns_none() {
        let media_playlist = "\
#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:4
#EXTINF:4.0,
seg0.m4s
#EXT-X-ENDLIST
";
        assert_eq!(master_playlist_variant(media_playlist), None);
    }

    #[test]
    fn media_playlist_parses_init_and_segments_relative_to_its_url() {
        let playlist = "\
#EXTM3U
#EXT-X-VERSION:7
#EXT-X-PLAYLIST-TYPE:VOD
#EXT-X-MAP:URI=\"init.mp4\"
#EXTINF:4.00000,
segment-0.m4s
#EXTINF:4.00000,
segment-1.m4s
#EXT-X-ENDLIST
";
        let base = url::Url::parse("https://video.example.com/watch/did/cid/media.m3u8").unwrap();
        let (init, segments) = parse_media_playlist(playlist, &base).expect("parses");
        assert_eq!(
            init.map(|u| u.to_string()),
            Some("https://video.example.com/watch/did/cid/init.mp4".to_string())
        );
        assert_eq!(segments.len(), 2);
        assert_eq!(
            segments[0].to_string(),
            "https://video.example.com/watch/did/cid/segment-0.m4s"
        );
        assert_eq!(
            segments[1].to_string(),
            "https://video.example.com/watch/did/cid/segment-1.m4s"
        );
    }

    #[test]
    fn encrypted_media_playlist_is_rejected() {
        let playlist = "\
#EXTM3U
#EXT-X-KEY:METHOD=AES-128,URI=\"https://keys.example.com/k\"
#EXTINF:4.0,
seg0.m4s
#EXT-X-ENDLIST
";
        let base = url::Url::parse("https://video.example.com/media.m3u8").unwrap();
        let err = parse_media_playlist(playlist, &base).expect_err("encrypted must be rejected");
        assert!(matches!(err, DownloadError::Unsupported(_)));
    }

    #[tokio::test]
    async fn hls_video_is_reassembled_into_a_single_playable_mp4() {
        let server = MockServer::start().await;

        // Master playlist → one variant (highest bandwidth of two) → media
        // playlist with an fMP4 init segment and two media segments, the
        // shape Bluesky's video CDN actually serves.
        Mock::given(method("GET"))
            .and(path("/watch/did/cid/playlist.m3u8"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/vnd.apple.mpegurl")
                    .set_body_string(
                        "#EXTM3U\n\
                         #EXT-X-STREAM-INF:BANDWIDTH=100000\n\
                         low.m3u8\n\
                         #EXT-X-STREAM-INF:BANDWIDTH=900000\n\
                         media.m3u8\n",
                    ),
            )
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/watch/did/cid/media.m3u8"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/vnd.apple.mpegurl")
                    .set_body_string(
                        "#EXTM3U\n\
                         #EXT-X-VERSION:7\n\
                         #EXT-X-MAP:URI=\"init.mp4\"\n\
                         #EXTINF:4.0,\n\
                         seg0.m4s\n\
                         #EXTINF:4.0,\n\
                         seg1.m4s\n\
                         #EXT-X-ENDLIST\n",
                    ),
            )
            .mount(&server)
            .await;

        for (route, segment_body) in [
            ("/watch/did/cid/init.mp4", "init-bytes"),
            ("/watch/did/cid/seg0.m4s", "seg0-bytes"),
            ("/watch/did/cid/seg1.m4s", "seg1-bytes"),
        ] {
            Mock::given(method("GET"))
                .and(path(route))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header("content-type", "video/iso.segment")
                        .set_body_bytes(segment_body.as_bytes().to_vec()),
                )
                .mount(&server)
                .await;
        }

        let (_dir, store) = open_store().await;
        let downloader = MediaDownloader::new(store.clone(), 4, 1_000_000);
        let (tx, mut rx) = candidate_post_channel(4);

        let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
        tx.send(candidate(
            at_uri,
            vec![MediaRef {
                cdn_url: format!("{}/watch/did/cid/playlist.m3u8", server.uri()),
                declared_mime_type: Some("application/vnd.apple.mpegurl".to_string()),
                declared_size_bytes: None,
            }],
        ))
        .await
        .unwrap();
        drop(tx);

        downloader.run(&mut rx).await;

        let record = store
            .get_post(Category::Post, at_uri)
            .await
            .unwrap()
            .expect("post should be archived");
        assert_eq!(record.media.len(), 1);
        assert_eq!(record.media[0].filename, "000.mp4");
        assert_eq!(
            record.media[0].content_type.as_deref(),
            Some("video/mp4"),
            "reassembled video must be stored with a playable content type"
        );
        assert_eq!(
            record.media[0].size_bytes,
            ("init-bytes".len() + "seg0-bytes".len() + "seg1-bytes".len()) as u64,
            "init + every media segment, concatenated in order"
        );
    }

    #[tokio::test]
    async fn hls_video_exceeding_the_size_cap_is_not_saved() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/watch/did/cid/playlist.m3u8"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "#EXTM3U\n\
                 #EXT-X-MAP:URI=\"init.mp4\"\n\
                 #EXTINF:4.0,\n\
                 seg0.m4s\n\
                 #EXT-X-ENDLIST\n",
            ))
            .mount(&server)
            .await;

        for (route, segment_body) in [
            ("/watch/did/cid/init.mp4", vec![0u8; 60]),
            ("/watch/did/cid/seg0.m4s", vec![0u8; 60]),
        ] {
            Mock::given(method("GET"))
                .and(path(route))
                .respond_with(ResponseTemplate::new(200).set_body_bytes(segment_body))
                .mount(&server)
                .await;
        }

        let (_dir, store) = open_store().await;
        // 100-byte cap: init alone fits, init + first segment does not.
        let downloader = MediaDownloader::new(store.clone(), 4, 100);
        let (tx, mut rx) = candidate_post_channel(4);

        let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
        tx.send(candidate(
            at_uri,
            vec![MediaRef {
                cdn_url: format!("{}/watch/did/cid/playlist.m3u8", server.uri()),
                declared_mime_type: Some("video/mp4".to_string()),
                declared_size_bytes: None,
            }],
        ))
        .await
        .unwrap();
        drop(tx);

        downloader.run(&mut rx).await;

        let record = store
            .get_post(Category::Post, at_uri)
            .await
            .unwrap()
            .expect("post record stays archived");
        assert!(record.media.is_empty(), "oversized video must not be saved");
    }

    #[tokio::test]
    async fn encrypted_hls_video_gives_up_without_crashing_but_keeps_the_post() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/watch/did/cid/playlist.m3u8"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "#EXTM3U\n\
                 #EXT-X-KEY:METHOD=AES-128,URI=\"https://keys.example.com/k\"\n\
                 #EXTINF:4.0,\n\
                 seg0.m4s\n\
                 #EXT-X-ENDLIST\n",
            ))
            .mount(&server)
            .await;

        let (_dir, store) = open_store().await;
        let downloader = MediaDownloader::new(store.clone(), 4, 1_000_000);
        let (tx, mut rx) = candidate_post_channel(4);

        let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
        tx.send(candidate(
            at_uri,
            vec![MediaRef {
                cdn_url: format!("{}/watch/did/cid/playlist.m3u8", server.uri()),
                declared_mime_type: None,
                declared_size_bytes: None,
            }],
        ))
        .await
        .unwrap();
        drop(tx);

        // Must return normally (no panic) even though the stream can't be
        // archived.
        downloader.run(&mut rx).await;

        let record = store
            .get_post(Category::Post, at_uri)
            .await
            .unwrap()
            .expect("post record stays archived");
        assert!(record.media.is_empty());
    }

    #[test]
    fn extension_from_url_reads_the_path_suffix() {
        assert_eq!(
            extension_from_url("https://cdn.example.com/a/b.PNG?x=1"),
            Some("png".to_string())
        );
        assert_eq!(extension_from_url("https://cdn.example.com/noext"), None);
    }

    #[test]
    fn base_mime_strips_parameters() {
        assert_eq!(base_mime("image/jpeg; charset=binary"), "image/jpeg");
        assert_eq!(base_mime("image/png"), "image/png");
    }
}
