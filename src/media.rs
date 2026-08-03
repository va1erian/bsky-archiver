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
pub enum DownloadError {
    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("declared content-length {0} bytes exceeds the configured cap")]
    DeclaredTooLarge(u64),
    #[error("downloaded body exceeded the configured cap after {0} bytes")]
    BodyTooLarge(u64),
    #[error("server returned status {0}")]
    Status(reqwest::StatusCode, Option<Duration>),
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
            DownloadError::DeclaredTooLarge(_) | DownloadError::BodyTooLarge(_) => false,
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

/// A media file downloaded into memory, size-capped, along with its
/// server-reported content type.
pub struct Downloaded {
    pub bytes: Vec<u8>,
    pub content_type: Option<String>,
}

/// Downloads one media file with the shared retry/backoff policy, honouring
/// `max_bytes` (per-file cap) and any `Retry-After`/`ratelimit-reset` hint on
/// a transient failure. Returns the bytes + content type on success, or the
/// terminal error after retries are exhausted or on a non-retryable failure.
///
/// Shared by [`MediaDownloader`] (the async pipeline consumer) and the feed
/// poller (which downloads inline so it can enforce its per-feed byte cap
/// before each file); both need identical streaming, capping, and retry
/// behaviour.
pub(crate) async fn fetch_media_with_retries(
    http: &reqwest::Client,
    request_limiter: Option<&Arc<RequestLimiter>>,
    media: &MediaRef,
    max_bytes: u64,
) -> Result<Downloaded, DownloadError> {
    let mut backoff = Backoff::new(BackoffConfig::new(RETRY_BASE_DELAY, RETRY_MAX_DELAY));
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match download_once(http, request_limiter, media, max_bytes).await {
            Ok(downloaded) => break Ok(downloaded),
            Err(err) if err.is_retryable() && attempt < MAX_ATTEMPTS => {
                let delay = backoff.on_failure(err.retry_after());
                tracing::warn!(
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
    }
}

/// Performs a single download attempt: streams the response body, aborting as
/// soon as the declared `Content-Length` or the actual bytes received exceed
/// `max_bytes`, without ever buffering an unbounded body into memory.
async fn download_once(
    http: &reqwest::Client,
    request_limiter: Option<&Arc<RequestLimiter>>,
    media: &MediaRef,
    max_bytes: u64,
) -> Result<Downloaded, DownloadError> {
    let _request_permit = match request_limiter {
        Some(limiter) => Some(limiter.acquire().await),
        None => None,
    };
    let response = http.get(&media.cdn_url).send().await?;

    let status = response.status();
    if !status.is_success() {
        let retry_after = crate::bluesky::parse_retry_hint(response.headers());
        return Err(DownloadError::Status(status, retry_after));
    }

    if let Some(declared_len) = response.content_length()
        && declared_len > max_bytes
    {
        return Err(DownloadError::DeclaredTooLarge(declared_len));
    }

    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);

    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        bytes.extend_from_slice(&chunk);
        if bytes.len() as u64 > max_bytes {
            return Err(DownloadError::BodyTooLarge(bytes.len() as u64));
        }
    }

    Ok(Downloaded {
        bytes,
        content_type,
    })
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
                    category.clone(),
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
                let category = category.clone();
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

        let result = fetch_media_with_retries(
            &self.http,
            self.request_limiter.as_ref(),
            &media,
            self.max_bytes,
        )
        .await;

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

        if let (Some(declared), Some(actual)) =
            (&media.declared_mime_type, &downloaded.content_type)
            && base_mime(declared) != base_mime(actual)
        {
            tracing::warn!(
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

}

fn map_category(category: PostCategory) -> Category {
    match category {
        PostCategory::Authored => Category::Post,
        PostCategory::Like => Category::Like,
        PostCategory::Bookmark => Category::Bookmark,
        PostCategory::Feed(slug) => Category::Feed(slug),
    }
}

/// The mimetype without any `;charset=...`-style parameters, for comparing
/// a declared type against an actually-received one.
fn base_mime(mime: &str) -> &str {
    mime.split(';').next().unwrap_or(mime).trim()
}

/// Picks a filename for the `index`-th media file on a post: an index
/// prefix (so multiple files never collide) plus an extension guessed from
/// the actual/declared content-type, falling back to the CDN URL's own
/// extension, and finally a generic `.bin`.
pub(crate) fn filename_for(
    index: usize,
    media: &MediaRef,
    actual_content_type: Option<&str>,
) -> String {
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
