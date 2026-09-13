//! Tumblr likes archiving: an OAuth 1.0a-signed Tumblr API v2 client
//! ([`TumblrClient`]) and the poller ([`TumblrLikesPoller`]) that walks the
//! authenticated account's liked posts, archiving each one's JSON record
//! and — for posts with Tumblr-hosted media — handing the media off to the
//! shared downloader channel.
//!
//! This is the Tumblr counterpart of [`crate::poller`]'s
//! `LikesBookmarksPoller`: likes are only ever visible via REST polling of
//! the current list, so the poll walks newest-first, dedups against the
//! archive, records each item's position in Tumblr's own list (0 = newest)
//! and its `liked_timestamp` (the action time) so the gallery can
//! reproduce Tumblr's ordering, and backs off adaptively on failures.
//!
//! Tumblr has no push/firehose equivalent, so this poller is the only
//! producer for the `tumblr_likes` category. It is enabled entirely by
//! configuration: when the four `TUMBLR_*` environment variables are set
//! the poller runs; when they are absent, Tumblr archiving is disabled and
//! nothing here is spawned ([`crate::app`]).

use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use hmac::Mac;
use rand::Rng;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tracing::{debug, info, warn};
use url::Url;

use crate::config::{Secret, TumblrConfig};
use crate::pipeline::{CandidatePost, CandidatePostSender, MediaRef, PostCategory};
use crate::ratelimit::{Backoff, BackoffConfig, RequestLimiter};
use crate::storage::{ArchiveStore, Category, SaveOutcome, StorageError};

/// The production Tumblr API v2 entryway. Not part of the canonical env var
/// schema; overridable only for tests, which point the client at a
/// `wiremock` server instead.
pub const DEFAULT_TUMBLR_BASE_URL: &str = "https://api.tumblr.com";

/// Page size requested per `/v2/user/likes` call — the endpoint's own
/// maximum.
pub const PAGE_LIMIT: u32 = 20;

// ---------------------------------------------------------------------
// OAuth 1.0a signing (RFC 5849, HMAC-SHA1)
// ---------------------------------------------------------------------

/// The percent-encoding set for OAuth 1.0a: RFC 3986 unreserved characters
/// (ALPHA / DIGIT / `-` / `.` / `_` / `~`) stay literal, everything else is
/// encoded.
const OAUTH_ENCODE_SET: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

fn oauth_encode(value: &str) -> String {
    percent_encoding::utf8_percent_encode(value, OAUTH_ENCODE_SET).to_string()
}

/// Computes the `oauth_signature` for one request per RFC 5849 §3.4:
/// an HMAC-SHA1 over the signature base string, keyed with the encoded
/// consumer secret and token secret joined by `&`.
///
/// `params` is every request parameter that participates in the signature —
/// all `oauth_*` protocol parameters (without `oauth_signature` itself)
/// plus any query parameters — as `(key, value)` pairs in raw form; they
/// are encoded, sorted, and folded into the base string here.
fn oauth_signature(
    method: &str,
    base_url: &str,
    params: &[(String, String)],
    consumer_secret: &str,
    token_secret: &str,
) -> String {
    // RFC 5849 §3.4.1.3.2: encode every key and value first, then sort the
    // encoded pairs (by key, then value).
    let mut encoded: Vec<(String, String)> = params
        .iter()
        .map(|(key, value)| (oauth_encode(key), oauth_encode(value)))
        .collect();
    encoded.sort();

    // §3.4.1.3: the parameter string joins the encoded pairs with `&`, and
    // is itself encoded once more as part of the base string.
    let param_string = encoded
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&");

    let base_string = format!(
        "{}&{}&{}",
        oauth_encode(method),
        oauth_encode(base_url),
        oauth_encode(&param_string)
    );
    let signing_key = format!(
        "{}&{}",
        oauth_encode(consumer_secret),
        oauth_encode(token_secret)
    );

    let mut mac = <hmac::Hmac<sha1::Sha1>>::new_from_slice(signing_key.as_bytes())
        .expect("HMAC accepts keys of any length");
    mac.update(base_string.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

/// Builds the `Authorization: OAuth ...` header value from the final
/// protocol parameter list (including `oauth_signature`). Every value is
/// percent-encoded per RFC 5849 §3.5.1.
fn oauth_authorization_header(params: &[(String, String)]) -> String {
    let pairs: Vec<String> = params
        .iter()
        .map(|(key, value)| format!("{}=\"{}\"", oauth_encode(key), oauth_encode(value)))
        .collect();
    format!("OAuth {}", pairs.join(", "))
}

// ---------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------

/// Errors from the Tumblr API client.
#[derive(Debug, thiserror::Error)]
pub enum TumblrError {
    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("tumblr api returned http status {0}")]
    Status(reqwest::StatusCode, Option<Duration>),
    #[error("tumblr api returned meta status {0}")]
    Api(i64),
    #[error("unexpected tumblr response shape: {0}")]
    Shape(&'static str),
}

impl TumblrError {
    /// A server-provided retry hint (`Retry-After`/`ratelimit-reset`), if
    /// this failure carries one.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            TumblrError::Status(_, hint) => *hint,
            _ => None,
        }
    }
}

/// One page of the authenticated account's likes, newest-first.
pub struct LikesPage {
    /// Raw liked-post JSON objects, in the API's own (newest-first) order.
    pub posts: Vec<serde_json::Value>,
    /// The account's total like count, as reported by the API.
    pub liked_count: u64,
}

impl LikesPage {
    fn from_json(body: &serde_json::Value) -> Result<Self, TumblrError> {
        let response = body
            .get("response")
            .ok_or(TumblrError::Shape("response body has no `response` object"))?;
        let posts = response
            .get("liked_posts")
            .and_then(|v| v.as_array())
            .ok_or(TumblrError::Shape("response has no `liked_posts` array"))?
            .clone();
        let liked_count = response
            .get("liked_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        Ok(LikesPage { posts, liked_count })
    }
}

/// A Tumblr API v2 client making OAuth 1.0a HMAC-SHA1-signed requests.
/// Credentials never expire (Tumblr's OAuth 1.0a user tokens are
/// long-lived), so — unlike [`crate::bluesky::BlueskyClient`] — there is no
/// session/refresh machinery here.
pub struct TumblrClient {
    http: reqwest::Client,
    base_url: Url,
    consumer_key: Secret,
    consumer_secret: Secret,
    oauth_token: Secret,
    oauth_secret: Secret,
    /// Process-wide soft cap on requests in flight, shared with the
    /// Bluesky client and the media downloader. `None` in tests/callers
    /// that don't opt in.
    request_limiter: Option<Arc<RequestLimiter>>,
}

impl TumblrClient {
    pub fn new(base_url: Url, config: TumblrConfig) -> Self {
        TumblrClient {
            http: reqwest::Client::new(),
            base_url,
            consumer_key: config.consumer_key,
            consumer_secret: config.consumer_secret,
            oauth_token: config.oauth_token,
            oauth_secret: config.oauth_secret,
            request_limiter: None,
        }
    }

    /// Attaches the process-wide request limiter, matching how the shared
    /// Bluesky client is built.
    pub fn with_request_limiter(mut self, limiter: Arc<RequestLimiter>) -> Self {
        self.request_limiter = Some(limiter);
        self
    }

    /// One page of the authenticated account's likes, newest-first, at the
    /// given `offset` (0 = the newest like).
    pub async fn get_likes(&self, offset: u64, limit: u32) -> Result<LikesPage, TumblrError> {
        let body = self
            .get_signed(
                "user/likes",
                &[("limit", limit.to_string()), ("offset", offset.to_string())],
            )
            .await?;
        LikesPage::from_json(&body)
    }

    /// Performs one GET against `{base_url}/v2/{path}` with an OAuth 1.0a
    /// signed `Authorization` header. Query parameters participate in the
    /// signature, per RFC 5849.
    async fn get_signed(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<serde_json::Value, TumblrError> {
        let _request_permit = match &self.request_limiter {
            Some(limiter) => Some(limiter.acquire().await),
            None => None,
        };

        let mut url = self.base_url.clone();
        url.set_path(&format!("v2/{path}"));
        // The query string is built with the same RFC 3986 encoding the
        // signature uses, so the request on the wire always matches what
        // was signed (`url`'s own query serializer is form-urlencoded,
        // which would encode a space as `+` and break the signature).
        let request_query = query
            .iter()
            .map(|(key, value)| format!("{}={}", oauth_encode(key), oauth_encode(value)))
            .collect::<Vec<_>>()
            .join("&");
        url.set_query(Some(&request_query));

        let timestamp = OffsetDateTime::now_utc().unix_timestamp().to_string();
        let nonce: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(24)
            .map(char::from)
            .collect();

        // Protocol parameters (everything but oauth_signature, which is
        // derived from them), plus the request's query parameters.
        let mut oauth_params = vec![
            (
                "oauth_consumer_key".to_string(),
                self.consumer_key.expose_secret().to_string(),
            ),
            ("oauth_nonce".to_string(), nonce),
            (
                "oauth_signature_method".to_string(),
                "HMAC-SHA1".to_string(),
            ),
            ("oauth_timestamp".to_string(), timestamp),
            (
                "oauth_token".to_string(),
                self.oauth_token.expose_secret().to_string(),
            ),
            ("oauth_version".to_string(), "1.0".to_string()),
        ];
        let mut signing_params = oauth_params.clone();
        signing_params.extend(
            query
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone())),
        );

        // The signature base string's URL is scheme://host[:port]/path with
        // no query or fragment (RFC 5849 §3.4.1.2).
        let base_url = format!("{}{}", url.origin().ascii_serialization(), url.path());
        let signature = oauth_signature(
            "GET",
            &base_url,
            &signing_params,
            self.consumer_secret.expose_secret(),
            self.oauth_secret.expose_secret(),
        );
        oauth_params.push(("oauth_signature".to_string(), signature));

        let response = self
            .http
            .get(url.clone())
            .header(
                reqwest::header::AUTHORIZATION,
                oauth_authorization_header(&oauth_params),
            )
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let retry_after = crate::bluesky::parse_retry_hint(response.headers());
            return Err(TumblrError::Status(status, retry_after));
        }

        let body: serde_json::Value = response.json().await?;
        let meta_status = body
            .get("meta")
            .and_then(|meta| meta.get("status"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        if meta_status != 200 {
            return Err(TumblrError::Api(meta_status));
        }
        Ok(body)
    }
}

// ---------------------------------------------------------------------
// Poller
// ---------------------------------------------------------------------

/// Errors that can end a single Tumblr likes poll pass.
#[derive(Debug, thiserror::Error)]
pub enum TumblrPollError {
    #[error(transparent)]
    Tumblr(#[from] TumblrError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl TumblrPollError {
    /// The server-provided retry hint carried by the underlying error, if
    /// any.
    fn retry_after(&self) -> Option<Duration> {
        match self {
            TumblrPollError::Tumblr(err) => err.retry_after(),
            TumblrPollError::Storage(_) => None,
        }
    }
}

/// Polls the configured Tumblr account's likes on a timer, archiving each
/// new liked post's JSON record and — for posts with Tumblr-hosted media —
/// handing a [`CandidatePost`] off to the shared downloader channel.
pub struct TumblrLikesPoller {
    client: Arc<TumblrClient>,
    store: ArchiveStore,
    sender: CandidatePostSender,
    base_interval: Duration,
    /// Posts requested per `/user/likes` page. [`PAGE_LIMIT`] in
    /// production; tests shrink it to exercise multi-page walks cheaply.
    page_limit: u32,
    /// Whether the next poll pass should walk the entire list with the
    /// dedup boundary disabled. `true` for the first pass after startup, so
    /// a backfill interrupted by a restart (or by the walk failing
    /// partway) resumes from where it stopped instead of stalling behind
    /// the boundary forever.
    first_pass: bool,
}

impl TumblrLikesPoller {
    pub fn new(
        client: Arc<TumblrClient>,
        store: ArchiveStore,
        sender: CandidatePostSender,
        base_interval: Duration,
    ) -> Self {
        Self {
            client,
            store,
            sender,
            base_interval,
            page_limit: PAGE_LIMIT,
            first_pass: true,
        }
    }

    /// Overrides the per-page request size (test-only knob; production
    /// callers use the endpoint maximum via [`Self::new`]).
    #[cfg(test)]
    pub(crate) fn with_page_limit(mut self, page_limit: u32) -> Self {
        self.page_limit = page_limit;
        self
    }

    /// Runs the poll loop forever, mirroring
    /// [`crate::poller::LikesBookmarksPoller::run`]: one pagination pass
    /// per cycle, backing off (with jitter, via the shared [`Backoff`]
    /// policy) after consecutive failures and resetting to `base_interval`
    /// on success, honoring server retry hints and opening a circuit
    /// breaker after too many consecutive failed cycles.
    ///
    /// The first pass after startup walks the entire list with the dedup
    /// boundary disabled (resuming any interrupted backfill and re-ranking
    /// the whole list); every later pass stops at the boundary once a full
    /// window of consecutively already-archived posts is seen.
    pub async fn run(mut self) {
        let mut backoff = Backoff::new(BackoffConfig::new(
            self.base_interval,
            self.base_interval.saturating_mul(32),
        ));
        loop {
            let full_walk = self.first_pass;
            self.first_pass = false;
            let result = self.poll_walk(full_walk).await;
            let delay = match &result {
                Ok(()) => {
                    backoff.on_success();
                    crate::ratelimit::jittered(self.base_interval, 0.2)
                }
                Err(err) => {
                    warn!(error = %err, "tumblr likes poll pass failed");
                    let delay = backoff.on_failure(err.retry_after());
                    if backoff.is_open() {
                        warn!(
                            cooldown_ms = delay.as_millis() as u64,
                            "tumblr likes poller circuit breaker open; pausing well past the normal backoff"
                        );
                    }
                    delay
                }
            };

            debug!(
                delay_ms = delay.as_millis() as u64,
                next_backoff_ms = backoff.current_delay().as_millis() as u64,
                "tumblr likes poller sleeping"
            );
            tokio::time::sleep(delay).await;
        }
    }

    /// One pagination pass over the account's likes, newest-first, stopping
    /// once a full window of consecutively already-archived posts is seen
    /// (the dedup boundary) or the list is exhausted. Every visited post is
    /// ranked with its position in the API's list (0 = newest) and its
    /// `liked_timestamp` is recorded as the action time, mirroring how the
    /// Bluesky likes/bookmarks pollers rank their items for the gallery's
    /// action-time sorts.
    pub async fn poll_likes(&self) -> Result<(), TumblrPollError> {
        self.poll_walk(false).await
    }

    /// The poll pass itself. With `full_walk` set, the dedup boundary is
    /// disabled and already-archived posts are re-ranked with their current
    /// list position — the whole list is walked to its end, mirroring the
    /// Bluesky nightly sweeper's full walks.
    ///
    /// Tumblr's `/user/likes` paginates by `offset` over its *unfiltered*
    /// internal list and then silently drops hidden posts from the response
    /// (observed: pages of 16–19 posts against a `limit` of 20). Two
    /// consequences:
    ///
    /// - a short page is *not* the end of the list — only an empty page is;
    /// - the offset must advance by the requested page size (the window
    ///   width), never by the number of posts returned. Advancing by the
    ///   returned count makes the next window start before the previous one
    ///   ended, re-serving the previous page's last post.
    ///
    /// Even with window-aligned offsets, the list shifts whenever the
    /// account likes something mid-walk, re-serving a post or two at later
    /// page boundaries. A hard "first archived post stops the walk"
    /// boundary would therefore stall any backfill of an actively-used
    /// account, so the boundary instead requires a *full window* of
    /// consecutively already-archived posts — a handful of shifted dups at
    /// a page boundary never trips it. Posts Tumblr hides are unreachable
    /// at any offset and simply never archived. As a belt-and-braces bound
    /// against runaway pagination, the walk also stops once the offset
    /// reaches the API's own `liked_count` total.
    async fn poll_walk(&self, full_walk: bool) -> Result<(), TumblrPollError> {
        let mut offset = 0u64;
        let mut rank: i64 = 0;
        let mut consecutive_archived = 0u32;
        loop {
            let page = self.client.get_likes(offset, self.page_limit).await?;
            info!(
                offset,
                posts = page.posts.len(),
                liked_count = page.liked_count,
                full_walk,
                "tumblr likes page fetched"
            );
            if page.posts.is_empty() {
                return Ok(());
            }

            for post in &page.posts {
                let position = rank;
                rank += 1;
                let Some((key, blog_name, liked_at)) = liked_post_identity(post) else {
                    debug!("tumblr like missing id/blog_name; skipping");
                    continue;
                };
                if self
                    .archive_one(
                        &key,
                        &blog_name,
                        liked_at.as_deref(),
                        post,
                        position,
                        full_walk,
                    )
                    .await?
                {
                    consecutive_archived += 1;
                    if !full_walk && consecutive_archived >= self.page_limit {
                        // A full window of already-archived posts: the walk
                        // has passed all the new content and re-entered
                        // archived territory, so pagination can stop here.
                        debug!(
                            offset,
                            "tumblr likes dedup boundary reached (full window archived)"
                        );
                        return Ok(());
                    }
                } else {
                    consecutive_archived = 0;
                }
            }

            offset += self.page_limit as u64;
            if page.liked_count > 0 && offset >= page.liked_count {
                return Ok(());
            }
        }
    }

    /// Archives one liked post (JSON record, plus a [`CandidatePost`] if it
    /// has Tumblr-hosted media), deduping against the archive. `position`
    /// is the post's rank in the API list this walk is traversing (0 =
    /// newest). Returns `true` if the post was already archived. On a full
    /// walk, an already-archived post is re-ranked with its current list
    /// position; on a boundary walk its recorded rank is left untouched (a
    /// list shift can re-serve it at a bogus position).
    async fn archive_one(
        &self,
        key: &str,
        blog_name: &str,
        liked_at: Option<&str>,
        post: &serde_json::Value,
        position: i64,
        full_walk: bool,
    ) -> Result<bool, TumblrPollError> {
        if self.store.is_archived(Category::TumblrLike, key).await? {
            if full_walk {
                if let Some(liked_at) = liked_at {
                    self.store
                        .set_action_at(Category::TumblrLike, key, liked_at)
                        .await?;
                }
                self.store
                    .set_action_seq(Category::TumblrLike, key, position)
                    .await?;
            }
            return Ok(true);
        }

        let outcome = self
            .store
            .save_post(Category::TumblrLike, key, "", post.clone())
            .await?;

        if outcome == SaveOutcome::Inserted {
            self.enqueue_media(key, blog_name, post).await;
            info!(key = %key, "archived new tumblr like");
        }

        if let Some(liked_at) = liked_at {
            self.store
                .set_action_at(Category::TumblrLike, key, liked_at)
                .await?;
        }
        self.store
            .set_action_seq(Category::TumblrLike, key, position)
            .await?;

        Ok(false)
    }

    /// Extracts a post's Tumblr-hosted media and sends it to the downloader.
    async fn enqueue_media(&self, key: &str, blog_name: &str, post: &serde_json::Value) {
        let media = extract_media_refs(post);
        if media.is_empty() {
            return;
        }
        let candidate = CandidatePost {
            at_uri: key.to_string(),
            cid: String::new(),
            author_did: blog_name.to_string(),
            category: PostCategory::TumblrLike,
            record: post.clone(),
            media,
        };
        if self.sender.send(candidate).await.is_err() {
            warn!(
                key = %key,
                "candidate post channel closed; media downloader not receiving"
            );
        }
    }
}

/// Extracts the poller's identity fields from one liked-post JSON object:
/// the dedup key (`tumblr:{blog_name}/{post_id}`), the posting blog's
/// name, and the like action time (`liked_timestamp`, converted to
/// RFC 3339). `None` when the post carries no usable id or blog name.
fn liked_post_identity(post: &serde_json::Value) -> Option<(String, String, Option<String>)> {
    let blog_name = post
        .get("blog_name")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    // `id_string` carries the exact post id as a string; `id` is the same
    // value as a JSON number. Prefer the string form, fall back to the
    // numeric one.
    let id = post
        .get("id_string")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            post.get("id")
                .and_then(|v| v.as_u64())
                .map(|n| n.to_string())
        })?;
    if blog_name.is_empty() {
        return None;
    }
    let liked_at = post
        .get("liked_timestamp")
        .and_then(|v| v.as_i64())
        .and_then(unix_to_rfc3339);
    Some((format!("tumblr:{blog_name}/{id}"), blog_name, liked_at))
}

/// Converts a Unix timestamp (seconds) to RFC 3339 UTC.
fn unix_to_rfc3339(secs: i64) -> Option<String> {
    OffsetDateTime::from_unix_timestamp(secs)
        .ok()?
        .format(&Rfc3339)
        .ok()
}

/// Whether `url` points at Tumblr-owned media hosting. External embeds
/// (YouTube, Spotify, ...) expose a player URL instead of a media file, so
/// they are not archivable and are excluded.
fn is_tumblr_hosted(url: &str) -> bool {
    url::Url::parse(url)
        .ok()
        .and_then(|url| {
            url.host_str().map(|host| {
                host.eq_ignore_ascii_case("tumblr.com") || host.ends_with(".tumblr.com")
            })
        })
        .unwrap_or(false)
}

/// The HTML fields a Tumblr post can carry inline media in: `body`, the
/// reblog's `comment`/`tree_html`, and every trail entry's `content`/
/// `content_raw`. NPF-era reblogs and text/answer posts carry their images
/// only inside this HTML — never in a `photos` array.
fn post_html_fields(post: &serde_json::Value) -> Vec<&str> {
    let mut fields = Vec::new();
    if let Some(body) = post.get("body").and_then(|v| v.as_str()) {
        fields.push(body);
    }
    if let Some(reblog) = post.get("reblog") {
        for key in ["comment", "tree_html"] {
            if let Some(html) = reblog.get(key).and_then(|v| v.as_str()) {
                fields.push(html);
            }
        }
    }
    if let Some(trail) = post.get("trail").and_then(|v| v.as_array()) {
        for entry in trail {
            for key in ["content", "content_raw"] {
                if let Some(html) = entry.get(key).and_then(|v| v.as_str()) {
                    fields.push(html);
                }
            }
        }
    }
    fields
}

/// Extracts every `<img>` tag's image URL from one HTML string, preferring
/// the largest `srcset` candidate when the tag carries one (Tumblr's HTML
/// embeds a `s640x960`-style `src` plus a `srcset` ladder up to the
/// original resolution).
fn html_image_urls(html: &str) -> Vec<String> {
    let lower = html.to_ascii_lowercase();
    let mut urls = Vec::new();
    let mut cursor = 0usize;
    while let Some(offset) = lower[cursor..].find("<img") {
        let tag_start = cursor + offset;
        let rest = &html[tag_start..];
        let tag_end = rest.find('>').unwrap_or(rest.len());
        let tag = &rest[..tag_end];
        cursor = tag_start + tag_end.max(1);

        let src = tag_attribute(tag, "src");
        let best = tag_attribute(tag, "srcset")
            .and_then(|set| largest_srcset_url(&set))
            .or(src);
        if let Some(url) = best {
            // Attribute values can carry HTML-escaped ampersands.
            urls.push(url.replace("&amp;", "&"));
        }
    }
    urls
}

/// Reads one attribute's quoted value out of a single HTML tag. The lookup
/// is case-insensitive and requires the attribute name to start at a tag
/// boundary (whitespace or the tag's start), so `src` never matches inside
/// `data-src`.
fn tag_attribute(tag: &str, name: &str) -> Option<String> {
    let lower = tag.to_ascii_lowercase();
    let needle = format!("{name}=");
    let mut search_from = 0usize;
    let value_start = loop {
        let pos = lower[search_from..].find(&needle)?;
        let at = search_from + pos;
        let boundary_ok = tag[..at]
            .chars()
            .next_back()
            .is_none_or(|c| c.is_whitespace());
        if boundary_ok {
            break at + needle.len();
        }
        search_from = at + needle.len();
    };

    let rest = &tag[value_start..];
    let quote = rest.chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let value = &rest[1..];
    let end = value.find(quote)?;
    Some(value[..end].to_string())
}

/// Picks the highest-resolution URL from a `srcset` value
/// (`"url1 75w, url2 640w, ..."`), ranking candidates by the pixel area of
/// Tumblr's `sNNNxMMM` size token embedded in its media CDN URLs.
fn largest_srcset_url(srcset: &str) -> Option<String> {
    let mut best: Option<(u64, &str)> = None;
    for candidate in srcset.split(',') {
        let url = candidate.split_whitespace().next().unwrap_or_default();
        if url.is_empty() {
            continue;
        }
        let area = tumblr_url_pixel_area(url).unwrap_or(0);
        if best.is_none_or(|(best_area, _)| area > best_area) {
            best = Some((area, url));
        }
    }
    best.map(|(_, url)| url.to_string())
}

/// The pixel area of the `sNNNxMMM` size token in a Tumblr media CDN URL
/// (`.../s640x960/...`, `.../s75x75_c1/...`). `None` when the URL carries
/// no recognizable size token (ranked lowest by [`largest_srcset_url`]).
fn tumblr_url_pixel_area(url: &str) -> Option<u64> {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    path.split('/').find_map(|segment| {
        let dims = segment.strip_prefix('s')?;
        let dims = dims.split('_').next().unwrap_or(dims);
        let (width, height) = dims.split_once('x')?;
        let width = width.parse::<u64>().ok()?;
        let height = height.parse::<u64>().ok()?;
        Some(width.saturating_mul(height))
    })
}

/// Extracts downloadable media references from one Tumblr post JSON object.
/// Recognizes photo posts (`photos[].original_size.url`, in declaration
/// order), Tumblr-hosted video posts (`video_url`), Tumblr-hosted audio
/// posts (`audio_url`), and — for NPF-era text/reblog/answer posts, which
/// embed their images only inside HTML — every Tumblr-hosted `<img>` in
/// the post's `body`/`reblog`/`trail` HTML. External embeds are excluded —
/// their bytes live on someone else's servers and cannot be archived.
///
/// The same image routinely appears several times in one post (the HTML
/// `src`, every `srcset` ladder entry, the trail's copy of the reblogged
/// content, ...) at different resolutions. Candidates are therefore deduped
/// by their size-token-invariant media key (see
/// [`tumblr_media_key`]), keeping the highest-resolution URL of each group.
pub(crate) fn extract_media_refs(post: &serde_json::Value) -> Vec<MediaRef> {
    // (media key, pixel area for ranking, the ref itself), first-seen order.
    let mut candidates: Vec<(String, u64, MediaRef)> = Vec::new();

    fn push(urls: impl IntoIterator<Item = String>, candidates: &mut Vec<(String, u64, MediaRef)>) {
        for url in urls {
            if !is_tumblr_hosted(&url) {
                continue;
            }
            let key = tumblr_media_key(&url);
            let area = tumblr_url_pixel_area(&url).unwrap_or(0);
            if let Some(existing) = candidates.iter_mut().find(|(k, _, _)| *k == key) {
                if area > existing.1 {
                    // A higher-resolution variant of an already-seen image.
                    existing.1 = area;
                    existing.2.cdn_url = url;
                }
            } else {
                candidates.push((
                    key,
                    area,
                    MediaRef {
                        cdn_url: url,
                        declared_mime_type: None,
                        declared_size_bytes: None,
                    },
                ));
            }
        }
    }

    if let Some(photos) = post.get("photos").and_then(|v| v.as_array()) {
        push(
            photos.iter().filter_map(|photo| {
                photo
                    .get("original_size")
                    .and_then(|original| original.get("url"))
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
            }),
            &mut candidates,
        );
    }

    for (url, mime) in [
        (
            post.get("video_url").and_then(|v| v.as_str()),
            Some("video/mp4".to_string()),
        ),
        (post.get("audio_url").and_then(|v| v.as_str()), None),
    ] {
        let Some(url) = url else { continue };
        if !is_tumblr_hosted(url) {
            continue;
        }
        // Video/audio URLs are unique per post; nothing to rank or dedup.
        candidates.push((
            url.to_string(),
            0,
            MediaRef {
                cdn_url: url.to_string(),
                declared_mime_type: mime,
                declared_size_bytes: None,
            },
        ));
    }

    for html in post_html_fields(post) {
        push(html_image_urls(html), &mut candidates);
    }

    candidates.into_iter().map(|(_, _, ref_)| ref_).collect()
}

/// The variant-invariant identity of a Tumblr media CDN URL.
///
/// Tumblr media URLs have the shape
/// `https://{host}/{imageKey}/{variantHash}/s{WxH}[_cN]/{variantHash2}.{ext}`
/// — the first path segment is the image's stable key, while *everything
/// after it* (including the final filename hash) differs per size variant.
/// Collapsing the URL to `host + first segment` therefore dedups the same
/// image across its `src`, every `srcset` ladder entry, and any repeated
/// appearance in `body`/`trail`, while distinct images (distinct first
/// segments) stay separate. URLs that don't match the shape are their own
/// key.
fn tumblr_media_key(url: &str) -> String {
    match url.split_once("://") {
        Some((scheme, rest)) => match rest.split_once('/') {
            Some((host, path)) => match path.split_once('/') {
                // `{host}/{imageKey}/...` — keep the image key, drop the
                // per-variant rest.
                Some((image_key, _)) => format!("{scheme}://{host}/{image_key}"),
                // A single-segment path (e.g. `avatar_xxx_64.png`) carries
                // no variant structure; the URL itself is the identity.
                None => url.to_string(),
            },
            None => url.to_string(),
        },
        None => url.to_string(),
    }
}

#[cfg(test)]
mod tests;
