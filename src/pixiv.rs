//! Pixiv bookmarks archiving: an OAuth2 client for Pixiv's unofficial App
//! API ([`PixivClient`]) and the poller ([`PixivBookmarksPoller`]) that
//! walks the configured account's bookmarked illustrations, archiving each
//! one's JSON record and handing the image files off to the shared
//! downloader channel.
//!
//! This is the Pixiv counterpart of [`crate::tumblr`]'s likes poller: the
//! App API is undocumented and unofficial (it is what Pixiv's own Android
//! app speaks), so endpoints and auth can change without notice. Auth uses
//! a long-lived refresh token (`PIXIV_REFRESH_TOKEN`, obtained once via
//! the documented PKCE login flow) exchanged for a short-lived (~1 hour)
//! access token, refreshed automatically here.
//!
//! Media lives on `i.pximg.net`, which rejects requests without a
//! `Referer: https://www.pixiv.net/` header — the media refs produced here
//! carry that requirement, and [`crate::media`]'s downloader sets it for
//! Pixiv URLs.

use std::sync::Arc;
use std::time::Duration;

use time::OffsetDateTime;
use tracing::{debug, info, warn};
use url::Url;

use crate::config::Secret;
use crate::pipeline::{CandidatePost, CandidatePostSender, MediaRef, PostCategory};
use crate::ratelimit::{Backoff, BackoffConfig, RequestLimiter};
use crate::storage::{ArchiveStore, Category, SaveOutcome, StorageError};

/// The production Pixiv App API entryway. Not part of the canonical env var
/// schema; overridable only for tests, which point the client at a
/// `wiremock` server instead.
pub const DEFAULT_PIXIV_APP_BASE_URL: &str = "https://app-api.pixiv.net";

/// The production Pixiv OAuth token endpoint host.
pub const DEFAULT_PIXIV_OAUTH_BASE_URL: &str = "https://oauth.secure.pixiv.net";

/// The Android app client id/secret pair the App API expects. These are
/// not user secrets — they are the public, hardcoded credentials of
/// Pixiv's own app (same values every community client ships).
const CLIENT_ID: &str = "MOBrBDS8blbauoSck0ZfDbtuzpyT";
const CLIENT_SECRET: &str = "lsACyCD94FhDUtGTXi3QzcFE2uU1hqtDaKeqrdwj";

/// The hash secret used to derive `X-Client-Hash` on the token endpoint
/// (MD5 of `X-Client-Time` + this). Also a public app constant, not a user
/// secret.
const HASH_SECRET: &str = "28c1fdd170a5204386cb1313c7077b34f83e4aaf4aa829ce78c231e05b0bae2c";

/// The User-Agent Pixiv's Android app sends; the App API rejects requests
/// without a plausible app UA.
const USER_AGENT: &str = "PixivAndroidApp/5.0.234 (Android 11; Pixel 5)";

/// Posts requested per `/v1/user/bookmarks/illust` call — the endpoint's
/// fixed page size.
pub const PAGE_LIMIT: u32 = 30;

/// Delay inserted between consecutive bookmark-list page fetches within a
/// single walk. The unofficial App API has no documented rate limit but
/// tolerates only ~1 request/second; 2s per page keeps a sustained
/// backfill comfortably under that.
const PAGE_DELAY: Duration = Duration::from_secs(2);

// ---------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------

/// Errors from the Pixiv API client.
#[derive(Debug, thiserror::Error)]
pub enum PixivError {
    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("pixiv api returned http status {0}")]
    Status(reqwest::StatusCode, Option<Duration>),
    #[error("pixiv auth failed: {0}")]
    Auth(String),
    #[error("unexpected pixiv response shape: {0}")]
    Shape(&'static str),
}

impl PixivError {
    /// A server-provided retry hint (`Retry-After`/`ratelimit-reset`), if
    /// this failure carries one.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            PixivError::Status(_, hint) => *hint,
            _ => None,
        }
    }
}

/// One page of the account's bookmarked illustrations, newest-first.
pub struct BookmarksPage {
    /// Raw bookmarked-illustration JSON objects, in the API's own
    /// (newest-first) order.
    pub illusts: Vec<serde_json::Value>,
    /// The `max_bookmark_id` cursor for the next (older) page, extracted
    /// from the API's `next_url`; `None` at the end of the list.
    pub next_cursor: Option<String>,
}

impl BookmarksPage {
    fn from_json(body: &serde_json::Value) -> Result<Self, PixivError> {
        let illusts = body
            .get("illusts")
            .and_then(|v| v.as_array())
            .ok_or(PixivError::Shape("response has no `illusts` array"))?
            .clone();
        let next_cursor = body
            .get("next_url")
            .and_then(|v| v.as_str())
            .and_then(next_cursor_from_url);
        Ok(BookmarksPage {
            illusts,
            next_cursor,
        })
    }
}

/// Extracts the `max_bookmark_id` query parameter from a `next_url`. The
/// API hands back the full next-page URL; the cursor is the only part the
/// poller needs to carry forward.
fn next_cursor_from_url(next_url: &str) -> Option<String> {
    let url = Url::parse(next_url).ok()?;
    url.query_pairs()
        .find(|(key, _)| key == "max_bookmark_id")
        .map(|(_, value)| value.into_owned())
}

/// A Pixiv App API client: exchanges the configured refresh token for a
/// short-lived access token (refreshing lazily on expiry) and issues
/// Bearer-authenticated App API requests.
pub struct PixivClient {
    http: reqwest::Client,
    app_base_url: Url,
    oauth_base_url: Url,
    refresh_token: Secret,
    /// Cached access token and its expiry. `None` until the first request
    /// triggers a refresh.
    access: tokio::sync::RwLock<Option<Access>>,
    /// Process-wide soft cap on requests in flight, shared with the
    /// Bluesky/Tumblr clients and the media downloader. `None` in
    /// tests/callers that don't opt in.
    request_limiter: Option<Arc<RequestLimiter>>,
}

#[derive(Clone)]
struct Access {
    token: Secret,
    expires_at: OffsetDateTime,
    /// The authenticated account's Pixiv user id, from the token
    /// response's `user` object (needed because the bookmarks endpoint is
    /// keyed by `user_id`).
    user_id: String,
}

impl PixivClient {
    pub fn new(app_base_url: Url, oauth_base_url: Url, refresh_token: Secret) -> Self {
        PixivClient {
            http: reqwest::Client::new(),
            app_base_url,
            oauth_base_url,
            refresh_token,
            access: tokio::sync::RwLock::new(None),
            request_limiter: None,
        }
    }

    /// Attaches the process-wide request limiter, matching how the other
    /// API clients are built.
    pub fn with_request_limiter(mut self, limiter: Arc<RequestLimiter>) -> Self {
        self.request_limiter = Some(limiter);
        self
    }

    /// One page of the account's public bookmarks, newest-first. `cursor`
    /// is the previous page's `max_bookmark_id`; `None` fetches the
    /// newest page.
    pub async fn get_bookmarks(
        &self,
        user_id: u64,
        cursor: Option<&str>,
    ) -> Result<BookmarksPage, PixivError> {
        let token = self.access_token().await?;
        let mut query = vec![
            ("user_id", user_id.to_string()),
            ("restrict", "public".to_string()),
            ("filter", "for_ios".to_string()),
        ];
        if let Some(cursor) = cursor {
            query.push(("max_bookmark_id", cursor.to_string()));
        }
        let body = self
            .get_signed("v1/user/bookmarks/illust", &query, &token)
            .await?;
        BookmarksPage::from_json(&body)
    }

    /// The Pixiv user id of the authenticated account, from the token
    /// response's `user` object. Needed because the bookmarks endpoint is
    /// keyed by `user_id`.
    pub async fn user_id(&self) -> Result<u64, PixivError> {
        let token = self.refresh().await?;
        token
            .user_id
            .parse::<u64>()
            .map_err(|_| PixivError::Shape("auth response user.id is not numeric"))
    }

    /// Returns a valid access token, refreshing it if none is cached or
    /// the cached one is at/near expiry.
    async fn access_token(&self) -> Result<Secret, PixivError> {
        let cached = self.access.read().await.clone();
        if let Some(access) = cached
            && access.expires_at > OffsetDateTime::now_utc() + Duration::from_secs(60)
        {
            return Ok(access.token);
        }
        let fresh = self.refresh().await?;
        Ok(fresh.token)
    }
    /// Exchanges the refresh token for a fresh access token and caches it.
    async fn refresh(&self) -> Result<Access, PixivError> {
        let fresh = self.request_token().await?;
        *self.access.write().await = Some(fresh.clone());
        Ok(fresh)
    }

    /// Performs the token-endpoint exchange. The X-Client-Time/Hash pair
    /// (MD5 of the timestamp + the app's hash secret) is what the token
    /// endpoint actually validates; the App API endpoints don't need them.
    async fn request_token(&self) -> Result<Access, PixivError> {
        let _request_permit = match &self.request_limiter {
            Some(limiter) => Some(limiter.acquire().await),
            None => None,
        };

        // `2021-01-01T00:00:00+00:00` style, per the app's own format.
        let now = OffsetDateTime::now_utc();
        let client_time = format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}+00:00",
            now.year(),
            u8::from(now.month()),
            now.day(),
            now.hour(),
            now.minute(),
            now.second()
        );
        let client_hash = md5_hex(&format!("{client_time}{HASH_SECRET}"));

        let mut url = self.oauth_base_url.clone();
        url.set_path("auth/token");
        let response = self
            .http
            .post(url)
            .header(reqwest::header::USER_AGENT, USER_AGENT)
            .header("X-Client-Time", &client_time)
            .header("X-Client-Hash", client_hash)
            .form(&[
                ("client_id", CLIENT_ID),
                ("client_secret", CLIENT_SECRET),
                ("grant_type", "refresh_token"),
                ("refresh_token", self.refresh_token.expose_secret()),
                ("include_policy", "true"),
                ("get_secure_url", "1"),
            ])
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let retry_after = crate::bluesky::parse_retry_hint(response.headers());
            return Err(PixivError::Status(status, retry_after));
        }

        let body: serde_json::Value = response.json().await?;
        if body.get("has_error").and_then(|v| v.as_bool()) == Some(true) {
            let message = body
                .pointer("/error/message")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            return Err(PixivError::Auth(message.to_string()));
        }

        let access_token = body
            .pointer("/response/access_token")
            .and_then(|v| v.as_str())
            .ok_or(PixivError::Shape("auth response has no access_token"))?;
        let expires_in = body
            .pointer("/response/expires_in")
            .and_then(|v| v.as_i64())
            .unwrap_or(3600);
        let user_id = body
            .pointer("/response/user/id")
            .and_then(|v| v.as_str())
            .ok_or(PixivError::Shape("auth response has no user.id"))?
            .to_string();

        Ok(Access {
            token: Secret::from(access_token.to_string()),
            expires_at: now + Duration::from_secs(expires_in.max(0) as u64),
            user_id,
        })
    }

    /// One GET against `{app_base_url}/{path}` with a Bearer token.
    async fn get_signed(
        &self,
        path: &str,
        query: &[(&str, String)],
        token: &Secret,
    ) -> Result<serde_json::Value, PixivError> {
        let _request_permit = match &self.request_limiter {
            Some(limiter) => Some(limiter.acquire().await),
            None => None,
        };

        let mut url = self.app_base_url.clone();
        url.set_path(path);
        {
            let mut pairs = url.query_pairs_mut();
            pairs.extend_pairs(query.iter().map(|(key, value)| (*key, value.as_str())));
        }

        let response = self
            .http
            .get(url)
            .header(reqwest::header::USER_AGENT, USER_AGENT)
            .header("App-OS", "android")
            .header("App-OS-Version", "11")
            .bearer_auth(token.expose_secret())
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let retry_after = crate::bluesky::parse_retry_hint(response.headers());
            return Err(PixivError::Status(status, retry_after));
        }

        Ok(response.json().await?)
    }
}

/// Lowercase-hex MD5 of `input`, for the token endpoint's
/// `X-Client-Hash` header.
fn md5_hex(input: &str) -> String {
    use md5::Digest as _;
    let digest = md5::Md5::digest(input.as_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

// ---------------------------------------------------------------------
// Poller
// ---------------------------------------------------------------------

/// Errors that can end a single Pixiv bookmarks poll pass.
#[derive(Debug, thiserror::Error)]
pub enum PixivPollError {
    #[error(transparent)]
    Pixiv(#[from] PixivError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl PixivPollError {
    /// The server-provided retry hint carried by the underlying error, if
    /// any.
    fn retry_after(&self) -> Option<Duration> {
        match self {
            PixivPollError::Pixiv(err) => err.retry_after(),
            PixivPollError::Storage(_) => None,
        }
    }
}

/// Polls the configured Pixiv account's bookmarks on a timer, archiving
/// each new bookmarked illustration's JSON record and handing its image
/// files (originals, every page of a multi-page work) to the shared
/// downloader channel.
pub struct PixivBookmarksPoller {
    client: Arc<PixivClient>,
    store: ArchiveStore,
    sender: CandidatePostSender,
    base_interval: Duration,
    /// Delay between consecutive page fetches within one walk
    /// ([`PAGE_DELAY`] in production; tests zero it out).
    page_delay: Duration,
    /// The authenticated account's Pixiv user id, resolved once from the
    /// first token refresh. `RwLock` because the poller is driven through
    /// `&self` by the supervisor's restart loop.
    user_id: tokio::sync::RwLock<Option<u64>>,
    /// Whether the next poll pass should walk the entire list with the
    /// dedup boundary disabled. `true` for the first pass after startup,
    /// so a backfill interrupted by a restart resumes from where it
    /// stopped.
    first_pass: bool,
}

impl PixivBookmarksPoller {
    pub fn new(
        client: Arc<PixivClient>,
        store: ArchiveStore,
        sender: CandidatePostSender,
        base_interval: Duration,
    ) -> Self {
        Self {
            client,
            store,
            sender,
            base_interval,
            page_delay: PAGE_DELAY,
            user_id: tokio::sync::RwLock::new(None),
            first_pass: true,
        }
    }

    /// Overrides the inter-page delay (test-only knob; production callers
    /// use [`PAGE_DELAY`] via [`Self::new`]).
    #[cfg(test)]
    pub(crate) fn with_page_delay(mut self, page_delay: Duration) -> Self {
        self.page_delay = page_delay;
        self
    }

    /// Runs the poll loop forever, mirroring [`crate::tumblr`]'s
    /// `TumblrLikesPoller::run`: one pagination pass per cycle, backing off
    /// (with jitter, via the shared [`Backoff`] policy) after consecutive
    /// failures and resetting to `base_interval` on success, honoring
    /// server retry hints and opening a circuit breaker after too many
    /// consecutive failed cycles.
    ///
    /// The first pass after startup walks the entire list with the dedup
    /// boundary disabled (resuming any interrupted backfill and re-ranking
    /// the whole list); every later pass stops at the boundary once a full
    /// page of consecutively already-archived posts is seen.
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
                    warn!(error = %err, "pixiv bookmarks poll pass failed");
                    let delay = backoff.on_failure(err.retry_after());
                    if backoff.is_open() {
                        warn!(
                            cooldown_ms = delay.as_millis() as u64,
                            "pixiv bookmarks poller circuit breaker open; pausing well past the normal backoff"
                        );
                    }
                    delay
                }
            };

            debug!(
                delay_ms = delay.as_millis() as u64,
                next_backoff_ms = backoff.current_delay().as_millis() as u64,
                "pixiv bookmarks poller sleeping"
            );
            tokio::time::sleep(delay).await;
        }
    }

    /// One pagination pass over the account's bookmarks, newest-first,
    /// stopping once a full page of consecutively already-archived posts
    /// is seen (the dedup boundary) or the list is exhausted. Every
    /// visited post is ranked with its position in the API's list
    /// (0 = newest) and its `create_date` recorded, mirroring how the
    /// Bluesky/Tumblr pollers rank their items for the gallery's
    /// action-time sorts.
    ///
    /// Pagination follows the API's own `next_url` cursor
    /// (`max_bookmark_id`, a server-side bookmark-timestamp cursor — not
    /// an offset). New bookmarks arriving mid-walk shift the list exactly
    /// like Tumblr's, so the dedup boundary requires a *full page* of
    /// consecutively already-archived posts; a lone shifted duplicate
    /// never trips it.
    async fn poll_walk(&self, full_walk: bool) -> Result<(), PixivPollError> {
        let user_id = {
            let cached = *self.user_id.read().await;
            match cached {
                Some(user_id) => user_id,
                None => {
                    let user_id = self.client.user_id().await?;
                    info!(user_id, "resolved pixiv user id from auth response");
                    *self.user_id.write().await = Some(user_id);
                    user_id
                }
            }
        };

        let mut cursor: Option<String> = None;
        let mut rank: i64 = 0;
        let mut consecutive_archived = 0u32;
        loop {
            let page = self
                .client
                .get_bookmarks(user_id, cursor.as_deref())
                .await?;
            info!(
                posts = page.illusts.len(),
                has_next = page.next_cursor.is_some(),
                full_walk,
                "pixiv bookmarks page fetched"
            );
            if page.illusts.is_empty() {
                return Ok(());
            }

            for illust in &page.illusts {
                let position = rank;
                rank += 1;
                let Some((key, user_name)) = bookmark_identity(illust) else {
                    debug!("pixiv bookmark missing id/user; skipping");
                    continue;
                };
                if self
                    .archive_one(&key, &user_name, illust, position, full_walk)
                    .await?
                {
                    consecutive_archived += 1;
                    if !full_walk && consecutive_archived >= PAGE_LIMIT {
                        // A full page of already-archived posts: the walk
                        // has passed all the new content and re-entered
                        // archived territory, so pagination can stop here.
                        debug!("pixiv bookmarks dedup boundary reached (full page archived)");
                        return Ok(());
                    }
                } else {
                    consecutive_archived = 0;
                }
            }

            match page.next_cursor {
                Some(next) => {
                    cursor = Some(next);
                    // Pace multi-page walks (see [`PAGE_DELAY`]).
                    tokio::time::sleep(self.page_delay).await;
                }
                None => return Ok(()),
            }
        }
    }

    /// Archives one bookmarked illustration (JSON record, plus a
    /// [`CandidatePost`] if it has downloadable media), deduping against
    /// the archive. `position` is the post's rank in the API list this
    /// walk is traversing (0 = newest). Returns `true` if the post was
    /// already archived. On a full walk, an already-archived post is
    /// re-ranked with its current list position; on a boundary walk its
    /// recorded rank is left untouched (a list shift can re-serve it at a
    /// bogus position).
    async fn archive_one(
        &self,
        key: &str,
        user_name: &str,
        illust: &serde_json::Value,
        position: i64,
        full_walk: bool,
    ) -> Result<bool, PixivPollError> {
        if self.store.is_archived(Category::PixivBookmark, key).await? {
            if full_walk {
                self.store
                    .set_action_seq(Category::PixivBookmark, key, position)
                    .await?;
            }
            return Ok(true);
        }

        let outcome = self
            .store
            .save_post(Category::PixivBookmark, key, "", illust.clone())
            .await?;

        if outcome == SaveOutcome::Inserted {
            self.enqueue_media(key, user_name, illust).await;
            info!(key = %key, "archived new pixiv bookmark");
        }

        self.store
            .set_action_seq(Category::PixivBookmark, key, position)
            .await?;

        Ok(false)
    }

    /// Extracts an illustration's downloadable media (the original URL for
    /// single-page works, every page's original for multi-page works) and
    /// sends them to the downloader.
    async fn enqueue_media(&self, key: &str, user_name: &str, illust: &serde_json::Value) {
        let media = extract_media_refs(illust);
        if media.is_empty() {
            return;
        }
        let candidate = CandidatePost {
            at_uri: key.to_string(),
            cid: String::new(),
            author_did: user_name.to_string(),
            category: PostCategory::PixivBookmark,
            record: illust.clone(),
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

/// Extracts the poller's identity fields from one bookmarked-illustration
/// JSON object: the dedup key (`pixiv:{user-id}/{illust-id}`) and the
/// posting user's display name. `None` when the object carries no usable
/// id or user.
fn bookmark_identity(illust: &serde_json::Value) -> Option<(String, String)> {
    let illust_id = illust
        .get("id")
        .and_then(|v| v.as_u64())
        .map(|n| n.to_string())
        .or_else(|| {
            illust
                .get("id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })?;
    let user_id = illust
        .pointer("/user/id")
        .and_then(|v| v.as_u64())
        .map(|n| n.to_string())
        .or_else(|| {
            illust
                .pointer("/user/id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })?;
    let user_name = illust
        .pointer("/user/name")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    Some((format!("pixiv:{user_id}/{illust_id}"), user_name))
}

/// Extracts downloadable media references from one bookmarked-illustration
/// JSON object. For single-page works that's `meta_single_page.
/// original_image_url`; for multi-page works it's every
/// `meta_pages[].image_urls.original`. Ugoira (animated) works carry no
/// original image URL — their frames ship as a zip + frame-delay metadata
/// through a separate endpoint, which this archiver does not (yet)
/// assemble; they are recorded without media. All URLs point at
/// `i.pximg.net`, which requires a `Referer: https://www.pixiv.net/`
/// header — the media downloader supplies it for this host.
pub(crate) fn extract_media_refs(illust: &serde_json::Value) -> Vec<MediaRef> {
    let mut refs = Vec::new();

    if let Some(url) = illust
        .pointer("/meta_single_page/original_image_url")
        .and_then(|v| v.as_str())
    {
        refs.push(MediaRef {
            cdn_url: url.to_string(),
            declared_mime_type: None,
            declared_size_bytes: None,
        });
    }

    if let Some(pages) = illust.get("meta_pages").and_then(|v| v.as_array()) {
        for page in pages {
            if let Some(url) = page
                .pointer("/image_urls/original")
                .and_then(|v| v.as_str())
            {
                refs.push(MediaRef {
                    cdn_url: url.to_string(),
                    declared_mime_type: None,
                    declared_size_bytes: None,
                });
            }
        }
    }

    refs
}

#[cfg(test)]
mod tests;
