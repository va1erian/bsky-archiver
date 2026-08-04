//! Bluesky API client: authentication and REST (XRPC) calls against the
//! `com.atproto.*` / `app.bsky.*` endpoints used for polling posts, likes,
//! and bookmarks.
//!
//! Covers what both the authored-post REST fallback (AR-6) and the
//! likes/bookmarks poller (AR-7) need: session auth (with automatic
//! re-login on a 401), `getAuthorFeed`, `getActorLikes`, and `getBookmarks`.

use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use crate::config::Secret;
use crate::ratelimit::RequestLimiter;

/// Errors produced by calls against the Bluesky HTTP API.
#[derive(Debug, thiserror::Error)]
pub enum BlueskyError {
    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("bluesky api error (status {status}): {body}")]
    Api {
        status: u16,
        body: String,
        /// How long the server asked us to wait before retrying, parsed
        /// from a `Retry-After` or `ratelimit-reset` response header (in
        /// that preference order) when present.
        retry_after: Option<Duration>,
    },
    #[error("failed to authenticate with bluesky: {0}")]
    Auth(String),
}

impl BlueskyError {
    /// The server-provided retry hint, if this error carries one. Feeds
    /// straight into [`crate::ratelimit::Backoff::on_failure`] so callers
    /// honor the server's stated wait over a guessed backoff.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            BlueskyError::Api { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}

/// Parses a retry hint out of response headers, preferring `Retry-After`
/// (seconds, per RFC 9110) over `ratelimit-reset` (a Unix timestamp of when
/// the limit resets) when both are present. Shared with [`crate::media`],
/// which honors the same headers on media CDN responses.
pub(crate) fn parse_retry_hint(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    if let Some(secs) = header_as_u64(headers, "retry-after") {
        return Some(Duration::from_secs(secs));
    }
    if let Some(reset_at) = header_as_u64(headers, "ratelimit-reset") {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        return Some(Duration::from_secs(reset_at.saturating_sub(now)));
    }
    None
}

fn header_as_u64(headers: &reqwest::header::HeaderMap, name: &str) -> Option<u64> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
}

/// A view of an author, as embedded in a [`PostView`].
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct AuthorView {
    pub did: String,
}

/// A hydrated post view, as returned inline by `getAuthorFeed` /
/// `getActorLikes` / `getBookmarks`: the raw record plus a rendered `embed`
/// (with real CDN URLs for any attached media).
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct PostView {
    pub uri: String,
    pub cid: String,
    pub author: AuthorView,
    pub record: serde_json::Value,
    #[serde(default)]
    pub embed: Option<serde_json::Value>,
}

/// One entry in a `getActorLikes` feed page.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct FeedViewPost {
    pub post: PostView,
}

/// A page of `app.bsky.feed.getActorLikes` results.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct LikesPage {
    #[serde(default)]
    pub feed: Vec<FeedViewPost>,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// One entry in a `getBookmarks` page: the bookmarked post.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct BookmarkView {
    pub subject: PostView,
}

/// A page of `app.bsky.bookmark.getBookmarks` results.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct BookmarksPage {
    #[serde(default)]
    pub bookmarks: Vec<BookmarkView>,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// One page of an account's authored feed, as returned by
/// `app.bsky.feed.getAuthorFeed`. Feed items are kept as raw JSON
/// (`app.bsky.feed.defs#feedViewPost`) rather than fully typed, since
/// callers only need `post.uri` / `post.cid` / `post.author.did` /
/// `post.record` / `post.embed`.
#[derive(Debug, Clone, Deserialize)]
pub struct AuthorFeedPage {
    #[serde(default)]
    pub feed: Vec<serde_json::Value>,
    pub cursor: Option<String>,
}

/// The subset of `app.bsky.feed.getFeedGenerator`'s response we use: the
/// generator view's human-readable display name, for UI labelling only.
#[derive(Debug, Clone, Deserialize)]
pub struct FeedGeneratorView {
    #[serde(rename = "displayName")]
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct GetFeedGeneratorResponse {
    view: FeedGeneratorView,
}

#[derive(Debug, Clone, Deserialize)]
struct CreateSessionResponse {
    #[serde(rename = "accessJwt")]
    access_jwt: String,
    did: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ResolveHandleResponse {
    did: String,
}

#[derive(Debug, Clone)]
struct Session {
    access_jwt: String,
    #[allow(dead_code)]
    did: String,
}

/// A thin async client for the subset of the `com.atproto.*` / `app.bsky.*`
/// XRPC surface this project needs. Holds a lazily-established, auto-renewed
/// (on 401) session behind an async lock so callers never have to manage
/// auth themselves.
pub struct BlueskyClient {
    http: reqwest::Client,
    base_url: url::Url,
    identifier: String,
    app_password: Secret,
    session: tokio::sync::RwLock<Option<Session>>,
    /// Process-wide soft cap on requests in flight, shared with the media
    /// downloader. `None` in tests/callers that don't opt in, in which case
    /// requests are never throttled here.
    request_limiter: Option<Arc<RequestLimiter>>,
}

impl BlueskyClient {
    /// Creates a client pointed at `base_url` (the XRPC host, e.g.
    /// `https://bsky.social`), authenticating as `identifier` /
    /// `app_password` on first request.
    pub fn new(base_url: url::Url, identifier: String, app_password: Secret) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url,
            identifier,
            app_password,
            session: tokio::sync::RwLock::new(None),
            request_limiter: None,
        }
    }

    /// Attaches a process-wide request limiter: every request this client
    /// makes will wait for a slot on `limiter` before hitting the network.
    pub fn with_request_limiter(mut self, limiter: Arc<RequestLimiter>) -> Self {
        self.request_limiter = Some(limiter);
        self
    }

    /// Waits for a slot on the shared request limiter, if one is attached.
    async fn throttle(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        match &self.request_limiter {
            Some(limiter) => Some(limiter.acquire().await),
            None => None,
        }
    }

    fn xrpc_url(&self, method: &str) -> url::Url {
        self.base_url
            .join(&format!("xrpc/{method}"))
            .expect("xrpc method names are valid URL path segments")
    }

    /// Logs in unconditionally, overwriting any cached session. Used both
    /// for the first login and to force a fresh session after a 401.
    async fn login(&self) -> Result<String, BlueskyError> {
        let url = self.xrpc_url("com.atproto.server.createSession");
        let _permit = self.throttle().await;
        let response = self
            .http
            .post(url)
            .json(&serde_json::json!({
                "identifier": self.identifier,
                "password": self.app_password.expose_secret(),
            }))
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(BlueskyError::Auth(format!(
                "createSession failed with status {status}: {body}"
            )));
        }

        let parsed: CreateSessionResponse = response.json().await?;
        let access_jwt = parsed.access_jwt.clone();
        *self.session.write().await = Some(Session {
            access_jwt: parsed.access_jwt,
            did: parsed.did,
        });
        Ok(access_jwt)
    }

    /// Returns a valid access token, logging in first if there is no
    /// session yet.
    async fn access_token(&self) -> Result<String, BlueskyError> {
        if let Some(session) = self.session.read().await.as_ref() {
            return Ok(session.access_jwt.clone());
        }
        self.login().await
    }

    /// Performs an authenticated GET against `method` with the given query
    /// parameters, retrying once with a fresh session on a 401.
    async fn get_authenticated<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        query: &[(&str, String)],
    ) -> Result<T, BlueskyError> {
        let url = self.xrpc_url(method);
        let mut access_token = self.access_token().await?;

        let mut response = {
            let _permit = self.throttle().await;
            self.http
                .get(url.clone())
                .bearer_auth(&access_token)
                .query(query)
                .send()
                .await?
        };

        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            access_token = self.login().await?;
            let _permit = self.throttle().await;
            response = self
                .http
                .get(url)
                .bearer_auth(&access_token)
                .query(query)
                .send()
                .await?;
        }

        let status = response.status();
        if !status.is_success() {
            let retry_after = parse_retry_hint(response.headers());
            let body = response.text().await.unwrap_or_default();
            return Err(BlueskyError::Api {
                status: status.as_u16(),
                body,
                retry_after,
            });
        }

        Ok(response.json().await?)
    }

    /// Fetches one page of `actor`'s authored feed
    /// (`app.bsky.feed.getAuthorFeed`), newest-first, optionally continuing
    /// from a previous page's `cursor`.
    pub async fn get_author_feed(
        &self,
        actor: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<AuthorFeedPage, BlueskyError> {
        let mut query = vec![("actor", actor.to_string()), ("limit", limit.to_string())];
        if let Some(cursor) = cursor {
            query.push(("cursor", cursor.to_string()));
        }
        self.get_authenticated("app.bsky.feed.getAuthorFeed", &query)
            .await
    }

    /// Fetches one page of a custom feed (`app.bsky.feed.getFeed`),
    /// newest-first, optionally continuing from a previous page's `cursor`.
    /// `feed` is the generator's `at://` URI. Returns the same hydrated
    /// `feedViewPost` shape as `getAuthorFeed`, so the shared media-detection
    /// and extraction paths apply unchanged.
    pub async fn get_feed(
        &self,
        feed: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<AuthorFeedPage, BlueskyError> {
        let mut query = vec![("feed", feed.to_string()), ("limit", limit.to_string())];
        if let Some(cursor) = cursor {
            query.push(("cursor", cursor.to_string()));
        }
        self.get_authenticated("app.bsky.feed.getFeed", &query)
            .await
    }

    /// Fetches a feed generator's human-readable display name via
    /// `app.bsky.feed.getFeedGenerator`. `None` if the generator returns no
    /// display name. Used once at startup for UI labelling; a failure here is
    /// non-fatal to the caller.
    pub async fn get_feed_generator(&self, feed: &str) -> Result<Option<String>, BlueskyError> {
        let response: GetFeedGeneratorResponse = self
            .get_authenticated(
                "app.bsky.feed.getFeedGenerator",
                &[("feed", feed.to_string())],
            )
            .await?;
        Ok(response.view.display_name)
    }

    /// Fetches one page of the watched account's likes, newest-first.
    pub async fn get_actor_likes(
        &self,
        actor: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<LikesPage, BlueskyError> {
        let mut query = vec![("actor", actor.to_string()), ("limit", limit.to_string())];
        if let Some(cursor) = cursor {
            query.push(("cursor", cursor.to_string()));
        }
        self.get_authenticated("app.bsky.feed.getActorLikes", &query)
            .await
    }

    /// Fetches one page of the watched account's bookmarks, newest-first.
    pub async fn get_bookmarks(
        &self,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<BookmarksPage, BlueskyError> {
        let mut query = vec![("limit", limit.to_string())];
        if let Some(cursor) = cursor {
            query.push(("cursor", cursor.to_string()));
        }
        self.get_authenticated("app.bsky.bookmark.getBookmarks", &query)
            .await
    }

    /// Authenticates immediately, returning the authenticated account's own
    /// DID. Used at startup to fail fast on bad credentials rather than
    /// discovering the problem on the first real API call.
    pub async fn authenticate(&self) -> Result<String, BlueskyError> {
        self.login().await?;
        let session = self.session.read().await;
        Ok(session
            .as_ref()
            .expect("login() just populated the session")
            .did
            .clone())
    }

    /// Resolves `handle` (a Bluesky handle, e.g. `alice.bsky.social`) to its
    /// DID via `com.atproto.identity.resolveHandle`.
    pub async fn resolve_handle(&self, handle: &str) -> Result<String, BlueskyError> {
        let response: ResolveHandleResponse = self
            .get_authenticated(
                "com.atproto.identity.resolveHandle",
                &[("handle", handle.to_string())],
            )
            .await?;
        Ok(response.did)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn client(server: &MockServer) -> BlueskyClient {
        BlueskyClient::new(
            url::Url::parse(&server.uri()).unwrap(),
            "alice.bsky.social".to_string(),
            Secret::from("app-password".to_string()),
        )
    }

    async fn mock_session(server: &MockServer) {
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "accessJwt": "token-1",
                "refreshJwt": "refresh-1",
                "did": "did:plc:alice",
                "handle": "alice.bsky.social",
            })))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn logs_in_lazily_and_fetches_author_feed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "accessJwt": "token-1",
                "refreshJwt": "refresh-1",
                "did": "did:plc:alice",
                "handle": "alice.bsky.social",
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getAuthorFeed"))
            .and(query_param("actor", "alice.bsky.social"))
            .and(header("authorization", "Bearer token-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "feed": [],
                "cursor": null,
            })))
            .mount(&server)
            .await;

        let client = client(&server);
        let page = client
            .get_author_feed("alice.bsky.social", None, 50)
            .await
            .expect("get author feed");
        assert!(page.feed.is_empty());
        assert_eq!(page.cursor, None);

        // A second call reuses the cached session: still only one login.
        client
            .get_author_feed("alice.bsky.social", None, 50)
            .await
            .expect("get author feed again");
    }

    #[tokio::test]
    async fn cursor_is_forwarded_on_subsequent_page() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getAuthorFeed"))
            .and(query_param("cursor", "page-2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "feed": [],
                "cursor": null,
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = client(&server);
        client
            .get_author_feed("alice.bsky.social", Some("page-2"), 50)
            .await
            .expect("get author feed with cursor");
    }

    #[tokio::test]
    async fn get_actor_likes_authenticates_and_parses_page() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getActorLikes"))
            .and(query_param("actor", "did:plc:alice"))
            .and(header("authorization", "Bearer token-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "feed": [{
                    "post": {
                        "uri": "at://did:plc:bob/app.bsky.feed.post/1",
                        "cid": "cid-1",
                        "author": {"did": "did:plc:bob"},
                        "record": {"text": "hi"},
                    }
                }],
                "cursor": "next-cursor",
            })))
            .mount(&server)
            .await;

        let client = client(&server);
        let page = client
            .get_actor_likes("did:plc:alice", None, 50)
            .await
            .expect("get_actor_likes should succeed");

        assert_eq!(page.feed.len(), 1);
        assert_eq!(
            page.feed[0].post.uri,
            "at://did:plc:bob/app.bsky.feed.post/1"
        );
        assert_eq!(page.cursor.as_deref(), Some("next-cursor"));
    }

    #[tokio::test]
    async fn get_bookmarks_authenticates_and_parses_page() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
            .and(header("authorization", "Bearer token-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "bookmarks": [{
                    "subject": {
                        "uri": "at://did:plc:carol/app.bsky.feed.post/2",
                        "cid": "cid-2",
                        "author": {"did": "did:plc:carol"},
                        "record": {"text": "bookmarked"},
                    }
                }],
                "cursor": null,
            })))
            .mount(&server)
            .await;

        let client = client(&server);
        let page = client
            .get_bookmarks(None, 50)
            .await
            .expect("get_bookmarks should succeed");

        assert_eq!(page.bookmarks.len(), 1);
        assert_eq!(
            page.bookmarks[0].subject.uri,
            "at://did:plc:carol/app.bsky.feed.post/2"
        );
        assert_eq!(page.cursor, None);
    }

    #[tokio::test]
    async fn expired_session_triggers_relogin_and_retry() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "accessJwt": "token-1",
                "refreshJwt": "refresh-1",
                "did": "did:plc:alice",
                "handle": "alice.bsky.social",
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;

        // Second login (after the 401) returns a fresh token.
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "accessJwt": "token-2",
                "refreshJwt": "refresh-2",
                "did": "did:plc:alice",
                "handle": "alice.bsky.social",
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getActorLikes"))
            .and(header("authorization", "Bearer token-1"))
            .respond_with(ResponseTemplate::new(401).set_body_json(json!({
                "error": "ExpiredToken",
                "message": "token expired",
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getActorLikes"))
            .and(header("authorization", "Bearer token-2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "feed": [],
                "cursor": null,
            })))
            .mount(&server)
            .await;

        let client = client(&server);
        let page = client
            .get_actor_likes("did:plc:alice", None, 50)
            .await
            .expect("should succeed after relogin");
        assert!(page.feed.is_empty());
    }

    #[tokio::test]
    async fn non_success_status_maps_to_api_error() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
            .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
            .mount(&server)
            .await;

        let client = client(&server);
        let err = client
            .get_bookmarks(None, 50)
            .await
            .expect_err("should surface api error");
        match err {
            BlueskyError::Api { status, .. } => assert_eq!(status, 500),
            other => panic!("expected Api error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn login_failure_produces_auth_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(401).set_body_json(json!({
                "error": "AuthenticationRequired",
                "message": "invalid credentials",
            })))
            .mount(&server)
            .await;

        let client = client(&server);
        let err = client
            .get_actor_likes("did:plc:alice", None, 50)
            .await
            .expect_err("should fail to authenticate");
        assert!(matches!(err, BlueskyError::Auth(_)));
    }

    #[tokio::test]
    async fn malformed_login_response_does_not_panic() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"unexpected": true})))
            .mount(&server)
            .await;

        let client = client(&server);
        let err = client
            .get_author_feed("alice.bsky.social", None, 50)
            .await
            .expect_err("missing accessJwt should fail cleanly");
        assert!(matches!(err, BlueskyError::Http(_)));
    }

    #[tokio::test]
    async fn authenticate_returns_own_did() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        let client = client(&server);
        let did = client.authenticate().await.expect("authenticate");
        assert_eq!(did, "did:plc:alice");
    }

    #[tokio::test]
    async fn authenticate_fails_fast_on_bad_credentials() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(401).set_body_json(json!({
                "error": "AuthenticationRequired",
                "message": "invalid credentials",
            })))
            .mount(&server)
            .await;

        let client = client(&server);
        let err = client
            .authenticate()
            .await
            .expect_err("bad credentials should fail authenticate");
        assert!(matches!(err, BlueskyError::Auth(_)));
    }

    #[tokio::test]
    async fn rate_limited_response_surfaces_retry_after_hint() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "30")
                    .set_body_string("slow down"),
            )
            .mount(&server)
            .await;

        let client = client(&server);
        let err = client
            .get_bookmarks(None, 50)
            .await
            .expect_err("should surface api error");
        match err {
            BlueskyError::Api {
                status,
                retry_after,
                ..
            } => {
                assert_eq!(status, 429);
                assert_eq!(retry_after, Some(Duration::from_secs(30)));
            }
            other => panic!("expected Api error, got {other:?}"),
        }
        assert_eq!(err.retry_after(), Some(Duration::from_secs(30)));
    }

    #[tokio::test]
    async fn retry_after_takes_precedence_over_ratelimit_reset() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "5")
                    .insert_header("ratelimit-reset", "9999999999"),
            )
            .mount(&server)
            .await;

        let client = client(&server);
        let err = client
            .get_bookmarks(None, 50)
            .await
            .expect_err("should surface api error");
        assert_eq!(err.retry_after(), Some(Duration::from_secs(5)));
    }

    #[tokio::test]
    async fn resolve_handle_returns_did() {
        let server = MockServer::start().await;
        mock_session(&server).await;
        Mock::given(method("GET"))
            .and(path("/xrpc/com.atproto.identity.resolveHandle"))
            .and(query_param("handle", "bob.bsky.social"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"did": "did:plc:bob"})))
            .mount(&server)
            .await;

        let client = client(&server);
        let did = client
            .resolve_handle("bob.bsky.social")
            .await
            .expect("resolve_handle");
        assert_eq!(did, "did:plc:bob");
    }
}
