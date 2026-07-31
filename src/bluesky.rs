//! Bluesky API client: authentication and REST (XRPC) calls against the
//! `com.atproto.*` / `app.bsky.*` endpoints used for polling posts, likes,
//! and bookmarks.
//!
//! Only the pieces the likes/bookmarks poller (AR-7) needs exist here today
//! (session auth, `getActorLikes`, `getBookmarks`); the authored-post REST
//! fallback (AR-6) is expected to extend this same client with its own
//! methods once it lands.

// Not wired into `main` yet: the poller ticket that constructs and drives
// this client is what actually uses it end to end once merged with the
// other in-flight producer tickets.
#![allow(dead_code)]

use serde::Deserialize;

use crate::config::Secret;

/// Errors produced by the Bluesky API client.
#[derive(Debug, thiserror::Error)]
pub enum BlueskyError {
    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("bluesky api error (status {status}): {body}")]
    Api { status: u16, body: String },
    #[error("failed to authenticate with bluesky: {0}")]
    Auth(String),
}

/// A view of an author, as embedded in a [`PostView`].
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct AuthorView {
    pub did: String,
}

/// A hydrated post view, as returned inline by `getActorLikes` /
/// `getBookmarks`: the raw record plus a rendered `embed` (with real CDN
/// URLs for any attached media), unlike the raw firehose/record-only shape.
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

#[derive(Debug, Clone, Deserialize)]
struct CreateSessionResponse {
    #[serde(rename = "accessJwt")]
    access_jwt: String,
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
}

impl BlueskyClient {
    pub fn new(base_url: url::Url, identifier: String, app_password: Secret) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url,
            identifier,
            app_password,
            session: tokio::sync::RwLock::new(None),
        }
    }

    fn xrpc_url(&self, method: &str) -> url::Url {
        self.base_url
            .join(&format!("xrpc/{method}"))
            .expect("xrpc method names are valid URL path segments")
    }

    async fn login(&self) -> Result<String, BlueskyError> {
        let url = self.xrpc_url("com.atproto.server.createSession");
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

        let mut response = self
            .http
            .get(url.clone())
            .bearer_auth(&access_token)
            .query(query)
            .send()
            .await?;

        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            access_token = self.login().await?;
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
            let body = response.text().await.unwrap_or_default();
            return Err(BlueskyError::Api {
                status: status.as_u16(),
                body,
            });
        }

        Ok(response.json().await?)
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
    async fn api_error_status_is_surfaced() {
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
}
