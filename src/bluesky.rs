//! Bluesky API client: authentication and REST (XRPC) calls against the
//! `com.atproto.*` / `app.bsky.*` endpoints used for polling posts, likes,
//! and bookmarks.
//!
//! This is a deliberately minimal client, scoped to exactly what the AR-6
//! REST-polling fallback needs (login + `app.bsky.feed.getAuthorFeed`
//! pagination). The full typed client (session refresh, likes/bookmarks
//! endpoints, rate-limit surfacing) is AR-3's ticket; AR-6 runs on its own
//! branch in parallel with no visibility into whether AR-3 has landed yet,
//! so it brings up just enough of this module to be self-sufficient. When
//! the parallel branches are merged, this will likely need reconciling with
//! AR-3's fuller implementation.

// Not yet wired into `main`: AR-9 (service orchestration) constructs and
// uses a Bluesky client. Silence dead-code lints on this module's public
// surface until then.
#![allow(dead_code)]

use crate::config::Secret;
use serde::Deserialize;
use tokio::sync::RwLock;

/// Errors produced by calls against the Bluesky HTTP API.
#[derive(Debug, thiserror::Error)]
pub enum BskyError {
    #[error("http transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("bluesky api error (status {status}): {body}")]
    Api { status: u16, body: String },
    #[error("invalid json from bluesky api: {0}")]
    Json(#[from] serde_json::Error),
    #[error("malformed response from bluesky api: {0}")]
    Malformed(String),
}

#[derive(Debug, Clone)]
struct Session {
    access_jwt: String,
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

/// A minimal Bluesky (AT Protocol) API client: login via
/// `com.atproto.server.createSession`, then authenticated XRPC calls.
/// Sessions are established lazily on first use and cached for the life of
/// the client.
pub struct BskyClient {
    http: reqwest::Client,
    service: url::Url,
    identifier: String,
    app_password: Secret,
    session: RwLock<Option<Session>>,
}

impl BskyClient {
    /// Creates a client pointed at `service` (the XRPC host, e.g.
    /// `https://bsky.social`), authenticating as `identifier` /
    /// `app_password` on first request.
    pub fn new(service: url::Url, identifier: String, app_password: Secret) -> Self {
        BskyClient {
            http: reqwest::Client::new(),
            service,
            identifier,
            app_password,
            session: RwLock::new(None),
        }
    }

    async fn ensure_session(&self) -> Result<String, BskyError> {
        if let Some(session) = self.session.read().await.as_ref() {
            return Ok(session.access_jwt.clone());
        }

        let mut guard = self.session.write().await;
        if let Some(session) = guard.as_ref() {
            return Ok(session.access_jwt.clone());
        }

        let url = self
            .service
            .join("/xrpc/com.atproto.server.createSession")
            .expect("createSession is a valid relative XRPC path");
        let resp = self
            .http
            .post(url)
            .json(&serde_json::json!({
                "identifier": self.identifier,
                "password": self.app_password.expose_secret(),
            }))
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(BskyError::Api {
                status: status.as_u16(),
                body,
            });
        }

        let body: serde_json::Value = resp.json().await?;
        let access_jwt = body
            .get("accessJwt")
            .and_then(|v| v.as_str())
            .ok_or_else(|| BskyError::Malformed("createSession response missing accessJwt".into()))?
            .to_string();

        let session = Session { access_jwt };
        let access_jwt = session.access_jwt.clone();
        *guard = Some(session);
        Ok(access_jwt)
    }

    /// Fetches one page of `actor`'s authored feed
    /// (`app.bsky.feed.getAuthorFeed`), newest-first, optionally continuing
    /// from a previous page's `cursor`.
    pub async fn get_author_feed(
        &self,
        actor: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<AuthorFeedPage, BskyError> {
        let access_jwt = self.ensure_session().await?;

        let mut url = self
            .service
            .join("/xrpc/app.bsky.feed.getAuthorFeed")
            .expect("getAuthorFeed is a valid relative XRPC path");
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("actor", actor);
            query.append_pair("limit", &limit.to_string());
            if let Some(cursor) = cursor {
                query.append_pair("cursor", cursor);
            }
        }

        let resp = self.http.get(url).bearer_auth(access_jwt).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(BskyError::Api {
                status: status.as_u16(),
                body,
            });
        }

        let page: AuthorFeedPage = resp.json().await?;
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn client(server: &MockServer) -> BskyClient {
        BskyClient::new(
            url::Url::parse(&server.uri()).unwrap(),
            "alice.bsky.social".to_string(),
            Secret::from("app-password".to_string()),
        )
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
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "accessJwt": "token-1",
                "refreshJwt": "refresh-1",
                "did": "did:plc:alice",
                "handle": "alice.bsky.social",
            })))
            .mount(&server)
            .await;

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
    async fn non_success_status_maps_to_api_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/xrpc/com.atproto.server.createSession"))
            .respond_with(ResponseTemplate::new(401).set_body_json(json!({
                "error": "AuthenticationRequired",
                "message": "Invalid identifier or password",
            })))
            .mount(&server)
            .await;

        let client = client(&server);
        let err = client
            .get_author_feed("alice.bsky.social", None, 50)
            .await
            .expect_err("bad credentials should fail");
        match err {
            BskyError::Api { status, .. } => assert_eq!(status, 401),
            other => panic!("expected Api error, got {other:?}"),
        }
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
        assert!(matches!(err, BskyError::Malformed(_)));
    }
}
