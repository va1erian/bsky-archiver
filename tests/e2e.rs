//! End-to-end test: drives a real [`bsky_archiver::state::AppState`] (real
//! `ArchiveStore` backed by a tempdir, real `axum` router) through the full
//! archiving pipeline, against mocked Bluesky infrastructure only — a
//! `wiremock` HTTP server standing in for the REST API (including the media
//! CDN) and a local mock websocket server standing in for Jetstream. No
//! component here ever talks to the real Bluesky network.
//!
//! Scope:
//! - An authored post with media is archived via the REST-polling fallback
//!   path ([`RestFallbackPoller`]), which is a full substitute for the
//!   firehose per the architecture (see `README.md`). Its `ConnectionHealth`
//!   is pinned to `Disabled` for the whole test (nothing here ever flips it
//!   to `Connected`), so the fallback is active immediately and
//!   deterministically rather than racing a real firehose reconnect.
//! - A real [`FirehoseConsumer`] is also run, against the mock Jetstream
//!   server, to prove that leg of the pipeline independently: it is wired to
//!   its own, separate candidate channel (never connected to the media
//!   downloader) because the firehose module derives CDN URLs for real
//!   `cdn.bsky.app`/`video.bsky.app` hosts directly from post content — a
//!   real, unmockable network host — so downloading firehose-sourced media
//!   would either hit the real network (forbidden for tests) or hang/fail
//!   trying to. Wiring it to a throwaway channel still exercises the mock
//!   websocket connection, event parsing, and media-detection filtering
//!   end-to-end without ever attempting such a request.
//! - A like and a bookmark, each with media, are archived via
//!   [`LikesBookmarksPoller`].
//! - All three land as JSON + downloaded media on disk and in the SQLite
//!   index, and are then verified through the real web UI routes (posts
//!   list, post detail, gallery, and raw media bytes).

use std::net::SocketAddr;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use serde_json::json;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use bsky_archiver::bluesky::BlueskyClient;
use bsky_archiver::config::{AppConfig, Secret};
use bsky_archiver::firehose::FirehoseConsumer;
use bsky_archiver::health::health_channel;
use bsky_archiver::pipeline::{
    ConnectionHealth, candidate_post_channel, connection_health_channel,
};
use bsky_archiver::poller::LikesBookmarksPoller;
use bsky_archiver::poller::{PollerConfig, RestFallbackPoller};
use bsky_archiver::state::{AppState, SharedAppState};
use bsky_archiver::storage::{ArchiveStore, Category};
use bsky_archiver::web;

const WATCHED_HANDLE: &str = "e2e.bsky.social";
const WATCHED_DID: &str = "did:plc:e2e-watched-account";

fn test_config(archive_dir: std::path::PathBuf, database_path: std::path::PathBuf) -> AppConfig {
    AppConfig {
        bsky_identifier: WATCHED_HANDLE.to_string(),
        bsky_app_password: Secret::from("e2e-app-password".to_string()),
        bsky_watch_handles: vec![WATCHED_HANDLE.to_string()],
        archive_dir,
        database_path,
        ui_password: Secret::from("e2e-ui-password".to_string()),
        ui_session_secret: Secret::from("e2e-session-secret-0123456789abcdef".to_string()),
        ui_port: 8080,
        poll_interval_seconds: 60,
        jetstream_url: url::Url::parse("wss://jetstream.example.invalid/subscribe").unwrap(),
        media_max_concurrent_downloads: 4,
        media_max_bytes: 10_000_000,
    }
}

async fn mount_login(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.server.createSession"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "accessJwt": "e2e-token",
            "refreshJwt": "e2e-refresh",
            "did": WATCHED_DID,
            "handle": WATCHED_HANDLE,
        })))
        .mount(server)
        .await;
}

/// A hydrated post-view (as `getAuthorFeed`/`getActorLikes`/`getBookmarks`
/// return it) carrying one image embed whose CDN URL points back at the
/// wiremock server, so the media downloader's fetch is fully mocked.
fn media_post_view(
    server_uri: &str,
    author_did: &str,
    rkey: &str,
    text: &str,
    cdn_path: &str,
) -> serde_json::Value {
    json!({
        "uri": format!("at://{author_did}/app.bsky.feed.post/{rkey}"),
        "cid": format!("cid-{rkey}"),
        "author": {"did": author_did},
        "record": {
            "text": text,
            "createdAt": "2024-06-01T00:00:00Z",
            "embed": {
                "$type": "app.bsky.embed.images",
                "images": [{"alt": text, "image": {"ref": "bafy-fake"}}],
            }
        },
        "embed": {
            "$type": "app.bsky.embed.images#view",
            "images": [{
                "alt": text,
                "fullsize": format!("{server_uri}{cdn_path}"),
                "thumb": format!("{server_uri}{cdn_path}"),
            }],
        },
    })
}

/// A minimal mock Jetstream server: accepts one connection, sends the
/// scripted event, then idles (rather than closing), so the firehose
/// consumer's single connection stays up for the rest of the test instead
/// of reconnecting.
async fn spawn_mock_jetstream(event: serde_json::Value) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");

    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let event = event.clone();
            tokio::spawn(async move {
                let Ok(ws_stream) = tokio_tungstenite::accept_async(stream).await else {
                    return;
                };
                let (mut write, _read) = ws_stream.split();
                let text = serde_json::to_string(&event).expect("serialize event");
                if write.send(Message::Text(text)).await.is_err() {
                    return;
                }
                // Keep the connection open instead of closing, so the
                // consumer doesn't spend the rest of the test reconnecting.
                std::future::pending::<()>().await;
            });
        }
    });

    addr
}

fn firehose_image_post_event(did: &str) -> serde_json::Value {
    json!({
        "did": did,
        "time_us": 1_700_000_000_000_000i64,
        "kind": "commit",
        "commit": {
            "rev": "rev1",
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": "e2e-firehose-post",
            "cid": "cid-e2e-firehose-post",
            "record": {
                "text": "an e2e firehose post",
                "createdAt": "2024-06-01T00:00:00Z",
                "embed": {
                    "$type": "app.bsky.embed.images",
                    "images": [{
                        "alt": "an e2e image",
                        "image": {
                            "$type": "blob",
                            "ref": {"$link": "bafyE2EFirehoseImage"},
                            "mimeType": "image/jpeg",
                            "size": 1234
                        }
                    }]
                }
            }
        }
    })
}

/// Polls `check` every 20ms until it returns `Some`, or panics after
/// `timeout` with `description`. Used throughout instead of a fixed sleep,
/// since the pollers under test run on their own timers.
async fn wait_until<T, F, Fut>(timeout: Duration, description: &str, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let start = std::time::Instant::now();
    loop {
        if let Some(value) = check().await {
            return value;
        }
        if start.elapsed() > timeout {
            panic!("timed out waiting for: {description}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn body_string(response: axum::response::Response) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn full_pipeline_archives_post_like_and_bookmark_and_renders_in_web_ui() {
    let dir = tempfile::tempdir().expect("tempdir");
    let archive_dir = dir.path().join("archive");
    let database_path = dir.path().join("index.sqlite3");

    let server = MockServer::start().await;
    mount_login(&server).await;

    let authored_at_uri = format!("at://{WATCHED_DID}/app.bsky.feed.post/e2e-authored");
    let like_at_uri = "at://did:plc:e2e-liked-author/app.bsky.feed.post/e2e-liked";
    let bookmark_at_uri = "at://did:plc:e2e-bookmarked-author/app.bsky.feed.post/e2e-bookmarked";

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getAuthorFeed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "feed": [{"post": media_post_view(
                &server.uri(),
                WATCHED_DID,
                "e2e-authored",
                "an e2e authored post with media",
                "/cdn/post-image.jpg",
            )}],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getActorLikes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "feed": [{"post": media_post_view(
                &server.uri(),
                "did:plc:e2e-liked-author",
                "e2e-liked",
                "an e2e liked post with media",
                "/cdn/like-image.jpg",
            )}],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "bookmarks": [{"subject": media_post_view(
                &server.uri(),
                "did:plc:e2e-bookmarked-author",
                "e2e-bookmarked",
                "an e2e bookmarked post with media",
                "/cdn/bookmark-image.jpg",
            )}],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    for (cdn_path, bytes) in [
        ("/cdn/post-image.jpg", b"post-image-bytes".to_vec()),
        ("/cdn/like-image.jpg", b"like-image-bytes".to_vec()),
        ("/cdn/bookmark-image.jpg", b"bookmark-image-bytes".to_vec()),
    ] {
        Mock::given(method("GET"))
            .and(path(cdn_path))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "image/jpeg")
                    .set_body_bytes(bytes),
            )
            .mount(&server)
            .await;
    }

    // Real `ArchiveStore`, backed by a real tempdir.
    let store = ArchiveStore::open(archive_dir.clone(), database_path.clone())
        .await
        .expect("open store");

    let base_url = url::Url::parse(&server.uri()).unwrap();
    let client = std::sync::Arc::new(BlueskyClient::new(
        base_url,
        WATCHED_HANDLE.to_string(),
        Secret::from("e2e-app-password".to_string()),
    ));
    let self_did = client.authenticate().await.expect("authenticate");
    assert_eq!(self_did, WATCHED_DID);

    // --- Firehose leg: a real FirehoseConsumer against a real (mock)
    // websocket server, wired to its own throwaway candidate channel so
    // nothing here ever attempts to download from the real bsky CDN.
    let jetstream_addr = spawn_mock_jetstream(firehose_image_post_event(WATCHED_DID)).await;
    let jetstream_url =
        url::Url::parse(&format!("ws://{jetstream_addr}/subscribe")).expect("jetstream url");
    let (firehose_candidate_tx, mut firehose_candidate_rx) = candidate_post_channel(8);
    let (firehose_health_tx, _firehose_health_rx) =
        connection_health_channel(ConnectionHealth::Disabled);
    let cursor_path = dir.path().join("jetstream_cursor");
    let mut firehose_consumer = FirehoseConsumer::new(
        jetstream_url,
        vec![WATCHED_DID.to_string()],
        cursor_path,
        firehose_candidate_tx,
        firehose_health_tx,
    );
    let (firehose_shutdown_tx, firehose_shutdown_rx) = tokio::sync::watch::channel(false);
    let firehose_handle = tokio::spawn(async move {
        firehose_consumer.run(firehose_shutdown_rx).await;
    });

    let firehose_candidate =
        tokio::time::timeout(Duration::from_secs(5), firehose_candidate_rx.recv())
            .await
            .expect("firehose candidate received in time")
            .expect("firehose channel open");
    assert_eq!(firehose_candidate.author_did, WATCHED_DID);
    assert_eq!(firehose_candidate.media.len(), 1);
    assert!(
        firehose_candidate.media[0].cdn_url.contains("cdn.bsky.app"),
        "firehose-derived media URLs should point at the real bsky CDN host \
         (proving this leg never had a chance to hit it): {}",
        firehose_candidate.media[0].cdn_url
    );

    firehose_shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), firehose_handle).await;

    // --- Real archive pipeline: one shared candidate channel feeding the
    // real media downloader, populated by a REST-fallback poller (for the
    // authored post) and a likes/bookmarks poller.
    let (candidate_tx, mut candidate_rx) = candidate_post_channel(16);

    // Pinned `Disabled` for the whole test (never updated): per
    // `ConnectionHealth`'s docs this means "the firehose consumer is not
    // running at all", so the fallback is active immediately and
    // deterministically rather than racing a 30s real-firehose grace period.
    let (_rest_health_tx, rest_health_rx) = connection_health_channel(ConnectionHealth::Disabled);
    let mut poller_config = PollerConfig::new(Duration::from_millis(50));
    poller_config.health_recheck_interval = Duration::from_millis(10);
    let rest_poller = RestFallbackPoller::new(
        std::sync::Arc::clone(&client),
        store.clone(),
        candidate_tx.clone(),
        rest_health_rx,
        vec![WATCHED_HANDLE.to_string()],
        poller_config,
    );
    // `RestFallbackPoller::run` loops forever (there is no single-pass
    // entry point, matching production usage in `app::serve`), so it's
    // spawned and later aborted rather than awaited to completion.
    let rest_poller_handle = tokio::spawn(rest_poller.run());

    let likes_bookmarks_poller = LikesBookmarksPoller::new(
        std::sync::Arc::clone(&client),
        store.clone(),
        candidate_tx.clone(),
        self_did.clone(),
        Duration::from_secs(60),
    );
    likes_bookmarks_poller
        .poll_likes()
        .await
        .expect("poll likes");
    likes_bookmarks_poller
        .poll_bookmarks()
        .await
        .expect("poll bookmarks");
    drop(candidate_tx);

    // Real `MediaDownloader`, spawned rather than awaited directly: the
    // still-running `rest_poller` holds its own clone of `candidate_tx`
    // (by design — it polls forever in production), so the channel never
    // actually closes within the test.
    let downloader = bsky_archiver::media::MediaDownloader::new(store.clone(), 4, 10_000_000);
    let downloader_handle = tokio::spawn(async move {
        downloader.run(&mut candidate_rx).await;
    });

    // --- Assert: JSON + media on disk and in the SQLite index for all
    // three categories. The record's JSON is saved as soon as its
    // `CandidatePost` is received, but the media downloader attaches media
    // asynchronously afterwards, so each wait is for the media to show up
    // (not just the bare record).
    async fn wait_for_archived_with_media(
        store: &ArchiveStore,
        category: Category,
        at_uri: &str,
    ) -> bsky_archiver::storage::ArchivedRecord {
        wait_until(
            Duration::from_secs(5),
            &format!("{category} {at_uri} archived with media"),
            || async {
                let record = store.get_post(category, at_uri).await.unwrap()?;
                if record.media.is_empty() {
                    None
                } else {
                    Some(record)
                }
            },
        )
        .await
    }

    let archived_post =
        wait_for_archived_with_media(&store, Category::Post, &authored_at_uri).await;
    assert_eq!(archived_post.media.len(), 1);
    assert_eq!(
        archived_post.media[0].content_type.as_deref(),
        Some("image/jpeg")
    );

    let archived_like = wait_for_archived_with_media(&store, Category::Like, like_at_uri).await;
    assert_eq!(archived_like.media.len(), 1);

    let archived_bookmark =
        wait_for_archived_with_media(&store, Category::Bookmark, bookmark_at_uri).await;
    assert_eq!(archived_bookmark.media.len(), 1);

    // Background tasks have done their job; stop them before the web-UI
    // assertions below (which don't need them running).
    rest_poller_handle.abort();
    downloader_handle.abort();

    let post_media_bytes = store
        .read_media(
            Category::Post,
            &authored_at_uri,
            &archived_post.media[0].filename,
        )
        .await
        .unwrap()
        .expect("post media bytes on disk");
    assert_eq!(post_media_bytes, b"post-image-bytes");

    let posts_page = store.list_posts(None, 1, 10).await.unwrap();
    assert_eq!(posts_page.total_items, 3);

    let gallery_page = store.list_media(None, None, 1, 10).await.unwrap();
    assert_eq!(gallery_page.total_items, 3);

    // --- Assert: visible and correctly rendered via the real web UI
    // routes (list, detail, gallery), through a real `axum::Router`.
    let config = test_config(archive_dir, database_path);
    let (_health_tx, health_rx) = health_channel();
    let state: SharedAppState = std::sync::Arc::new(AppState {
        config,
        store: store.clone(),
        health: health_rx,
    });

    let app = web::router(state);
    let login_response = app
        .clone()
        .oneshot(
            Request::post("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("password=e2e-ui-password"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login_response.status(), StatusCode::SEE_OTHER);
    let cookie = login_response
        .headers()
        .get(header::SET_COOKIE)
        .expect("login sets a session cookie")
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let posts_list_response = app
        .clone()
        .oneshot(
            Request::get("/posts")
                .header(header::COOKIE, cookie.clone())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(posts_list_response.status(), StatusCode::OK);
    let posts_list_body = body_string(posts_list_response).await;
    assert!(posts_list_body.contains("an e2e authored post with media"));
    assert!(posts_list_body.contains("an e2e liked post with media"));
    assert!(posts_list_body.contains("an e2e bookmarked post with media"));
    assert!(posts_list_body.contains("badge-post"));
    assert!(posts_list_body.contains("badge-like"));
    assert!(posts_list_body.contains("badge-bookmark"));

    let post_id = web_encode(&authored_at_uri);
    let post_detail_response = app
        .clone()
        .oneshot(
            Request::get(format!("/posts/{post_id}"))
                .header(header::COOKIE, cookie.clone())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(post_detail_response.status(), StatusCode::OK);
    let post_detail_body = body_string(post_detail_response).await;
    assert!(post_detail_body.contains("an e2e authored post with media"));
    assert!(post_detail_body.contains("<img"));

    let gallery_response = app
        .clone()
        .oneshot(
            Request::get("/gallery")
                .header(header::COOKIE, cookie.clone())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(gallery_response.status(), StatusCode::OK);
    let gallery_body = body_string(gallery_response).await;
    assert!(gallery_body.contains("data-lightbox"));
    assert!(gallery_body.contains("(3 total)"));

    let media_url = format!(
        "/media/{}/{}/{}",
        Category::Post,
        post_id,
        archived_post.media[0].filename
    );
    let media_response = app
        .oneshot(
            Request::get(media_url)
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(media_response.status(), StatusCode::OK);
    let media_bytes = media_response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(media_bytes.as_ref(), b"post-image-bytes");
}

/// Mirrors `web::encode_post_id` (private to that module): percent-encodes
/// every non-alphanumeric byte so the whole `at_uri` fits in one path
/// segment.
fn web_encode(at_uri: &str) -> String {
    percent_encoding::utf8_percent_encode(at_uri, percent_encoding::NON_ALPHANUMERIC).to_string()
}
