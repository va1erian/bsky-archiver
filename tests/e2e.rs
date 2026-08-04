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
use bsky_archiver::config::{AppConfig, FeedConfig, ResolvedFeed, Secret, feed_at_uri, feed_slug};
use bsky_archiver::firehose::FirehoseConsumer;
use bsky_archiver::health::health_channel;
use bsky_archiver::pipeline::{
    ConnectionHealth, candidate_post_channel, connection_health_channel,
};
use bsky_archiver::poller::FeedPoller;
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
        watch_feeds: Vec::new(),
        archive_dir,
        database_path,
        ui_password: Secret::from("e2e-ui-password".to_string()),
        ui_session_secret: Secret::from("e2e-session-secret-0123456789abcdef".to_string()),
        ui_port: 8080,
        poll_interval_seconds: 60,
        jetstream_url: url::Url::parse("wss://jetstream.example.invalid/subscribe").unwrap(),
        media_max_concurrent_downloads: 4,
        media_max_bytes: 10_000_000,
        feed_max_bytes: 2_147_483_648,
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
                let record = store.get_post(category.clone(), at_uri).await.unwrap()?;
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

    let gallery_page = store.list_media(None, 1, 10).await.unwrap();
    assert_eq!(gallery_page.total_items, 3);

    // --- Assert: visible and correctly rendered via the real web UI
    // routes (list, detail, gallery), through a real `axum::Router`.
    let config = test_config(archive_dir, database_path);
    let (_health_tx, health_rx) = health_channel();
    let state: SharedAppState = std::sync::Arc::new(AppState {
        config,
        store: store.clone(),
        health: health_rx,
        feeds: Vec::new(),
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

/// End-to-end for a custom feed: a real [`FeedPoller`] against a mocked
/// `getFeed` + media CDN archives a media-bearing feed post under its own
/// `feeds/<slug>` category (keyed by the generator's DID+rkey via the real
/// `feed_slug`), then it is asserted through the real web UI — the post list,
/// the post detail page, the feed-filtered gallery, the raw media-bytes route
/// (whose category segment is the `/`-containing `feeds/<slug>` form), the
/// feed-filtered zip export, and the `/config` feed listing.
#[tokio::test]
async fn full_pipeline_archives_a_custom_feed_post_and_renders_in_web_ui() {
    use std::io::Read as _;

    let dir = tempfile::tempdir().expect("tempdir");
    let archive_dir = dir.path().join("archive");
    let database_path = dir.path().join("index.sqlite3");

    let server = MockServer::start().await;
    mount_login(&server).await;

    // The feed generator's stable identity: slug + canonical URI derived only
    // from its DID + rkey (never its display name), exactly as `app::init`
    // would after resolution.
    let feedgen_did = "did:plc:e2e-feedgen";
    let feed_rkey = "e2ecats";
    let feed_uri = feed_at_uri(feedgen_did, feed_rkey);
    let slug = feed_slug(feedgen_did, feed_rkey);
    let feed_category = Category::Feed(slug.clone());

    let feed_post_author = "did:plc:e2e-feedauthor";
    let feed_post_at_uri = format!("at://{feed_post_author}/app.bsky.feed.post/e2e-feedpost");

    // `getFeed` returns the same hydrated `feedViewPost` shape as
    // `getAuthorFeed`, so `media_post_view` is reused verbatim.
    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getFeed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "feed": [
                // A text-only post that must be dropped by `has_archivable_media`.
                {"post": {
                    "uri": format!("at://{feed_post_author}/app.bsky.feed.post/text-only"),
                    "cid": "cid-text-only",
                    "author": {"did": feed_post_author},
                    "record": {"text": "a text-only feed post", "createdAt": "2024-06-01T00:00:00Z"},
                }},
                // A media-bearing post that must be archived.
                {"post": media_post_view(
                    &server.uri(),
                    feed_post_author,
                    "e2e-feedpost",
                    "an e2e custom-feed post with media",
                    "/cdn/feed-image.jpg",
                )},
            ],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/cdn/feed-image.jpg"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "image/jpeg")
                .set_body_bytes(b"feed-image-bytes".to_vec()),
        )
        .mount(&server)
        .await;

    let store = ArchiveStore::open(archive_dir.clone(), database_path.clone())
        .await
        .expect("open store");

    let client = std::sync::Arc::new(BlueskyClient::new(
        url::Url::parse(&server.uri()).unwrap(),
        WATCHED_HANDLE.to_string(),
        Secret::from("e2e-app-password".to_string()),
    ));

    let resolved = ResolvedFeed {
        slug: slug.clone(),
        at_uri: feed_uri.clone(),
        input: feed_uri.clone(),
        display_name: Some("E2E Cats".to_string()),
    };

    // Real `FeedPoller`, which downloads media inline (to enforce the per-feed
    // cap) rather than via the shared downloader. `run` loops forever, so it's
    // spawned and later aborted, like the REST poller above.
    let (feed_health_tx, _feed_health_rx) = health_channel();
    let feed_poller = FeedPoller::new(
        std::sync::Arc::clone(&client),
        store.clone(),
        None,
        vec![resolved.clone()],
        2_147_483_648,
        10_000_000,
        feed_health_tx,
        PollerConfig::new(Duration::from_millis(50)),
    );
    let feed_poller_handle = tokio::spawn(feed_poller.run());

    let archived_feed_post = wait_until(
        Duration::from_secs(5),
        "custom-feed post archived with media",
        || async {
            let record = store
                .get_post(feed_category.clone(), &feed_post_at_uri)
                .await
                .unwrap()?;
            (!record.media.is_empty()).then_some(record)
        },
    )
    .await;
    feed_poller_handle.abort();

    assert_eq!(archived_feed_post.media.len(), 1);
    let media_filename = archived_feed_post.media[0].filename.clone();

    // The text-only feed post must not have been archived.
    assert!(
        store
            .get_post(
                feed_category.clone(),
                &format!("at://{feed_post_author}/app.bsky.feed.post/text-only")
            )
            .await
            .unwrap()
            .is_none(),
        "text-only feed posts must not be archived"
    );

    // Media bytes and the index row landed under the feed's own category.
    assert_eq!(
        store
            .read_media(feed_category.clone(), &feed_post_at_uri, &media_filename)
            .await
            .unwrap()
            .expect("feed media bytes on disk"),
        b"feed-image-bytes"
    );
    assert_eq!(
        store.category_media_bytes(&feed_category).await.unwrap(),
        16
    );

    // --- Web UI, driven through a real router with the feed configured.
    let mut config = test_config(archive_dir, database_path);
    config.watch_feeds = vec![FeedConfig {
        input: feed_uri.clone(),
        actor: feedgen_did.to_string(),
        rkey: feed_rkey.to_string(),
    }];
    let (_health_tx, health_rx) = health_channel();
    let state: SharedAppState = std::sync::Arc::new(AppState {
        config,
        store: store.clone(),
        health: health_rx,
        feeds: vec![resolved.clone()],
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

    let get = |uri: String| {
        let app = app.clone();
        let cookie = cookie.clone();
        async move {
            app.oneshot(
                Request::get(uri)
                    .header(header::COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
        }
    };

    // The feed category token as it travels in a query string: `feeds/<slug>`,
    // with the `/` (and every other non-alphanumeric byte) percent-encoded so
    // it survives as one value.
    let feed_cat = format!("feeds/{slug}");
    let feed_cat_enc =
        percent_encoding::utf8_percent_encode(&feed_cat, percent_encoding::NON_ALPHANUMERIC)
            .to_string();
    let feed_post_id = web_encode(&feed_post_at_uri);

    // Post list (all): the feed post shows with the feed badge.
    let list_body = body_string(get("/posts".to_string()).await).await;
    assert!(list_body.contains("an e2e custom-feed post with media"));
    assert!(list_body.contains("badge-feed"));
    // The category filter offers the feed by its display name.
    assert!(list_body.contains("E2E Cats"));

    // Post list filtered to just this feed.
    let filtered = get(format!("/posts?category={feed_cat_enc}")).await;
    assert_eq!(filtered.status(), StatusCode::OK);
    let filtered_body = body_string(filtered).await;
    assert!(filtered_body.contains("an e2e custom-feed post with media"));

    // Post detail.
    let detail = get(format!("/posts/{feed_post_id}")).await;
    assert_eq!(detail.status(), StatusCode::OK);
    let detail_body = body_string(detail).await;
    assert!(detail_body.contains("an e2e custom-feed post with media"));
    assert!(detail_body.contains("<img"));
    assert!(detail_body.contains("badge-feed"));

    // Gallery filtered to the feed.
    let gallery = get(format!("/gallery?category={feed_cat_enc}")).await;
    assert_eq!(gallery.status(), StatusCode::OK);
    let gallery_body = body_string(gallery).await;
    assert!(gallery_body.contains("data-lightbox"));
    assert!(gallery_body.contains("(1 total)"));

    // Raw media bytes, via the `/`-containing feed category segment.
    let media = get(format!(
        "/media/{feed_cat_enc}/{feed_post_id}/{media_filename}"
    ))
    .await;
    assert_eq!(media.status(), StatusCode::OK);
    let media_bytes = media.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(media_bytes.as_ref(), b"feed-image-bytes");

    // Zip export of just this feed's selection.
    let export = get(format!("/gallery/export?category={feed_cat_enc}")).await;
    assert_eq!(export.status(), StatusCode::OK);
    assert_eq!(
        export.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/zip"
    );
    let zip_bytes = export.into_body().collect().await.unwrap().to_bytes();
    let mut archive =
        zip::ZipArchive::new(std::io::Cursor::new(zip_bytes.to_vec())).expect("parse zip");
    assert_eq!(archive.len(), 1, "just the one feed image");
    let mut entry = archive.by_index(0).unwrap();
    assert_eq!(
        entry.name(),
        format!("feeds/{slug}/{feed_post_id}/{media_filename}")
    );
    let mut buf = Vec::new();
    entry.read_to_end(&mut buf).unwrap();
    assert_eq!(buf, b"feed-image-bytes");

    // /config lists the feed by name and canonical URI.
    let config_body = body_string(get("/config".to_string()).await).await;
    assert!(config_body.contains("E2E Cats"));
    assert!(config_body.contains(&feed_uri));
}

/// Drives the real `/gallery/export` route through the real `axum` router
/// against a real on-disk store, parses the streamed response as a zip, and
/// checks the entry count, the `<category>/<encoded-post-id>/<filename>`
/// layout, that an archived video is excluded, and that one entry's bytes
/// round-trip to the original file's contents.
#[tokio::test]
async fn gallery_export_streams_a_zip_of_images_only() {
    use std::io::Read;

    let dir = tempfile::tempdir().expect("tempdir");
    let archive_dir = dir.path().join("archive");
    let database_path = dir.path().join("index.sqlite3");

    let store = ArchiveStore::open(archive_dir.clone(), database_path.clone())
        .await
        .expect("open store");

    // Two images (one with an explicit content type, one with a null
    // content type but an image filename extension) and one video.
    let image_post = "at://did:plc:e2e-alice/app.bsky.feed.post/img";
    store
        .save_post(Category::Post, image_post, "cid-img", json!({}))
        .await
        .unwrap();
    store
        .save_media(
            Category::Post,
            image_post,
            "000.jpg",
            Some("image/jpeg".to_string()),
            b"the-original-image-bytes".to_vec(),
        )
        .await
        .unwrap();

    let null_ct_like = "at://did:plc:e2e-bob/app.bsky.feed.post/png";
    store
        .save_post(Category::Like, null_ct_like, "cid-png", json!({}))
        .await
        .unwrap();
    store
        .save_media(Category::Like, null_ct_like, "000.png", None, vec![1u8; 16])
        .await
        .unwrap();

    let video_post = "at://did:plc:e2e-alice/app.bsky.feed.post/vid";
    store
        .save_post(Category::Post, video_post, "cid-vid", json!({}))
        .await
        .unwrap();
    store
        .save_media(
            Category::Post,
            video_post,
            "000.mp4",
            Some("video/mp4".to_string()),
            vec![9u8; 32],
        )
        .await
        .unwrap();

    let config = test_config(archive_dir, database_path);
    let (_health_tx, health_rx) = health_channel();
    let state: SharedAppState = std::sync::Arc::new(AppState {
        config,
        store: store.clone(),
        health: health_rx,
        feeds: Vec::new(),
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

    let export_response = app
        .oneshot(
            Request::get("/gallery/export")
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(export_response.status(), StatusCode::OK);
    assert_eq!(
        export_response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/zip"
    );
    assert!(
        export_response
            .headers()
            .get(header::CONTENT_DISPOSITION)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("bsky-archive-all-"),
        "content-disposition names the whole-archive export"
    );

    let zip_bytes = export_response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();

    let mut archive =
        zip::ZipArchive::new(std::io::Cursor::new(zip_bytes.to_vec())).expect("parse zip");

    // Two images in, one video excluded.
    assert_eq!(
        archive.len(),
        2,
        "video must be absent, both images present"
    );

    let mut names: Vec<String> = Vec::new();
    let mut image_entry_bytes: Option<Vec<u8>> = None;
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).unwrap();
        let name = entry.name().to_string();
        // `<category>/<encoded-post-id>/<filename>` layout.
        let parts: Vec<&str> = name.split('/').collect();
        assert_eq!(parts.len(), 3, "unexpected entry layout: {name}");
        assert!(
            ["posts", "likes", "bookmarks"].contains(&parts[0]),
            "unexpected category segment: {name}"
        );
        assert!(
            !name.contains("000.mp4"),
            "video leaked into the zip: {name}"
        );

        if name == format!("posts/{}/000.jpg", web_encode(image_post)) {
            let mut buf = Vec::new();
            entry.read_to_end(&mut buf).unwrap();
            image_entry_bytes = Some(buf);
        }
        names.push(name);
    }

    assert!(
        names.contains(&format!("posts/{}/000.jpg", web_encode(image_post))),
        "expected the jpg image entry, got: {names:?}"
    );
    assert!(
        names.contains(&format!("likes/{}/000.png", web_encode(null_ct_like))),
        "expected the null-content-type png image entry, got: {names:?}"
    );
    assert_eq!(
        image_entry_bytes.as_deref(),
        Some(b"the-original-image-bytes".as_ref()),
        "entry bytes must round-trip to the original file contents"
    );
}
