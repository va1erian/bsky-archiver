//! `#[cfg(test)]` coverage for the web UI: drives the real router (real
//! `ArchiveStore` on a tempdir) through auth, every page, pagination
//! boundaries, htmx fragment swaps, and media caching/revalidation.

use super::config::config_view;
use super::gallery::{GalleryQuery, gallery};
use super::posts::{PostsQuery, list_posts};
use super::*;
use crate::config::{AppConfig, Secret};
use crate::health::health_channel;
use crate::state::AppState;
use crate::storage::{ArchiveStore, Category as StorageCategory, SourceKind};
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, Request, StatusCode, header};
use axum::response::Response;
use axum_extra::extract::cookie::Key;
use http_body_util::BodyExt;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;
async fn test_state() -> (tempfile::TempDir, SharedAppState) {
    let dir = tempfile::tempdir().expect("tempdir");
    let archive_dir = dir.path().join("archive");
    let database_path = dir.path().join("index.sqlite3");
    let store = ArchiveStore::open(archive_dir.clone(), database_path)
        .await
        .expect("open store");

    let config = AppConfig {
        bsky_identifier: "alice.bsky.social".to_string(),
        bsky_app_password: Secret::from("bsky-app-password-secret".to_string()),
        tumblr: None,
        archive_dir,
        database_path: dir.path().join("index.sqlite3"),
        ui_password: Secret::from("correct horse battery staple".to_string()),
        ui_session_secret: Secret::from("a".repeat(64)),
        ui_port: 8080,
        poll_interval_seconds: 120,
        jetstream_url: url::Url::parse("wss://jetstream.example.com/subscribe").unwrap(),
        media_max_concurrent_downloads: 4,
        media_max_bytes: 104_857_600,
        nightly_sweep_local_hour: 3,
        tumblr_poll_interval_seconds: 300,
    };

    let (_health_tx, health_rx) = health_channel();
    let bluesky = Arc::new(crate::bluesky::BlueskyClient::new(
        url::Url::parse("https://bsky.social").unwrap(),
        "alice.bsky.social".to_string(),
        Secret::from("bsky-app-password-secret".to_string()),
    ));
    let (candidate_tx, _candidate_rx) = crate::pipeline::candidate_post_channel(16);
    let watchlist = crate::watchlist::Watchlist::new(Vec::new());
    let state: SharedAppState = Arc::new(AppState {
        config,
        store,
        health: health_rx,
        bluesky,
        candidate_weak: candidate_tx.downgrade(),
        watchlist,
    });
    (dir, state)
}

async fn body_string(response: Response) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn unauthenticated_request_redirects_to_login() {
    let (_dir, state) = test_state().await;
    let app = router(state);

    let response = app
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(response.headers().get(header::LOCATION).unwrap(), "/login");
}

#[tokio::test]
async fn healthz_is_reachable_without_auth() {
    let (_dir, state) = test_state().await;
    let app = router(state);

    let response = app
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn healthz_response_contains_version() {
    let (_dir, state) = test_state().await;
    let app = router(state);

    let response = app
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_string(response).await;
    let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid json");
    assert_eq!(
        parsed["version"].as_str().expect("version field"),
        env!("CARGO_PKG_VERSION")
    );
}

#[tokio::test]
async fn wrong_password_is_rejected() {
    let (_dir, state) = test_state().await;
    let app = router(state);

    let response = app
        .oneshot(
            Request::post("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("password=nope"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = body_string(response).await;
    assert!(body.contains("Incorrect password"));
}

#[tokio::test]
async fn correct_password_logs_in_and_grants_access() {
    let (_dir, state) = test_state().await;
    let app = router(state);

    let login_response = app
        .clone()
        .oneshot(
            Request::post("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("password=correct%20horse%20battery%20staple"))
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
        .to_string();
    let cookie_pair = cookie.split(';').next().unwrap().to_string();

    let protected_response = app
        .oneshot(
            Request::get("/")
                .header(header::COOKIE, cookie_pair)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(protected_response.status(), StatusCode::OK);
}

#[tokio::test]
async fn posts_pagination_behaves_at_boundaries() {
    let (_dir, state) = test_state().await;
    for i in 0..25 {
        state
            .store
            .save_post(
                StorageCategory::Post,
                &format!("at://did:plc:alice/app.bsky.feed.post/{i:03}"),
                &format!("cid-{i}"),
                json!({"i": i}),
            )
            .await
            .unwrap();
    }

    let key = Key::derive_from(state.config.ui_session_secret.expose_secret().as_bytes());
    let app_state = WebState {
        app: Arc::clone(&state),
        key,
    };

    let page1 = list_posts(
        State(app_state.clone()),
        Query(PostsQuery {
            category: None,
            page: Some(1),
            page_size: Some(10),
        }),
        HeaderMap::new(),
    )
    .await
    .unwrap();
    assert!(
        body_string(page1.into_response())
            .await
            .contains("Page 1 of 3")
    );

    let page3 = list_posts(
        State(app_state.clone()),
        Query(PostsQuery {
            category: None,
            page: Some(3),
            page_size: Some(10),
        }),
        HeaderMap::new(),
    )
    .await
    .unwrap();
    assert!(
        body_string(page3.into_response())
            .await
            .contains("Page 3 of 3")
    );

    let page4 = list_posts(
        State(app_state.clone()),
        Query(PostsQuery {
            category: None,
            page: Some(4),
            page_size: Some(10),
        }),
        HeaderMap::new(),
    )
    .await
    .unwrap();
    let page4_body = body_string(page4.into_response()).await;
    assert!(page4_body.contains("Page 4 of 3"));
    assert!(!page4_body.contains("class=\"item-card\""));

    // Middle pages: first/last controls join prev/next, and the
    // adjacent-page links carry the rel attributes the lightbox
    // page-walk uses to continue past a page boundary.
    let page2 = list_posts(
        State(app_state.clone()),
        Query(PostsQuery {
            category: None,
            page: Some(2),
            page_size: Some(10),
        }),
        HeaderMap::new(),
    )
    .await
    .unwrap();
    let page2_body = body_string(page2.into_response()).await;
    assert!(page2_body.contains("rel=\"first\""));
    assert!(page2_body.contains("rel=\"prev\""));
    assert!(page2_body.contains("rel=\"next\""));
    assert!(page2_body.contains("rel=\"last\""));

    let huge_page_size = list_posts(
        State(app_state),
        Query(PostsQuery {
            category: None,
            page: Some(1),
            page_size: Some(u32::MAX),
        }),
        HeaderMap::new(),
    )
    .await
    .unwrap();
    let body = body_string(huge_page_size.into_response()).await;
    assert!(body.contains(&format!("Page 1 of {}", 25u32.div_ceil(MAX_PAGE_SIZE))));
}

#[tokio::test]
async fn gallery_pagination_behaves_at_boundaries() {
    let (_dir, state) = test_state().await;
    for i in 0..15 {
        let at_uri = format!("at://did:plc:alice/app.bsky.feed.post/{i:03}");
        state
            .store
            .save_post(
                StorageCategory::Post,
                &at_uri,
                &format!("cid-{i}"),
                json!({}),
            )
            .await
            .unwrap();
        state
            .store
            .save_media(
                StorageCategory::Post,
                &at_uri,
                "img.jpg",
                Some("image/jpeg".to_string()),
                vec![0u8; 4],
            )
            .await
            .unwrap();
    }

    let key = Key::derive_from(state.config.ui_session_secret.expose_secret().as_bytes());
    let app_state = WebState {
        app: Arc::clone(&state),
        key,
    };

    let page1 = gallery(
        State(app_state.clone()),
        Query(GalleryQuery {
            category: None,
            page: Some(1),
            page_size: Some(10),
            sort: None,
        }),
        HeaderMap::new(),
    )
    .await
    .unwrap();
    assert!(
        body_string(page1.into_response())
            .await
            .contains("Page 1 of 2")
    );

    let page2 = gallery(
        State(app_state),
        Query(GalleryQuery {
            category: None,
            page: Some(2),
            page_size: Some(10),
            sort: None,
        }),
        HeaderMap::new(),
    )
    .await
    .unwrap();
    let body = body_string(page2.into_response()).await;
    assert!(body.contains("Page 2 of 2"));
    assert!(body.contains("(15 total)"));
}

#[tokio::test]
async fn config_response_never_leaks_secrets() {
    let (_dir, state) = test_state().await;
    let app_password = state.config.bsky_app_password.expose_secret().to_string();
    let ui_password = state.config.ui_password.expose_secret().to_string();
    let session_secret = state.config.ui_session_secret.expose_secret().to_string();

    let key = Key::derive_from(session_secret.as_bytes());
    let app_state = WebState { app: state, key };

    let response = config_view(State(app_state)).await;
    let body = body_string(response.into_response()).await;

    assert!(!body.contains(&app_password));
    assert!(!body.contains(&ui_password));
    assert!(!body.contains(&session_secret));
    assert!(body.contains("&lt;redacted&gt;"));
}

#[tokio::test]
async fn post_detail_round_trips_and_missing_id_is_404() {
    let (_dir, state) = test_state().await;
    let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
    state
        .store
        .save_post(
            StorageCategory::Post,
            at_uri,
            "cid-1",
            json!({"text": "hi"}),
        )
        .await
        .unwrap();

    let app = router(Arc::clone(&state));
    let login_response = app
        .clone()
        .oneshot(
            Request::post("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("password=correct%20horse%20battery%20staple"))
                .unwrap(),
        )
        .await
        .unwrap();
    let cookie = login_response
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let found = app
        .clone()
        .oneshot(
            Request::get(format!("/posts/{}", encode_post_id(at_uri)))
                .header(header::COOKIE, cookie.clone())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(found.status(), StatusCode::OK);
    let body = body_string(found).await;
    assert!(body.contains("hi"));

    let missing = app
        .oneshot(
            Request::get(format!("/posts/{}", encode_post_id("at://missing")))
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
}

/// Logs in against `app` and returns the `Cookie` header value to reuse
/// on subsequent requests.
async fn login(app: &Router) -> String {
    let response = app
        .clone()
        .oneshot(
            Request::post("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("password=correct%20horse%20battery%20staple"))
                .unwrap(),
        )
        .await
        .unwrap();
    response
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string()
}

/// Seeds one image post and one video-bearing like, covering both
/// media branches (`<img>` and `<video>`) every template needs to
/// handle.
async fn seed_fixture_media(store: &ArchiveStore) -> (String, String) {
    let image_post = "at://did:plc:alice/app.bsky.feed.post/img1";
    store
        .save_post(
            StorageCategory::Post,
            image_post,
            "cid-img1",
            json!({"text": "a photo post", "createdAt": "2024-01-01T00:00:00Z"}),
        )
        .await
        .unwrap();
    store
        .save_media(
            StorageCategory::Post,
            image_post,
            "img1.jpg",
            Some("image/jpeg".to_string()),
            vec![0xFFu8; 8],
        )
        .await
        .unwrap();

    let video_like = "at://did:plc:bob/app.bsky.feed.post/vid1";
    store
        .save_post(
            StorageCategory::Like,
            video_like,
            "cid-vid1",
            json!({"text": "a liked video"}),
        )
        .await
        .unwrap();
    store
        .save_media(
            StorageCategory::Like,
            video_like,
            "vid1.mp4",
            Some("video/mp4".to_string()),
            vec![0x00u8; 8],
        )
        .await
        .unwrap();

    (image_post.to_string(), video_like.to_string())
}

#[tokio::test]
async fn dashboard_renders_counts_health_and_recent_activity() {
    let (_dir, state) = test_state().await;
    seed_fixture_media(&state.store).await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let response = app
        .clone()
        .oneshot(
            Request::get("/")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_string(response).await;

    // The dashboard's fast path renders the recent grid excerpt-less and
    // self-refreshes it via htmx after load.
    assert!(
        body.contains("Connected") || body.contains("Degraded"),
        "health status missing"
    );
    assert!(body.contains("badge-post"), "category badge missing");
    assert!(
        body.contains(&format!("bsky-archiver v{}", env!("CARGO_PKG_VERSION"))),
        "version footer missing"
    );

    let fragment = app
        .clone()
        .oneshot(
            Request::get("/recent")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(fragment.status(), StatusCode::OK);
    let fragment_body = body_string(fragment).await;
    assert!(
        fragment_body.contains("a photo post"),
        "post excerpt missing from /recent fragment"
    );
    assert!(
        fragment_body.contains("badge-post")
            && fragment_body.contains("<img")
            && fragment_body.contains("alt="),
        "fragment cards/badges/thumbnails missing"
    );
}

#[tokio::test]
async fn posts_list_renders_cards_badges_thumbnails_and_pagination() {
    let (_dir, state) = test_state().await;
    seed_fixture_media(&state.store).await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let response = app
        .clone()
        .oneshot(
            Request::get("/posts")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_string(response).await;

    // Fast path: skeleton rows without excerpts; the excerpted list arrives
    // via the self-refresh htmx fragment.
    assert!(
        body.contains("hx-get=\"/") && body.contains("hx-trigger=\"load\""),
        "posts list self-refresh markers missing"
    );
    assert!(body.contains("badge-post") && body.contains("badge-like"));
    assert!(body.contains("<img") && body.contains("alt="));
    assert!(body.contains("<video"));
    assert!(body.contains("class=\"pagination\""));
    assert!(body.contains("Page 1 of 1"));
    // First/last page controls render (disabled) even on a single page,
    // and the PWA integration links land in the head of every full page.
    assert!(body.contains("&laquo; First"));
    assert!(body.contains("Last &raquo;"));
    assert!(body.contains("rel=\"manifest\""));
    assert!(body.contains("name=\"theme-color\""));

    // The htmx fragment swap carries the excerpts.
    let fragment = app
        .clone()
        .oneshot(
            Request::get("/posts")
                .header(header::COOKIE, &cookie)
                .header(header::HeaderName::from_static("hx-request"), "true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(fragment.status(), StatusCode::OK);
    let fragment_body = body_string(fragment).await;
    assert!(
        fragment_body.contains("a photo post") && fragment_body.contains("a liked video"),
        "post excerpts missing from fragment"
    );
    assert!(
        fragment_body.contains("badge-post") && fragment_body.contains("badge-like"),
        "fragment badges missing"
    );
}

#[tokio::test]
async fn posts_list_htmx_request_returns_only_the_fragment() {
    let (_dir, state) = test_state().await;
    seed_fixture_media(&state.store).await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let response = app
        .oneshot(
            Request::get("/posts")
                .header(header::COOKIE, cookie)
                .header("HX-Request", "true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_string(response).await;

    assert!(body.contains("id=\"posts-list\""));
    assert!(
        !body.contains("<nav>\n<ul>"),
        "fragment must not include the base layout nav"
    );
    assert!(
        !body.contains("Dashboard</a>"),
        "fragment must not include full-page chrome"
    );
}

#[tokio::test]
async fn post_detail_renders_image_and_video_with_raw_json_and_bluesky_link() {
    let (_dir, state) = test_state().await;
    let (image_post, video_like) = seed_fixture_media(&state.store).await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let image_response = app
        .clone()
        .oneshot(
            Request::get(format!("/posts/{}", encode_post_id(&image_post)))
                .header(header::COOKIE, cookie.clone())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let image_body = body_string(image_response).await;
    assert!(image_body.contains("a photo post"));
    assert!(image_body.contains("<img") && image_body.contains("alt="));
    assert!(image_body.contains("<details>") && image_body.contains("Raw JSON"));
    assert!(image_body.contains("View on Bluesky"));
    assert!(image_body.contains("did:plc:alice"));

    let video_response = app
        .oneshot(
            Request::get(format!("/posts/{}", encode_post_id(&video_like)))
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let video_body = body_string(video_response).await;
    assert!(video_body.contains("a liked video"));
    assert!(video_body.contains("<video") && video_body.contains("controls"));
    assert!(video_body.contains("badge-like"));
}

#[tokio::test]
async fn gallery_renders_media_grid_with_alt_text_and_pagination() {
    let (_dir, state) = test_state().await;
    seed_fixture_media(&state.store).await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let response = app
        .oneshot(
            Request::get("/gallery")
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = body_string(response).await;

    assert!(body.contains("<img") && body.contains("alt="));
    assert!(body.contains("<video"));
    assert!(body.contains("data-lightbox"));
    assert!(body.contains("id=\"lightbox\""));
    assert!(body.contains("class=\"pagination\""));
}

#[tokio::test]
async fn media_route_serves_bytes_with_guessed_content_type() {
    let (_dir, state) = test_state().await;
    let (image_post, _) = seed_fixture_media(&state.store).await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let url = format!(
        "/media/{}/{}/img1.jpg",
        StorageCategory::Post,
        encode_post_id(&image_post)
    );
    let response = app
        .oneshot(
            Request::get(url)
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "image/jpeg"
    );
    assert_eq!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .unwrap()
            .to_str()
            .unwrap(),
        "public, max-age=31536000, immutable"
    );
    assert!(response.headers().get(header::ETAG).is_some());
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(bytes.as_ref(), [0xFFu8; 8]);
}

#[tokio::test]
async fn media_route_etag_is_consistent_across_requests() {
    let (_dir, state) = test_state().await;
    let (image_post, _) = seed_fixture_media(&state.store).await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let url = format!(
        "/media/{}/{}/img1.jpg",
        StorageCategory::Post,
        encode_post_id(&image_post)
    );

    let first = app
        .clone()
        .oneshot(
            Request::get(&url)
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let etag = first
        .headers()
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let second = app
        .oneshot(
            Request::get(&url)
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        second
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap(),
        etag
    );
}

#[tokio::test]
async fn media_route_revalidates_with_if_none_match_into_a_304() {
    let (_dir, state) = test_state().await;
    let (image_post, _) = seed_fixture_media(&state.store).await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let url = format!(
        "/media/{}/{}/img1.jpg",
        StorageCategory::Post,
        encode_post_id(&image_post)
    );

    let first = app
        .clone()
        .oneshot(
            Request::get(&url)
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let etag = first
        .headers()
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let content_length: u64 = first
        .headers()
        .get(header::CONTENT_LENGTH)
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(content_length, 8, "content-length must match the file size");

    // A browser revalidating with the ETag it holds gets the bodyless
    // 304, with fresh lifetime headers per RFC 9110's 304 guidance.
    let revalidated = app
        .clone()
        .oneshot(
            Request::get(&url)
                .header(header::COOKIE, &cookie)
                .header(header::IF_NONE_MATCH, &etag)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(revalidated.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(
        revalidated
            .headers()
            .get(header::CACHE_CONTROL)
            .unwrap()
            .to_str()
            .unwrap(),
        "public, max-age=31536000, immutable"
    );
    assert_eq!(
        revalidated
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap(),
        etag
    );

    // A changed/unknown ETag and the weak form of a *different* etag
    // must not match; a full body comes back.
    let mismatched = app
        .clone()
        .oneshot(
            Request::get(&url)
                .header(header::COOKIE, &cookie)
                .header(header::IF_NONE_MATCH, "\"0-0\"")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(mismatched.status(), StatusCode::OK);

    // The weak-prefixed form of the matching etag compares equal.
    let weak_match = app
        .clone()
        .oneshot(
            Request::get(&url)
                .header(header::COOKIE, &cookie)
                .header(header::IF_NONE_MATCH, format!("W/{etag}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(weak_match.status(), StatusCode::NOT_MODIFIED);

    // Comma-separated candidate lists are honoured too.
    let list_match = app
        .oneshot(
            Request::get(&url)
                .header(header::COOKIE, cookie)
                .header(header::IF_NONE_MATCH, format!("\"nope\", {etag}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_match.status(), StatusCode::NOT_MODIFIED);
}

#[tokio::test]
async fn manifest_is_reachable_without_auth() {
    let (_dir, state) = test_state().await;
    let app = router(state);

    let response = app
        .oneshot(
            Request::get("/manifest.webmanifest")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap(),
        "application/manifest+json"
    );
    let body = body_string(response).await;
    let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid manifest json");
    assert_eq!(parsed["display"], "standalone");
    assert!(parsed["icons"].as_array().unwrap().len() >= 2);
}

#[tokio::test]
async fn pwa_icons_are_reachable_without_auth() {
    let (_dir, state) = test_state().await;
    let app = router(state);

    for path in ["/static/icons/icon-192.png", "/static/icons/icon-512.png"] {
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "image/png"
        );
        assert_eq!(
            response
                .headers()
                .get(header::CACHE_CONTROL)
                .unwrap()
                .to_str()
                .unwrap(),
            "public, max-age=604800"
        );
    }
}

#[tokio::test]
async fn media_route_404s_for_unknown_file() {
    let (_dir, state) = test_state().await;
    let (image_post, _) = seed_fixture_media(&state.store).await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let url = format!(
        "/media/{}/{}/does-not-exist.jpg",
        StorageCategory::Post,
        encode_post_id(&image_post)
    );
    let response = app
        .oneshot(
            Request::get(url)
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn config_page_marks_secrets_redacted_but_shows_plain_values() {
    let (_dir, state) = test_state().await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let response = app
        .oneshot(
            Request::get("/config")
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = body_string(response).await;

    assert!(body.contains("UI_PASSWORD"));
    assert!(body.contains("BSKY_APP_PASSWORD"));
    assert!(body.contains("UI_SESSION_SECRET"));
    assert!(body.matches("&lt;redacted&gt;").count() >= 3);
    assert!(body.contains("alice.bsky.social"));
    assert!(body.contains("class=\"redacted\""));
    // The watched-sources panel is part of /config so htmx has a
    // `#sources-panel` target to swap add/remove responses into.
    assert!(
        body.contains("id=\"sources-panel\""),
        "watched-sources panel must render on /config"
    );
    assert!(
        !body.contains("BSKY_WATCH_HANDLES"),
        "BSKY_WATCH_HANDLES must be gone from the config page"
    );
    assert!(
        body.contains(&format!("bsky-archiver v{}", env!("CARGO_PKG_VERSION"))),
        "version footer missing"
    );
}

#[tokio::test]
async fn login_page_renders_form_and_error_state() {
    let (_dir, state) = test_state().await;
    let app = router(state);

    let blank = app
        .clone()
        .oneshot(Request::get("/login").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let blank_body = body_string(blank).await;
    assert!(blank_body.contains("type=\"password\""));
    assert!(!blank_body.contains("Incorrect password"));

    let rejected = app
        .oneshot(
            Request::post("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("password=nope"))
                .unwrap(),
        )
        .await
        .unwrap();
    let error_body = body_string(rejected).await;
    assert!(error_body.contains("Incorrect password"));
    assert!(error_body.contains("role=\"alert\""));
}

/// Seeds one image under `Post` and one video under `Like`, for the
/// gallery filter / export predicate tests.
async fn seed_image_and_video(store: &ArchiveStore) -> (String, String) {
    let image_post = "at://did:plc:alice/app.bsky.feed.post/img";
    store
        .save_post(StorageCategory::Post, image_post, "cid-img", json!({}))
        .await
        .unwrap();
    store
        .save_media(
            StorageCategory::Post,
            image_post,
            "000.jpg",
            Some("image/jpeg".to_string()),
            vec![0xFFu8; 8],
        )
        .await
        .unwrap();

    let video_like = "at://did:plc:bob/app.bsky.feed.post/vid";
    store
        .save_post(StorageCategory::Like, video_like, "cid-vid", json!({}))
        .await
        .unwrap();
    store
        .save_media(
            StorageCategory::Like,
            video_like,
            "000.mp4",
            Some("video/mp4".to_string()),
            vec![0x00u8; 8],
        )
        .await
        .unwrap();

    (image_post.to_string(), video_like.to_string())
}

#[tokio::test]
async fn gallery_category_filter_isolates_media() {
    let (_dir, state) = test_state().await;
    let (image_post, video_like) = seed_image_and_video(&state.store).await;
    let key = Key::derive_from(state.config.ui_session_secret.expose_secret().as_bytes());
    let app_state = WebState {
        app: Arc::clone(&state),
        key,
    };

    // Filtering to likes shows only the like's media, not the post's.
    let likes = gallery(
        State(app_state.clone()),
        Query(GalleryQuery {
            category: Some("like".to_string()),
            page: Some(1),
            page_size: Some(20),
            sort: None,
        }),
        HeaderMap::new(),
    )
    .await
    .unwrap();
    let likes_body = body_string(likes.into_response()).await;
    assert!(likes_body.contains(&encode_post_id(&video_like)));
    assert!(!likes_body.contains(&encode_post_id(&image_post)));
    // The category nav marks the current selection.
    assert!(likes_body.contains("<span class=\"current\">Likes</span>"));
}

#[tokio::test]
async fn gallery_unknown_sort_is_a_400() {
    let (_dir, state) = test_state().await;
    let key = Key::derive_from(state.config.ui_session_secret.expose_secret().as_bytes());
    let app_state = WebState {
        app: Arc::clone(&state),
        key,
    };

    let result = gallery(
        State(app_state),
        Query(GalleryQuery {
            category: None,
            page: Some(1),
            page_size: Some(10),
            sort: Some("bogus".to_string()),
        }),
        HeaderMap::new(),
    )
    .await;
    assert!(matches!(result, Err(WebError::BadRequest { .. })));
}

#[tokio::test]
async fn gallery_sort_is_marked_in_options_and_survives_pagination() {
    let (_dir, state) = test_state().await;
    seed_image_and_video(&state.store).await;
    // A second post-category image so the posts selection spans 2 pages
    // at page_size=1 and the pagination carries the sort along.
    let extra = "at://did:plc:alice/app.bsky.feed.post/extra";
    state
        .store
        .save_post(StorageCategory::Post, extra, "cid-extra", json!({}))
        .await
        .unwrap();
    state
        .store
        .save_media(
            StorageCategory::Post,
            extra,
            "001.jpg",
            Some("image/jpeg".to_string()),
            vec![0x11u8; 6],
        )
        .await
        .unwrap();
    let key = Key::derive_from(state.config.ui_session_secret.expose_secret().as_bytes());
    let app_state = WebState {
        app: Arc::clone(&state),
        key,
    };

    let page = gallery(
        State(app_state),
        Query(GalleryQuery {
            category: Some("post".to_string()),
            page: Some(1),
            page_size: Some(1),
            sort: Some("created-oldest".to_string()),
        }),
        HeaderMap::new(),
    )
    .await
    .unwrap();
    let body = body_string(page.into_response()).await;

    // The sort select is present, with every option offered. Askama
    // escapes the `&` separators inside option values.
    for href in [
        "/gallery?category=post&amp;sort=newest&amp;page=1&amp;page_size=1",
        "/gallery?category=post&amp;sort=oldest&amp;page=1&amp;page_size=1",
        "/gallery?category=post&amp;sort=created-newest&amp;page=1&amp;page_size=1",
        "/gallery?category=post&amp;sort=created-oldest&amp;page=1&amp;page_size=1",
    ] {
        assert!(
            body.contains(href),
            "sort option missing from the select: {href}"
        );
    }
    // The active option is marked selected.
    assert!(
        body.contains("created-oldest"),
        "created-oldest option rendered"
    );

    // Pagination links keep the active sort while moving pages.
    assert!(body.contains("sort=created-oldest&amp;page=2"));
}

#[tokio::test]
async fn gallery_export_estimate_counts_images_only_including_null_content_type() {
    let (_dir, state) = test_state().await;
    // A null-content-type row whose filename extension marks it an image
    // (the case a bare `content_type LIKE 'image/%'` predicate would drop).
    let null_ct = "at://did:plc:alice/app.bsky.feed.post/pngnull";
    state
        .store
        .save_post(StorageCategory::Post, null_ct, "cid-png", json!({}))
        .await
        .unwrap();
    state
        .store
        .save_media(
            StorageCategory::Post,
            null_ct,
            "000.png",
            None,
            vec![0u8; 4],
        )
        .await
        .unwrap();
    // A video under the same category — excluded from the count.
    seed_image_and_video(&state.store).await;

    let key = Key::derive_from(state.config.ui_session_secret.expose_secret().as_bytes());
    let app_state = WebState {
        app: Arc::clone(&state),
        key,
    };

    // Posts category: the jpg image + the null-content-type png = 2, no video.
    let posts = gallery(
        State(app_state.clone()),
        Query(GalleryQuery {
            category: Some("post".to_string()),
            page: Some(1),
            page_size: Some(20),
            sort: None,
        }),
        HeaderMap::new(),
    )
    .await
    .unwrap();
    let posts_body = body_string(posts.into_response()).await;
    assert!(posts_body.contains("2 images"));
    assert!(posts_body.contains("Download zip"));

    // Likes category: only a video → empty export selection.
    let likes = gallery(
        State(app_state),
        Query(GalleryQuery {
            category: Some("like".to_string()),
            page: Some(1),
            page_size: Some(20),
            sort: None,
        }),
        HeaderMap::new(),
    )
    .await
    .unwrap();
    let likes_body = body_string(likes.into_response()).await;
    assert!(likes_body.contains("No images in this selection to export."));
    assert!(!likes_body.contains("Download zip"));
}

#[tokio::test]
async fn gallery_and_export_reject_unknown_category_with_400() {
    let (_dir, state) = test_state().await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    for url in ["/gallery?category=bogus", "/gallery/export?category=bogus"] {
        let response = app
            .clone()
            .oneshot(
                Request::get(url)
                    .header(header::COOKIE, cookie.clone())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "unknown category on {url} should be 400"
        );
    }
}

#[tokio::test]
async fn gallery_export_empty_selection_is_404() {
    let (_dir, state) = test_state().await;
    // Only a video is archived, so the image export selection is empty.
    seed_image_and_video(&state.store).await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let response = app
        .oneshot(
            Request::get("/gallery/export?category=like")
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn gallery_export_streams_store_only_zip_of_images() {
    use std::io::Read as _;

    let (_dir, state) = test_state().await;
    let (image_post, _video_like) = seed_image_and_video(&state.store).await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let response = app
        .oneshot(
            Request::get("/gallery/export")
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/zip"
    );

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let mut archive =
        zip::ZipArchive::new(std::io::Cursor::new(bytes.to_vec())).expect("valid zip");
    assert_eq!(archive.len(), 1, "only the image, not the video");
    let mut entry = archive.by_index(0).unwrap();
    assert_eq!(
        entry.name(),
        format!("posts/{}/000.jpg", encode_post_id(&image_post))
    );
    // Store-only: compression method 0.
    assert_eq!(entry.compression(), zip::CompressionMethod::Stored);
    let mut buf = Vec::new();
    entry.read_to_end(&mut buf).unwrap();
    assert_eq!(buf, vec![0xFFu8; 8]);
}

// ---------------------------------------------------------------------
// Watched sources (UI-managed watch list)
// ---------------------------------------------------------------------

/// A test state whose shared Bluesky client points at `base_uri` (a
/// wiremock server), so the sources routes' outbound resolve_handle /
/// get_feed / backfill calls are fully mocked. Returns the strong
/// candidate sender too, so the add-handlers' immediate-backfill tasks
/// can actually upgrade the weak handle (dropping it would simulate
/// shutdown and silently skip backfill).
async fn test_state_with_bluesky(
    base_uri: url::Url,
) -> (
    tempfile::TempDir,
    SharedAppState,
    crate::pipeline::CandidatePostSender,
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let archive_dir = dir.path().join("archive");
    let database_path = dir.path().join("index.sqlite3");
    let store = ArchiveStore::open(archive_dir.clone(), database_path)
        .await
        .expect("open store");

    let config = AppConfig {
        bsky_identifier: "alice.bsky.social".to_string(),
        bsky_app_password: Secret::from("bsky-app-password-secret".to_string()),
        tumblr: None,
        archive_dir,
        database_path: dir.path().join("index.sqlite3"),
        ui_password: Secret::from("correct horse battery staple".to_string()),
        ui_session_secret: Secret::from("a".repeat(64)),
        ui_port: 8080,
        poll_interval_seconds: 120,
        jetstream_url: url::Url::parse("wss://jetstream.example.com/subscribe").unwrap(),
        media_max_concurrent_downloads: 4,
        media_max_bytes: 104_857_600,
        nightly_sweep_local_hour: 3,
        tumblr_poll_interval_seconds: 300,
    };

    let (candidate_tx, _candidate_rx) = crate::pipeline::candidate_post_channel(8);
    let state: SharedAppState = Arc::new(AppState {
        config,
        store,
        health: health_channel().1,
        watchlist: crate::watchlist::Watchlist::new(Vec::new()),
        bluesky: Arc::new(crate::bluesky::BlueskyClient::new(
            base_uri,
            "alice.bsky.social".to_string(),
            Secret::from("bsky-app-password-secret".to_string()),
        )),
        candidate_weak: crate::pipeline::weak_from_sender(&candidate_tx),
    });
    (dir, state, candidate_tx)
}

async fn mount_ui_session(server: &wiremock::MockServer) {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, ResponseTemplate};
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.server.createSession"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "accessJwt": "token",
            "refreshJwt": "refresh",
            "did": "did:plc:alice",
            "handle": "alice.bsky.social",
        })))
        .mount(server)
        .await;
}

async fn add_source_form(
    app: &Router,
    cookie: &str,
    body: &str,
    htmx: bool,
) -> (StatusCode, String) {
    let mut request_builder =
        Request::post("/sources").header(header::CONTENT_TYPE, "application/x-www-form-urlencoded");
    if htmx {
        request_builder = request_builder.header("HX-Request", "true");
    }
    let response = app
        .clone()
        .oneshot(
            request_builder
                .header(header::COOKIE, cookie)
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = body_string(response).await;
    (status, body)
}

#[tokio::test]
async fn sources_routes_require_authentication() {
    let (_dir, state) = test_state().await;
    let app = router(state);

    let response = app
        .oneshot(Request::post("/sources").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(response.headers().get(header::LOCATION).unwrap(), "/login");
}

#[tokio::test]
async fn adding_an_account_via_htmx_persists_resolves_reloads_and_backfills() {
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, ResponseTemplate};

    let server = wiremock::MockServer::start().await;
    mount_ui_session(&server).await;
    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.identity.resolveHandle"))
        .and(query_param("handle", "bob.bsky.social"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"did": "did:plc:bob"})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getAuthorFeed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"feed": [], "cursor": null})))
        .mount(&server)
        .await;

    let (_dir, state, _candidate_tx) =
        test_state_with_bluesky(url::Url::parse(&server.uri()).unwrap()).await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let (status, body) =
        add_source_form(&app, &cookie, "kind=account&value=bob.bsky.social", true).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("id=\"sources-panel\""),
        "fragment is the panel: {body}"
    );
    assert!(
        body.contains("bob.bsky.social"),
        "added account must appear: {body}"
    );

    // The account is persisted and the live roster reloaded.
    let sources = state.store.list_watched_sources().await.unwrap();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].value, "bob.bsky.social");
    assert_eq!(sources[0].did.as_deref(), Some("did:plc:bob"));
    assert_eq!(state.watchlist.snapshot(), sources);

    // Adding an account kicks an immediate backfill (not waiting for the
    // next poll interval).
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .any(|r| r.url.path() == "/xrpc/app.bsky.feed.getAuthorFeed")
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("immediate backfill after adding an account");
}

#[tokio::test]
async fn adding_an_unresolvable_handle_returns_an_inline_error_and_persists_nothing() {
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, ResponseTemplate};

    let server = wiremock::MockServer::start().await;
    mount_ui_session(&server).await;
    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.identity.resolveHandle"))
        .and(query_param("handle", "nobody.bsky.social"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": "InvalidRequest",
            "message": "unable to resolve handle",
        })))
        .mount(&server)
        .await;

    let (_dir, state, _candidate_tx) =
        test_state_with_bluesky(url::Url::parse(&server.uri()).unwrap()).await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let (status, body) =
        add_source_form(&app, &cookie, "kind=account&value=nobody.bsky.social", true).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("could not resolve handle"),
        "error must be inline in the panel: {body}"
    );
    assert!(
        state.store.list_watched_sources().await.unwrap().is_empty(),
        "an unresolvable handle must not be persisted"
    );
}

#[tokio::test]
async fn adding_a_feed_validates_with_get_feed_and_persists_it() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, ResponseTemplate};

    let server = wiremock::MockServer::start().await;
    mount_ui_session(&server).await;
    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getFeed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"feed": [], "cursor": null})))
        .mount(&server)
        .await;

    let (_dir, state, _candidate_tx) =
        test_state_with_bluesky(url::Url::parse(&server.uri()).unwrap()).await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let feed_uri = "at://did:plc:alice/app.bsky.feed.generator/whats-hot";
    let body = format!("kind=feed&value={feed_uri}");
    let (status, response_body) = add_source_form(&app, &cookie, &body, true).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        response_body.contains(feed_uri),
        "added feed must appear: {response_body}"
    );

    let sources = state.store.list_watched_sources().await.unwrap();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].kind, SourceKind::Feed);
    assert_eq!(sources[0].value, feed_uri);
    assert_eq!(sources[0].did, None);
}

#[tokio::test]
async fn adding_a_malformed_feed_uri_returns_an_inline_error() {
    let (_dir, state) = test_state().await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let (status, body) = add_source_form(
        &app,
        &cookie,
        "kind=feed&value=https%3A%2F%2Fexample.com",
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("is not an at:// feed URI"),
        "malformed feed must be rejected inline: {body}"
    );
    assert!(state.store.list_watched_sources().await.unwrap().is_empty());
}

#[tokio::test]
async fn removing_a_source_stops_watching_it_live() {
    let (_dir, state) = test_state().await;
    let id = state
        .store
        .add_watched_source(
            SourceKind::Account,
            "carol.bsky.social",
            Some("did:plc:carol"),
        )
        .await
        .unwrap();

    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let response = app
        .clone()
        .oneshot(
            Request::post(format!("/sources/{id}"))
                .header(header::COOKIE, cookie)
                .header("HX-Request", "true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_string(response).await;
    assert!(
        !body.contains("carol.bsky.social"),
        "removed source must vanish: {body}"
    );
    assert!(
        state.store.list_watched_sources().await.unwrap().is_empty(),
        "removed source must be gone from the database"
    );
}

/// Non-htmx (no-JS) source mutation posts redirect back to the config
/// page rather than returning a bare fragment.
#[tokio::test]
async fn non_htmx_source_mutation_redirects_to_config() {
    let (_dir, state) = test_state().await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let (status, body) =
        add_source_form(&app, &cookie, "kind=account&value=bob.bsky.social", false).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    assert_eq!(body, "");
}

// ---------------------------------------------------------------------
// Browser (/browser: the account viewer + saved favorites)
// ---------------------------------------------------------------------

/// A getAuthorFeed page (`filter=posts_with_media`) mixing an authored
/// two-image post, a repost of someone else's picture, and a video post.
fn account_viewer_feed_page() -> serde_json::Value {
    json!({
        "feed": [
            {
                "post": {
                    "uri": "at://did:plc:bob/app.bsky.feed.post/1",
                    "cid": "cid-1",
                    "author": {"did": "did:plc:bob"},
                    "record": {"text": "two pics"},
                    "embed": {
                        "$type": "app.bsky.embed.images#view",
                        "images": [
                            {
                                "thumb": "https://cdn.example.com/t1.jpg",
                                "fullsize": "https://cdn.example.com/f1.jpg",
                                "alt": "first pic",
                            },
                            {
                                "thumb": "https://cdn.example.com/t2.jpg",
                                "fullsize": "https://cdn.example.com/f2.jpg",
                                "alt": "",
                            },
                        ],
                    },
                }
            },
            {
                "reason": {"$type": "app.bsky.feed.defs#reasonRepost", "by": {"did": "did:plc:bob"}},
                "post": {
                    "uri": "at://did:plc:carol/app.bsky.feed.post/9",
                    "cid": "cid-9",
                    "author": {"did": "did:plc:carol"},
                    "record": {"text": "carol's pic"},
                    "embed": {
                        "$type": "app.bsky.embed.images#view",
                        "images": [{
                            "thumb": "https://cdn.example.com/t9.jpg",
                            "fullsize": "https://cdn.example.com/f9.jpg",
                            "alt": "carol's",
                        }],
                    },
                }
            },
            {
                "post": {
                    "uri": "at://did:plc:bob/app.bsky.feed.post/2",
                    "cid": "cid-2",
                    "author": {"did": "did:plc:bob"},
                    "record": {"text": "a video"},
                    "embed": {
                        "$type": "app.bsky.embed.video#view",
                        "playlist": "https://video.example.com/playlist.m3u8",
                    },
                }
            },
        ],
        "cursor": "next-cursor",
    })
}

#[tokio::test]
async fn account_gallery_without_actor_renders_just_the_form() {
    let (_dir, state) = test_state().await;
    let app = router(state);
    let cookie = login(&app).await;

    let response = app
        .oneshot(
            Request::get("/browser")
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_string(response).await;

    assert!(body.contains("Browser"));
    assert!(body.contains("name=\"actor\""));
    assert!(body.contains("name=\"skip_reposts\""));
    assert!(
        !body.contains("id=\"account-gallery-grid\""),
        "no grid before an actor is given"
    );
}

#[tokio::test]
async fn account_viewer_browses_live_pictures_with_repost_and_video_handling() {
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, ResponseTemplate};

    let server = wiremock::MockServer::start().await;
    mount_ui_session(&server).await;
    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getAuthorFeed"))
        .and(query_param("actor", "bob.bsky.social"))
        .and(query_param("filter", "posts_with_media"))
        .respond_with(ResponseTemplate::new(200).set_body_json(account_viewer_feed_page()))
        .mount(&server)
        .await;

    let (_dir, state, _candidate_tx) =
        test_state_with_bluesky(url::Url::parse(&server.uri()).unwrap()).await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let response = app
        .clone()
        .oneshot(
            Request::get("/browser?actor=bob.bsky.social")
                .header(header::COOKIE, cookie.clone())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_string(response).await;

    // Authored pictures render (thumbnail in the grid, fullsize for the
    // lightbox), with the empty alt filled in.
    assert!(body.contains("https://cdn.example.com/t1.jpg"));
    assert!(body.contains("https://cdn.example.com/f1.jpg"));
    assert!(body.contains("data-alt=\"first pic\""));
    assert!(body.contains("data-alt=\"Picture from bob.bsky.social\""));
    // The repost's picture is included by default...
    assert!(body.contains("https://cdn.example.com/f9.jpg"));
    // ...videos are not (the viewer is pictures-only)...
    assert!(!body.contains("playlist.m3u8"));
    // ...and each picture links back to its post on bsky.app.
    assert!(body.contains("https://bsky.app/profile/did:plc:bob/post/1"));

    // The API returned a cursor, so an "older" link continues the browse
    // with the same actor and htmx swap targets.
    assert!(body.contains("rel=\"next\""));
    assert!(body.contains("cursor=next"));

    // With the repost option on, the repost's picture is skipped.
    let response = app
        .oneshot(
            Request::get("/browser?actor=bob.bsky.social&skip_reposts=on")
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = body_string(response).await;
    assert!(!body.contains("https://cdn.example.com/f9.jpg"));
    assert!(body.contains("https://cdn.example.com/f1.jpg"));
    assert!(body.contains("checked"), "the checkbox state is preserved");

    // Every picture carries its post's strong ref for the lightbox's
    // like/bookmark buttons.
    assert!(body.contains("data-post-uri=\"at://did:plc:bob/app.bsky.feed.post/1\""));
    assert!(body.contains("data-post-cid=\"cid-1\""));
}

#[tokio::test]
async fn account_viewer_sort_reverses_the_page_and_survives_pagination() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, ResponseTemplate};

    let server = wiremock::MockServer::start().await;
    mount_ui_session(&server).await;
    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getAuthorFeed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(account_viewer_feed_page()))
        .mount(&server)
        .await;

    let (_dir, state, _candidate_tx) =
        test_state_with_bluesky(url::Url::parse(&server.uri()).unwrap()).await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    // Default (newest) keeps the API order: post 1's first image precedes
    // its second.
    let response = app.clone()
        .oneshot(
            Request::get("/browser?actor=bob.bsky.social&skip_reposts=on")
                .header(header::COOKIE, cookie.clone())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = body_string(response).await;
    assert!(body.find("f1.jpg") < body.find("f2.jpg"));
    assert!(
        body.contains("<option value=\"/browser?actor=bob%2Ebsky%2Esocial&amp;skip_reposts=on\" selected>"),
        "the default run marks 'newest first' selected"
    );

    // Oldest reverses the page: post 1's images come back in the opposite
    // order, the picker marks the option, and the "older" link keeps the
    // sort so subsequent pages stay reversed.
    let response = app
        .oneshot(
            Request::get("/browser?actor=bob.bsky.social&skip_reposts=on&sort=oldest")
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = body_string(response).await;
    assert!(body.find("f2.jpg") < body.find("f1.jpg"));
    assert!(
        body.contains("sort=oldest&amp;cursor=next"),
        "the older link keeps the sort"
    );
    assert!(
        body.contains("<option value=\"/browser?actor=bob%2Ebsky%2Esocial&amp;skip_reposts=on&amp;sort=oldest\" selected>"),
        "the sort picker marks the active option"
    );
}

#[tokio::test]
async fn like_and_bookmark_actions_create_records_with_the_post_ref() {
    use wiremock::matchers::{body_partial_json, method, path};
    use wiremock::{Mock, ResponseTemplate};

    let server = wiremock::MockServer::start().await;
    mount_ui_session(&server).await;
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.createRecord"))
        .and(body_partial_json(json!({
            "collection": "app.bsky.feed.like",
            "record": {
                "subject": {
                    "uri": "at://did:plc:bob/app.bsky.feed.post/1",
                    "cid": "cid-1",
                },
            },
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "uri": "at://did:plc:alice/app.bsky.feed.like/abc",
            "cid": "like-cid",
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/xrpc/app.bsky.bookmark.createBookmark"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;

    let (_dir, state, _candidate_tx) =
        test_state_with_bluesky(url::Url::parse(&server.uri()).unwrap()).await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let post = |endpoint: &str| {
        let cookie = cookie.clone();
        app.clone()
            .oneshot(
                Request::post(endpoint)
                    .header(header::COOKIE, cookie)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "uri": "at://did:plc:bob/app.bsky.feed.post/1",
                            "cid": "cid-1",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
    };

    let response = post("/browser/like").await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_string(response).await;
    assert!(body.contains("\"ok\":true"));
    assert!(body.contains("Liked"));

    let response = post("/browser/bookmark").await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_string(response).await;
    assert!(body.contains("\"ok\":true"));
    assert!(body.contains("Bookmarked"));

    // A malformed ref is rejected with a 400 and no API write attempted.
    let response = app
        .oneshot(
            Request::post("/browser/like")
                .header(header::COOKIE, cookie)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({"uri": "not-a-uri", "cid": ""}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn browser_limit_param_drives_the_feed_page_size_and_persists_in_links() {
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, ResponseTemplate};

    let server = wiremock::MockServer::start().await;
    mount_ui_session(&server).await;
    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getAuthorFeed"))
        .and(query_param("limit", "57"))
        .respond_with(ResponseTemplate::new(200).set_body_json(account_viewer_feed_page()))
        .mount(&server)
        .await;

    let (_dir, state, _candidate_tx) =
        test_state_with_bluesky(url::Url::parse(&server.uri()).unwrap()).await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let response = app
        .oneshot(
            Request::get("/browser?actor=bob.bsky.social&limit=57")
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_string(response).await;

    // The wrapper echoes the effective limit for the fill top-up, and the
    // "older" link keeps it so subsequent pages keep the alignment.
    assert!(body.contains("data-fill-current=\"57\""));
    assert!(
        body.contains("&amp;limit=57&amp;cursor=next"),
        "the older link keeps the limit"
    );
}

#[tokio::test]
async fn account_viewer_shows_an_inline_error_when_the_feed_fails() {
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, ResponseTemplate};

    let server = wiremock::MockServer::start().await;
    mount_ui_session(&server).await;
    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getAuthorFeed"))
        .and(query_param("actor", "nobody.bsky.social"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": "InvalidRequest",
            "message": "unable to resolve handle",
        })))
        .mount(&server)
        .await;

    let (_dir, state, _candidate_tx) =
        test_state_with_bluesky(url::Url::parse(&server.uri()).unwrap()).await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let response = app
        .oneshot(
            Request::get("/browser?actor=nobody.bsky.social")
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_string(response).await;
    assert!(
        body.contains("could not load"),
        "the failure is shown inline: {body}"
    );
    assert!(body.contains("role=\"alert\""));
}

// ---------------------------------------------------------------------
// Saved accounts (favorites) on /browser
// ---------------------------------------------------------------------

async fn add_saved_form(
    app: &Router,
    cookie: &str,
    body: &str,
    htmx: bool,
) -> (StatusCode, String) {
    let mut request_builder = Request::post("/browser/saved")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded");
    if htmx {
        request_builder = request_builder.header("HX-Request", "true");
    }
    let response = app
        .clone()
        .oneshot(
            request_builder
                .header(header::COOKIE, cookie)
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = body_string(response).await;
    (status, body)
}

#[tokio::test]
async fn browser_page_shows_saved_accounts_and_the_add_strips_the_at_sign() {
    let (_dir, state) = test_state().await;
    state
        .store
        .add_saved_account("alice.bsky.social")
        .await
        .expect("seed saved account");
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let response = app
        .clone()
        .oneshot(
            Request::get("/browser")
                .header(header::COOKIE, cookie.clone())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_string(response).await;
    assert!(body.contains("Saved accounts"));
    assert!(body.contains("alice.bsky.social"));
    assert!(body.contains("action=\"/browser/saved/1\""));

    // Saving strips the leading @ so the stored (and browsed) handle stays
    // uniform.
    let (status, panel) = add_saved_form(&app, &cookie, "handle=%40bob.bsky.social", true).await;
    assert_eq!(status, StatusCode::OK);
    assert!(panel.contains("bob.bsky.social"));
    let saved = state.store.list_saved_accounts().await.unwrap();
    let handles: Vec<_> = saved.iter().map(|a| a.handle.as_str()).collect();
    assert_eq!(handles, vec!["alice.bsky.social", "bob.bsky.social"]);
}

#[tokio::test]
async fn re_saving_a_saved_handle_stays_a_single_row() {
    let (_dir, state) = test_state().await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let (status, _) = add_saved_form(&app, &cookie, "handle=bob.bsky.social", true).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = add_saved_form(&app, &cookie, "handle=bob.bsky.social", true).await;
    assert_eq!(status, StatusCode::OK);

    let saved = state.store.list_saved_accounts().await.unwrap();
    assert_eq!(saved.len(), 1);
}

#[tokio::test]
async fn removing_a_saved_account_via_htmx_swaps_the_panel_without_it() {
    let (_dir, state) = test_state().await;
    state
        .store
        .add_saved_account("alice.bsky.social")
        .await
        .unwrap();
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let response = app
        .oneshot(
            Request::post("/browser/saved/1")
                .header("HX-Request", "true")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_string(response).await;
    assert!(body.contains("No saved accounts yet"));
    assert!(state.store.list_saved_accounts().await.unwrap().is_empty());
}

#[tokio::test]
async fn non_htmx_saved_account_mutation_redirects_to_the_browser() {
    let (_dir, state) = test_state().await;
    let app = router(Arc::clone(&state));
    let cookie = login(&app).await;

    let response = app
        .clone()
        .oneshot(
            Request::post("/browser/saved")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header(header::COOKIE, &cookie)
                .body(Body::from("handle=dave.bsky.social"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        response.headers().get(header::LOCATION).unwrap(),
        "/browser"
    );
    let saved = state.store.list_saved_accounts().await.unwrap();
    assert_eq!(
        saved.iter().map(|a| a.handle.as_str()).collect::<Vec<_>>(),
        vec!["dave.bsky.social"]
    );
}

#[tokio::test]
async fn old_gallery_account_url_redirects_to_the_browser() {
    let (_dir, state) = test_state().await;
    let app = router(state);
    let cookie = login(&app).await;

    let response = app
        .oneshot(
            Request::get("/gallery/account?actor=bob.bsky.social&skip_reposts=on")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let location = response
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        location.starts_with("/browser?actor=bob%2Ebsky%2Esocial"),
        "location: {location}"
    );
    assert!(location.contains("skip_reposts=on"));
}
