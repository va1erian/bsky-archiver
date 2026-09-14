use super::*;
use serde_json::json;
use std::sync::Arc;
use wiremock::matchers::{body_string_contains, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------
// Auth response parsing
// ---------------------------------------------------------------------

fn auth_success_body() -> serde_json::Value {
    json!({
        "has_error": false,
        "response": {
            "access_token": "access-token-1",
            "expires_in": 3600,
            "refresh_token": "refresh-token-2",
            "user": {"id": "4321", "name": "tester", "account": "tester"}
        }
    })
}

async fn mount_auth(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/auth/token"))
        .and(body_string_contains("grant_type=refresh_token"))
        .and(body_string_contains(
            "client_id=MOBrBDS8blbauoSck0ZfDbtuzpyT",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(auth_success_body()))
        .mount(server)
        .await;
}

#[tokio::test]
async fn auth_parses_access_token_and_user_id() {
    let server = MockServer::start().await;
    mount_auth(&server).await;

    let client = PixivClient::new(
        Url::parse(&server.uri()).unwrap(),
        Url::parse(&server.uri()).unwrap(),
        Secret::from("refresh-token".to_string()),
    );
    assert_eq!(client.user_id().await.expect("user id"), 4321);

    // The same token is reused while fresh: a second call must not hit the
    // auth endpoint again.
    assert_eq!(client.user_id().await.expect("user id"), 4321);
}

#[tokio::test]
async fn auth_error_surface_has_error_flag() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/auth/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "has_error": true,
            "error": {"message": "invalid_grant", "code": 919},
            "errors": {"system": {"message": "invalid_grant", "code": 919}}
        })))
        .mount(&server)
        .await;

    let client = PixivClient::new(
        Url::parse(&server.uri()).unwrap(),
        Url::parse(&server.uri()).unwrap(),
        Secret::from("bad-refresh-token".to_string()),
    );
    let err = client.user_id().await.expect_err("auth should fail");
    assert!(err.to_string().contains("invalid_grant"), "{err}");
}

// ---------------------------------------------------------------------
// Bookmark page parsing
// ---------------------------------------------------------------------

#[test]
fn bookmarks_page_parses_illusts_and_next_cursor() {
    let body = json!({
        "illusts": [{"id": 1}, {"id": 2}],
        "next_url": "https://app-api.pixiv.net/v1/user/bookmarks/illust?restrict=public&user_id=4321&max_bookmark_id=13765616633"
    });
    let page = BookmarksPage::from_json(&body).expect("parse");
    assert_eq!(page.illusts.len(), 2);
    assert_eq!(page.next_cursor.as_deref(), Some("13765616633"));
}

#[test]
fn bookmarks_page_without_next_url_is_the_end() {
    let body = json!({"illusts": [{"id": 1}], "next_url": null});
    let page = BookmarksPage::from_json(&body).expect("parse");
    assert_eq!(page.next_cursor, None);
}

#[test]
fn bookmarks_page_missing_illusts_is_a_shape_error() {
    let body = json!({});
    assert!(matches!(
        BookmarksPage::from_json(&body),
        Err(PixivError::Shape(_))
    ));
}

// ---------------------------------------------------------------------
// Identity + media extraction
// ---------------------------------------------------------------------

fn bookmarked_illust(id: u64, user_id: u64, user_name: &str) -> serde_json::Value {
    json!({
        "id": id,
        "title": "a picture",
        "type": "illust",
        "create_date": "2026-01-01T00:00:00+09:00",
        "page_count": 1,
        "user": {"id": user_id, "name": user_name},
        "image_urls": {"square_medium": "https://i.pximg.net/c/360x360/img-master/img/x.jpg"},
        "meta_single_page": {"original_image_url": format!("https://i.pximg.net/img-original/img/2026/01/01/00/00/00/{id}_p0.png")},
    })
}

#[test]
fn bookmark_identity_uses_pixiv_user_and_illust_ids() {
    let illust = bookmarked_illust(123, 456, "artist");
    let (key, name) = bookmark_identity(&illust).expect("identity");
    assert_eq!(key, "pixiv:456/123");
    assert_eq!(name, "artist");
}

#[test]
fn bookmark_identity_handles_string_ids() {
    let illust = json!({
        "id": "123",
        "user": {"id": "456", "name": "artist"},
    });
    let (key, _) = bookmark_identity(&illust).expect("identity");
    assert_eq!(key, "pixiv:456/123");
}

#[test]
fn bookmark_identity_rejects_missing_ids() {
    assert!(bookmark_identity(&json!({"user": {"id": 1}})).is_none());
    assert!(bookmark_identity(&json!({"id": 1})).is_none());
}

#[test]
fn single_page_illust_extracts_original() {
    let illust = bookmarked_illust(1, 2, "artist");
    let refs = extract_media_refs(&illust);
    assert_eq!(refs.len(), 1);
    assert_eq!(
        refs[0].cdn_url,
        "https://i.pximg.net/img-original/img/2026/01/01/00/00/00/1_p0.png"
    );
}

#[test]
fn multi_page_illust_extracts_every_page_original() {
    let illust = json!({
        "id": 9,
        "page_count": 3,
        "user": {"id": 1, "name": "artist"},
        "meta_pages": [
            {"image_urls": {"original": "https://i.pximg.net/img-original/img/a/9_p0.png"}},
            {"image_urls": {"original": "https://i.pximg.net/img-original/img/a/9_p1.png"}},
            {"image_urls": {"original": "https://i.pximg.net/img-original/img/a/9_p2.png"}},
        ],
    });
    let refs = extract_media_refs(&illust);
    assert_eq!(refs.len(), 3);
    assert!(refs[0].cdn_url.ends_with("9_p0.png"));
    assert!(refs[2].cdn_url.ends_with("9_p2.png"));
}

#[test]
fn ugoira_without_originals_has_no_media() {
    // Ugoira works carry no original image URL (their frames ship as a zip
    // through a separate endpoint); they archive record-only for now.
    let illust = json!({
        "id": 5,
        "type": "ugoira",
        "page_count": 1,
        "user": {"id": 1, "name": "artist"},
        "image_urls": {"large": "https://i.pximg.net/c/600x1200/img-master/img/x.jpg"},
    });
    assert!(extract_media_refs(&illust).is_empty());
}

#[test]
fn malformed_media_fields_are_skipped_not_fatal() {
    for malformed in [
        json!({"meta_pages": "not an array"}),
        json!({"meta_pages": [{"image_urls": {}}]}),
        json!({}),
    ] {
        assert!(extract_media_refs(&malformed).is_empty(), "{malformed:?}");
    }
}

// ---------------------------------------------------------------------
// Poller (against a mock Pixiv App API)
// ---------------------------------------------------------------------

async fn open_store() -> (tempfile::TempDir, ArchiveStore) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ArchiveStore::open(dir.path().to_path_buf(), dir.path().join("index.sqlite3"))
        .await
        .expect("open store");
    (dir, store)
}

fn bookmarks_response(
    illusts: Vec<serde_json::Value>,
    next_cursor: Option<&str>,
) -> serde_json::Value {
    let next_url = next_cursor.map(|cursor| {
        format!("https://app-api.pixiv.net/v1/user/bookmarks/illust?user_id=4321&restrict=public&max_bookmark_id={cursor}")
    });
    json!({"illusts": illusts, "next_url": next_url})
}

#[tokio::test]
async fn poll_archives_new_bookmarks_and_enqueues_media() {
    let server = MockServer::start().await;
    mount_auth(&server).await;
    Mock::given(method("GET"))
        .and(path("/v1/user/bookmarks/illust"))
        .and(query_param("user_id", "4321"))
        .and(query_param("restrict", "public"))
        .respond_with(ResponseTemplate::new(200).set_body_json(bookmarks_response(
            vec![
                bookmarked_illust(1, 11, "artist-a"),
                bookmarked_illust(2, 12, "artist-b"),
            ],
            None,
        )))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let (tx, mut rx) = crate::pipeline::candidate_post_channel(8);
    let client = Arc::new(PixivClient::new(
        Url::parse(&server.uri()).unwrap(),
        Url::parse(&server.uri()).unwrap(),
        Secret::from("refresh-token".to_string()),
    ));
    let poller = PixivBookmarksPoller::new(client, store.clone(), tx, Duration::from_secs(60))
        .with_page_delay(Duration::ZERO);

    poller.poll_walk(true).await.expect("poll succeeds");

    let page = store
        .list_posts(Some(Category::PixivBookmark), 1, 10)
        .await
        .expect("list");
    assert_eq!(page.total_items, 2);

    // Media was enqueued for both bookmarks, in newest-first order.
    let first = rx.recv().await.expect("candidate 1");
    assert_eq!(first.at_uri, "pixiv:11/1");
    assert_eq!(first.category, PostCategory::PixivBookmark);
    assert_eq!(first.author_did, "artist-a");
    assert_eq!(first.media.len(), 1);
    assert!(first.media[0].cdn_url.contains("1_p0.png"));
    let second = rx.recv().await.expect("candidate 2");
    assert_eq!(second.at_uri, "pixiv:12/2");
    assert!(rx.try_recv().is_err());

    // The walk ranked each bookmark with its list position.
    let record = store
        .get_post(Category::PixivBookmark, "pixiv:11/1")
        .await
        .expect("get")
        .expect("archived");
    assert_eq!(record.action_seq, Some(0));
}

#[tokio::test]
async fn poll_walks_pages_until_next_url_runs_out() {
    let server = MockServer::start().await;
    mount_auth(&server).await;
    // The cursor-specific mock is mounted first: wiremock serves the first
    // matching mock, and the cursor-less mock below must not swallow the
    // second page's request (which would loop forever).
    Mock::given(method("GET"))
        .and(path("/v1/user/bookmarks/illust"))
        .and(query_param("max_bookmark_id", "1000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(bookmarks_response(
            vec![bookmarked_illust(3, 11, "artist-a")],
            None,
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/user/bookmarks/illust"))
        .respond_with(ResponseTemplate::new(200).set_body_json(bookmarks_response(
            vec![
                bookmarked_illust(1, 11, "artist-a"),
                bookmarked_illust(2, 11, "artist-a"),
            ],
            Some("1000"),
        )))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let (tx, _rx) = crate::pipeline::candidate_post_channel(8);
    let client = Arc::new(PixivClient::new(
        Url::parse(&server.uri()).unwrap(),
        Url::parse(&server.uri()).unwrap(),
        Secret::from("refresh-token".to_string()),
    ));
    let poller = PixivBookmarksPoller::new(client, store.clone(), tx, Duration::from_secs(60))
        .with_page_delay(Duration::ZERO);

    poller.poll_walk(true).await.expect("poll succeeds");

    let page = store
        .list_posts(Some(Category::PixivBookmark), 1, 10)
        .await
        .expect("list");
    assert_eq!(page.total_items, 3);
    // The last page ranked the oldest bookmark with its list position.
    let record = store
        .get_post(Category::PixivBookmark, "pixiv:11/3")
        .await
        .expect("get")
        .expect("archived");
    assert_eq!(record.action_seq, Some(2));
}

#[tokio::test]
async fn poll_stops_at_dedup_boundary_after_a_full_page_of_archived_posts() {
    // The boundary requires a FULL page (30) of consecutively
    // already-archived posts, so a single shifted duplicate can't stall
    // the walk. Here page 1 is entirely already-archived: the boundary
    // fires and page 2 is never requested.
    let server = MockServer::start().await;
    mount_auth(&server).await;
    let archived: Vec<serde_json::Value> = (1..=30)
        .map(|n| bookmarked_illust(n, 11, "artist-a"))
        .collect();
    Mock::given(method("GET"))
        .and(path("/v1/user/bookmarks/illust"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(bookmarks_response(archived, Some("1000"))),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/user/bookmarks/illust"))
        .and(query_param("max_bookmark_id", "1000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(bookmarks_response(
            vec![bookmarked_illust(31, 11, "artist-a")],
            None,
        )))
        .expect(0)
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    for n in 1..=30 {
        store
            .save_post(
                Category::PixivBookmark,
                &format!("pixiv:11/{n}"),
                "",
                json!({"id": n, "user": {"id": 11, "name": "artist-a"}}),
            )
            .await
            .expect("pre-archive");
    }

    let (tx, mut rx) = crate::pipeline::candidate_post_channel(8);
    let client = Arc::new(PixivClient::new(
        Url::parse(&server.uri()).unwrap(),
        Url::parse(&server.uri()).unwrap(),
        Secret::from("refresh-token".to_string()),
    ));
    let poller = PixivBookmarksPoller::new(client, store.clone(), tx, Duration::from_secs(60))
        .with_page_delay(Duration::ZERO);

    poller.poll_walk(false).await.expect("poll succeeds");

    let page = store
        .list_posts(Some(Category::PixivBookmark), 1, 100)
        .await
        .expect("list");
    assert_eq!(
        page.total_items, 30,
        "nothing new archived past the boundary"
    );
    assert!(
        rx.try_recv().is_err(),
        "no media candidates past the boundary"
    );
}

#[tokio::test]
async fn poll_survives_shifted_duplicates_at_page_boundaries() {
    // When the account bookmarks something mid-walk, the cursor-based list
    // shifts and later windows re-serve a post or two the walk already
    // archived. A lone duplicate must not trip the boundary, so the walk
    // continues and archives the genuinely new posts behind it.
    let server = MockServer::start().await;
    mount_auth(&server).await;
    // The cursor-specific mock is mounted first: wiremock serves the first
    // matching mock, and the cursor-less mock below must not swallow the
    // second page's request.
    Mock::given(method("GET"))
        .and(path("/v1/user/bookmarks/illust"))
        .and(query_param("max_bookmark_id", "1000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(bookmarks_response(
            vec![bookmarked_illust(3, 11, "artist-a")], // new
            None,
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/user/bookmarks/illust"))
        .respond_with(ResponseTemplate::new(200).set_body_json(bookmarks_response(
            vec![
                bookmarked_illust(1, 11, "artist-a"), // already archived (shifted dup)
                bookmarked_illust(2, 11, "artist-a"), // new
            ],
            Some("1000"),
        )))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    store
        .save_post(
            Category::PixivBookmark,
            "pixiv:11/1",
            "",
            json!({"id": 1, "user": {"id": 11, "name": "artist-a"}}),
        )
        .await
        .expect("pre-archive");

    let (tx, mut rx) = crate::pipeline::candidate_post_channel(8);
    let client = Arc::new(PixivClient::new(
        Url::parse(&server.uri()).unwrap(),
        Url::parse(&server.uri()).unwrap(),
        Secret::from("refresh-token".to_string()),
    ));
    let poller = PixivBookmarksPoller::new(client, store.clone(), tx, Duration::from_secs(60))
        .with_page_delay(Duration::ZERO);

    poller.poll_walk(false).await.expect("poll succeeds");

    let page = store
        .list_posts(Some(Category::PixivBookmark), 1, 10)
        .await
        .expect("list");
    assert_eq!(
        page.total_items, 3,
        "the walk continued past the shifted dup"
    );

    // Only the two genuinely new bookmarks produced media candidates.
    drop(poller);
    let mut uris = Vec::new();
    while let Ok(candidate) = rx.try_recv() {
        uris.push(candidate.at_uri);
    }
    uris.sort();
    assert_eq!(uris, vec!["pixiv:11/2", "pixiv:11/3"]);
}

#[tokio::test]
async fn poll_surfaces_api_errors_with_retry_hints() {
    let server = MockServer::start().await;
    mount_auth(&server).await;
    Mock::given(method("GET"))
        .and(path("/v1/user/bookmarks/illust"))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "7"))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let (tx, _rx) = crate::pipeline::candidate_post_channel(8);
    let client = Arc::new(PixivClient::new(
        Url::parse(&server.uri()).unwrap(),
        Url::parse(&server.uri()).unwrap(),
        Secret::from("refresh-token".to_string()),
    ));
    let poller = PixivBookmarksPoller::new(client, store, tx, Duration::from_secs(60));

    let err = poller.poll_walk(true).await.expect_err("rate limited");
    assert_eq!(err.retry_after(), Some(Duration::from_secs(7)));
}

#[test]
fn pixiv_cdn_urls_get_a_referer() {
    assert!(crate::media::is_pixiv_cdn_url_for_test(
        "https://i.pximg.net/img-original/img/2026/01/01/00/00/00/1_p0.png"
    ));
    assert!(!crate::media::is_pixiv_cdn_url_for_test(
        "https://cdn.bsky.app/img/feed_fullsize/plain/x/y"
    ));
}
