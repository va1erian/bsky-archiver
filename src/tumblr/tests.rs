use super::*;
use serde_json::json;
use std::sync::Arc;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------
// OAuth 1.0a signing
// ---------------------------------------------------------------------

/// The RFC 5849 appendix A test vector: GET
/// `http://photos.example.net/photos?file=vacation.jpg&size=original`
/// signed with the RFC's example credentials and a fixed nonce/timestamp
/// must produce exactly the RFC's documented signature.
#[test]
fn oauth_signature_matches_rfc_5849_test_vector() {
    let params = vec![
        ("file".to_string(), "vacation.jpg".to_string()),
        ("size".to_string(), "original".to_string()),
        (
            "oauth_consumer_key".to_string(),
            "dpf43f3p2l4k3l03".to_string(),
        ),
        ("oauth_token".to_string(), "nnch734d00sl2jdk".to_string()),
        ("oauth_version".to_string(), "1.0".to_string()),
        (
            "oauth_signature_method".to_string(),
            "HMAC-SHA1".to_string(),
        ),
        ("oauth_timestamp".to_string(), "1191242096".to_string()),
        ("oauth_nonce".to_string(), "kllo9940pd9333jh".to_string()),
    ];
    let signature = oauth_signature(
        "GET",
        "http://photos.example.net/photos",
        &params,
        "kd94hf93k423kf44",
        "pfkkdhi9sl3r4s00",
    );
    assert_eq!(signature, "tR3+Ty81lMeYAr/Fid0kMTYa/WM=");
}

#[test]
fn oauth_encode_follows_rfc_3986_unreserved_set() {
    assert_eq!(oauth_encode("abcXYZ019-._~"), "abcXYZ019-._~");
    assert_eq!(oauth_encode("a b"), "a%20b");
    assert_eq!(oauth_encode("a&b=c"), "a%26b%3Dc");
    assert_eq!(oauth_encode("a+b"), "a%2Bb");
    assert_eq!(oauth_encode("100%"), "100%25");
}

#[test]
fn oauth_authorization_header_encodes_values_and_quotes_them() {
    let params = vec![
        (
            "oauth_consumer_key".to_string(),
            "key/with&special".to_string(),
        ),
        ("oauth_signature".to_string(), "sig==".to_string()),
    ];
    let header = oauth_authorization_header(&params);
    assert_eq!(
        header,
        "OAuth oauth_consumer_key=\"key%2Fwith%26special\", oauth_signature=\"sig%3D%3D\""
    );
}

#[test]
fn signature_sorts_params_before_signing() {
    // The same parameter set in two different insertion orders must produce
    // the same signature.
    let build = |order: u8| {
        let params = if order == 0 {
            vec![
                ("b".to_string(), "2".to_string()),
                ("a".to_string(), "1".to_string()),
            ]
        } else {
            vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string()),
            ]
        };
        oauth_signature("GET", "https://api.example.com/x", &params, "cs", "ts")
    };
    assert_eq!(build(0), build(1));
}

// ---------------------------------------------------------------------
// Response parsing
// ---------------------------------------------------------------------

#[test]
fn likes_page_parses_liked_posts_and_count() {
    let body = json!({
        "meta": {"status": 200, "msg": "OK"},
        "response": {
            "liked_posts": [{"id": 1}, {"id": 2}],
            "liked_count": 42
        }
    });
    let page = LikesPage::from_json(&body).expect("parse");
    assert_eq!(page.posts.len(), 2);
    assert_eq!(page.liked_count, 42);
}

#[test]
fn likes_page_missing_liked_posts_is_a_shape_error() {
    let body = json!({"meta": {"status": 200}, "response": {}});
    assert!(matches!(
        LikesPage::from_json(&body),
        Err(TumblrError::Shape(_))
    ));
}

#[test]
fn liked_post_identity_prefers_id_string_and_converts_liked_timestamp() {
    let post = json!({
        "id": 748263675498103358_i64,
        "id_string": "748263675498103358",
        "blog_name": "example-blog",
        "liked_timestamp": 1_700_000_000_i64,
    });
    let (key, blog_name, liked_at) = liked_post_identity(&post).expect("identity");
    assert_eq!(key, "tumblr:example-blog/748263675498103358");
    assert_eq!(blog_name, "example-blog");
    assert_eq!(liked_at.as_deref(), Some("2023-11-14T22:13:20Z"));
}

#[test]
fn liked_post_identity_falls_back_to_numeric_id() {
    let post = json!({
        "id": 12345,
        "blog_name": "example-blog",
    });
    let (key, _, liked_at) = liked_post_identity(&post).expect("identity");
    assert_eq!(key, "tumblr:example-blog/12345");
    assert_eq!(liked_at, None, "no liked_timestamp means no action time");
}

#[test]
fn liked_post_identity_rejects_missing_id_or_blog() {
    assert!(liked_post_identity(&json!({"blog_name": "blog"})).is_none());
    assert!(liked_post_identity(&json!({"id": 1})).is_none());
    assert!(liked_post_identity(&json!({"id": 1, "blog_name": ""})).is_none());
}

// ---------------------------------------------------------------------
// Media extraction
// ---------------------------------------------------------------------

#[test]
fn photo_post_extracts_every_photo_in_order() {
    // Real photo-post URLs: each photo has its own first-path-segment
    // image key.
    let post = json!({
        "type": "photo",
        "photos": [
            {"original_size": {"url": "https://64.media.tumblr.com/9d9c9e7be7526ba36a5b88d74437f12d/85fa30fbda32f36e-93/s1280x1920/8fddf86972ad84cbbfb48af3a5d399d806ca6622.jpg"}},
            {"original_size": {"url": "https://64.media.tumblr.com/dca296980b3de7c962343e29007c88f8/f127a6a8c7a1b979-1e/s1280x1920/3274837dcf625b1c3e40e2b165d2ce232026b990.jpg"}},
        ],
    });
    let refs = extract_media_refs(&post);
    assert_eq!(refs.len(), 2);
    assert!(refs[0].cdn_url.contains("9d9c9e7be7526ba36a5b88d74437f12d"));
    assert!(refs[1].cdn_url.contains("dca296980b3de7c962343e29007c88f8"));
    assert_eq!(refs[0].declared_mime_type, None);
}

#[test]
fn tumblr_hosted_video_and_audio_are_extracted() {
    let post = json!({
        "type": "video",
        "video_url": "https://va.media.tumblr.com/tumboe_720.mp4",
        "audio_url": "https://a.tumblr.com/tumboe_audio.mp3",
    });
    let refs = extract_media_refs(&post);
    assert_eq!(refs.len(), 2);
    assert_eq!(
        refs[0].cdn_url,
        "https://va.media.tumblr.com/tumboe_720.mp4"
    );
    assert_eq!(refs[0].declared_mime_type.as_deref(), Some("video/mp4"));
    assert_eq!(refs[1].cdn_url, "https://a.tumblr.com/tumboe_audio.mp3");
}

#[test]
fn external_video_and_audio_embeds_are_excluded() {
    let post = json!({
        "type": "video",
        "video_url": "https://www.youtube.com/watch?v=abc",
        "audio_url": "https://open.spotify.com/track/xyz",
    });
    assert!(extract_media_refs(&post).is_empty());
}

#[test]
fn reblog_html_images_are_extracted_with_largest_srcset() {
    // The shape Tumblr actually returns for NPF-era reblogs: `type=text`,
    // images only inside `body`/`trail` HTML, `src` at a mid resolution and
    // a `srcset` ladder up to the original. Every size variant carries a
    // *different filename hash* — only the first path segment identifies
    // the image.
    let post = json!({
        "type": "text",
        "body": concat!(
            "<div class=\"npf_row\"><figure class=\"tmblr-full\" data-orig-width=\"804\">",
            "<img src=\"https://64.media.tumblr.com/dca296980b3de7c962343e29007c88f8/f127a6a8c7a1b979-1e/s640x960/4c7bd0cb850043f50705bd987ae3027e4508f211.jpg\" ",
            "srcset=\"https://64.media.tumblr.com/dca296980b3de7c962343e29007c88f8/f127a6a8c7a1b979-1e/s75x75_c1/ef47f626440a67619338870285c34970928212b2.jpg 75w, ",
            "https://64.media.tumblr.com/dca296980b3de7c962343e29007c88f8/f127a6a8c7a1b979-1e/s640x960/4c7bd0cb850043f50705bd987ae3027e4508f211.jpg 640w, ",
            "https://64.media.tumblr.com/dca296980b3de7c962343e29007c88f8/f127a6a8c7a1b979-1e/s1280x1920/3274837dcf625b1c3e40e2b165d2ce232026b990.jpg 804w\" /></figure></div>"
        ),
        "trail": [{
            "content": "<figure><img src=\"https://64.media.tumblr.com/dca296980b3de7c962343e29007c88f8/f127a6a8c7a1b979-1e/s640x960/4c7bd0cb850043f50705bd987ae3027e4508f211.jpg\" alt=\"image\" /></figure>",
        }],
    });
    let refs = extract_media_refs(&post);
    assert_eq!(refs.len(), 1, "all size variants of one image collapse");
    assert_eq!(
        refs[0].cdn_url,
        "https://64.media.tumblr.com/dca296980b3de7c962343e29007c88f8/f127a6a8c7a1b979-1e/s1280x1920/3274837dcf625b1c3e40e2b165d2ce232026b990.jpg"
    );
}

#[test]
fn distinct_images_with_distinct_keys_stay_separate() {
    // Two different images in one post (different first path segments, as
    // observed in real photoset/reblog bodies) must not collapse.
    let post = json!({
        "body": concat!(
            "<img src=\"https://64.media.tumblr.com/bf8c79510eae65edfcda3b23999117a1/2f0ceceb28825141-a8/s640x960/f2ab65d2dfe7d6d1cf0660e54d74c254e9c60d7a.png\">",
            "<img src=\"https://64.media.tumblr.com/79f6b8e32cee570d4cd2396d5e9a1128/2f0ceceb28825141-a5/s640x960/bce8fce3816426373010a2fa63d6a4f5addaef4f.png\">"
        ),
    });
    assert_eq!(extract_media_refs(&post).len(), 2);
}

#[test]
fn media_key_ignores_everything_after_the_first_segment() {
    assert_eq!(
        tumblr_media_key(
            "https://64.media.tumblr.com/dca296980b3de7c962343e29007c88f8/f127a6a8c7a1b979-1e/s640x960/4c7bd0cb850043f50705bd987ae3027e4508f211.jpg"
        ),
        tumblr_media_key(
            "https://64.media.tumblr.com/dca296980b3de7c962343e29007c88f8/f127a6a8c7a1b979-1e/s1280x1920/3274837dcf625b1c3e40e2b165d2ce232026b990.jpg"
        )
    );
    assert_ne!(
        tumblr_media_key("https://64.media.tumblr.com/aaa/s640x960/x.jpg"),
        tumblr_media_key("https://64.media.tumblr.com/bbb/s640x960/y.jpg")
    );
}

#[test]
fn html_img_without_srcset_uses_src() {
    let post = json!({
        "body": "<p><figure class=\"tmblr-full\"><img src=\"https://66.media.tumblr.com/x/y/s500x750/pic.png\"/></figure></p>",
    });
    let refs = extract_media_refs(&post);
    assert_eq!(refs.len(), 1);
    assert_eq!(
        refs[0].cdn_url,
        "https://66.media.tumblr.com/x/y/s500x750/pic.png"
    );
}

#[test]
fn external_html_images_are_excluded() {
    let post = json!({
        "body": "<img src=\"https://example.com/cat.jpg\"> <img src=\"https://i.imgur.com/dog.png\">",
    });
    assert!(extract_media_refs(&post).is_empty());
}

#[test]
fn data_src_is_not_mistaken_for_src() {
    let post = json!({
        "body": "<img data-src=\"https://lazy.example.com/a.jpg\" src=\"https://64.media.tumblr.com/x/y/s640x960/real.jpg\">",
    });
    let refs = extract_media_refs(&post);
    assert_eq!(refs.len(), 1);
    assert_eq!(
        refs[0].cdn_url,
        "https://64.media.tumblr.com/x/y/s640x960/real.jpg"
    );
}

#[test]
fn html_ampersand_entities_are_decoded() {
    let post = json!({
        "body": "<img src=\"https://64.media.tumblr.com/x/y/s640x960/a.jpg?w=1&amp;h=2\">",
    });
    let refs = extract_media_refs(&post);
    assert_eq!(refs.len(), 1);
    assert_eq!(
        refs[0].cdn_url,
        "https://64.media.tumblr.com/x/y/s640x960/a.jpg?w=1&h=2"
    );
}

#[test]
fn malformed_html_is_skipped_not_fatal() {
    for malformed in [
        json!({"body": "<img"}),
        json!({"body": "<img src>"}),
        json!({"body": "<img src=noquotes>"}),
        json!({"body": "plain text"}),
        json!({"body": 42}),
    ] {
        assert!(extract_media_refs(&malformed).is_empty(), "{malformed:?}");
    }
}

#[test]
fn text_post_has_no_media() {
    let post = json!({"type": "text", "body": "<p>just words</p>"});
    assert!(extract_media_refs(&post).is_empty());
}

#[test]
fn malformed_media_fields_are_skipped_not_fatal() {
    for malformed in [
        json!({"photos": "not an array"}),
        json!({"photos": [{"original_size": {}}]}),
        json!({"video_url": 42}),
        json!({}),
    ] {
        assert!(extract_media_refs(&malformed).is_empty(), "{malformed:?}");
    }
}

// ---------------------------------------------------------------------
// Poller (against a mock Tumblr API)
// ---------------------------------------------------------------------

fn tumblr_config() -> TumblrConfig {
    TumblrConfig {
        consumer_key: Secret::from("consumer-key".to_string()),
        consumer_secret: Secret::from("consumer-secret".to_string()),
        oauth_token: Secret::from("oauth-token".to_string()),
        oauth_secret: Secret::from("oauth-secret".to_string()),
    }
}

fn liked_post(id: u64, blog: &str, liked_ts: i64) -> serde_json::Value {
    json!({
        "id": id,
        "id_string": id.to_string(),
        "blog_name": blog,
        "liked_timestamp": liked_ts,
        "type": "photo",
        "post_url": format!("https://{blog}.tumblr.com/post/{id}"),
        "photos": [{"original_size": {"url": format!("https://64.media.tumblr.com/x/{id}.jpg")}}],
    })
}

fn likes_response(posts: Vec<serde_json::Value>, liked_count: u64) -> serde_json::Value {
    json!({"meta": {"status": 200, "msg": "OK"}, "response": {"liked_posts": posts, "liked_count": liked_count}})
}

async fn open_store() -> (tempfile::TempDir, ArchiveStore) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ArchiveStore::open(dir.path().to_path_buf(), dir.path().join("index.sqlite3"))
        .await
        .expect("open store");
    (dir, store)
}

#[tokio::test]
async fn poll_archives_new_likes_and_enqueues_media() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/user/likes"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(likes_response(
            vec![
                liked_post(1, "blog-a", 1_700_000_000),
                liked_post(2, "blog-b", 1_699_000_000),
            ],
            2,
        )))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let (tx, mut rx) = crate::pipeline::candidate_post_channel(8);
    let client = Arc::new(TumblrClient::new(
        Url::parse(&server.uri()).unwrap(),
        tumblr_config(),
    ));
    let poller = TumblrLikesPoller::new(client, store.clone(), tx, Duration::from_secs(60));
    poller.poll_likes().await.expect("poll succeeds");

    let page = store
        .list_posts(Some(Category::TumblrLike), 1, 10)
        .await
        .expect("list");
    assert_eq!(page.total_items, 2);

    // Media was enqueued for both posts, in newest-first order.
    let first = rx.recv().await.expect("candidate 1");
    assert_eq!(first.at_uri, "tumblr:blog-a/1");
    assert_eq!(first.category, PostCategory::TumblrLike);
    assert_eq!(first.author_did, "blog-a");
    assert_eq!(first.media.len(), 1);
    assert_eq!(
        first.media[0].cdn_url,
        "https://64.media.tumblr.com/x/1.jpg"
    );
    let second = rx.recv().await.expect("candidate 2");
    assert_eq!(second.at_uri, "tumblr:blog-b/2");
    // The channel only closes once every sender is gone — drop the poller
    // (which holds the sender) before asserting nothing else arrives.
    drop(poller);
    assert!(rx.recv().await.is_none());

    // Action metadata was recorded for the gallery's ordering.
    let record = store
        .get_post(Category::TumblrLike, "tumblr:blog-a/1")
        .await
        .expect("get")
        .expect("archived");
    assert_eq!(record.action_at.as_deref(), Some("2023-11-14T22:13:20Z"));
    assert_eq!(record.action_seq, Some(0));
}

#[tokio::test]
async fn poll_stops_at_dedup_boundary_after_a_full_window_of_archived_posts() {
    // The boundary requires a FULL window (page_limit) of consecutively
    // already-archived posts, so a single shifted duplicate at a page
    // boundary can't stall the walk. Here page 1 is entirely
    // already-archived: the boundary fires at the page_limit'th
    // consecutive archived post and page 2 is never requested.
    let server = MockServer::start().await;
    let archived: Vec<serde_json::Value> = (1..=20)
        .map(|n| liked_post(n, "blog-a", 1_700_000_000 - n as i64 * 1000))
        .collect();
    Mock::given(method("GET"))
        .and(path("/v2/user/likes"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(likes_response(archived, 100)))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/user/likes"))
        .and(query_param("offset", "20"))
        .respond_with(ResponseTemplate::new(200).set_body_json(likes_response(
            vec![liked_post(21, "blog-a", 1_678_000_000)],
            100,
        )))
        .expect(0)
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    for n in 1..=20 {
        store
            .save_post(
                Category::TumblrLike,
                &format!("tumblr:blog-a/{n}"),
                "",
                json!({"id": n, "blog_name": "blog-a"}),
            )
            .await
            .expect("pre-archive");
    }

    let (tx, mut rx) = crate::pipeline::candidate_post_channel(8);
    let client = Arc::new(TumblrClient::new(
        Url::parse(&server.uri()).unwrap(),
        tumblr_config(),
    ));
    let poller = TumblrLikesPoller::new(client, store.clone(), tx, Duration::from_secs(60));

    poller.poll_likes().await.expect("poll succeeds");

    let page = store
        .list_posts(Some(Category::TumblrLike), 1, 100)
        .await
        .expect("list");
    assert_eq!(
        page.total_items, 20,
        "nothing new archived past the boundary"
    );
    assert!(
        rx.try_recv().is_err(),
        "no media candidates past the boundary"
    );
}

#[tokio::test]
async fn poll_survives_shifted_duplicates_at_page_boundaries() {
    // When the account likes something mid-walk, the list shifts and later
    // windows re-serve a post or two the walk already archived. A lone
    // duplicate must not trip the boundary (that requires a full window of
    // consecutive archived posts), so the walk continues and archives the
    // genuinely new posts behind it.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/user/likes"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(likes_response(
            vec![
                liked_post(1, "blog-a", 1_700_000_000), // already archived (shifted dup)
                liked_post(2, "blog-a", 1_699_000_000), // new
            ],
            3,
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/user/likes"))
        .and(query_param("offset", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(likes_response(
            vec![liked_post(3, "blog-a", 1_698_000_000)], // new
            3,
        )))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    store
        .save_post(
            Category::TumblrLike,
            "tumblr:blog-a/1",
            "",
            json!({"id": 1, "blog_name": "blog-a"}),
        )
        .await
        .expect("pre-archive");

    let (tx, mut rx) = crate::pipeline::candidate_post_channel(8);
    let client = Arc::new(TumblrClient::new(
        Url::parse(&server.uri()).unwrap(),
        tumblr_config(),
    ));
    let poller = TumblrLikesPoller::new(client, store.clone(), tx, Duration::from_secs(60))
        .with_page_limit(2)
        .with_page_delay(Duration::ZERO);

    poller.poll_likes().await.expect("poll succeeds");

    let page = store
        .list_posts(Some(Category::TumblrLike), 1, 10)
        .await
        .expect("list");
    assert_eq!(
        page.total_items, 3,
        "the walk continued past the shifted dup"
    );

    // Only the two genuinely new posts produced media candidates.
    drop(poller);
    let mut uris = Vec::new();
    while let Ok(candidate) = rx.try_recv() {
        uris.push(candidate.at_uri);
    }
    uris.sort();
    assert_eq!(uris, vec!["tumblr:blog-a/2", "tumblr:blog-a/3"]);
}

#[tokio::test]
async fn full_walk_re_ranks_archived_posts_and_walks_to_the_end() {
    // The first pass after startup walks the entire list with the boundary
    // disabled: already-archived posts are re-ranked with their current
    // list position, and the walk continues to the end of the list.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/user/likes"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(likes_response(
            vec![
                liked_post(1, "blog-a", 1_700_000_000),
                liked_post(2, "blog-a", 1_699_000_000),
            ],
            3,
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/user/likes"))
        .and(query_param("offset", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(likes_response(
            vec![liked_post(3, "blog-a", 1_698_000_000)],
            3,
        )))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    // Pre-archive post 1 with a stale (bogus) rank from an interrupted
    // earlier walk.
    store
        .save_post(
            Category::TumblrLike,
            "tumblr:blog-a/1",
            "",
            json!({"id": 1, "blog_name": "blog-a"}),
        )
        .await
        .expect("pre-archive");
    store
        .set_action_seq(Category::TumblrLike, "tumblr:blog-a/1", 999)
        .await
        .expect("stale rank");

    let (tx, _rx) = crate::pipeline::candidate_post_channel(8);
    let client = Arc::new(TumblrClient::new(
        Url::parse(&server.uri()).unwrap(),
        tumblr_config(),
    ));
    let poller = TumblrLikesPoller::new(client, store.clone(), tx, Duration::from_secs(60))
        .with_page_limit(2)
        .with_page_delay(Duration::ZERO);

    poller.poll_walk(true).await.expect("full walk succeeds");

    let page = store
        .list_posts(Some(Category::TumblrLike), 1, 10)
        .await
        .expect("list");
    assert_eq!(page.total_items, 3, "full walk ignores the boundary");

    // The stale rank was corrected to the post's current list position.
    let re_ranked = store
        .get_post(Category::TumblrLike, "tumblr:blog-a/1")
        .await
        .expect("get")
        .expect("archived");
    assert_eq!(re_ranked.action_seq, Some(0));
}

#[tokio::test]
async fn poll_walks_every_page_when_no_boundary_is_hit() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/user/likes"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(likes_response(
            vec![
                liked_post(1, "blog-a", 1_700_000_000),
                liked_post(2, "blog-a", 1_699_000_000),
            ],
            3,
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/user/likes"))
        .and(query_param("offset", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(likes_response(
            vec![liked_post(3, "blog-a", 1_698_000_000)],
            3,
        )))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let (tx, _rx) = crate::pipeline::candidate_post_channel(8);
    let client = Arc::new(TumblrClient::new(
        Url::parse(&server.uri()).unwrap(),
        tumblr_config(),
    ));
    // A 2-post page size makes the walk continue onto the second page.
    let poller = TumblrLikesPoller::new(client, store.clone(), tx, Duration::from_secs(60))
        .with_page_limit(2)
        .with_page_delay(Duration::ZERO);

    poller.poll_likes().await.expect("poll succeeds");

    let page = store
        .list_posts(Some(Category::TumblrLike), 1, 10)
        .await
        .expect("list");
    assert_eq!(page.total_items, 3);
    // The last page ranked the oldest post with its list position.
    let record = store
        .get_post(Category::TumblrLike, "tumblr:blog-a/3")
        .await
        .expect("get")
        .expect("archived");
    assert_eq!(record.action_seq, Some(2));
}

#[tokio::test]
async fn poll_paces_page_fetches_within_a_walk() {
    // A multi-page walk sleeps the inter-page delay between fetches (the
    // Tumblr API rate limit), so a 2-page walk with a 40ms delay takes at
    // least one delay longer than the requests themselves. At-least
    // assertions on sleeps are reliable; at-most ones are not.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/user/likes"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(likes_response(
            vec![
                liked_post(1, "blog-a", 1_700_000_000),
                liked_post(2, "blog-a", 1_699_000_000),
            ],
            3,
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/user/likes"))
        .and(query_param("offset", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(likes_response(
            vec![liked_post(3, "blog-a", 1_698_000_000)],
            3,
        )))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let (tx, _rx) = crate::pipeline::candidate_post_channel(8);
    let client = Arc::new(TumblrClient::new(
        Url::parse(&server.uri()).unwrap(),
        tumblr_config(),
    ));
    let poller = TumblrLikesPoller::new(client, store.clone(), tx, Duration::from_secs(60))
        .with_page_limit(2)
        .with_page_delay(Duration::from_millis(40));

    let started = std::time::Instant::now();
    poller.poll_likes().await.expect("poll succeeds");
    assert!(
        started.elapsed() >= Duration::from_millis(40),
        "the walk must sleep the inter-page delay between page fetches"
    );
}

#[tokio::test]
#[ignore = "hits the real Tumblr API; run manually to diagnose pagination"]
async fn manual_probe_real_likes_pagination() {
    dotenvy::dotenv().expect(".env");
    let config = TumblrConfig {
        consumer_key: Secret::from(std::env::var("TUMBLR_CONSUMER_KEY").unwrap()),
        consumer_secret: Secret::from(std::env::var("TUMBLR_CONSUMER_SECRET").unwrap()),
        oauth_token: Secret::from(std::env::var("TUMBLR_OAUTH_TOKEN").unwrap()),
        oauth_secret: Secret::from(std::env::var("TUMBLR_OAUTH_SECRET").unwrap()),
    };
    let client = TumblrClient::new(Url::parse(DEFAULT_TUMBLR_BASE_URL).unwrap(), config);

    let mut offset = 0u64;
    loop {
        let page = client
            .get_likes(offset, PAGE_LIMIT)
            .await
            .expect("api call");
        let ids: Vec<String> = page
            .posts
            .iter()
            .filter_map(|p| {
                p.get("id_string")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
            })
            .collect();
        println!(
            "offset={offset} posts={} liked_count={} first_ids={:?}",
            page.posts.len(),
            page.liked_count,
            &ids[..ids.len().min(3)]
        );
        if page.posts.is_empty() || offset > 100 {
            break;
        }
        offset += page.posts.len() as u64;
    }
}

#[tokio::test]
async fn poll_continues_past_short_pages_until_liked_count_is_reached() {
    // Tumblr's API applies `offset` to its unfiltered internal list and
    // drops hidden posts from the response, so pages come back short (3 of
    // a 20-post window here) while more likes remain. A short page must NOT
    // end the walk, and the offset must advance by the requested window
    // size (20), not the returned count — otherwise the next window
    // re-serves the previous page's last post and the dedup boundary stalls
    // the walk.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/user/likes"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(likes_response(
            vec![
                liked_post(1, "blog-a", 1_700_000_000),
                liked_post(2, "blog-a", 1_699_000_000),
                liked_post(3, "blog-a", 1_698_000_000),
            ],
            50,
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/user/likes"))
        .and(query_param("offset", "20"))
        .respond_with(ResponseTemplate::new(200).set_body_json(likes_response(
            vec![
                liked_post(4, "blog-a", 1_697_000_000),
                liked_post(5, "blog-a", 1_696_000_000),
            ],
            50,
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v2/user/likes"))
        .and(query_param("offset", "40"))
        .respond_with(ResponseTemplate::new(200).set_body_json(likes_response(vec![], 50)))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let (tx, _rx) = crate::pipeline::candidate_post_channel(8);
    let client = Arc::new(TumblrClient::new(
        Url::parse(&server.uri()).unwrap(),
        tumblr_config(),
    ));
    let poller = TumblrLikesPoller::new(client, store.clone(), tx, Duration::from_secs(60));

    poller.poll_likes().await.expect("poll succeeds");

    let page = store
        .list_posts(Some(Category::TumblrLike), 1, 10)
        .await
        .expect("list");
    assert_eq!(page.total_items, 5, "short page 1 must not end the walk");
    // The walk re-ranked every post with its list position.
    let last = store
        .get_post(Category::TumblrLike, "tumblr:blog-a/5")
        .await
        .expect("get")
        .expect("archived");
    assert_eq!(last.action_seq, Some(4));
}

#[tokio::test]
async fn poll_surfaces_api_errors_with_retry_hints() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v2/user/likes"))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "7"))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let (tx, _rx) = crate::pipeline::candidate_post_channel(8);
    let client = Arc::new(TumblrClient::new(
        Url::parse(&server.uri()).unwrap(),
        tumblr_config(),
    ));
    let poller = TumblrLikesPoller::new(client, store, tx, Duration::from_secs(60));

    let err = poller.poll_likes().await.expect_err("rate limited");
    assert_eq!(err.retry_after(), Some(Duration::from_secs(7)));
}
