use super::*;
use crate::bluesky::BookmarkView;
use crate::config::Secret;
use crate::pipeline::candidate_post_channel;
use serde_json::json;
use time::macros::datetime;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn open_store() -> (tempfile::TempDir, ArchiveStore) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ArchiveStore::open(dir.path().join("archive"), dir.path().join("index.sqlite3"))
        .await
        .expect("open store");
    (dir, store)
}

fn make_client(server: &MockServer) -> Arc<BlueskyClient> {
    Arc::new(BlueskyClient::new(
        url::Url::parse(&server.uri()).unwrap(),
        "alice.bsky.social".to_string(),
        Secret::from("app-password".to_string()),
    ))
}

async fn mount_login(server: &MockServer) {
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

/// A resolvable bookmarked post with the given uri/cid and text.
fn bookmark_entry(uri: &str, cid: &str, text: &str) -> serde_json::Value {
    json!({
        "subject": {"uri": uri, "cid": cid},
        "item": {
            "uri": uri,
            "cid": cid,
            "author": {"did": "did:plc:carol"},
            "record": {"text": text},
        },
    })
}

fn not_found_entry(uri: &str, cid: &str) -> serde_json::Value {
    json!({
        "subject": {"uri": uri, "cid": cid},
        "item": {"uri": uri, "notFound": true},
    })
}

fn blocked_entry(uri: &str, cid: &str) -> serde_json::Value {
    json!({
        "subject": {"uri": uri, "cid": cid},
        "item": {
            "uri": uri,
            "cid": cid,
            "author": {"did": "did:plc:y"},
            "blocked": true,
        },
    })
}

async fn is_marked_deleted(store: &ArchiveStore, at_uri: &str) -> bool {
    store
        .get_post(Category::Bookmark, at_uri)
        .await
        .expect("get_post")
        .expect("post archived")
        .deleted_at
        .is_some()
}

#[test]
fn sweep_delay_targets_configured_local_hour() {
    let now = datetime!(2026-09-07 15:00:00 UTC);
    assert_eq!(
        duration_until_next_sweep(3, now),
        Duration::from_secs(12 * 3600)
    );

    let now = datetime!(2026-09-07 01:30:00 UTC);
    assert_eq!(
        duration_until_next_sweep(3, now),
        Duration::from_secs(90 * 60)
    );

    // Exactly on the hour: the next occurrence is tomorrow.
    let now = datetime!(2026-09-07 03:00:00 UTC);
    assert_eq!(
        duration_until_next_sweep(3, now),
        Duration::from_secs(24 * 3600)
    );

    // Hour 23: 22:30 is tonight, 23:30 has to be tomorrow night.
    let now = datetime!(2026-09-07 22:30:00 UTC);
    assert_eq!(
        duration_until_next_sweep(23, now),
        Duration::from_secs(30 * 60)
    );
    let now = datetime!(2026-09-07 23:30:00 UTC);
    assert_eq!(
        duration_until_next_sweep(23, now),
        Duration::from_secs(23 * 3600 + 30 * 60)
    );
}

#[tokio::test]
async fn sweep_archives_missed_items_and_marks_deleted_upstream_posts() {
    let server = MockServer::start().await;
    mount_login(&server).await;
    let (_dir, store) = open_store().await;

    // Post 1 is already archived (the poller got it before its author
    // deleted it); post 2 was bookmarked while the service was down.
    store
        .save_post(
            Category::Bookmark,
            "at://did:plc:carol/app.bsky.feed.post/1",
            "cid-1",
            json!({"text": "deleted later"}),
        )
        .await
        .unwrap();

    // Page 1: a new resolvable bookmark, the deleted post (notFound),
    // and a blocked one. Page 2: another new bookmark, then exhausted.
    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
        .and(query_param("cursor", "page-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "bookmarks": [bookmark_entry(
                "at://did:plc:carol/app.bsky.feed.post/4",
                "cid-4",
                "second page",
            )],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "bookmarks": [
                bookmark_entry(
                    "at://did:plc:carol/app.bsky.feed.post/2",
                    "cid-2",
                    "missed by poller",
                ),
                not_found_entry("at://did:plc:carol/app.bsky.feed.post/1", "cid-1"),
                blocked_entry("at://did:plc:y/app.bsky.feed.post/3", "cid-3"),
            ],
            "cursor": "page-2",
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getActorLikes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "feed": [],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    // The verify pass batches every archived bookmark/like URI; report
    // post 2 as still present and post 4 as deleted-while-down (e.g.
    // the author deleted it right after it was bookmarked).
    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getPosts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "posts": [],
            "notFoundPosts": [
                {"uri": "at://did:plc:carol/app.bsky.feed.post/4"},
            ],
            "blockedPosts": [],
        })))
        .mount(&server)
        .await;

    let (tx, _rx) = candidate_post_channel(8);
    let sweeper = NightlySweeper::new(
        make_client(&server),
        store.clone(),
        tx,
        "did:plc:alice".to_string(),
        3,
    );
    let summary = sweeper.sweep_once().await.expect("sweep succeeds");

    // Posts 2 and 4 were archived by the walk; post 1 was already there.
    assert_eq!(summary.newly_archived, 2);
    // Post 1 was marked by the walk's notFound signal; post 4 by the
    // verify pass. The blocked post (3) must never be marked.
    assert_eq!(summary.newly_marked_deleted, 2);
    assert!(
        is_marked_deleted(&store, "at://did:plc:carol/app.bsky.feed.post/1").await,
        "deleted bookmarked post should be marked"
    );
    assert!(
        is_marked_deleted(&store, "at://did:plc:carol/app.bsky.feed.post/4").await,
        "post deleted while the service was down should be marked"
    );
    assert!(
        store
            .get_post(
                Category::Bookmark,
                "at://did:plc:carol/app.bsky.feed.post/2"
            )
            .await
            .expect("get_post")
            .expect("post 2 archived")
            .deleted_at
            .is_none(),
        "resolvable post must not be marked deleted"
    );
    assert!(
        store
            .get_post(Category::Bookmark, "at://did:plc:y/app.bsky.feed.post/3")
            .await
            .expect("get_post")
            .is_none(),
        "blocked post was never archived (it did not resolve)"
    );
}

#[tokio::test]
async fn sweep_verifies_archived_likes_and_marks_them_deleted() {
    let server = MockServer::start().await;
    mount_login(&server).await;
    let (_dir, store) = open_store().await;

    // A liked post whose author deleted it while the service was down:
    // it no longer appears in getActorLikes at all, so only the
    // getPosts verify pass can catch it.
    store
        .save_post(
            Category::Like,
            "at://did:plc:dave/app.bsky.feed.post/9",
            "cid-9",
            json!({"text": "liked, then deleted"}),
        )
        .await
        .unwrap();

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getActorLikes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "feed": [],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "bookmarks": [],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getPosts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "posts": [],
            "notFoundPosts": [
                {"uri": "at://did:plc:dave/app.bsky.feed.post/9"},
            ],
            "blockedPosts": [],
        })))
        .mount(&server)
        .await;

    let (tx, _rx) = candidate_post_channel(8);
    let sweeper = NightlySweeper::new(
        make_client(&server),
        store.clone(),
        tx,
        "did:plc:alice".to_string(),
        3,
    );
    let summary = sweeper.sweep_once().await.expect("sweep succeeds");

    assert_eq!(summary.newly_archived, 0);
    assert_eq!(summary.newly_marked_deleted, 1);
    let record = store
        .get_post(Category::Like, "at://did:plc:dave/app.bsky.feed.post/9")
        .await
        .expect("get_post")
        .expect("like still archived");
    assert!(
        record.deleted_at.is_some(),
        "deleted liked post must be kept and marked"
    );
}

#[tokio::test]
async fn sweep_counts_remarks_and_missing_items_without_error() {
    let server = MockServer::start().await;
    mount_login(&server).await;
    let (_dir, store) = open_store().await;

    // A bookmark that is already marked deleted: neither the walk nor
    // the verify pass may double-count it, and neither may fail.
    store
        .save_post(
            Category::Bookmark,
            "at://did:plc:carol/app.bsky.feed.post/1",
            "cid-1",
            json!({"text": "gone"}),
        )
        .await
        .unwrap();
    store
        .mark_post_deleted("at://did:plc:carol/app.bsky.feed.post/1")
        .await
        .unwrap();

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "bookmarks": [
                not_found_entry("at://did:plc:carol/app.bsky.feed.post/1", "cid-1"),
            ],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getActorLikes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "feed": [],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getPosts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "posts": [],
            "notFoundPosts": [
                {"uri": "at://did:plc:carol/app.bsky.feed.post/1"},
            ],
            "blockedPosts": [],
        })))
        .mount(&server)
        .await;

    let (tx, _rx) = candidate_post_channel(8);
    let sweeper = NightlySweeper::new(
        make_client(&server),
        store.clone(),
        tx,
        "did:plc:alice".to_string(),
        3,
    );
    let summary = sweeper.sweep_once().await.expect("sweep succeeds");

    assert_eq!(summary.newly_archived, 0);
    assert_eq!(
        summary.newly_marked_deleted, 0,
        "an already-marked post must not be re-counted"
    );
}

/// The lexicon allows a bookmark entry with no hydrated item at all;
/// the sweeper must skip it rather than treat it as a deletion.
#[tokio::test]
async fn sweep_skips_bookmark_entries_with_no_item() {
    let server = MockServer::start().await;
    mount_login(&server).await;
    let (_dir, store) = open_store().await;

    let entry: BookmarkView = serde_json::from_value(json!({
        "subject": {
            "uri": "at://did:plc:carol/app.bsky.feed.post/1",
            "cid": "cid-1",
        },
    }))
    .unwrap();
    assert!(entry.item.is_none());

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "bookmarks": [{
                "subject": {
                    "uri": "at://did:plc:carol/app.bsky.feed.post/1",
                    "cid": "cid-1",
                },
            }],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getActorLikes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "feed": [],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getPosts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "posts": [],
            "notFoundPosts": [],
            "blockedPosts": [],
        })))
        .mount(&server)
        .await;

    let (tx, _rx) = candidate_post_channel(8);
    let sweeper = NightlySweeper::new(
        make_client(&server),
        store.clone(),
        tx,
        "did:plc:alice".to_string(),
        3,
    );
    let summary = sweeper.sweep_once().await.expect("sweep succeeds");
    assert_eq!(summary.newly_marked_deleted, 0);
}

/// The sweep's full bookmark walk backfills the bookmark action time
/// for rows archived before it was captured (the periodic poller stops
/// at its dedup boundary and never revisits them).
#[tokio::test]
async fn sweep_backfills_action_time_for_already_archived_bookmarks() {
    let server = MockServer::start().await;
    mount_login(&server).await;
    let (_dir, store) = open_store().await;

    let uri = "at://did:plc:carol/app.bsky.feed.post/1";
    store
        .save_post(Category::Bookmark, uri, "cid-1", json!({"text": "old row"}))
        .await
        .unwrap();

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "bookmarks": [{
                "subject": {"uri": uri, "cid": "cid-1"},
                "createdAt": "2026-09-04T09:00:00.000Z",
                "item": {
                    "uri": uri,
                    "cid": "cid-1",
                    "author": {"did": "did:plc:carol"},
                    "record": {"text": "old row"},
                },
            }],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getActorLikes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "feed": [],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getPosts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "posts": [],
            "notFoundPosts": [],
            "blockedPosts": [],
        })))
        .mount(&server)
        .await;

    let (tx, _rx) = candidate_post_channel(8);
    let sweeper = NightlySweeper::new(
        make_client(&server),
        store.clone(),
        tx,
        "did:plc:alice".to_string(),
        3,
    );
    sweeper.sweep_once().await.expect("sweep succeeds");

    let record = store
        .get_post(Category::Bookmark, uri)
        .await
        .expect("get_post")
        .expect("still archived");
    assert_eq!(
        record.action_at.as_deref(),
        Some("2026-09-04T09:00:00.000Z"),
        "the sweep should backfill the bookmark action time"
    );
}

/// The missing-media heal re-queues authored posts whose record declares
/// more media than is stored (a failed download), flowing them through the
/// candidate channel as `Authored` candidates built from the stored
/// record's blob references.
#[tokio::test]
async fn sweep_requeues_media_for_authored_posts_stored_incomplete() {
    let server = MockServer::start().await;
    mount_login(&server).await;
    let (_dir, store) = open_store().await;

    // An authored post with two declared images but only one stored.
    let uri = "at://did:plc:alice/app.bsky.feed.post/1";
    store
        .save_post(
            Category::Post,
            uri,
            "cid-1",
            json!({
                "text": "two images, one stored",
                "embed": {
                    "$type": "app.bsky.embed.images",
                    "images": [
                        {"image": {"ref": {"$link": "bafy1"}, "mimeType": "image/jpeg", "size": 10}},
                        {"image": {"ref": {"$link": "bafy2"}, "mimeType": "image/jpeg", "size": 10}},
                    ],
                }
            }),
        )
        .await
        .unwrap();
    store
        .save_media(
            Category::Post,
            uri,
            "000.jpg",
            Some("image/jpeg".to_string()),
            b"jpg".to_vec(),
        )
        .await
        .unwrap();

    // A complete authored post: nothing to heal.
    let complete = "at://did:plc:alice/app.bsky.feed.post/2";
    store
        .save_post(
            Category::Post,
            complete,
            "cid-2",
            json!({
                "text": "one image, stored",
                "embed": {
                    "$type": "app.bsky.embed.images",
                    "images": [
                        {"image": {"ref": {"$link": "bafy3"}, "mimeType": "image/jpeg", "size": 10}},
                    ],
                }
            }),
        )
        .await
        .unwrap();
    store
        .save_media(
            Category::Post,
            complete,
            "000.jpg",
            Some("image/jpeg".to_string()),
            b"jpg".to_vec(),
        )
        .await
        .unwrap();

    for endpoint in [
        "/xrpc/app.bsky.bookmark.getBookmarks",
        "/xrpc/app.bsky.feed.getActorLikes",
        "/xrpc/app.bsky.feed.getPosts",
    ] {
        Mock::given(method("GET"))
            .and(path(endpoint))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .mount(&server)
            .await;
    }

    let (tx, mut rx) = candidate_post_channel(8);
    let summary = {
        let sweeper = NightlySweeper::new(
            make_client(&server),
            store.clone(),
            tx.clone(),
            "did:plc:alice".to_string(),
            3,
        );
        sweeper.sweep_once().await.expect("sweep succeeds")
    };
    // The sweeper (and its sender clone) is dropped above, so `drop(tx)`
    // below actually closes the channel and the final recv returns None.

    assert_eq!(summary.media_requeued, 1, "only the incomplete post");
    drop(tx);
    let candidate = rx.recv().await.expect("re-queued candidate");
    assert_eq!(candidate.at_uri, uri);
    assert_eq!(candidate.category, PostCategory::Authored);
    assert_eq!(candidate.media.len(), 2);
    assert!(candidate.media[0].cdn_url.contains("bafy1"));
    assert!(candidate.media[1].cdn_url.contains("bafy2"));
    assert!(rx.recv().await.is_none(), "the complete post is untouched");
}
