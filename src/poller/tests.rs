use super::*;
use crate::config::Secret;
use crate::pipeline::{candidate_post_channel, connection_health_channel};
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn feed_view_post(n: u32, with_media: bool) -> serde_json::Value {
    let record = if with_media {
        json!({
            "text": format!("post {n}"),
            "embed": {
                "$type": "app.bsky.embed.images",
                "images": [{"alt": "", "image": {"ref": "bafy"}}],
            }
        })
    } else {
        json!({"text": format!("post {n}")})
    };
    let embed = if with_media {
        json!({
            "$type": "app.bsky.embed.images#view",
            "images": [{"fullsize": format!("https://cdn.example.com/{n}.jpg"), "thumb": "", "alt": ""}],
        })
    } else {
        serde_json::Value::Null
    };

    let mut post = json!({
        "uri": format!("at://did:plc:alice/app.bsky.feed.post/{n}"),
        "cid": format!("cid-{n}"),
        "author": {"did": "did:plc:alice", "handle": "alice.bsky.social"},
        "record": record,
    });
    if with_media {
        post["embed"] = embed;
    }
    json!({"post": post})
}

async fn open_store() -> (tempfile::TempDir, ArchiveStore) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ArchiveStore::open(dir.path().join("archive"), dir.path().join("index.sqlite3"))
        .await
        .expect("open store");
    (dir, store)
}

fn test_client(server: &MockServer) -> BlueskyClient {
    BlueskyClient::new(
        url::Url::parse(&server.uri()).unwrap(),
        "alice.bsky.social".to_string(),
        Secret::from("app-password".to_string()),
    )
}

fn make_client(server: &MockServer) -> std::sync::Arc<BlueskyClient> {
    std::sync::Arc::new(BlueskyClient::new(
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

fn text_only_post(uri: &str, cid: &str) -> serde_json::Value {
    json!({
        "post": {
            "uri": uri,
            "cid": cid,
            "author": {"did": "did:plc:bob"},
            "record": {"text": "hello"},
        }
    })
}

fn image_post(uri: &str, cid: &str) -> serde_json::Value {
    json!({
        "post": {
            "uri": uri,
            "cid": cid,
            "author": {"did": "did:plc:bob"},
            "record": {
                "text": "look",
                "embed": {
                    "$type": "app.bsky.embed.images",
                    "images": [{"alt": "a cat"}],
                }
            },
            "embed": {
                "$type": "app.bsky.embed.images#view",
                "images": [{
                    "alt": "a cat",
                    "fullsize": "https://cdn.example.com/img1.jpg",
                    "thumb": "https://cdn.example.com/img1-thumb.jpg",
                }],
            },
        }
    })
}

/// Wraps a `postView` the way `app.bsky.bookmark.defs#bookmarkView`
/// actually does: `subject` is only a strong ref and the hydrated post
/// lives under `item`.
fn bookmark_wrap(post: serde_json::Value) -> serde_json::Value {
    json!({
        "subject": {
            "uri": post["post"]["uri"],
            "cid": post["post"]["cid"],
        },
        "item": post["post"],
    })
}

fn account_source(id: i64, value: &str, did: &str) -> WatchedSource {
    WatchedSource {
        id,
        kind: SourceKind::Account,
        value: value.to_string(),
        did: Some(did.to_string()),
        added_at: "2024-01-01T00:00:00Z".to_string(),
    }
}

fn feed_source(id: i64, uri: &str) -> WatchedSource {
    WatchedSource {
        id,
        kind: SourceKind::Feed,
        value: uri.to_string(),
        did: None,
        added_at: "2024-01-01T00:00:00Z".to_string(),
    }
}

/// A roster receiver seeded with the given accounts (the sender is
/// dropped, exactly the closed-channel case tests must tolerate).
fn account_roster(values: &[(&str, &str)]) -> watch::Receiver<Vec<WatchedSource>> {
    let sources = values
        .iter()
        .enumerate()
        .map(|(i, (value, did))| account_source(i as i64 + 1, value, did))
        .collect();
    let (_tx, rx) = watch::channel(sources);
    rx
}

async fn wait_for_candidate(mut rx: crate::pipeline::CandidatePostReceiver) -> CandidatePost {
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("candidate received in time")
        .expect("channel open")
}

#[tokio::test]
async fn pagination_walks_to_dedup_boundary_and_stops() {
    let server = MockServer::start().await;
    mount_login(&server).await;
    let (_dir, store) = open_store().await;

    // Post 1 (oldest of the three) is already archived; it should be
    // hit on page 2 and stop pagination immediately, so a would-be
    // page 3 is never requested.
    store
        .save_post(
            Category::Post,
            "at://did:plc:alice/app.bsky.feed.post/1",
            "cid-1",
            json!({}),
        )
        .await
        .unwrap();

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getAuthorFeed"))
        .and(query_param("cursor", "page-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "feed": [feed_view_post(1, true)],
            "cursor": "page-3-should-never-be-requested",
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getAuthorFeed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "feed": [feed_view_post(3, true), feed_view_post(2, false)],
            "cursor": "page-2",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let (tx, mut rx) = candidate_post_channel(8);
    let client = test_client(&server);
    let new_count = poll_handle_once(&client, &store, &tx, "alice.bsky.social", 50)
        .await
        .expect("poll succeeds");

    // Post 3 has media (sent), post 2 has none (skipped), post 1 is
    // the dedup boundary (stops pagination, never sent).
    assert_eq!(new_count, 1);
    drop(tx);
    let sent = rx.recv().await.expect("candidate sent");
    assert_eq!(sent.at_uri, "at://did:plc:alice/app.bsky.feed.post/3");
    assert_eq!(sent.media.len(), 1);
    assert!(rx.recv().await.is_none());
}

#[tokio::test]
async fn empty_feed_is_not_treated_as_new_content() {
    let server = MockServer::start().await;
    mount_login(&server).await;
    let (_dir, store) = open_store().await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getAuthorFeed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"feed": [], "cursor": null})))
        .mount(&server)
        .await;

    let (tx, _rx) = candidate_post_channel(8);
    let client = test_client(&server);
    let new_count = poll_handle_once(&client, &store, &tx, "alice.bsky.social", 50)
        .await
        .expect("poll succeeds");
    assert_eq!(new_count, 0);
}

#[test]
fn adaptive_interval_grows_on_empty_and_error_then_resets_on_content() {
    let baseline = Duration::from_secs(10);
    let max = Duration::from_secs(80);
    let mut interval = AdaptiveInterval::new(baseline, max);
    assert_eq!(interval.current, baseline);

    interval.on_empty();
    assert_eq!(interval.current, Duration::from_secs(20));
    interval.on_empty();
    assert_eq!(interval.current, Duration::from_secs(40));
    interval.on_error();
    assert_eq!(interval.current, Duration::from_secs(80));
    // Capped at max, does not keep growing past it.
    interval.on_error();
    assert_eq!(interval.current, Duration::from_secs(80));

    interval.on_content_found();
    assert_eq!(interval.current, baseline);
}

#[test]
fn jitter_stays_within_plus_minus_twenty_percent() {
    let base = Duration::from_secs(100);
    for _ in 0..200 {
        let jittered = jitter(base);
        assert!(jittered >= Duration::from_millis(79_000));
        assert!(jittered <= Duration::from_millis(121_000));
    }
}

#[test]
fn is_active_reflects_connection_health() {
    let now = Instant::now();
    let grace = Duration::from_secs(30);

    assert!(!is_active(ConnectionHealth::Connected, now, grace));
    assert!(is_active(ConnectionHealth::Disabled, now, grace));

    let just_started = ConnectionHealth::Reconnecting { since: now };
    assert!(!is_active(just_started, now, grace));

    let long_ago = now - Duration::from_secs(60);
    let stale = ConnectionHealth::Reconnecting { since: long_ago };
    assert!(is_active(stale, now, grace));
}

#[tokio::test]
async fn stays_idle_while_connection_healthy_then_polls_once_disconnected() {
    let server = MockServer::start().await;
    mount_login(&server).await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getAuthorFeed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"feed": [], "cursor": null})))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let (tx, _rx) = candidate_post_channel(8);
    let (health_tx, health_rx) = connection_health_channel(ConnectionHealth::Connected);
    let client = test_client(&server);

    // Real (small) durations rather than paused virtual time: the
    // wiremock server does real loopback network I/O, which paused
    // Tokio time doesn't reliably drive forward.
    let mut config = PollerConfig::new(Duration::from_millis(20));
    config.disconnected_grace_period = Duration::from_millis(10);
    config.health_recheck_interval = Duration::from_millis(10);

    let poller = RestFallbackPoller::new(
        std::sync::Arc::new(client),
        store,
        tx,
        health_rx,
        account_roster(&[("alice.bsky.social", "did:plc:alice")]),
        config,
    );
    let handle = tokio::spawn(poller.run());

    // While healthy, waiting well past several health-recheck/baseline
    // intervals must never trigger a poll.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        0,
        "must not poll while firehose is connected"
    );

    // Once the firehose reports disabled, the fallback should become
    // active and eventually poll.
    health_tx.send(ConnectionHealth::Disabled).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let requests = server.received_requests().await.unwrap();
    assert!(
        requests
            .iter()
            .any(|r| r.url.path() == "/xrpc/app.bsky.feed.getAuthorFeed"),
        "expected a poll once disconnected, got: {requests:?}"
    );
    handle.abort();
}

#[tokio::test]
async fn rest_fallback_polls_only_account_sources_not_feeds() {
    let server = MockServer::start().await;
    mock_session(&server).await;

    let (_dir, store) = open_store().await;
    let (tx, _rx) = candidate_post_channel(8);
    let (_, health_rx) = connection_health_channel(ConnectionHealth::Disabled);

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getAuthorFeed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"feed": [], "cursor": null})))
        .mount(&server)
        .await;

    // The roster holds one account and one feed; the fallback must only
    // ever poll the account.
    let (_roster_tx, roster) = watch::channel(vec![
        account_source(1, "alice.bsky.social", "did:plc:alice"),
        feed_source(2, "at://did:plc:alice/app.bsky.feed.generator/whats-hot"),
    ]);

    let poller = RestFallbackPoller::new(
        make_client(&server),
        store,
        tx,
        health_rx,
        roster,
        PollerConfig::new(Duration::from_secs(60)),
    );

    assert_eq!(poller.poll_all_handles().await, CycleOutcome::Empty);

    let requests = server.received_requests().await.unwrap();
    let polls: Vec<_> = requests
        .iter()
        .filter(|r| r.url.path() == "/xrpc/app.bsky.feed.getAuthorFeed")
        .collect();
    assert_eq!(polls.len(), 1, "exactly one account poll: {polls:?}");
    assert!(
        !requests
            .iter()
            .any(|r| r.url.path() == "/xrpc/app.bsky.feed.getFeed"),
        "feeds must not be polled by the rest fallback"
    );
}

#[tokio::test]
async fn rest_fallback_polls_a_newly_added_account_on_reload() {
    let server = MockServer::start().await;
    mount_login(&server).await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getAuthorFeed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"feed": [], "cursor": null})))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let (tx, _rx) = candidate_post_channel(8);
    let (_, health_rx) = connection_health_channel(ConnectionHealth::Disabled);

    let (roster_tx, roster) = watch::channel(vec![account_source(
        1,
        "alice.bsky.social",
        "did:plc:alice",
    )]);

    let poller = RestFallbackPoller::new(
        make_client(&server),
        store,
        tx,
        health_rx,
        roster,
        PollerConfig::new(Duration::from_secs(60)),
    );

    // Adding an account via the UI reloads the roster; the fallback must
    // pick it up on its next poll. `send_replace` returns the previous
    // roster, which is not what this test is checking.
    roster_tx.send_replace(vec![
        account_source(1, "alice.bsky.social", "did:plc:alice"),
        account_source(3, "bob.bsky.social", "did:plc:bob"),
    ]);

    assert_eq!(poller.poll_all_handles().await, CycleOutcome::Empty);
    let requests = server.received_requests().await.unwrap();
    let polls: Vec<_> = requests
        .iter()
        .filter(|r| r.url.path() == "/xrpc/app.bsky.feed.getAuthorFeed")
        .collect();
    assert_eq!(polls.len(), 2, "{polls:?}");
    let queries: Vec<&str> = polls
        .iter()
        .map(|r| r.url.query().unwrap_or_default())
        .collect();
    assert!(
        queries
            .iter()
            .any(|q| q.contains("actor=alice.bsky.social"))
    );
    assert!(queries.iter().any(|q| q.contains("actor=bob.bsky.social")));
}

#[tokio::test]
async fn likes_and_bookmarks_are_archived_under_distinct_categories() {
    let server = MockServer::start().await;
    mock_session(&server).await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getActorLikes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "feed": [text_only_post("at://did:plc:bob/app.bsky.feed.post/1", "cid-1")],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "bookmarks": [bookmark_wrap(text_only_post(
                "at://did:plc:carol/app.bsky.feed.post/2",
                "cid-2",
            ))],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let (tx, _rx) = candidate_post_channel(8);
    let client = make_client(&server);
    let poller = LikesBookmarksPoller::new(
        client,
        store.clone(),
        tx,
        "did:plc:alice".to_string(),
        Duration::from_secs(60),
    );

    poller.poll_likes().await.expect("poll likes");
    poller.poll_bookmarks().await.expect("poll bookmarks");

    assert!(
        store
            .is_archived(Category::Like, "at://did:plc:bob/app.bsky.feed.post/1")
            .await
            .unwrap()
    );
    assert!(
        !store
            .is_archived(Category::Bookmark, "at://did:plc:bob/app.bsky.feed.post/1")
            .await
            .unwrap()
    );
    assert!(
        store
            .is_archived(
                Category::Bookmark,
                "at://did:plc:carol/app.bsky.feed.post/2"
            )
            .await
            .unwrap()
    );
    assert!(
        !store
            .is_archived(Category::Like, "at://did:plc:carol/app.bsky.feed.post/2")
            .await
            .unwrap()
    );
}

/// A bookmarked post deleted upstream comes back as a `notFound`
/// bookmark item: the archived copy must be kept and marked with a
/// `deleted_at` timestamp. A blocked post still exists, so it must
/// never be marked.
#[tokio::test]
async fn deleted_bookmarked_post_is_marked_deleted_in_index() {
    let server = MockServer::start().await;
    mock_session(&server).await;

    let (_dir, store) = open_store().await;
    let deleted_uri = "at://did:plc:carol/app.bsky.feed.post/1";
    store
        .save_post(
            Category::Bookmark,
            deleted_uri,
            "cid-1",
            json!({"text": "deleted later"}),
        )
        .await
        .unwrap();

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "bookmarks": [
                {
                    "subject": {"uri": deleted_uri, "cid": "cid-1"},
                    "item": {"uri": deleted_uri, "notFound": true},
                },
                {
                    "subject": {
                        "uri": "at://did:plc:y/app.bsky.feed.post/2",
                        "cid": "cid-2",
                    },
                    "item": {
                        "uri": "at://did:plc:y/app.bsky.feed.post/2",
                        "cid": "cid-2",
                        "author": {"did": "did:plc:y"},
                        "blocked": true,
                    },
                },
            ],
        })))
        .mount(&server)
        .await;

    let (tx, _rx) = candidate_post_channel(8);
    let poller = LikesBookmarksPoller::new(
        make_client(&server),
        store.clone(),
        tx,
        "did:plc:alice".to_string(),
        Duration::from_secs(60),
    );
    poller.poll_bookmarks().await.expect("poll bookmarks");

    let record = store
        .get_post(Category::Bookmark, deleted_uri)
        .await
        .expect("get_post")
        .expect("deleted post is still archived (kept, not removed)");
    assert!(
        record.deleted_at.is_some(),
        "deleted bookmarked post should be marked"
    );
}

/// The bookmark action time (`bookmarkView.createdAt` — what Bluesky's
/// own bookmarks list is ordered by) is captured at archive time so the
/// gallery's "bookmarked" sorts can reproduce that ordering.
#[tokio::test]
async fn bookmark_action_time_is_recorded_at_archive_time() {
    let server = MockServer::start().await;
    mock_session(&server).await;

    let (_dir, store) = open_store().await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "bookmarks": [{
                "subject": {
                    "uri": "at://did:plc:carol/app.bsky.feed.post/1",
                    "cid": "cid-1",
                },
                "createdAt": "2026-09-05T08:30:00.000Z",
                "item": {
                    "uri": "at://did:plc:carol/app.bsky.feed.post/1",
                    "cid": "cid-1",
                    "author": {"did": "did:plc:carol"},
                    "record": {"text": "bookmarked"},
                },
            }],
        })))
        .mount(&server)
        .await;

    let (tx, _rx) = candidate_post_channel(8);
    let poller = LikesBookmarksPoller::new(
        make_client(&server),
        store.clone(),
        tx,
        "did:plc:alice".to_string(),
        Duration::from_secs(60),
    );
    poller.poll_bookmarks().await.expect("poll bookmarks");

    let record = store
        .get_post(
            Category::Bookmark,
            "at://did:plc:carol/app.bsky.feed.post/1",
        )
        .await
        .expect("get_post")
        .expect("bookmark archived");
    assert_eq!(
        record.action_at.as_deref(),
        Some("2026-09-05T08:30:00.000Z"),
        "the bookmark action time should be stored"
    );
}

#[tokio::test]
async fn bookmark_walk_ranks_items_in_list_order() {
    let server = MockServer::start().await;
    mock_session(&server).await;

    let (_dir, store) = open_store().await;

    // Page 1 lists two bookmarks, newest first: post/1 is the newest
    // (rank 0), post/2 the older one (rank 1).
    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "bookmarks": [
                {
                    "subject": {
                        "uri": "at://did:plc:carol/app.bsky.feed.post/1",
                        "cid": "cid-1",
                    },
                    "item": {
                        "uri": "at://did:plc:carol/app.bsky.feed.post/1",
                        "cid": "cid-1",
                        "author": {"did": "did:plc:carol"},
                        "record": {"text": "newest"},
                    },
                },
                {
                    "subject": {
                        "uri": "at://did:plc:carol/app.bsky.feed.post/2",
                        "cid": "cid-2",
                    },
                    "item": {
                        "uri": "at://did:plc:carol/app.bsky.feed.post/2",
                        "cid": "cid-2",
                        "author": {"did": "did:plc:carol"},
                        "record": {"text": "older"},
                    },
                },
            ],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    let (tx, _rx) = candidate_post_channel(8);
    let poller = LikesBookmarksPoller::new(
        make_client(&server),
        store.clone(),
        tx,
        "did:plc:alice".to_string(),
        Duration::from_secs(60),
    );
    poller.poll_bookmarks().await.expect("poll bookmarks");

    async fn seq(store: &ArchiveStore, uri: &str) -> Option<i64> {
        store
            .get_post(Category::Bookmark, uri)
            .await
            .expect("get_post")
            .expect("archived")
            .action_seq
    }
    assert_eq!(
        seq(&store, "at://did:plc:carol/app.bsky.feed.post/1").await,
        Some(0),
        "the first entry in the API's list is rank 0"
    );
    assert_eq!(
        seq(&store, "at://did:plc:carol/app.bsky.feed.post/2").await,
        Some(1)
    );
}

/// An already-archived post whose stored media carries a pre-fix
/// corruption signature (TS bytes stored as .mp4) is re-downloaded on
/// the next walk: the bad file is cleared and the current downloader
/// logic replaces it with a valid remuxed MP4.
#[tokio::test]
async fn archived_post_with_corrupt_media_is_re_downloaded() {
    let server = MockServer::start().await;
    mock_session(&server).await;

    let (_dir, store) = open_store().await;
    let uri = "at://did:plc:carol/app.bsky.feed.post/1";

    // Archived by an older version: the record is fine, the media is
    // raw TS wearing an .mp4 name.
    store
        .save_post(
            Category::Bookmark,
            uri,
            "cid-1",
            json!({"text": "bookmarked"}),
        )
        .await
        .unwrap();
    let mut ts = vec![0x47u8; 3 * 188];
    store
        .save_media(
            Category::Bookmark,
            uri,
            "000.mp4",
            Some("video/mp4".to_string()),
            std::mem::take(&mut ts),
        )
        .await
        .unwrap();

    // The API's hydration now exposes the video's HLS playlist.
    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "bookmarks": [{
                "subject": {"uri": uri, "cid": "cid-1"},
                "item": {
                    "uri": uri,
                    "cid": "cid-1",
                    "author": {"did": "did:plc:carol"},
                    "record": {"text": "bookmarked"},
                    "embed": {
                        "$type": "app.bsky.embed.video#view",
                        "playlist": format!("{}/watch/did/cid/playlist.m3u8", server.uri()),
                    },
                },
            }],
            "cursor": null,
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/watch/did/cid/playlist.m3u8"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            "#EXTM3U\n#EXT-X-VERSION:3\n#EXTINF:1.0,\nvideo0.ts\n#EXT-X-ENDLIST\n",
        ))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/watch/did/cid/video0.ts"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "video/iso.segment")
                .set_body_bytes(crate::remux::synthetic_test_ts()),
        )
        .mount(&server)
        .await;

    let (tx, mut rx) = candidate_post_channel(8);
    let poller = LikesBookmarksPoller::new(
        make_client(&server),
        store.clone(),
        tx,
        "did:plc:alice".to_string(),
        Duration::from_secs(60),
    );
    poller.poll_bookmarks().await.expect("poll bookmarks");
    assert!(
        store
            .list_post_media(Category::Bookmark, uri)
            .await
            .unwrap()
            .is_empty(),
        "the repair pass must have cleared the corrupt rows"
    );

    // The downloader consumes the repair candidate the walk queued;
    // dropping the poller closes the channel so `run` can finish.
    drop(poller);
    crate::media::MediaDownloader::new(store.clone(), 4, 5_000_000)
        .run(&mut rx)
        .await;

    let record = store
        .get_post(Category::Bookmark, uri)
        .await
        .expect("get_post")
        .expect("still archived");
    assert_eq!(record.media.len(), 1, "repaired media row present");
    assert_eq!(record.media[0].filename, "000.mp4");
    assert_eq!(
        record.media[0].content_type.as_deref(),
        Some("video/mp4"),
        "the replacement is a remuxed, playable MP4"
    );
    let head = store
        .read_media_head(Category::Bookmark, uri, "000.mp4", 16)
        .await
        .unwrap()
        .expect("replacement file on disk");
    assert_eq!(&head[4..8], b"ftyp", "stored bytes are a real MP4");
}

#[tokio::test]
async fn likes_pagination_stops_at_dedup_boundary() {
    let server = MockServer::start().await;
    mock_session(&server).await;

    let (_dir, store) = open_store().await;
    // Pre-archive the item that will appear as the last one on page 1.
    store
        .save_post(
            Category::Like,
            "at://did:plc:bob/app.bsky.feed.post/already",
            "cid-already",
            json!({"text": "old"}),
        )
        .await
        .unwrap();

    Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getActorLikes"))
            .and(query_param("cursor", "page-2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "feed": [text_only_post("at://did:plc:bob/app.bsky.feed.post/should-not-fetch", "cid-x")],
                "cursor": null,
            })))
            .mount(&server)
            .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getActorLikes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "feed": [
                text_only_post("at://did:plc:bob/app.bsky.feed.post/new", "cid-new"),
                text_only_post("at://did:plc:bob/app.bsky.feed.post/already", "cid-already"),
            ],
            "cursor": "page-2",
        })))
        .mount(&server)
        .await;

    let (tx, _rx) = candidate_post_channel(8);
    let client = make_client(&server);
    let poller = LikesBookmarksPoller::new(
        client,
        store.clone(),
        tx,
        "did:plc:alice".to_string(),
        Duration::from_secs(60),
    );

    poller.poll_likes().await.expect("poll likes");

    assert!(
        store
            .is_archived(Category::Like, "at://did:plc:bob/app.bsky.feed.post/new")
            .await
            .unwrap()
    );
    assert!(
        !store
            .is_archived(
                Category::Like,
                "at://did:plc:bob/app.bsky.feed.post/should-not-fetch"
            )
            .await
            .unwrap(),
        "pagination must not have continued past the dedup boundary"
    );
}

#[tokio::test]
async fn bookmarks_pagination_stops_at_dedup_boundary_independently_of_likes() {
    let server = MockServer::start().await;
    mock_session(&server).await;

    let (_dir, store) = open_store().await;
    // The same at_uri is already archived as a *like*, but not as a
    // bookmark: bookmarks pagination must not treat this as a boundary.
    store
        .save_post(
            Category::Like,
            "at://did:plc:bob/app.bsky.feed.post/shared",
            "cid-shared",
            json!({"text": "shared"}),
        )
        .await
        .unwrap();

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "bookmarks": [bookmark_wrap(text_only_post(
                "at://did:plc:bob/app.bsky.feed.post/shared",
                "cid-shared",
            ))],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    let (tx, _rx) = candidate_post_channel(8);
    let client = make_client(&server);
    let poller = LikesBookmarksPoller::new(
        client,
        store.clone(),
        tx,
        "did:plc:alice".to_string(),
        Duration::from_secs(60),
    );

    poller.poll_bookmarks().await.expect("poll bookmarks");

    assert!(
        store
            .is_archived(
                Category::Bookmark,
                "at://did:plc:bob/app.bsky.feed.post/shared"
            )
            .await
            .unwrap(),
        "bookmarks dedup must be independent of the likes archive"
    );
}

#[tokio::test]
async fn liked_post_with_media_produces_candidate_post_with_correct_category() {
    let server = MockServer::start().await;
    mock_session(&server).await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getActorLikes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "feed": [image_post("at://did:plc:bob/app.bsky.feed.post/img", "cid-img")],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let (tx, mut rx) = candidate_post_channel(8);
    let client = make_client(&server);
    let poller = LikesBookmarksPoller::new(
        client,
        store,
        tx,
        "did:plc:alice".to_string(),
        Duration::from_secs(60),
    );

    poller.poll_likes().await.expect("poll likes");

    let candidate = rx.try_recv().expect("candidate post should be sent");
    assert_eq!(candidate.at_uri, "at://did:plc:bob/app.bsky.feed.post/img");
    assert_eq!(candidate.category, PostCategory::Like);
    assert_eq!(candidate.media.len(), 1);
    assert_eq!(
        candidate.media[0].cdn_url,
        "https://cdn.example.com/img1.jpg"
    );
    assert!(rx.try_recv().is_err(), "only one candidate expected");
}

#[tokio::test]
async fn bookmarked_post_with_media_produces_candidate_post_with_correct_category() {
    let server = MockServer::start().await;
    mock_session(&server).await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.bookmark.getBookmarks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "bookmarks": [bookmark_wrap(image_post(
                "at://did:plc:bob/app.bsky.feed.post/img2",
                "cid-img2",
            ))],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let (tx, mut rx) = candidate_post_channel(8);
    let client = make_client(&server);
    let poller = LikesBookmarksPoller::new(
        client,
        store,
        tx,
        "did:plc:alice".to_string(),
        Duration::from_secs(60),
    );

    poller.poll_bookmarks().await.expect("poll bookmarks");

    let candidate = rx.try_recv().expect("candidate post should be sent");
    assert_eq!(candidate.at_uri, "at://did:plc:bob/app.bsky.feed.post/img2");
    assert_eq!(candidate.category, PostCategory::Bookmark);
    assert_eq!(candidate.media.len(), 1);
}

#[tokio::test]
async fn text_only_liked_post_produces_no_candidate() {
    let server = MockServer::start().await;
    mock_session(&server).await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getActorLikes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "feed": [text_only_post("at://did:plc:bob/app.bsky.feed.post/text", "cid-text")],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let (tx, mut rx) = candidate_post_channel(8);
    let client = make_client(&server);
    let poller = LikesBookmarksPoller::new(
        client,
        store,
        tx,
        "did:plc:alice".to_string(),
        Duration::from_secs(60),
    );

    poller.poll_likes().await.expect("poll likes");
    assert!(rx.try_recv().is_err(), "text-only post has no media");
}

#[test]
fn poll_error_retry_after_extracts_bluesky_hint() {
    let bluesky_err = PollError::Bluesky(BlueskyError::Api {
        status: 429,
        body: String::new(),
        retry_after: Some(Duration::from_secs(30)),
    });
    assert_eq!(bluesky_err.retry_after(), Some(Duration::from_secs(30)));

    let storage_err = PollError::Storage(StorageError::NotFound("at://x".to_string()));
    assert_eq!(storage_err.retry_after(), None);
}

#[test]
fn extract_media_refs_handles_record_with_media() {
    let embed = json!({
        "$type": "app.bsky.embed.recordWithMedia#view",
        "record": {"record": {"uri": "at://...", "cid": "..."}},
        "media": {
            "$type": "app.bsky.embed.video#view",
            "playlist": "https://cdn.example.com/video.m3u8",
        }
    });
    let refs = extract_media_refs(&embed);
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].cdn_url, "https://cdn.example.com/video.m3u8");
}

#[test]
fn extract_media_refs_ignores_unknown_embed() {
    let embed = json!({"$type": "app.bsky.embed.external#view"});
    assert!(extract_media_refs(&embed).is_empty());
}

// ---------------------------------------------------------------------
// Feed poller + live-roster reload behavior
// ---------------------------------------------------------------------

const FEED_URI: &str = "at://did:plc:alice/app.bsky.feed.generator/whats-hot";

#[tokio::test]
async fn feed_poll_walks_to_dedup_boundary_and_sends_media_candidates() {
    let server = MockServer::start().await;
    mount_login(&server).await;
    let (_dir, store) = open_store().await;

    // The oldest item is already archived; hitting it must stop
    // pagination so the would-be page 3 is never requested.
    store
        .save_post(
            Category::Post,
            "at://did:plc:alice/app.bsky.feed.post/1",
            "cid-1",
            json!({}),
        )
        .await
        .unwrap();

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getFeed"))
        .and(query_param("feed", FEED_URI))
        .and(query_param("cursor", "page-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "feed": [feed_view_post(1, true)],
            "cursor": "page-3-should-never-be-requested",
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getFeed"))
        .and(query_param("feed", FEED_URI))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "feed": [feed_view_post(3, true), feed_view_post(2, false)],
            "cursor": "page-2",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let (tx, mut rx) = candidate_post_channel(8);
    let client = test_client(&server);
    let new_count = poll_feed_once(&client, &store, &tx, FEED_URI, 50)
        .await
        .expect("feed poll succeeds");

    assert_eq!(new_count, 1, "only the media post is newly sent");
    drop(tx);
    let sent = rx.recv().await.expect("candidate sent");
    assert_eq!(sent.at_uri, "at://did:plc:alice/app.bsky.feed.post/3");
    assert_eq!(sent.category, PostCategory::Authored);
    assert_eq!(sent.media.len(), 1);
    assert!(rx.recv().await.is_none());
}

#[tokio::test]
async fn feed_poller_run_archives_media_posts_and_wakes_on_roster_reload() {
    let server = MockServer::start().await;
    mount_login(&server).await;
    let (_dir, store) = open_store().await;
    let (tx, rx) = candidate_post_channel(8);

    let (roster_tx, roster_rx) = watch::channel(vec![feed_source(1, FEED_URI)]);

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getFeed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "feed": [feed_view_post(5, true)],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    let poller = FeedPoller::new(
        make_client(&server),
        store,
        tx,
        roster_rx,
        PollerConfig::new(Duration::from_millis(200)),
    );
    let poller_handle = tokio::spawn(poller.run());

    let candidate = wait_for_candidate(rx).await;
    assert_eq!(candidate.at_uri, "at://did:plc:alice/app.bsky.feed.post/5");
    assert_eq!(candidate.category, PostCategory::Authored);

    // A roster reload must wake the poller into an immediate extra poll.
    let before_wake = server.received_requests().await.unwrap().len();
    roster_tx.send_replace(vec![feed_source(1, FEED_URI)]);
    tokio::time::sleep(Duration::from_millis(40)).await;
    let after_wake = server.received_requests().await.unwrap().len();
    assert!(
        after_wake > before_wake,
        "roster reload should wake the feed poller for an immediate poll"
    );

    poller_handle.abort();
    let _ = poller_handle.await;
}

#[tokio::test]
async fn feed_poller_wakes_to_poll_a_newly_added_feed() {
    let server = MockServer::start().await;
    mock_session(&server).await;

    let feed_uri = "at://did:plc:alice/app.bsky.feed.generator/fresh";
    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getFeed"))
        .and(query_param("feed", feed_uri))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "feed": [feed_view_post(6, true)],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let (tx, rx) = candidate_post_channel(8);
    let (roster_tx, roster_rx) = watch::channel(Vec::new());

    let poller = FeedPoller::new(
        make_client(&server),
        store,
        tx,
        roster_rx,
        PollerConfig::new(Duration::from_millis(200)),
    );
    let poller_handle = tokio::spawn(poller.run());

    // An empty roster means no feed requests at all.
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        !server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|r| r.url.path() == "/xrpc/app.bsky.feed.getFeed"),
        "no feed to poll yet"
    );

    // The UI adds the feed: the roster reload wakes the poller.
    roster_tx.send_replace(vec![feed_source(1, feed_uri)]);
    let candidate = wait_for_candidate(rx).await;
    assert_eq!(candidate.at_uri, "at://did:plc:alice/app.bsky.feed.post/6");

    poller_handle.abort();
    let _ = poller_handle.await;
}

#[tokio::test]
async fn backfill_account_once_archives_media_candidates() {
    let server = MockServer::start().await;
    mock_session(&server).await;

    Mock::given(method("GET"))
        .and(path("/xrpc/app.bsky.feed.getAuthorFeed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "feed": [feed_view_post(7, true)],
            "cursor": null,
        })))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let (tx, mut rx) = candidate_post_channel(8);
    let client = test_client(&server);
    let new_count = backfill_account_once(&client, &store, &tx, "bob.bsky.social")
        .await
        .expect("backfill succeeds");
    assert_eq!(new_count, 1);

    drop(tx);
    let sent = rx.recv().await.expect("candidate sent");
    assert_eq!(sent.at_uri, "at://did:plc:alice/app.bsky.feed.post/7");
    assert!(rx.recv().await.is_none());
}

#[tokio::test]
async fn backfill_feed_once_archives_only_archive_worthy_posts() {
    let server = MockServer::start().await;
    mock_session(&server).await;

    Mock::given(method("GET"))
            .and(path("/xrpc/app.bsky.feed.getFeed"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "feed": [
                    feed_view_post(8, true),
                    { "post": { "uri": "at://did:plc:alice/app.bsky.feed.post/two", "cid": "cid", "author": {"did": "did:plc:alice"}, "record": {"text": "no media"} } },
                ],
                "cursor": null,
            })))
            .mount(&server)
            .await;

    let (_dir, store) = open_store().await;
    let (tx, mut rx) = candidate_post_channel(8);
    let client = test_client(&server);
    let new_count = backfill_feed_once(&client, &store, &tx, FEED_URI)
        .await
        .expect("backfill succeeds");
    assert_eq!(new_count, 1);

    drop(tx);
    let sent = rx.recv().await.expect("candidate sent");
    assert_eq!(sent.at_uri, "at://did:plc:alice/app.bsky.feed.post/8");
    assert!(rx.recv().await.is_none(), "text-only post skipped");
}
