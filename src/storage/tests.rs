use super::*;
use serde_json::json;

async fn open_store() -> (tempfile::TempDir, ArchiveStore) {
    let dir = tempfile::tempdir().expect("create temp dir");
    let archive_dir = dir.path().join("archive");
    let database_path = dir.path().join("index.sqlite3");
    let store = ArchiveStore::open(archive_dir, database_path)
        .await
        .expect("open store");
    (dir, store)
}

#[tokio::test]
async fn save_then_list_round_trip() {
    let (_dir, store) = open_store().await;

    let outcome = store
        .save_post(
            Category::Post,
            "at://did:plc:alice/app.bsky.feed.post/1",
            "cid-1",
            json!({"text": "hello"}),
        )
        .await
        .expect("save post");
    assert_eq!(outcome, SaveOutcome::Inserted);

    let page = store.list_posts(None, 1, 10).await.expect("list posts");
    assert_eq!(page.total_items, 1);
    assert_eq!(page.items.len(), 1);
    assert_eq!(
        page.items[0].at_uri,
        "at://did:plc:alice/app.bsky.feed.post/1"
    );
    assert_eq!(page.items[0].cid, "cid-1");

    let fetched = store
        .get_post(Category::Post, "at://did:plc:alice/app.bsky.feed.post/1")
        .await
        .expect("get post")
        .expect("post exists");
    assert_eq!(fetched.record, json!({"text": "hello"}));
}

#[tokio::test]
async fn saving_same_post_twice_is_a_dedup_no_op() {
    let (_dir, store) = open_store().await;
    let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";

    let first = store
        .save_post(Category::Post, at_uri, "cid-1", json!({"text": "v1"}))
        .await
        .expect("first save");
    assert_eq!(first, SaveOutcome::Inserted);

    let second = store
        .save_post(Category::Post, at_uri, "cid-2", json!({"text": "v2"}))
        .await
        .expect("second save");
    assert_eq!(second, SaveOutcome::AlreadyArchived);

    let page = store.list_posts(None, 1, 10).await.expect("list posts");
    assert_eq!(page.total_items, 1);

    let fetched = store
        .get_post(Category::Post, at_uri)
        .await
        .expect("get post")
        .expect("post exists");
    assert_eq!(
        fetched.record,
        json!({"text": "v1"}),
        "second save must not overwrite"
    );
    assert_eq!(fetched.cid, "cid-1");
}

#[tokio::test]
async fn is_archived_reflects_disk_state() {
    let (_dir, store) = open_store().await;
    let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";

    assert!(!store.is_archived(Category::Post, at_uri).await.unwrap());
    store
        .save_post(Category::Post, at_uri, "cid-1", json!({}))
        .await
        .unwrap();
    assert!(store.is_archived(Category::Post, at_uri).await.unwrap());
}

#[tokio::test]
async fn categories_are_isolated() {
    let (_dir, store) = open_store().await;
    let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";

    store
        .save_post(Category::Post, at_uri, "cid-1", json!({"kind": "post"}))
        .await
        .unwrap();
    store
        .save_post(Category::Like, at_uri, "cid-1", json!({"kind": "like"}))
        .await
        .unwrap();

    let posts = store.list_posts(Some(Category::Post), 1, 10).await.unwrap();
    let likes = store.list_posts(Some(Category::Like), 1, 10).await.unwrap();
    assert_eq!(posts.total_items, 1);
    assert_eq!(likes.total_items, 1);

    let post = store
        .get_post(Category::Post, at_uri)
        .await
        .unwrap()
        .unwrap();
    let like = store
        .get_post(Category::Like, at_uri)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(post.record, json!({"kind": "post"}));
    assert_eq!(like.record, json!({"kind": "like"}));
}

#[tokio::test]
async fn save_media_updates_record_and_gallery_index() {
    let (_dir, store) = open_store().await;
    let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
    store
        .save_post(Category::Post, at_uri, "cid-1", json!({}))
        .await
        .unwrap();

    store
        .save_media(
            Category::Post,
            at_uri,
            "image1.jpg",
            Some("image/jpeg".to_string()),
            b"fake-image-bytes".to_vec(),
        )
        .await
        .unwrap();

    let record = store
        .get_post(Category::Post, at_uri)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.media.len(), 1);
    assert_eq!(record.media[0].filename, "image1.jpg");
    assert_eq!(record.media[0].size_bytes, "fake-image-bytes".len() as u64);

    let gallery = store
        .list_media(None, 1, 10, MediaSort::NewestArchived)
        .await
        .unwrap();
    assert_eq!(gallery.total_items, 1);
    assert_eq!(gallery.items[0].filename, "image1.jpg");
    assert_eq!(gallery.items[0].post_at_uri, at_uri);

    let post_list = store.list_posts(None, 1, 10).await.unwrap();
    assert_eq!(
        post_list.items[0].thumbnail_filename.as_deref(),
        Some("image1.jpg")
    );
    assert_eq!(
        post_list.items[0].thumbnail_content_type.as_deref(),
        Some("image/jpeg")
    );

    let bytes = store
        .read_media(Category::Post, at_uri, "image1.jpg")
        .await
        .unwrap()
        .expect("media file should be readable");
    assert_eq!(bytes, b"fake-image-bytes");

    assert!(
        store
            .read_media(Category::Post, at_uri, "missing.jpg")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .read_media(Category::Post, at_uri, "../record.json")
            .await
            .unwrap()
            .is_none(),
        "path traversal attempts must be rejected"
    );

    // Re-saving the same filename is a no-op: no duplicate media rows,
    // no duplicate entries in the record's media list.
    store
        .save_media(
            Category::Post,
            at_uri,
            "image1.jpg",
            Some("image/jpeg".to_string()),
            b"different-bytes-should-be-ignored".to_vec(),
        )
        .await
        .unwrap();
    let record_again = store
        .get_post(Category::Post, at_uri)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record_again.media.len(), 1);
    let gallery_again = store
        .list_media(None, 1, 10, MediaSort::NewestArchived)
        .await
        .unwrap();
    assert_eq!(gallery_again.total_items, 1);
}

#[tokio::test]
async fn save_media_without_post_fails() {
    let (_dir, store) = open_store().await;
    let err = store
        .save_media(
            Category::Post,
            "at://did:plc:alice/app.bsky.feed.post/missing",
            "x.jpg",
            None,
            b"bytes".to_vec(),
        )
        .await
        .expect_err("media for unarchived post should fail");
    assert!(matches!(err, StorageError::NotFound(_)));
}

#[tokio::test]
async fn atomic_write_never_exposes_a_partial_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested").join("record.json");

    atomic_write(&path, b"{\"complete\": true}").unwrap();
    assert!(path.exists());
    let contents = std::fs::read_to_string(&path).unwrap();
    assert_eq!(contents, "{\"complete\": true}");

    // No leftover temp files after a successful write.
    let leftovers: Vec<_> = std::fs::read_dir(path.parent().unwrap())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with(".tmp-"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "temp file was not cleaned up: {leftovers:?}"
    );
}

#[tokio::test]
async fn pagination_returns_correct_slices_and_counts() {
    let (_dir, store) = open_store().await;
    for i in 0..25 {
        store
            .save_post(
                Category::Post,
                &format!("at://did:plc:alice/app.bsky.feed.post/{i:03}"),
                &format!("cid-{i}"),
                json!({"i": i}),
            )
            .await
            .unwrap();
    }

    let page1 = store.list_posts(None, 1, 10).await.unwrap();
    assert_eq!(page1.items.len(), 10);
    assert_eq!(page1.total_items, 25);
    assert_eq!(page1.total_pages, 3);

    let page2 = store.list_posts(None, 2, 10).await.unwrap();
    assert_eq!(page2.items.len(), 10);

    let page3 = store.list_posts(None, 3, 10).await.unwrap();
    assert_eq!(page3.items.len(), 5);

    let page4 = store.list_posts(None, 4, 10).await.unwrap();
    assert_eq!(page4.items.len(), 0);

    // No overlap between pages.
    let mut all_uris: Vec<_> = page1
        .items
        .iter()
        .chain(page2.items.iter())
        .chain(page3.items.iter())
        .map(|p| p.at_uri.clone())
        .collect();
    all_uris.sort();
    all_uris.dedup();
    assert_eq!(all_uris.len(), 25);
}

#[tokio::test]
async fn reindex_from_empty_database_matches_incremental_index() {
    let dir = tempfile::tempdir().unwrap();
    let archive_dir = dir.path().join("archive");
    let database_path = dir.path().join("index.sqlite3");

    let store = ArchiveStore::open(archive_dir.clone(), database_path.clone())
        .await
        .unwrap();
    for i in 0..5 {
        let at_uri = format!("at://did:plc:alice/app.bsky.feed.post/{i}");
        store
            .save_post(
                Category::Post,
                &at_uri,
                &format!("cid-{i}"),
                json!({"i": i}),
            )
            .await
            .unwrap();
        store
            .save_media(
                Category::Post,
                &at_uri,
                "img.jpg",
                Some("image/jpeg".to_string()),
                vec![i as u8; 10],
            )
            .await
            .unwrap();
    }
    store
        .save_post(
            Category::Like,
            "at://did:plc:alice/app.bsky.feed.like/1",
            "cid-like",
            json!({}),
        )
        .await
        .unwrap();

    let before_posts = store.list_posts(None, 1, 100).await.unwrap();
    let before_media = store
        .list_media(None, 1, 100, MediaSort::NewestArchived)
        .await
        .unwrap();

    // A brand-new store pointed at a fresh database file, over the
    // same on-disk archive: the index starts empty.
    let fresh_database_path = dir.path().join("index-fresh.sqlite3");
    let fresh_store = ArchiveStore::open(archive_dir.clone(), fresh_database_path)
        .await
        .unwrap();
    let empty_before_reindex = fresh_store.list_posts(None, 1, 100).await.unwrap();
    assert_eq!(empty_before_reindex.total_items, 0);

    fresh_store.reindex().await.unwrap();

    let after_posts = fresh_store.list_posts(None, 1, 100).await.unwrap();
    let after_media = fresh_store
        .list_media(None, 1, 100, MediaSort::NewestArchived)
        .await
        .unwrap();

    assert_eq!(after_posts.total_items, before_posts.total_items);
    let mut before_uris: Vec<_> = before_posts
        .items
        .iter()
        .map(|p| p.at_uri.clone())
        .collect();
    let mut after_uris: Vec<_> = after_posts.items.iter().map(|p| p.at_uri.clone()).collect();
    before_uris.sort();
    after_uris.sort();
    assert_eq!(before_uris, after_uris);

    assert_eq!(after_media.total_items, before_media.total_items);
    let mut before_files: Vec<_> = before_media
        .items
        .iter()
        .map(|m| (m.post_at_uri.clone(), m.filename.clone()))
        .collect();
    let mut after_files: Vec<_> = after_media
        .items
        .iter()
        .map(|m| (m.post_at_uri.clone(), m.filename.clone()))
        .collect();
    before_files.sort();
    after_files.sort();
    assert_eq!(before_files, after_files);
}

#[tokio::test]
async fn reindex_on_existing_index_replaces_stale_rows() {
    let (dir, store) = open_store().await;
    let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
    store
        .save_post(Category::Post, at_uri, "cid-1", json!({}))
        .await
        .unwrap();

    // Simulate index drift: delete the on-disk record directly,
    // bypassing the store, then reindex and confirm the stale row is
    // gone.
    let path = record_path(&dir.path().join("archive"), Category::Post, at_uri);
    std::fs::remove_file(&path).unwrap();

    store.reindex().await.unwrap();
    let page = store.list_posts(None, 1, 10).await.unwrap();
    assert_eq!(page.total_items, 0);
}

/// Seeds one image under each category so category-filtered queries have
/// something to isolate.
async fn seed_one_image_per_category(store: &ArchiveStore) {
    for category in Category::ALL {
        let at_uri = format!("at://did:plc:alice/app.bsky.feed.post/{category}");
        store
            .save_post(category, &at_uri, "cid", json!({}))
            .await
            .unwrap();
        store
            .save_media(
                category,
                &at_uri,
                "000.jpg",
                Some("image/jpeg".to_string()),
                vec![0u8; 10],
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn list_media_filters_by_category() {
    let (_dir, store) = open_store().await;
    seed_one_image_per_category(&store).await;

    let all = store
        .list_media(None, 1, 100, MediaSort::NewestArchived)
        .await
        .unwrap();
    assert_eq!(all.total_items, 3);

    let likes = store
        .list_media(Some(Category::Like), 1, 100, MediaSort::NewestArchived)
        .await
        .unwrap();
    assert_eq!(likes.total_items, 1);
    assert_eq!(likes.items.len(), 1);
    assert!(likes.items.iter().all(|m| m.category == Category::Like));
}

/// The created-time sort mirrors the record's own `createdAt`, so the
/// ordering can differ from the archive-time ordering; missing
/// `createdAt` falls back to the archive time.
#[tokio::test]
async fn list_media_sorts_by_created_at_when_available() {
    let (_dir, store) = open_store().await;

    // Archive order: A stored first, then B. Record times: A is the
    // newest post ever, B much older — the opposite ordering.
    let old = "at://did:plc:alice/app.bsky.feed.post/1";
    let new = "at://did:plc:alice/app.bsky.feed.post/2";
    store
        .save_post(
            Category::Post,
            new,
            "cid-new",
            json!({"createdAt": "2030-01-01T00:00:00.000Z"}),
        )
        .await
        .unwrap();
    store
        .save_media(
            Category::Post,
            new,
            "aa.jpg",
            Some("image/jpeg".into()),
            vec![0u8; 10],
        )
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    store
        .save_post(
            Category::Post,
            old,
            "cid-old",
            json!({"createdAt": "2000-01-01T00:00:00.000Z"}),
        )
        .await
        .unwrap();
    store
        .save_media(
            Category::Post,
            old,
            "bb.jpg",
            Some("image/jpeg".into()),
            vec![0u8; 10],
        )
        .await
        .unwrap();

    let archived_newest = store
        .list_media(None, 1, 10, MediaSort::NewestArchived)
        .await
        .unwrap();
    assert_eq!(
        archived_newest
            .items
            .iter()
            .map(|m| m.filename.as_str())
            .collect::<Vec<_>>(),
        ["bb.jpg", "aa.jpg"],
    );

    let created_newest = store
        .list_media(None, 1, 10, MediaSort::NewestCreated)
        .await
        .unwrap();
    assert_eq!(
        created_newest
            .items
            .iter()
            .map(|m| m.filename.as_str())
            .collect::<Vec<_>>(),
        ["aa.jpg", "bb.jpg"],
    );
    assert_eq!(
        created_newest.items[0].record_created_at.as_deref(),
        Some("2030-01-01T00:00:00.000Z"),
    );

    let created_oldest = store
        .list_media(None, 1, 10, MediaSort::OldestCreated)
        .await
        .unwrap();
    assert_eq!(
        created_oldest
            .items
            .iter()
            .map(|m| m.filename.as_str())
            .collect::<Vec<_>>(),
        ["bb.jpg", "aa.jpg"],
    );

    let archived_oldest = store
        .list_media(None, 1, 10, MediaSort::OldestArchived)
        .await
        .unwrap();
    assert_eq!(
        archived_oldest
            .items
            .iter()
            .map(|m| m.filename.as_str())
            .collect::<Vec<_>>(),
        ["aa.jpg", "bb.jpg"],
    );
}

#[tokio::test]
async fn watched_sources_round_trip_add_list_remove() {
    let (_dir, store) = open_store().await;

    assert!(store.list_watched_sources().await.unwrap().is_empty());

    let account_id = store
        .add_watched_source(
            SourceKind::Account,
            "alice.bsky.social",
            Some("did:plc:alice"),
        )
        .await
        .unwrap();
    let feed_id = store
        .add_watched_source(
            SourceKind::Feed,
            "at://did:plc:alice/app.bsky.feed.generator/whats-hot",
            None,
        )
        .await
        .unwrap();
    assert_ne!(account_id, feed_id);

    let sources = store.list_watched_sources().await.unwrap();
    assert_eq!(sources.len(), 2);
    let account = sources
        .iter()
        .find(|s| s.kind == SourceKind::Account)
        .unwrap();
    assert_eq!(account.value, "alice.bsky.social");
    assert_eq!(account.did.as_deref(), Some("did:plc:alice"));
    let feed = sources.iter().find(|s| s.kind == SourceKind::Feed).unwrap();
    assert_eq!(
        feed.value,
        "at://did:plc:alice/app.bsky.feed.generator/whats-hot"
    );
    assert_eq!(feed.did, None);
    // Each row carries a parseable RFC3339 added_at.
    assert!(!account.added_at.is_empty());
    assert!(!feed.added_at.is_empty());

    assert!(store.remove_watched_source(account_id).await.unwrap());
    assert!(
        !store.remove_watched_source(account_id).await.unwrap(),
        "second remove is a no-op"
    );
    let remaining = store.list_watched_sources().await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, feed_id);
}

#[tokio::test]
async fn adding_same_watched_source_twice_is_idempotent() {
    let (_dir, store) = open_store().await;

    let first_id = store
        .add_watched_source(
            SourceKind::Account,
            "alice.bsky.social",
            Some("did:plc:alice"),
        )
        .await
        .unwrap();
    let second_id = store
        .add_watched_source(
            SourceKind::Account,
            "alice.bsky.social",
            Some("did:plc:new-did"),
        )
        .await
        .unwrap();
    assert_eq!(
        first_id, second_id,
        "upsert must not create a duplicate row"
    );

    let sources = store.list_watched_sources().await.unwrap();
    assert_eq!(sources.len(), 1);

    // Re-adding with a fresh did refresh is an in-place update, not a
    // second row. The DID is updated to the new value.
    assert_eq!(sources[0].did.as_deref(), Some("did:plc:new-did"));
    let _ = store
        .add_watched_source(
            SourceKind::Account,
            "alice.bsky.social",
            Some("did:plc:newer"),
        )
        .await
        .unwrap();
    let after = store.list_watched_sources().await.unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].did.as_deref(), Some("did:plc:newer"));
    assert_eq!(after[0].value, "alice.bsky.social");
    assert_eq!(
        after[0].added_at, sources[0].added_at,
        "re-adding must not reset the original added_at"
    );
}

#[tokio::test]
async fn watched_sources_with_feed_and_account_kinds_stay_distinct() {
    let (_dir, store) = open_store().await;

    // The same string can appear for both kinds without colliding, since
    // uniqueness is on (kind, value).
    store
        .add_watched_source(
            SourceKind::Account,
            "at://did:plc:alice/app.bsky.feed.generator/x",
            Some("did:plc:alice"),
        )
        .await
        .unwrap();
    store
        .add_watched_source(
            SourceKind::Feed,
            "at://did:plc:alice/app.bsky.feed.generator/x",
            None,
        )
        .await
        .unwrap();

    let sources = store.list_watched_sources().await.unwrap();
    assert_eq!(sources.len(), 2);
}

#[test]
fn source_kind_parses_and_displays() {
    assert_eq!(
        "account".parse::<SourceKind>().unwrap(),
        SourceKind::Account
    );
    assert_eq!("feed".parse::<SourceKind>().unwrap(), SourceKind::Feed);
    assert_eq!(SourceKind::Account.to_string(), "account");
    assert_eq!(SourceKind::Feed.to_string(), "feed");
    assert!(matches!(
        "gallery".parse::<SourceKind>(),
        Err(StorageError::InvalidSourceKind(_))
    ));
}

async fn open_v1_database() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let database_path = dir.path().join("index.sqlite3");
    let conn = Connection::open(&database_path).unwrap();
    conn.execute_batch(
        "
            CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO schema_meta (key, value) VALUES ('schema_version', '1');
            CREATE TABLE posts (
                category TEXT NOT NULL,
                at_uri TEXT NOT NULL,
                cid TEXT NOT NULL,
                indexed_at TEXT NOT NULL,
                record_path TEXT NOT NULL,
                PRIMARY KEY (category, at_uri)
            );
            CREATE TABLE media (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL,
                post_at_uri TEXT NOT NULL,
                filename TEXT NOT NULL,
                content_type TEXT,
                size_bytes INTEGER NOT NULL,
                indexed_at TEXT NOT NULL,
                UNIQUE(category, post_at_uri, filename)
            );
            ",
    )
    .unwrap();
    // A v1 archive row that must survive the upgrade untouched.
    conn.execute(
        "INSERT INTO posts (category, at_uri, cid, indexed_at, record_path)
             VALUES ('posts', 'at://did:plc:v1/app.bsky.feed.post/1', 'cid', '2024-01-01', 'x')",
        [],
    )
    .unwrap();
    dir
}

#[tokio::test]
async fn opening_a_v1_database_upgrades_to_schema_v2_in_place() {
    let dir = open_v1_database().await;
    let store = ArchiveStore::open(dir.path().join("archive"), dir.path().join("index.sqlite3"))
        .await
        .expect("open store upgrades v1");

    // The v1 posts row is untouched...
    let page = store.list_posts(None, 1, 10).await.unwrap();
    assert_eq!(page.total_items, 1);
    assert_eq!(page.items[0].at_uri, "at://did:plc:v1/app.bsky.feed.post/1");

    // ...the schema version always reads the latest stamp (v1 data has
    // been upgraded in place through every intermediate version)...
    let bytes = std::fs::read(dir.path().join("index.sqlite3")).unwrap();
    drop(bytes);
    let conn = rusqlite::Connection::open(dir.path().join("index.sqlite3")).unwrap();
    let version: String = conn
        .query_row(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, "5");

    // ...and the new table is usable immediately (no data had to move).
    assert!(store.list_watched_sources().await.unwrap().is_empty());
    store
        .add_watched_source(
            SourceKind::Account,
            "alice.bsky.social",
            Some("did:plc:alice"),
        )
        .await
        .unwrap();
    assert_eq!(store.list_watched_sources().await.unwrap().len(), 1);
}

#[tokio::test]
async fn export_predicate_selects_images_and_skips_video_and_bin() {
    let (_dir, store) = open_store().await;
    let at_uri = "at://did:plc:alice/app.bsky.feed.post/mixed";
    store
        .save_post(Category::Post, at_uri, "cid", json!({}))
        .await
        .unwrap();
    // An image with an explicit content type.
    store
        .save_media(
            Category::Post,
            at_uri,
            "000.jpg",
            Some("image/jpeg".to_string()),
            vec![0u8; 100],
        )
        .await
        .unwrap();
    // A null-content-type row whose filename extension marks it as an
    // image — must still be included.
    store
        .save_media(Category::Post, at_uri, "001.png", None, vec![0u8; 200])
        .await
        .unwrap();
    // A video — excluded.
    store
        .save_media(
            Category::Post,
            at_uri,
            "002.mp4",
            Some("video/mp4".to_string()),
            vec![0u8; 400],
        )
        .await
        .unwrap();
    // A `.bin` fallback with an unknown/null content type — excluded.
    store
        .save_media(Category::Post, at_uri, "003.bin", None, vec![0u8; 800])
        .await
        .unwrap();

    let estimate = store.export_estimate(None).await.unwrap();
    assert_eq!(estimate.image_count, 2);
    assert_eq!(estimate.total_bytes, 300);

    let items = store.list_export_media(None).await.unwrap();
    let names: Vec<_> = items.iter().map(|m| m.filename.as_str()).collect();
    assert_eq!(items.len(), 2);
    assert!(names.contains(&"000.jpg"));
    assert!(names.contains(&"001.png"));
    assert!(!names.contains(&"002.mp4"));
    assert!(!names.contains(&"003.bin"));
}

#[tokio::test]
async fn mark_post_deleted_is_idempotent() {
    let (_dir, store) = open_store().await;
    let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";

    store
        .save_post(Category::Post, at_uri, "cid-1", json!({"text": "test"}))
        .await
        .unwrap();

    let first_deleted_at = "2024-01-01T00:00:00Z";
    {
        let db = Arc::clone(&store.db);
        let conn = db.lock().unwrap();
        conn.execute(
            "UPDATE posts SET deleted_at = ?1 WHERE at_uri = ?2",
            params![first_deleted_at, at_uri],
        )
        .unwrap();
    }

    let newly_marked = store.mark_post_deleted(at_uri).await.unwrap();
    assert!(
        !newly_marked,
        "mark_post_deleted should report an already-marked post as not newly marked"
    );

    let record = store
        .get_post(Category::Post, at_uri)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        record.deleted_at.as_deref(),
        Some(first_deleted_at),
        "mark_post_deleted should not overwrite earlier timestamp"
    );
}

#[tokio::test]
async fn mark_post_deleted_reports_whether_it_marked() {
    let (_dir, store) = open_store().await;
    let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";

    store
        .save_post(Category::Post, at_uri, "cid-1", json!({"text": "test"}))
        .await
        .unwrap();

    let newly_marked = store.mark_post_deleted(at_uri).await.unwrap();
    assert!(newly_marked, "first mark should report newly marked");

    // A URI that was never archived marks nothing.
    let missing = store
        .mark_post_deleted("at://did:plc:alice/app.bsky.feed.post/missing")
        .await
        .unwrap();
    assert!(
        !missing,
        "marking an unarchived URI reports not newly marked"
    );
}

#[tokio::test]
async fn list_archived_uris_returns_each_category_in_index_order() {
    let (_dir, store) = open_store().await;

    store
        .save_post(
            Category::Bookmark,
            "at://did:plc:carol/app.bsky.feed.post/2",
            "cid-2",
            json!({}),
        )
        .await
        .unwrap();
    store
        .save_post(
            Category::Bookmark,
            "at://did:plc:carol/app.bsky.feed.post/1",
            "cid-1",
            json!({}),
        )
        .await
        .unwrap();
    store
        .save_post(
            Category::Like,
            "at://did:plc:dave/app.bsky.feed.post/9",
            "cid-9",
            json!({}),
        )
        .await
        .unwrap();

    let bookmarks = store.list_archived_uris(Category::Bookmark).await.unwrap();
    assert_eq!(
        bookmarks,
        vec![
            "at://did:plc:carol/app.bsky.feed.post/2",
            "at://did:plc:carol/app.bsky.feed.post/1",
        ]
    );

    let likes = store.list_archived_uris(Category::Like).await.unwrap();
    assert_eq!(likes, vec!["at://did:plc:dave/app.bsky.feed.post/9"]);

    let posts = store.list_archived_uris(Category::Post).await.unwrap();
    assert!(posts.is_empty());
}

#[tokio::test]
async fn list_posts_includes_deleted_at() {
    let (_dir, store) = open_store().await;
    let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";

    store
        .save_post(Category::Post, at_uri, "cid-1", json!({"text": "test"}))
        .await
        .unwrap();

    let page = store.list_posts(None, 1, 10).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert!(
        page.items[0].deleted_at.is_none(),
        "newly archived post should not be deleted"
    );

    store.mark_post_deleted(at_uri).await.unwrap();

    let page = store.list_posts(None, 1, 10).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert!(
        page.items[0].deleted_at.is_some(),
        "deleted post should have deleted_at set"
    );
}

/// `set_action_at` records the bookmark action time in both the index
/// and the on-disk envelope, is idempotent, and survives a reindex
/// (which rebuilds the index purely from disk).
#[tokio::test]
async fn set_action_at_updates_index_and_envelope_and_survives_reindex() {
    let (dir, store) = open_store().await;
    let at_uri = "at://did:plc:carol/app.bsky.feed.post/1";

    store
        .save_post(
            Category::Bookmark,
            at_uri,
            "cid-1",
            json!({"text": "saved"}),
        )
        .await
        .unwrap();

    let newly = store
        .set_action_at(Category::Bookmark, at_uri, "2026-09-01T10:00:00.000Z")
        .await
        .unwrap();
    assert!(newly, "first set should report newly recorded");

    let again = store
        .set_action_at(Category::Bookmark, at_uri, "2026-09-02T10:00:00.000Z")
        .await
        .unwrap();
    assert!(!again, "second set must not overwrite the first value");

    let record = store
        .get_post(Category::Bookmark, at_uri)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        record.action_at.as_deref(),
        Some("2026-09-01T10:00:00.000Z")
    );

    // The envelope on disk carries it too, so a full reindex — which
    // rebuilds the index from the record.json files alone — preserves
    // the ordering key.
    store.reindex().await.unwrap();
    let record = store
        .get_post(Category::Bookmark, at_uri)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        record.action_at.as_deref(),
        Some("2026-09-01T10:00:00.000Z"),
        "action_at must survive a reindex"
    );

    // An unknown URI reports nothing recorded; a save_post that races
    // in a second category is untouched.
    let missing = store
        .set_action_at(
            Category::Bookmark,
            "at://did:plc:carol/app.bsky.feed.post/x",
            "2026-09-01T10:00:00.000Z",
        )
        .await
        .unwrap();
    assert!(!missing);
    let _ = dir;
}

/// The gallery's "bookmarked" sorts order by `action_seq` — the item's
/// position in Bluesky's own list (0 = newest), which is what the
/// Bluesky app displays — falling back to archive order for rows no
/// walk has ranked yet. `action_at` (the bookmark action time) is
/// display-only: the live API does not reliably populate it, and its
/// values don't always agree with the list order.
#[tokio::test]
async fn list_media_bookmarked_sort_uses_action_seq_with_archive_fallback() {
    let (_dir, store) = open_store().await;

    // Archive order: first, second, third. Bluesky list positions
    // deliberately scramble that: "third" is the newest bookmark
    // (seq 0), "first" the oldest of the three (seq 2), and "second"
    // is never ranked by a walk, so it falls back to its archive time.
    for (name, uri, text) in [
        (
            "first.png",
            "at://did:plc:carol/app.bsky.feed.post/1",
            "one",
        ),
        (
            "second.png",
            "at://did:plc:carol/app.bsky.feed.post/2",
            "two",
        ),
        (
            "third.png",
            "at://did:plc:carol/app.bsky.feed.post/3",
            "three",
        ),
    ] {
        store
            .save_post(Category::Bookmark, uri, name, json!({"text": text}))
            .await
            .unwrap();
        store
            .save_media(
                Category::Bookmark,
                uri,
                name,
                Some("image/png".to_string()),
                b"png-bytes".to_vec(),
            )
            .await
            .unwrap();
    }
    store
        .set_action_seq(
            Category::Bookmark,
            "at://did:plc:carol/app.bsky.feed.post/1",
            2,
        )
        .await
        .unwrap();
    store
        .set_action_seq(
            Category::Bookmark,
            "at://did:plc:carol/app.bsky.feed.post/3",
            0,
        )
        .await
        .unwrap();

    fn names(page: &Page<MediaSummary>) -> Vec<String> {
        page.items.iter().map(|m| m.filename.clone()).collect()
    }

    // Newest bookmark first (seq 0 → 1 → 2); the unranked row falls
    // back to its (just-now) archive time, so it lands at the very
    // front of the ranked tail — i.e. between the seq-1 and seq-2 rows
    // only if its archive time sorts there; it is newer than every
    // ranked row's fallback, but ranked rows always precede unranked
    // ones.
    let newest = store
        .list_media(Some(Category::Bookmark), 1, 10, MediaSort::NewestAction)
        .await
        .unwrap();
    assert_eq!(names(&newest), vec!["third.png", "first.png", "second.png"]);

    let oldest = store
        .list_media(Some(Category::Bookmark), 1, 10, MediaSort::OldestAction)
        .await
        .unwrap();
    assert_eq!(names(&oldest), vec!["first.png", "third.png", "second.png"]);
}

/// Media repaired after a corruption finding: rows and files are
/// cleared by `delete_post_media` and re-saved by the downloader, and
/// the gallery shows only the replacement.
#[tokio::test]
async fn delete_post_media_clears_rows_files_and_envelope() {
    let (dir, store) = open_store().await;
    let uri = "at://did:plc:carol/app.bsky.feed.post/1";
    store
        .save_post(Category::Bookmark, uri, "cid-1", json!({"text": "x"}))
        .await
        .unwrap();
    store
        .save_media(
            Category::Bookmark,
            uri,
            "000.mp4",
            Some("video/mp4".to_string()),
            b"video-bytes".to_vec(),
        )
        .await
        .unwrap();

    let removed = store
        .delete_post_media(Category::Bookmark, uri)
        .await
        .unwrap();
    assert_eq!(removed, 1);
    assert!(
        store
            .list_post_media(Category::Bookmark, uri)
            .await
            .unwrap()
            .is_empty(),
        "index row removed"
    );
    let record = store
        .get_post(Category::Bookmark, uri)
        .await
        .unwrap()
        .unwrap();
    assert!(record.media.is_empty(), "envelope media list cleared");
    let media_dir = dir.path().join("archive").join("bookmarks");
    let mp4_left = walk_media_files(media_dir).any(|p| p.extension().is_some_and(|e| e == "mp4"));
    assert!(!mp4_left, "media file removed from disk");
}

fn walk_media_files(root: std::path::PathBuf) -> impl Iterator<Item = std::path::PathBuf> {
    std::fs::read_dir(root)
        .into_iter()
        .flatten()
        .flatten()
        .flat_map(|e| {
            let p = e.path();
            if p.is_dir() {
                walk_media_files(p).collect::<Vec<_>>()
            } else {
                vec![p]
            }
        })
}

/// The corruption sniffer recognizes every pre-fix archive artifact:
/// raw HLS playlists, MPEG-TS bytes mislabeled as MP4, and dumped HTTP
/// responses — and passes healthy files.
#[test]
fn stored_media_corruption_signatures() {
    let mut ts = vec![0x47u8; 3 * 188];
    assert!(stored_media_looks_corrupt(
        "000.mp4",
        Some("video/mp4"),
        &ts
    ));
    ts[188] = 0;
    assert!(
        !stored_media_looks_corrupt("000.mp4", Some("video/mp4"), &ts),
        "non-TS video bytes are intact"
    );
    assert!(stored_media_looks_corrupt(
        "000.m3u8",
        Some("application/vnd.apple.mpegurl"),
        b"#EXTM3U\n#EXT-X-VERSION:3\n"
    ));
    assert!(stored_media_looks_corrupt(
        "000.mp4",
        None,
        b"HTTP/1.1 200 OK\r\ncontent-type: video/mp4\r\n\r\n..."
    ));
    assert!(!stored_media_looks_corrupt(
        "000.jpg",
        Some("image/jpeg"),
        b"\xff\xd8\xff\xe0"
    ));
    assert!(
        !stored_media_looks_corrupt("000.mp4", Some("video/mp4"), b"\x00\x00\x00 ftypisom"),
        "a real MP4 header is not corruption"
    );
}
