//! On-disk archive storage and the SQLite query index built on top of it.
//! The JSON files and media on disk are the source of truth; the SQLite
//! index is a rebuildable query layer over them.
//!
//! ## On-disk layout
//!
//! ```text
//! {ARCHIVE_DIR}/
//!   posts/{shard}/{id}/record.json
//!   posts/{shard}/{id}/media/{filename}
//!   likes/{shard}/{id}/record.json
//!   likes/{shard}/{id}/media/{filename}
//!   bookmarks/{shard}/{id}/record.json
//!   bookmarks/{shard}/{id}/media/{filename}
//! ```
//!
//! `{id}` is the hex-encoded SHA-256 digest of the item's AT URI (the
//! dedup key), and `{shard}` is its first two hex characters, so a single
//! directory never has to hold every archived item. Hashing the AT URI
//! (rather than using it verbatim as a path) sidesteps path-separator and
//! length issues in DIDs/rkeys while keeping the mapping deterministic:
//! re-archiving the same `at_uri` always resolves to the same directory,
//! which is what makes save-if-absent a safe, idempotent no-op.
//!
//! Every write (the JSON record, or a media file) is made durable by
//! writing to a temp file in the same directory and renaming it into
//! place, so a reader can never observe a partially written file.

use std::fmt;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// The three top-level categories of archived item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Category {
    Post,
    Like,
    Bookmark,
}

impl Category {
    const ALL: [Category; 3] = [Category::Post, Category::Like, Category::Bookmark];

    fn as_dir(self) -> &'static str {
        match self {
            Category::Post => "posts",
            Category::Like => "likes",
            Category::Bookmark => "bookmarks",
        }
    }
}

impl fmt::Display for Category {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_dir())
    }
}

impl std::str::FromStr for Category {
    type Err = StorageError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "posts" => Ok(Category::Post),
            "likes" => Ok(Category::Like),
            "bookmarks" => Ok(Category::Bookmark),
            other => Err(StorageError::InvalidCategory(other.to_string())),
        }
    }
}

/// What kind of source a watched row points at: a single account (handle or
/// DID) whose authored posts are archived, or an algorithm/custom feed (an
/// `at://` `app.bsky.feed.generator` URI) whose posts are archived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    Account,
    Feed,
}

impl SourceKind {
    /// The canonical string form stored in `watched_sources.source_kind`.
    fn as_str(self) -> &'static str {
        match self {
            SourceKind::Account => "account",
            SourceKind::Feed => "feed",
        }
    }
}

impl std::fmt::Display for SourceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for SourceKind {
    type Err = StorageError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "account" => Ok(SourceKind::Account),
            "feed" => Ok(SourceKind::Feed),
            other => Err(StorageError::InvalidSourceKind(other.to_string())),
        }
    }
}

/// One row of the `watched_sources` table: a UI-managed, live-reloadable
/// account or feed the archiver watches. This table is the single source of
/// truth for what the daemon watches; the in-memory roster
/// ([`crate::watchlist::Watchlist`]) is a live cache of it.
#[derive(Debug, Clone, PartialEq)]
pub struct WatchedSource {
    pub id: i64,
    pub kind: SourceKind,
    /// For accounts: the handle or DID the user entered. For feeds: the
    /// `at://` feed URI.
    pub value: String,
    /// Resolved DID for accounts (always `Some` once resolution has
    /// happened); `None` for feeds, which aren't DID-scoped.
    pub did: Option<String>,
    pub added_at: String,
}

/// Errors produced by the storage layer.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("database error: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unknown category on disk: {0:?}")]
    InvalidCategory(String),
    #[error("unknown watched source kind on disk: {0:?}")]
    InvalidSourceKind(String),
    #[error("post not archived: {0}")]
    NotFound(String),
    #[error("background storage task panicked")]
    TaskJoin,
}

/// Folds `spawn_blocking`'s `JoinError` into `StorageError` so call sites
/// can `?` a single `Result` instead of matching two layers of failure.
fn join_result<T>(
    result: Result<Result<T, StorageError>, tokio::task::JoinError>,
) -> Result<T, StorageError> {
    match result {
        Ok(inner) => inner,
        Err(_) => Err(StorageError::TaskJoin),
    }
}

/// A record as archived: the raw JSON payload plus the metadata the
/// storage layer itself tracks (when it was archived, what media files
/// are attached).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArchivedRecord {
    pub at_uri: String,
    pub cid: String,
    pub indexed_at: String,
    #[serde(default)]
    pub media: Vec<MediaMeta>,
    pub record: serde_json::Value,
    #[serde(default)]
    pub deleted_at: Option<String>,
    /// When the authenticated account saved this item into the category —
    /// the bookmark action time for bookmarks, from the API's
    /// `bookmarkView.createdAt`. `None` for posts/likes (no action time
    /// exists) and for rows archived before the column was added. Mirrored
    /// into the index so the gallery can reproduce Bluesky's own bookmarks
    /// ordering.
    #[serde(default)]
    pub action_at: Option<String>,
    /// The item's position in Bluesky's own list (0 = newest) as of the
    /// last full poll/sweep walk that ranked it. More reliable than
    /// `action_at` for ordering — the live API does not always populate
    /// `bookmarkView.createdAt`, but the list order is exactly what the
    /// Bluesky app displays. `None` until first ranked.
    #[serde(default)]
    pub action_seq: Option<i64>,
}

/// Metadata about one media file attached to an archived record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MediaMeta {
    pub filename: String,
    pub content_type: Option<String>,
    pub size_bytes: u64,
}

/// A lightweight row for post-list views (no full JSON body, no media
/// bytes).
#[derive(Debug, Clone, PartialEq)]
pub struct PostSummary {
    pub category: Category,
    pub at_uri: String,
    pub cid: String,
    pub indexed_at: String,
    pub media_count: u32,
    pub thumbnail_filename: Option<String>,
    pub thumbnail_content_type: Option<String>,
    pub deleted_at: Option<String>,
}

/// How rows are ordered for the gallery view. The media index stores the
/// archive time (`indexed_at`); the record's own `createdAt` is mirrored
/// into the posts table (`record_created_at`) so the original post/edit
/// time can also drive ordering, and the bookmark action time is mirrored
/// into `action_at` so the gallery can reproduce Bluesky's own bookmarks
/// ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MediaSort {
    #[default]
    NewestArchived,
    OldestArchived,
    NewestCreated,
    OldestCreated,
    NewestAction,
    OldestAction,
}

impl MediaSort {
    /// SQL used as the ordering key (column/tiebreak pair inline) in
    /// `list_media`, where `media` and `posts` are the table aliases in
    /// scope. `record_created_at` falls back to the media's archive time
    /// for rows whose record carried no `createdAt`; the action sorts key
    /// off `action_seq` (the item's position in Bluesky's own list — what
    /// the Bluesky app displays), falling back to the post's archive time
    /// for rows not yet ranked.
    fn sql(self) -> &'static str {
        match self {
            MediaSort::NewestArchived => "media.indexed_at DESC, media.id DESC",
            MediaSort::OldestArchived => "media.indexed_at ASC, media.id ASC",
            // Joined through the posts table; NULL falls back to the media's
            // archive time.
            MediaSort::NewestCreated => {
                "COALESCE(record_created_at, media.indexed_at) DESC, media.id DESC"
            }
            MediaSort::OldestCreated => {
                "COALESCE(record_created_at, media.indexed_at) ASC, media.id ASC"
            }
            // Ranked rows first (`action_seq` 0 = newest in Bluesky's
            // list), then rows the walk hasn't ranked yet, in archive
            // order. `posts.indexed_at` is the archive-walk order, which
            // mirrors the API's list order.
            MediaSort::NewestAction => concat!(
                "(posts.action_seq IS NULL) ASC, posts.action_seq ASC, ",
                "posts.indexed_at DESC, media.id DESC"
            ),
            MediaSort::OldestAction => concat!(
                "(posts.action_seq IS NULL) ASC, posts.action_seq DESC, ",
                "posts.indexed_at ASC, media.id ASC"
            ),
        }
    }
}

/// A row for the gallery view: one media file plus a pointer back to its
/// post.
#[derive(Debug, Clone, PartialEq)]
pub struct MediaSummary {
    pub category: Category,
    pub post_at_uri: String,
    pub filename: String,
    pub content_type: Option<String>,
    pub size_bytes: u64,
    pub indexed_at: String,
    /// The record's own `createdAt` (RFC 3339) when it carried one; `None`
    /// otherwise (e.g. pre-backfill rows).
    pub record_created_at: Option<String>,
}

/// The image count and total byte size of an export selection, for the
/// gallery's size estimate / soft warning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ExportEstimate {
    pub image_count: u64,
    pub total_bytes: u64,
}

/// SQL predicate (over a `media` row) selecting exactly the rows the zip
/// export treats as an image.
///
/// `content_type` is nullable and `media::extension_for` falls back to the
/// CDN URL's extension when the MIME type is unknown, so a bare
/// `content_type LIKE 'image/%'` predicate would silently drop real images
/// archived with a null content type. This predicate therefore also admits
/// a null-content-type row whose filename ends in a known image extension,
/// while still excluding `video/*`, `.bin` fallbacks, and unrecognised
/// extensions. The identical predicate backs both [`ArchiveStore::export_estimate`]
/// and [`ArchiveStore::list_export_media`] so the warning can never disagree
/// with what the download actually contains.
const IMAGE_PREDICATE_SQL: &str = "(content_type LIKE 'image/%' \
    OR (content_type IS NULL AND ( \
        lower(filename) LIKE '%.jpg' \
        OR lower(filename) LIKE '%.png' \
        OR lower(filename) LIKE '%.gif' \
        OR lower(filename) LIKE '%.webp')))";

/// The outcome of a save-if-absent call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaveOutcome {
    Inserted,
    AlreadyArchived,
}

/// One page of a paginated listing.
#[derive(Debug, Clone, PartialEq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub page: u32,
    pub page_size: u32,
    pub total_items: u64,
    pub total_pages: u32,
}

fn paginate<T>(items: Vec<T>, page: u32, page_size: u32, total_items: u64) -> Page<T> {
    let total_pages = if total_items == 0 {
        0
    } else {
        ((total_items - 1) / page_size as u64 + 1) as u32
    };
    Page {
        items,
        page,
        page_size,
        total_items,
        total_pages,
    }
}

fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .expect("OffsetDateTime::now_utc always formats as RFC3339")
}

/// Hex-encoded SHA-256 digest of `at_uri`: the deterministic, filesystem-safe
/// directory id used to dedup and locate an archived item.
fn item_id(at_uri: &str) -> String {
    let digest = Sha256::digest(at_uri.as_bytes());
    hex_encode(&digest)
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(s, "{byte:02x}").expect("writing to a String never fails");
    }
    s
}

fn item_dir(archive_dir: &Path, category: Category, at_uri: &str) -> PathBuf {
    let id = item_id(at_uri);
    let shard = &id[..2];
    archive_dir.join(category.as_dir()).join(shard).join(id)
}

fn record_path(archive_dir: &Path, category: Category, at_uri: &str) -> PathBuf {
    item_dir(archive_dir, category, at_uri).join("record.json")
}

fn media_dir(archive_dir: &Path, category: Category, at_uri: &str) -> PathBuf {
    item_dir(archive_dir, category, at_uri).join("media")
}

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Writes `bytes` to `path` atomically: write to a sibling temp file in
/// the same directory, `fsync` it, then `rename` into place. A reader can
/// never observe a partially written file, and a crash mid-write leaves
/// only an orphaned temp file, never a corrupt `path`.
fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let dir = path
        .parent()
        .expect("record/media paths always have a parent directory");
    std::fs::create_dir_all(dir)?;

    let unique = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp_path = dir.join(format!(
        ".tmp-{}-{}-{unique}",
        std::process::id(),
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    ));

    let write_result = (|| {
        let mut file = std::fs::File::create(&tmp_path)?;
        file.write_all(bytes)?;
        file.sync_all()
    })();

    if let Err(err) = write_result {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(err);
    }

    std::fs::rename(&tmp_path, path)
}

/// Schema version 2 adds the `watched_sources` table (the UI-managed watch
/// list); version 1 had only `posts`/`media`. Both statements are
/// `CREATE TABLE IF NOT EXISTS`, so a version-1 database upgrades in place
/// (the new table is created empty) with no data rows to move.
///
/// Version 3 adds `posts.record_created_at`, mirroring the record's own
/// `createdAt` so the gallery can sort by it. The column is nullable and
/// backfilled from the on-disk `record.json` for rows that predate it.
///
/// Version 4 adds `posts.action_at`, the bookmark action time (from the
/// API's `bookmarkView.createdAt`), so the gallery can sort bookmarks by
/// when they were bookmarked. Nullable; backfilled for bookmarks by the
/// nightly sweeper's full walks.
///
/// Version 5 adds `posts.action_seq`, the item's *position in Bluesky's
/// own list* at the last full poll/sweep walk (0 = newest). The live API
/// does not reliably populate `bookmarkView.createdAt`, and its list order
/// is what the Bluesky app actually displays — so the gallery's
/// "bookmarked"/"liked" sorts key off the captured list position, falling
/// back to archive order for rows not yet ranked.
const SCHEMA_VERSION: i64 = 5;

fn bootstrap_schema(conn: &Connection) -> Result<(), StorageError> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS schema_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS posts (
            category TEXT NOT NULL,
            at_uri TEXT NOT NULL,
            cid TEXT NOT NULL,
            indexed_at TEXT NOT NULL,
            record_path TEXT NOT NULL,
            record_created_at TEXT,
            action_at TEXT,
            action_seq INTEGER,
            PRIMARY KEY (category, at_uri)
        );
        CREATE INDEX IF NOT EXISTS idx_posts_category_indexed_at
            ON posts(category, indexed_at, at_uri);

        CREATE TABLE IF NOT EXISTS media (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL,
            post_at_uri TEXT NOT NULL,
            filename TEXT NOT NULL,
            content_type TEXT,
            size_bytes INTEGER NOT NULL,
            indexed_at TEXT NOT NULL,
            UNIQUE(category, post_at_uri, filename),
            FOREIGN KEY (category, post_at_uri) REFERENCES posts(category, at_uri) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_media_indexed_at ON media(indexed_at, id);
        CREATE INDEX IF NOT EXISTS idx_media_category_indexed_at
            ON media(category, indexed_at, id);

        CREATE TABLE IF NOT EXISTS watched_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_kind TEXT NOT NULL CHECK (source_kind IN ('account', 'feed')),
            source_value TEXT NOT NULL,
            did TEXT,
            added_at TEXT NOT NULL,
            UNIQUE(source_kind, source_value)
        );
        ",
    )?;

    let current_version: Option<i64> = conn
        .query_row(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .and_then(|v| v.parse().ok());

    let previous_version = current_version.unwrap_or(0);

    if previous_version < 2 {
        migrate_v1_to_v2(conn)?;
    }

    if previous_version < 3 {
        migrate_v2_to_v3(conn)?;
    }

    if previous_version < 4 {
        migrate_v3_to_v4(conn)?;
    }

    if previous_version < 5 {
        migrate_v4_to_v5(conn)?;
    }

    if current_version != Some(SCHEMA_VERSION) {
        conn.execute(
            "INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![SCHEMA_VERSION.to_string()],
        )?;
    }

    Ok(())
}

fn migrate_v1_to_v2(conn: &Connection) -> Result<(), StorageError> {
    let has_column: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('posts') WHERE name = 'deleted_at'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|n| n > 0)?;
    if !has_column {
        conn.execute_batch(
            "ALTER TABLE posts ADD COLUMN deleted_at TEXT;
             CREATE INDEX IF NOT EXISTS idx_posts_deleted_at ON posts(deleted_at);",
        )?;
    }
    Ok(())
}

/// Adds the `record_created_at` mirror column to existing `posts` tables
/// (fresh databases already have it from `bootstrap_schema`), then its
/// ordering index. Backfilling values from `record.json` happens in
/// [`ArchiveStore::open`], which owns the filesystem paths the column
/// derives from.
fn migrate_v2_to_v3(conn: &Connection) -> Result<(), StorageError> {
    let has_column: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('posts') WHERE name = 'record_created_at'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|n| n > 0)?;
    if !has_column {
        conn.execute_batch("ALTER TABLE posts ADD COLUMN record_created_at TEXT;")?;
    }
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_posts_category_record_created_at
             ON posts(category, record_created_at, at_uri);",
    )?;
    Ok(())
}

/// Adds the `action_at` mirror column (the bookmark action time) to
/// existing `posts` tables (fresh databases already have it from
/// [`bootstrap_schema`]), then its ordering index.
fn migrate_v3_to_v4(conn: &Connection) -> Result<(), StorageError> {
    let has_column: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('posts') WHERE name = 'action_at'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|n| n > 0)?;
    if !has_column {
        conn.execute_batch("ALTER TABLE posts ADD COLUMN action_at TEXT;")?;
    }
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_posts_category_action_at
             ON posts(category, action_at, at_uri);",
    )?;
    Ok(())
}

/// Adds the `action_seq` column (the item's position in Bluesky's own
/// list) to existing `posts` tables, then its ordering index.
fn migrate_v4_to_v5(conn: &Connection) -> Result<(), StorageError> {
    let has_column: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('posts') WHERE name = 'action_seq'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|n| n > 0)?;
    if !has_column {
        conn.execute_batch("ALTER TABLE posts ADD COLUMN action_seq INTEGER;")?;
    }
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_posts_category_action_seq
             ON posts(category, action_seq, at_uri);",
    )?;
    Ok(())
}

/// Extracts the RFC 3339 timestamp a record self-reports under `createdAt`
/// (posts under every category carry one), for the `record_created_at`
/// mirror used by the gallery's created-time sorting.
fn record_created_at_from(record: &serde_json::Value) -> Option<String> {
    record
        .get("createdAt")
        .and_then(|value| value.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Number of `posts` rows still missing their mirrored `record_created_at`.
fn count_missing_record_created_at(conn: &Connection) -> Result<i64, StorageError> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM posts WHERE record_created_at IS NULL",
        [],
        |row| row.get(0),
    )?)
}

/// Backfills `posts.record_created_at` from each row's on-disk
/// `record.json` (inside its `ArchivedRecord` envelope). Idempotent: only
/// rows with a NULL column are read and updated, so it costs nothing once
/// every row has a value.
fn backfill_record_created_at(conn: &Connection, archive_dir: &Path) -> Result<(), StorageError> {
    if count_missing_record_created_at(conn)? == 0 {
        return Ok(());
    }

    let mut stmt = conn.prepare(
        "SELECT category, at_uri, record_path, indexed_at
             FROM posts WHERE record_created_at IS NULL",
    )?;
    let rows: Vec<(String, String, String)> = stmt
        .query_map([], |row| {
            let category: String = row.get(0)?;
            let at_uri: String = row.get(1)?;
            let record_path: String = row.get(2)?;
            Ok((category, at_uri, record_path))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(stmt);

    let mut updated = 0usize;
    for (category, at_uri, record_path) in rows {
        // Best effort: a missing/corrupt file just leaves the column NULL,
        // mirroring a record with no `createdAt`.
        let Ok(bytes) = std::fs::read(archive_dir.join(&record_path)) else {
            continue;
        };
        let Ok(archived) = serde_json::from_slice::<ArchivedRecord>(&bytes) else {
            continue;
        };
        let Some(created_at) = record_created_at_from(&archived.record) else {
            continue;
        };
        conn.execute(
            "UPDATE posts SET record_created_at = ?1
                 WHERE category = ?2 AND at_uri = ?3",
            params![created_at, category, at_uri],
        )?;
        updated += 1;
    }
    tracing::info!(updated, "backfilled posts.record_created_at");
    Ok(())
}

/// Owns the on-disk archive under `archive_dir` and the SQLite query
/// index that mirrors it. All SQLite access happens on a blocking thread
/// via `tokio::task::spawn_blocking`; nothing here blocks the async
/// runtime.
#[derive(Clone)]
pub struct ArchiveStore {
    archive_dir: PathBuf,
    db: Arc<Mutex<Connection>>,
}

impl ArchiveStore {
    /// Opens (creating if necessary) the archive at `archive_dir` and the
    /// SQLite index at `database_path`, bootstrapping the schema if
    /// needed. Does not scan the filesystem; call [`ArchiveStore::reindex`]
    /// to (re)build the index from what's on disk.
    pub async fn open(archive_dir: PathBuf, database_path: PathBuf) -> Result<Self, StorageError> {
        tokio::fs::create_dir_all(&archive_dir).await?;
        if let Some(parent) = database_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let archive_dir_for_backfill = archive_dir.clone();
        let conn = tokio::task::spawn_blocking(move || -> Result<Connection, StorageError> {
            let conn = Connection::open(&database_path)?;
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            bootstrap_schema(&conn)?;
            // Restore mirrored `createdAt` for rows archived before the
            // column existed. Cheap no-op once backfilled.
            backfill_record_created_at(&conn, &archive_dir_for_backfill)?;
            Ok(conn)
        })
        .await;
        let conn = join_result(conn)?;

        Ok(ArchiveStore {
            archive_dir,
            db: Arc::new(Mutex::new(conn)),
        })
    }

    /// Saves a post/like/bookmark's JSON record if it isn't already
    /// archived (deduped by `at_uri`). Re-saving the same `at_uri` is a
    /// safe no-op: the existing file and index row are left untouched.
    pub async fn save_post(
        &self,
        category: Category,
        at_uri: &str,
        cid: &str,
        record: serde_json::Value,
    ) -> Result<SaveOutcome, StorageError> {
        let archive_dir = self.archive_dir.clone();
        let db = Arc::clone(&self.db);
        let at_uri = at_uri.to_string();
        let cid = cid.to_string();

        let result = tokio::task::spawn_blocking(move || -> Result<SaveOutcome, StorageError> {
            let path = record_path(&archive_dir, category, &at_uri);
            if path.exists() {
                return Ok(SaveOutcome::AlreadyArchived);
            }

            let indexed_at = now_rfc3339();
            let archived = ArchivedRecord {
                at_uri: at_uri.clone(),
                cid: cid.clone(),
                indexed_at: indexed_at.clone(),
                media: Vec::new(),
                record,
                deleted_at: None,
                action_at: None,
                action_seq: None,
            };
            let bytes = serde_json::to_vec_pretty(&archived)?;
            atomic_write(&path, &bytes)?;

            let conn = db.lock().unwrap_or_else(|e| e.into_inner());
            conn.execute(
                "INSERT OR IGNORE INTO posts (at_uri, category, cid, indexed_at, record_path, record_created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    at_uri,
                    category.as_dir(),
                    cid,
                    indexed_at,
                    relative_str(&archive_dir, &path),
                    record_created_at_from(&archived.record),
                ],
            )?;

            Ok(SaveOutcome::Inserted)
        })
        .await;

        join_result(result)
    }

    /// Lists every watched source, oldest-added first, from the
    /// `watched_sources` table that [`ArchiveStore::open`] bootstrapped.
    /// This is the single source of truth the live roster
    /// ([`crate::watchlist::Watchlist`]) and the producers read from.
    pub async fn list_watched_sources(&self) -> Result<Vec<WatchedSource>, StorageError> {
        let db = Arc::clone(&self.db);
        let result =
            tokio::task::spawn_blocking(move || -> Result<Vec<WatchedSource>, StorageError> {
                let conn = db.lock().unwrap_or_else(|e| e.into_inner());
                let mut stmt = conn.prepare(
                    "SELECT id, source_kind, source_value, did, added_at
                 FROM watched_sources
                 ORDER BY added_at, id",
                )?;
                let rows = stmt.query_map([], |row| {
                    let kind: String = row.get(1)?;
                    Ok((
                        row.get::<_, i64>(0)?,
                        kind,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                })?;
                let mut sources = Vec::new();
                for row in rows {
                    let (id, kind, value, did, added_at) = row?;
                    sources.push(WatchedSource {
                        id,
                        kind: kind.parse()?,
                        value,
                        did,
                        added_at,
                    });
                }
                Ok(sources)
            })
            .await;
        join_result(result)
    }

    /// Adds (or re-adds) a watched source. Idempotent: adding a source that
    /// already exists under the same `(kind, value)` is a no-op upsert that
    /// refreshes the resolved `did` (in case the handle now resolves
    /// differently) but never resets `added_at`. Returns the row's id in
    /// both cases.
    pub async fn add_watched_source(
        &self,
        kind: SourceKind,
        value: &str,
        did: Option<&str>,
    ) -> Result<i64, StorageError> {
        let db = Arc::clone(&self.db);
        let kind = kind.as_str().to_string();
        let value = value.to_string();
        let did = did.map(str::to_string);
        let added_at = now_rfc3339();

        let result = tokio::task::spawn_blocking(move || -> Result<i64, StorageError> {
            let conn = db.lock().unwrap_or_else(|e| e.into_inner());
            conn.execute(
                "INSERT INTO watched_sources (source_kind, source_value, did, added_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(source_kind, source_value) DO UPDATE SET
                     did = COALESCE(excluded.did, watched_sources.did)",
                params![kind, value, did, added_at],
            )?;
            let id = conn.query_row(
                "SELECT id FROM watched_sources WHERE source_kind = ?1 AND source_value = ?2",
                params![kind, value],
                |row| row.get(0),
            )?;
            Ok(id)
        })
        .await;
        join_result(result)
    }

    /// Removes a watched source by row id. Returns whether a row was
    /// actually deleted (removing an already-absent id is a no-op `false`).
    pub async fn remove_watched_source(&self, id: i64) -> Result<bool, StorageError> {
        let db = Arc::clone(&self.db);
        let result = tokio::task::spawn_blocking(move || -> Result<bool, StorageError> {
            let conn = db.lock().unwrap_or_else(|e| e.into_inner());
            let affected =
                conn.execute("DELETE FROM watched_sources WHERE id = ?1", params![id])?;
            Ok(affected > 0)
        })
        .await;
        join_result(result)
    }

    /// Saves a media file alongside its post's record, updating the
    /// record's `media` list. Requires the post to already be archived.
    /// Re-saving the same filename for the same post is a safe no-op.
    pub async fn save_media(
        &self,
        category: Category,
        at_uri: &str,
        filename: &str,
        content_type: Option<String>,
        bytes: Vec<u8>,
    ) -> Result<(), StorageError> {
        let archive_dir = self.archive_dir.clone();
        let db = Arc::clone(&self.db);
        let at_uri = at_uri.to_string();
        let filename = filename.to_string();

        let result = tokio::task::spawn_blocking(move || -> Result<(), StorageError> {
            let record_file = record_path(&archive_dir, category, &at_uri);
            if !record_file.exists() {
                return Err(StorageError::NotFound(at_uri.clone()));
            }

            let media_path = media_dir(&archive_dir, category, &at_uri).join(&filename);
            let size_bytes = bytes.len() as u64;
            if !media_path.exists() {
                atomic_write(&media_path, &bytes)?;
            }

            let existing = std::fs::read(&record_file)?;
            let mut archived: ArchivedRecord = serde_json::from_slice(&existing)?;
            if !archived.media.iter().any(|m| m.filename == filename) {
                archived.media.push(MediaMeta {
                    filename: filename.clone(),
                    content_type: content_type.clone(),
                    size_bytes,
                });
                let updated = serde_json::to_vec_pretty(&archived)?;
                atomic_write(&record_file, &updated)?;
            }

            let indexed_at = now_rfc3339();
            let conn = db.lock().unwrap_or_else(|e| e.into_inner());
            conn.execute(
                "INSERT OR IGNORE INTO media
                    (post_at_uri, category, filename, content_type, size_bytes, indexed_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    at_uri,
                    category.as_dir(),
                    filename,
                    content_type,
                    size_bytes as i64,
                    indexed_at
                ],
            )?;

            Ok(())
        })
        .await;

        join_result(result)
    }

    /// Whether `at_uri` is already archived under `category`. Checked
    /// directly against disk (the source of truth), so this is correct
    /// even if the index is stale or missing.
    pub async fn is_archived(
        &self,
        category: Category,
        at_uri: &str,
    ) -> Result<bool, StorageError> {
        let archive_dir = self.archive_dir.clone();
        let at_uri = at_uri.to_string();
        let result = tokio::task::spawn_blocking(move || {
            Ok(record_path(&archive_dir, category, &at_uri).exists())
        })
        .await;
        join_result(result)
    }

    /// Fetches a single archived record by category + `at_uri`, reading
    /// straight from disk. Enriches the result with `deleted_at` from the
    /// index, since that status is index-only metadata.
    pub async fn get_post(
        &self,
        category: Category,
        at_uri: &str,
    ) -> Result<Option<ArchivedRecord>, StorageError> {
        let archive_dir = self.archive_dir.clone();
        let db = Arc::clone(&self.db);
        let at_uri = at_uri.to_string();
        let result = tokio::task::spawn_blocking(move || -> Result<_, StorageError> {
            let path = record_path(&archive_dir, category, &at_uri);
            if !path.exists() {
                return Ok(None);
            }
            let bytes = std::fs::read(&path)?;
            let mut record: ArchivedRecord = serde_json::from_slice(&bytes)?;

            let conn = db.lock().unwrap_or_else(|e| e.into_inner());
            let deleted_at: Option<String> = conn
                .query_row(
                    "SELECT deleted_at FROM posts WHERE category = ?1 AND at_uri = ?2",
                    params![category.as_dir(), &at_uri],
                    |row| row.get(0),
                )
                .optional()?
                .flatten();
            record.deleted_at = deleted_at;

            Ok(Some(record))
        })
        .await;
        join_result(result)
    }

    /// Lists posts (optionally filtered by category) newest-first, via
    /// the SQLite index.
    pub async fn list_posts(
        &self,
        category: Option<Category>,
        page: u32,
        page_size: u32,
    ) -> Result<Page<PostSummary>, StorageError> {
        let db = Arc::clone(&self.db);
        let page = page.max(1);
        let page_size = page_size.max(1);

        let result =
            tokio::task::spawn_blocking(move || -> Result<Page<PostSummary>, StorageError> {
                let conn = db.lock().unwrap_or_else(|e| e.into_inner());
                let category_filter = category.map(|c| c.as_dir().to_string());

                let total_items: u64 = conn.query_row(
                    "SELECT COUNT(*) FROM posts WHERE ?1 IS NULL OR category = ?1",
                    params![category_filter],
                    |row| row.get::<_, i64>(0),
                )? as u64;

                let offset = (page - 1) as i64 * page_size as i64;
                let mut stmt = conn.prepare(
                    "SELECT p.at_uri, p.category, p.cid, p.indexed_at,
                        (SELECT COUNT(*) FROM media m WHERE m.post_at_uri = p.at_uri),
                        (SELECT m.filename FROM media m WHERE m.post_at_uri = p.at_uri
                            ORDER BY m.id ASC LIMIT 1),
                        (SELECT m.content_type FROM media m WHERE m.post_at_uri = p.at_uri
                            ORDER BY m.id ASC LIMIT 1),
                        p.deleted_at
                 FROM posts p
                 WHERE ?1 IS NULL OR p.category = ?1
                 ORDER BY p.indexed_at DESC, p.at_uri DESC
                 LIMIT ?2 OFFSET ?3",
                )?;
                let rows =
                    stmt.query_map(params![category_filter, page_size as i64, offset], |row| {
                        let category: String = row.get(1)?;
                        Ok(PostSummary {
                            at_uri: row.get(0)?,
                            category: category.parse().unwrap_or(Category::Post),
                            cid: row.get(2)?,
                            indexed_at: row.get(3)?,
                            media_count: row.get::<_, i64>(4)? as u32,
                            thumbnail_filename: row.get(5)?,
                            thumbnail_content_type: row.get(6)?,
                            deleted_at: row.get(7)?,
                        })
                    })?;
                let items = rows.collect::<Result<Vec<_>, _>>()?;

                Ok(paginate(items, page, page_size, total_items))
            })
            .await;

        join_result(result)
    }

    /// Lists archived media for the gallery view, filtered by `category`
    /// (`None` lists every category — today's behaviour) and ordered by
    /// `sort` (see [`MediaSort`]). This still returns every media kind
    /// (images *and* video) — only the zip export narrows to images.
    pub async fn list_media(
        &self,
        category: Option<Category>,
        page: u32,
        page_size: u32,
        sort: MediaSort,
    ) -> Result<Page<MediaSummary>, StorageError> {
        let db = Arc::clone(&self.db);
        let page = page.max(1);
        let page_size = page_size.max(1);

        let result =
            tokio::task::spawn_blocking(move || -> Result<Page<MediaSummary>, StorageError> {
                let conn = db.lock().unwrap_or_else(|e| e.into_inner());
                let category_filter = category.map(|c| c.as_dir().to_string());

                let total_items: u64 = conn.query_row(
                    "SELECT COUNT(*) FROM media WHERE ?1 IS NULL OR category = ?1",
                    params![category_filter],
                    |row| row.get::<_, i64>(0),
                )? as u64;

                let offset = (page - 1) as i64 * page_size as i64;
                let sql = format!(
                    "SELECT media.post_at_uri, media.category, media.filename,
                            media.content_type, media.size_bytes, media.indexed_at,
                            posts.record_created_at AS record_created_at
                     FROM media
                     LEFT JOIN posts
                         ON posts.category = media.category
                         AND posts.at_uri = media.post_at_uri
                     WHERE ?1 IS NULL OR media.category = ?1
                     ORDER BY {order}
                     LIMIT ?2 OFFSET ?3
                    ",
                    order = sort.sql()
                );
                let mut stmt = conn.prepare(&sql)?;
                let rows =
                    stmt.query_map(params![category_filter, page_size as i64, offset], |row| {
                        let category: String = row.get(1)?;
                        Ok(MediaSummary {
                            post_at_uri: row.get(0)?,
                            category: category.parse().unwrap_or(Category::Post),
                            filename: row.get(2)?,
                            content_type: row.get(3)?,
                            size_bytes: row.get::<_, i64>(4)? as u64,
                            indexed_at: row.get(5)?,
                            record_created_at: row.get(6)?,
                        })
                    })?;
                let items = rows.collect::<Result<Vec<_>, _>>()?;

                Ok(paginate(items, page, page_size, total_items))
            })
            .await;

        join_result(result)
    }

    /// The image count and total byte size of an export selection
    /// (optionally filtered by `category`), from a single aggregate query
    /// using [`IMAGE_PREDICATE_SQL`]. Reads `size_bytes` straight from the
    /// index, so it needs no filesystem access.
    pub async fn export_estimate(
        &self,
        category: Option<Category>,
    ) -> Result<ExportEstimate, StorageError> {
        let db = Arc::clone(&self.db);

        let result =
            tokio::task::spawn_blocking(move || -> Result<ExportEstimate, StorageError> {
                let conn = db.lock().unwrap_or_else(|e| e.into_inner());
                let category_filter = category.map(|c| c.as_dir().to_string());

                let sql = format!(
                    "SELECT COUNT(*), COALESCE(SUM(size_bytes), 0) FROM media
                 WHERE (?1 IS NULL OR category = ?1) AND {IMAGE_PREDICATE_SQL}"
                );
                let (image_count, total_bytes) =
                    conn.query_row(&sql, params![category_filter], |row| {
                        Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64))
                    })?;

                Ok(ExportEstimate {
                    image_count,
                    total_bytes,
                })
            })
            .await;

        join_result(result)
    }

    /// Lists every image row (optionally filtered by `category`) the zip
    /// export should contain, newest-first, using the same
    /// [`IMAGE_PREDICATE_SQL`] as [`ArchiveStore::export_estimate`]. Unpaged:
    /// the export is a single archive of the whole selection.
    pub async fn list_export_media(
        &self,
        category: Option<Category>,
    ) -> Result<Vec<MediaSummary>, StorageError> {
        let db = Arc::clone(&self.db);

        let result =
            tokio::task::spawn_blocking(move || -> Result<Vec<MediaSummary>, StorageError> {
                let conn = db.lock().unwrap_or_else(|e| e.into_inner());
                let category_filter = category.map(|c| c.as_dir().to_string());

                let sql = format!(
                    "SELECT post_at_uri, category, filename, content_type, size_bytes, indexed_at
                 FROM media
                 WHERE (?1 IS NULL OR category = ?1) AND {IMAGE_PREDICATE_SQL}
                 ORDER BY indexed_at DESC, id DESC"
                );
                let mut stmt = conn.prepare(&sql)?;
                let rows = stmt.query_map(params![category_filter], |row| {
                    let category: String = row.get(1)?;
                    Ok(MediaSummary {
                        post_at_uri: row.get(0)?,
                        category: category.parse().unwrap_or(Category::Post),
                        filename: row.get(2)?,
                        content_type: row.get(3)?,
                        size_bytes: row.get::<_, i64>(4)? as u64,
                        indexed_at: row.get(5)?,
                        // The export is a byte stream; the created-time
                        // mirror isn't needed by the zipped layout, so no
                        // posts join is attempted here.
                        record_created_at: None,
                    })
                })?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(StorageError::from)
            })
            .await;

        join_result(result)
    }

    /// Opens a previously-archived media file for streaming reads, without
    /// loading it into memory (unlike [`ArchiveStore::read_media`], which is
    /// a download-side path). Returns `None` if the file is missing on disk
    /// — the export skips such orphaned rows rather than aborting. Applies
    /// the same bare-filename guard as `read_media`.
    pub async fn open_media(
        &self,
        category: Category,
        at_uri: &str,
        filename: &str,
    ) -> Result<Option<tokio::fs::File>, StorageError> {
        if filename.is_empty()
            || filename.contains('/')
            || filename.contains('\\')
            || filename == ".."
        {
            return Ok(None);
        }

        let path = media_dir(&self.archive_dir, category, at_uri).join(filename);
        match tokio::fs::File::open(&path).await {
            Ok(file) => Ok(Some(file)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(StorageError::Io(err)),
        }
    }

    /// Reads a previously-archived media file's raw bytes straight from
    /// disk. `filename` must be a bare filename (no path separators or
    /// `..` components) — callers pass this through from a URL path
    /// segment, and this guards against escaping the item's media
    /// directory.
    pub async fn read_media(
        &self,
        category: Category,
        at_uri: &str,
        filename: &str,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        if filename.is_empty()
            || filename.contains('/')
            || filename.contains('\\')
            || filename == ".."
        {
            return Ok(None);
        }

        let archive_dir = self.archive_dir.clone();
        let at_uri = at_uri.to_string();
        let filename = filename.to_string();
        let result =
            tokio::task::spawn_blocking(move || -> Result<Option<Vec<u8>>, StorageError> {
                let path = media_dir(&archive_dir, category, &at_uri).join(&filename);
                if !path.is_file() {
                    return Ok(None);
                }
                Ok(Some(std::fs::read(&path)?))
            })
            .await;
        join_result(result)
    }

    /// Rebuilds the entire SQLite index from scratch by scanning
    /// `archive_dir`'s on-disk `record.json` files. After this call,
    /// queries behave identically to an index that was populated
    /// incrementally via [`ArchiveStore::save_post`] /
    /// [`ArchiveStore::save_media`] for the same on-disk data.
    pub async fn reindex(&self) -> Result<(), StorageError> {
        let archive_dir = self.archive_dir.clone();
        let db = Arc::clone(&self.db);

        let result = tokio::task::spawn_blocking(move || -> Result<(), StorageError> {
            let mut found_posts = Vec::new();
            let mut found_media = Vec::new();

            for category in Category::ALL {
                let category_dir = archive_dir.join(category.as_dir());
                if !category_dir.is_dir() {
                    continue;
                }
                for shard_entry in std::fs::read_dir(&category_dir)? {
                    let shard_path = shard_entry?.path();
                    if !shard_path.is_dir() {
                        continue;
                    }
                    for item_entry in std::fs::read_dir(&shard_path)? {
                        let item_path = item_entry?.path();
                        let record_file = item_path.join("record.json");
                        if !record_file.is_file() {
                            continue;
                        }
                        let bytes = std::fs::read(&record_file)?;
                        let archived: ArchivedRecord = serde_json::from_slice(&bytes)?;

                        for media in &archived.media {
                            found_media.push((
                                archived.at_uri.clone(),
                                category,
                                media.filename.clone(),
                                media.content_type.clone(),
                                media.size_bytes,
                                archived.indexed_at.clone(),
                            ));
                        }

                        found_posts.push((
                            archived.at_uri.clone(),
                            category,
                            archived.cid.clone(),
                            archived.indexed_at.clone(),
                            relative_str(&archive_dir, &record_file),
                            record_created_at_from(&archived.record),
                            archived.action_at.clone(),
                            archived.action_seq,
                        ));
                    }
                }
            }

            let mut conn = db.lock().unwrap_or_else(|e| e.into_inner());

            // Preserve deleted_at status before rebuilding the index.
            let mut deleted_uris = std::collections::HashSet::new();
            if let Ok(mut stmt) =
                conn.prepare("SELECT at_uri FROM posts WHERE deleted_at IS NOT NULL")
            {
                let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
                for uri in rows.flatten() {
                    deleted_uris.insert(uri);
                }
            }

            let tx = conn.transaction()?;
            tx.execute("DELETE FROM media", [])?;
            tx.execute("DELETE FROM posts", [])?;
            for (at_uri, category, cid, indexed_at, record_path, record_created_at, action_at, action_seq) in
                found_posts
            {
                let deleted_at = if deleted_uris.contains(&at_uri) {
                    Some(indexed_at.clone())
                } else {
                    None
                };
                tx.execute(
                    "INSERT INTO posts (at_uri, category, cid, indexed_at, record_path, deleted_at, record_created_at, action_at, action_seq)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        at_uri,
                        category.as_dir(),
                        cid,
                        indexed_at,
                        record_path,
                        deleted_at,
                        record_created_at,
                        action_at,
                        action_seq
                    ],
                )?;
            }
            for (post_at_uri, category, filename, content_type, size_bytes, indexed_at) in
                found_media
            {
                tx.execute(
                    "INSERT INTO media
                        (post_at_uri, category, filename, content_type, size_bytes, indexed_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        post_at_uri,
                        category.as_dir(),
                        filename,
                        content_type,
                        size_bytes as i64,
                        indexed_at
                    ],
                )?;
            }
            tx.commit()?;

            Ok(())
        })
        .await;

        join_result(result)
    }

    /// Total bytes of media archived under `category`, from
    /// `SUM(media.size_bytes)` in the index. This is the quantity the
    /// per-feed size cap is enforced against (JSON records are not counted).
    pub async fn category_media_bytes(&self, category: &Category) -> Result<u64, StorageError> {
        let db = Arc::clone(&self.db);
        let category = category.as_dir().to_string();
        let result = tokio::task::spawn_blocking(move || -> Result<u64, StorageError> {
            let conn = db.lock().unwrap_or_else(|e| e.into_inner());
            let total: i64 = conn.query_row(
                "SELECT COALESCE(SUM(size_bytes), 0) FROM media WHERE category = ?1",
                params![category],
                |row| row.get(0),
            )?;
            Ok(total.max(0) as u64)
        })
        .await;
        join_result(result)
    }

    /// Every category `at_uri` is archived under, according to the index
    /// (an `at_uri` can appear under several — e.g. liked *and* surfaced by a
    /// feed). Used by post detail to locate an item without knowing its
    /// category up front, including feed categories no longer in config.
    pub async fn find_categories(&self, at_uri: &str) -> Result<Vec<Category>, StorageError> {
        let db = Arc::clone(&self.db);
        let at_uri = at_uri.to_string();
        let result = tokio::task::spawn_blocking(move || -> Result<Vec<Category>, StorageError> {
            let conn = db.lock().unwrap_or_else(|e| e.into_inner());
            let mut stmt =
                conn.prepare("SELECT category FROM posts WHERE at_uri = ?1 ORDER BY category")?;
            let rows = stmt.query_map(params![at_uri], |row| row.get::<_, String>(0))?;
            let mut categories = Vec::new();
            for row in rows {
                if let Ok(category) = row?.parse::<Category>() {
                    categories.push(category);
                }
            }
            Ok(categories)
        })
        .await;
        join_result(result)
    }

    /// Marks a post as deleted in the index. Idempotent: if already marked,
    /// the earlier timestamp is preserved. Only affects the index; the
    /// on-disk record.json remains untouched. Returns `true` if this call
    /// newly marked the post (i.e. it wasn't already marked).
    pub async fn mark_post_deleted(&self, at_uri: &str) -> Result<bool, StorageError> {
        let db = Arc::clone(&self.db);
        let at_uri = at_uri.to_string();
        let result = tokio::task::spawn_blocking(move || -> Result<bool, StorageError> {
            let deleted_at = now_rfc3339();
            let conn = db.lock().unwrap_or_else(|e| e.into_inner());
            let changed = conn.execute(
                "UPDATE posts SET deleted_at = ?1
                 WHERE at_uri = ?2 AND deleted_at IS NULL",
                params![deleted_at, at_uri],
            )?;
            Ok(changed > 0)
        })
        .await;
        join_result(result)
    }

    /// Lists every archived `at_uri` under `category`, oldest-indexed first.
    /// Used by the nightly sweeper to batch-verify that liked/bookmarked
    /// posts still exist upstream.
    pub async fn list_archived_uris(
        &self,
        category: Category,
    ) -> Result<Vec<String>, StorageError> {
        let db = Arc::clone(&self.db);
        let result = tokio::task::spawn_blocking(move || -> Result<Vec<String>, StorageError> {
            let conn = db.lock().unwrap_or_else(|e| e.into_inner());
            let mut stmt =
                conn.prepare("SELECT at_uri FROM posts WHERE category = ?1 ORDER BY indexed_at")?;
            let rows = stmt.query_map(params![category.as_dir()], |row| row.get::<_, String>(0))?;
            let mut uris = Vec::new();
            for row in rows {
                uris.push(row?);
            }
            Ok(uris)
        })
        .await;
        join_result(result)
    }

    /// Records when the authenticated account saved this item into the
    /// category (`bookmarkView.createdAt` — the bookmark action time the
    /// Bluesky UI itself orders by). Only writes rows that don't have the
    /// value yet, and mirrors it into the on-disk `record.json` envelope so
    /// [`ArchiveStore::reindex`] preserves it. Returns `true` if this call
    /// newly recorded a value.
    ///
    /// A failure here is the caller's problem to log and swallow: an
    /// unrecorded action time only degrades the bookmarked sorting for that
    /// row (it falls back to archive time), never the archive itself.
    pub async fn set_action_at(
        &self,
        category: Category,
        at_uri: &str,
        action_at: &str,
    ) -> Result<bool, StorageError> {
        let archive_dir = self.archive_dir.clone();
        let db = Arc::clone(&self.db);
        let at_uri = at_uri.to_string();
        let category_dir = category.as_dir().to_string();
        let action_at = action_at.to_string();

        let result = tokio::task::spawn_blocking(move || -> Result<bool, StorageError> {
            let conn = db.lock().unwrap_or_else(|e| e.into_inner());
            let changed = conn.execute(
                "UPDATE posts SET action_at = ?1
                 WHERE category = ?2 AND at_uri = ?3 AND action_at IS NULL",
                params![action_at, category_dir, at_uri],
            )? > 0;

            if changed {
                // Mirror into the envelope so a reindex (which rebuilds the
                // index from disk) keeps the value. Best effort: an
                // unreadable record just leaves the envelope without it.
                let record_path: Option<String> = conn
                    .query_row(
                        "SELECT record_path FROM posts
                         WHERE category = ?1 AND at_uri = ?2",
                        params![category_dir, at_uri],
                        |row| row.get(0),
                    )
                    .ok();
                if let Some(record_path) = record_path {
                    let path = archive_dir.join(&record_path);
                    if let Ok(bytes) = std::fs::read(&path)
                        && let Ok(mut archived) = serde_json::from_slice::<ArchivedRecord>(&bytes)
                        && archived.action_at.is_none()
                    {
                        archived.action_at = Some(action_at);
                        match serde_json::to_vec_pretty(&archived) {
                            Ok(out) => {
                                if let Err(err) = atomic_write(&path, &out) {
                                    tracing::warn!(
                                        at_uri = %at_uri,
                                        error = %err,
                                        "failed to persist action_at into record.json"
                                    );
                                }
                            }
                            Err(err) => tracing::warn!(
                                at_uri = %at_uri,
                                error = %err,
                                "failed to serialize record.json for action_at"
                            ),
                        }
                    }
                }
            }

            Ok(changed)
        })
        .await;
        join_result(result)
    }

    /// Records the item's position in Bluesky's own list (0 = newest) as
    /// observed by the poller/sweeper walk that visited it. Unlike
    /// [`ArchiveStore::set_action_at`] this OVERWRITES any previous value:
    /// every item older than a new bookmark shifts position, so each walk
    /// (the nightly sweeper's full walk especially) re-ranks what it sees.
    /// Mirrored into the on-disk `record.json` envelope so
    /// [`ArchiveStore::reindex`] preserves it.
    ///
    /// A failure here is the caller's problem to log and swallow: an
    /// unrecorded rank only degrades the bookmarked/liked sorting for that
    /// row (it falls back to archive order), never the archive itself.
    pub async fn set_action_seq(
        &self,
        category: Category,
        at_uri: &str,
        seq: i64,
    ) -> Result<(), StorageError> {
        let archive_dir = self.archive_dir.clone();
        let db = Arc::clone(&self.db);
        let at_uri = at_uri.to_string();
        let category_dir = category.as_dir().to_string();

        let result = tokio::task::spawn_blocking(move || -> Result<(), StorageError> {
            let conn = db.lock().unwrap_or_else(|e| e.into_inner());
            let changed = conn.execute(
                "UPDATE posts SET action_seq = ?1
                     WHERE category = ?2 AND at_uri = ?3",
                params![seq, category_dir, at_uri],
            )? > 0;

            if changed {
                // Mirror into the envelope so a reindex keeps the value.
                // Best effort, same as `set_action_at`.
                let record_path: Option<String> = conn
                    .query_row(
                        "SELECT record_path FROM posts
                         WHERE category = ?1 AND at_uri = ?2",
                        params![category_dir, at_uri],
                        |row| row.get(0),
                    )
                    .ok();
                drop(conn);
                if let Some(record_path) = record_path {
                    let path = archive_dir.join(&record_path);
                    if let Ok(bytes) = std::fs::read(&path)
                        && let Ok(mut archived) = serde_json::from_slice::<ArchivedRecord>(&bytes)
                        && archived.action_seq != Some(seq)
                    {
                        archived.action_seq = Some(seq);
                        match serde_json::to_vec_pretty(&archived) {
                            Ok(out) => {
                                if let Err(err) = atomic_write(&path, &out) {
                                    tracing::warn!(
                                        at_uri = %at_uri,
                                        error = %err,
                                        "failed to persist action_seq into record.json"
                                    );
                                }
                            }
                            Err(err) => tracing::warn!(
                                at_uri = %at_uri,
                                error = %err,
                                "failed to serialize record.json for action_seq"
                            ),
                        }
                    }
                }
            }

            Ok(())
        })
        .await;
        join_result(result)
    }

    /// Reads up to `max_bytes` from the start of one stored media file.
    /// `None` when the post or the file doesn't exist. Used to sniff
    /// stored bytes for pre-fix corruption without loading whole files.
    pub async fn read_media_head(
        &self,
        category: Category,
        at_uri: &str,
        filename: &str,
        max_bytes: usize,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let archive_dir = self.archive_dir.clone();
        let at_uri = at_uri.to_string();
        let filename = filename.to_string();

        let result =
            tokio::task::spawn_blocking(move || -> Result<Option<Vec<u8>>, StorageError> {
                let path = media_dir(&archive_dir, category, &at_uri).join(&filename);
                if !path.is_file() {
                    return Ok(None);
                }
                use std::io::Read;
                let mut file = std::fs::File::open(&path)?;
                let mut head = vec![0u8; max_bytes];
                let read = file.read(&mut head).unwrap_or(0);
                head.truncate(read);
                Ok(Some(head))
            })
            .await;
        join_result(result)
    }

    /// Lists one post's media rows (filename + content type), from the
    /// index. Empty when the post has no media or isn't indexed.
    pub async fn list_post_media(
        &self,
        category: Category,
        at_uri: &str,
    ) -> Result<Vec<(String, Option<String>)>, StorageError> {
        let db = Arc::clone(&self.db);
        let at_uri = at_uri.to_string();
        let category_dir = category.as_dir().to_string();

        let result = tokio::task::spawn_blocking(
            move || -> Result<Vec<(String, Option<String>)>, StorageError> {
                let conn = db.lock().unwrap_or_else(|e| e.into_inner());
                let mut stmt = conn.prepare(
                    "SELECT filename, content_type FROM media
                     WHERE category = ?1 AND post_at_uri = ?2
                     ORDER BY id ASC",
                )?;
                let rows = stmt
                    .query_map(params![category_dir, at_uri], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            },
        )
        .await;
        join_result(result)
    }

    /// Deletes every media file + index row for one post (the record's
    /// JSON stays). Used when stored media is found to be corrupt from a
    /// pre-fix archive version: the caller re-queues the post's media for
    /// download, and the fresh files land on clean rows. Returns how many
    /// media rows were removed.
    pub async fn delete_post_media(
        &self,
        category: Category,
        at_uri: &str,
    ) -> Result<usize, StorageError> {
        let archive_dir = self.archive_dir.clone();
        let db = Arc::clone(&self.db);
        let at_uri = at_uri.to_string();
        let category_dir = category.as_dir().to_string();

        let result = tokio::task::spawn_blocking(move || -> Result<usize, StorageError> {
            let conn = db.lock().unwrap_or_else(|e| e.into_inner());

            let filenames: Vec<String> = conn
                .prepare("SELECT filename FROM media WHERE category = ?1 AND post_at_uri = ?2")?
                .query_map(params![category_dir, at_uri], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?;
            if filenames.is_empty() {
                return Ok(0);
            }

            conn.execute(
                "DELETE FROM media WHERE category = ?1 AND post_at_uri = ?2",
                params![category_dir, at_uri],
            )?;
            drop(conn);

            // Remove the files on disk.
            let media_path = media_dir(&archive_dir, category, &at_uri);
            for filename in &filenames {
                match std::fs::remove_file(media_path.join(filename)) {
                    Ok(()) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => {
                        tracing::warn!(
                            at_uri = %at_uri,
                            filename = %filename,
                            error = %err,
                            "failed to remove corrupt media file"
                        );
                    }
                }
            }

            // And the envelope's media list, so a reindex doesn't
            // resurrect the removed rows.
            let record_file = record_path(&archive_dir, category, &at_uri);
            if record_file.exists()
                && let Ok(bytes) = std::fs::read(&record_file)
                && let Ok(mut archived) = serde_json::from_slice::<ArchivedRecord>(&bytes)
                && !archived.media.is_empty()
            {
                archived.media.clear();
                if let Ok(out) = serde_json::to_vec_pretty(&archived) {
                    atomic_write(&record_file, &out)?;
                }
            }

            Ok(filenames.len())
        })
        .await;
        join_result(result)
    }
}

fn relative_str(base: &Path, path: &Path) -> String {
    path.strip_prefix(base)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

/// Whether a stored media file's first bytes show corruption from a
/// pre-fix archive version. Three signatures exist:
///
/// - a raw HLS playlist saved as the media file itself (`#EXTM3U…`);
/// - an MPEG-TS stream stored under an `.mp4` name/type (the old HLS
///   backup concatenated Bluesky's TS segments without remuxing);
/// - a full HTTP response dump (an older download bug).
///
/// `head` is the first few bytes of the file (see
/// [`ArchiveStore::read_media_head`]).
pub(crate) fn stored_media_looks_corrupt(
    filename: &str,
    content_type: Option<&str>,
    head: &[u8],
) -> bool {
    if head.starts_with(b"#EXTM3U") || head.starts_with(b"HTTP/1.") {
        return true;
    }
    let base = content_type
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if base == "application/vnd.apple.mpegurl" || base == "application/x-mpegurl" {
        return true; // a playlist stored as if it were media
    }
    let looks_ts =
        head.len() >= 3 * 188 && head[0] == 0x47 && head[188] == 0x47 && head[2 * 188] == 0x47;
    if looks_ts && base.starts_with("video/") {
        return true; // TS bytes wearing a video/mp4 label
    }
    if looks_ts && filename.ends_with(".mp4") {
        return true;
    }
    false
}

#[cfg(test)]
mod tests;
