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
const SCHEMA_VERSION: i64 = 2;

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

    if current_version != Some(SCHEMA_VERSION) {
        conn.execute(
            "INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![SCHEMA_VERSION.to_string()],
        )?;
    }

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

        let conn = tokio::task::spawn_blocking(move || -> Result<Connection, StorageError> {
            let conn = Connection::open(&database_path)?;
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            bootstrap_schema(&conn)?;
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
            };
            let bytes = serde_json::to_vec_pretty(&archived)?;
            atomic_write(&path, &bytes)?;

            let conn = db.lock().unwrap_or_else(|e| e.into_inner());
            conn.execute(
                "INSERT OR IGNORE INTO posts (at_uri, category, cid, indexed_at, record_path)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    at_uri,
                    category.as_dir(),
                    cid,
                    indexed_at,
                    relative_str(&archive_dir, &path)
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
    /// straight from disk.
    pub async fn get_post(
        &self,
        category: Category,
        at_uri: &str,
    ) -> Result<Option<ArchivedRecord>, StorageError> {
        let archive_dir = self.archive_dir.clone();
        let at_uri = at_uri.to_string();
        let result = tokio::task::spawn_blocking(move || -> Result<_, StorageError> {
            let path = record_path(&archive_dir, category, &at_uri);
            if !path.exists() {
                return Ok(None);
            }
            let bytes = std::fs::read(&path)?;
            let record: ArchivedRecord = serde_json::from_slice(&bytes)?;
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
                            ORDER BY m.id ASC LIMIT 1)
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
                        })
                    })?;
                let items = rows.collect::<Result<Vec<_>, _>>()?;

                Ok(paginate(items, page, page_size, total_items))
            })
            .await;

        join_result(result)
    }

    /// Lists archived media newest-first, for the gallery view. `category`
    /// filters to a single category; `None` lists every category (today's
    /// behaviour). This still returns every media kind (images *and* video)
    /// — only the zip export narrows to images.
    pub async fn list_media(
        &self,
        category: Option<Category>,
        page: u32,
        page_size: u32,
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
                let mut stmt = conn.prepare(
                    "SELECT post_at_uri, category, filename, content_type, size_bytes, indexed_at
                 FROM media
                 WHERE ?1 IS NULL OR category = ?1
                 ORDER BY indexed_at DESC, id DESC
                 LIMIT ?2 OFFSET ?3",
                )?;
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
                        ));
                    }
                }
            }

            let mut conn = db.lock().unwrap_or_else(|e| e.into_inner());
            let tx = conn.transaction()?;
            tx.execute("DELETE FROM media", [])?;
            tx.execute("DELETE FROM posts", [])?;
            for (at_uri, category, cid, indexed_at, record_path) in found_posts {
                tx.execute(
                    "INSERT INTO posts (at_uri, category, cid, indexed_at, record_path)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![at_uri, category.as_dir(), cid, indexed_at, record_path],
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
}

fn relative_str(base: &Path, path: &Path) -> String {
    path.strip_prefix(base)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

#[cfg(test)]
mod tests {
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

        let gallery = store.list_media(None, 1, 10).await.unwrap();
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
        let gallery_again = store.list_media(None, 1, 10).await.unwrap();
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
        let before_media = store.list_media(None, 1, 100).await.unwrap();

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
        let after_media = fresh_store.list_media(None, 1, 100).await.unwrap();

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

        let all = store.list_media(None, 1, 100).await.unwrap();
        assert_eq!(all.total_items, 3);

        let likes = store
            .list_media(Some(Category::Like), 1, 100)
            .await
            .unwrap();
        assert_eq!(likes.total_items, 1);
        assert_eq!(likes.items.len(), 1);
        assert!(likes.items.iter().all(|m| m.category == Category::Like));
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
        let store =
            ArchiveStore::open(dir.path().join("archive"), dir.path().join("index.sqlite3"))
                .await
                .expect("open store upgrades v1");

        // The v1 posts row is untouched...
        let page = store.list_posts(None, 1, 10).await.unwrap();
        assert_eq!(page.total_items, 1);
        assert_eq!(page.items[0].at_uri, "at://did:plc:v1/app.bsky.feed.post/1");

        // ...the schema version says 2...
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
        assert_eq!(version, "2");

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
}
