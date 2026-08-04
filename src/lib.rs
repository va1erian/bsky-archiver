//! Library crate for `bsky-archiver`: a self-hosted Bluesky watcher/archiver.
//!
//! `src/main.rs` is a thin binary shim over [`app::run`]. Every module is
//! exposed here (rather than just from the binary) so integration tests
//! under `tests/` can exercise the real application in-process — real
//! [`state::AppState`], real [`storage::ArchiveStore`], real
//! [`web::router`] — against mocked Bluesky infrastructure instead of
//! duplicating internals or only testing at the unit level.
//!
//! ## Module map
//!
//! - [`config`] — loads and validates every `BSKY_*`/`UI_*`/`ARCHIVE_*`/
//!   `MEDIA_*` environment variable into one typed [`config::AppConfig`].
//! - [`bluesky`] — the XRPC REST client (session auth, `getAuthorFeed`,
//!   `getActorLikes`, `getBookmarks`, handle resolution).
//! - [`firehose`] — the Jetstream websocket consumer: real-time capture of
//!   the watched account's authored posts with media.
//! - [`poller`] — REST-polling fallback for authored posts (when the
//!   firehose is unavailable) plus the periodic likes/bookmarks poller.
//! - [`pipeline`] — the shared `CandidatePost` channel and
//!   `has_archivable_media` predicate connecting every producer
//!   (firehose/poller) to the one consumer (media downloader).
//! - [`media`] — concurrency-limited, size-capped media downloading.
//! - [`storage`] — the on-disk JSON archive (source of truth) and the
//!   SQLite query index built on top of it.
//! - [`ratelimit`] — the shared backoff/circuit-breaker policy and
//!   process-wide inflight request cap used by the pollers and downloader.
//! - [`health`] — per-subsystem health tracking, read by `/healthz` and the
//!   dashboard.
//! - [`state`] — [`state::AppState`], the shared handle passed to request
//!   handlers and background tasks.
//! - [`web`] — the axum HTTP server: routing, session auth, and the
//!   JSON/query surface backing the UI.
//! - [`templates`] — askama templates and their view models.
//! - [`app`] — startup sequencing (fail fast on bad config/credentials) and
//!   supervised orchestration of every background task plus the web server.

pub mod app;
pub mod bluesky;
pub mod config;
pub mod firehose;
pub mod health;
pub mod media;
pub mod pipeline;
pub mod poller;
pub mod ratelimit;
pub mod state;
pub mod storage;
pub mod templates;
pub mod watchlist;
pub mod web;
