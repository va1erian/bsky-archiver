//! Shared application state, built once at startup ([`crate::app`]) and
//! handed to the web UI and every background producer, rather than having
//! each consumer re-plumb its own access to config/storage/health.
//!
//! `candidate_weak` is deliberately a [`tokio::sync::mpsc::WeakSender`]: the
//! producers (firehose, pollers) each hold a strong sender themselves, and a
//! strong sender cloned into `AppState` would keep the candidate channel open
//! forever — breaking the graceful-shutdown guarantee that the media
//! downloader drains once every producer has stopped. A weak sender lets the
//! web UI's immediate-backfill task upgrade to a strong sender only while
//! some producer is still alive (which is always the case during normal
//! operation), and degrades to a no-op during/after shutdown.

use std::sync::Arc;

use crate::bluesky::BlueskyClient;
use crate::config::AppConfig;
use crate::health::HealthReceiver;
use crate::pipeline::{CandidatePostSender, WeakCandidatePostSender};
use crate::storage::ArchiveStore;
use crate::watchlist::Watchlist;

/// Everything a request handler or background task needs that isn't
/// specific to one subsystem: the active configuration, the archive store,
/// a live view of subsystem health, the live watch-list roster, the shared
/// Bluesky client, and a weak handle on the producer -> downloader channel.
pub struct AppState {
    pub config: AppConfig,
    pub store: ArchiveStore,
    pub health: HealthReceiver,
    pub watchlist: Watchlist,
    pub bluesky: Arc<BlueskyClient>,
    pub candidate_weak: WeakCandidatePostSender,
}

impl AppState {
    /// Upgrades the shared producer -> downloader channel to a strong sender
    /// so an immediate-backfill task can hand candidates to the media
    /// downloader. Returns `None` when the channel is already closed (every
    /// producer has stopped — e.g. the process is shutting down), in which
    /// case the caller should skip the backfill rather than block.
    pub fn candidates(&self) -> Option<CandidatePostSender> {
        self.candidate_weak.upgrade()
    }
}

/// The shared, cheaply-cloneable handle to [`AppState`] passed around the
/// application (e.g. as `axum::extract::State`).
pub type SharedAppState = Arc<AppState>;
