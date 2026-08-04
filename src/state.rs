//! Shared application state, built once at startup ([`crate::app`]) and
//! handed to later tickets — the web UI (AR-10) in particular — rather than
//! having each consumer re-plumb its own access to config/storage/health.

use std::sync::Arc;

use crate::config::{AppConfig, ResolvedFeed};
use crate::health::HealthReceiver;
use crate::storage::ArchiveStore;

/// Everything a request handler or background task needs that isn't
/// specific to one subsystem: the active configuration, the archive store,
/// and a live view of subsystem health.
pub struct AppState {
    pub config: AppConfig,
    pub store: ArchiveStore,
    pub health: HealthReceiver,
    /// The configured custom feeds, resolved to stable slugs + canonical
    /// URIs (and best-effort display names) at startup. Empty when no feeds
    /// are configured. Drives the feed category filters and the `/config`
    /// feed listing.
    pub feeds: Vec<ResolvedFeed>,
}

/// The shared, cheaply-cloneable handle to [`AppState`] passed around the
/// application (e.g. as `axum::extract::State` in AR-10).
pub type SharedAppState = Arc<AppState>;
