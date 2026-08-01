//! Shared application state, built once at startup ([`crate::app`]) and
//! handed to later tickets — the web UI (AR-10) in particular — rather than
//! having each consumer re-plumb its own access to config/storage/health.

use std::sync::Arc;

use crate::config::AppConfig;
use crate::health::HealthReceiver;
use crate::storage::ArchiveStore;

/// Everything a request handler or background task needs that isn't
/// specific to one subsystem: the active configuration, the archive store,
/// and a live view of subsystem health.
pub struct AppState {
    pub config: AppConfig,
    pub store: ArchiveStore,
    // Not yet read outside tests: AR-10 (the web UI) serves this via
    // `/healthz` and the config page.
    #[allow(dead_code)]
    pub health: HealthReceiver,
}

/// The shared, cheaply-cloneable handle to [`AppState`] passed around the
/// application (e.g. as `axum::extract::State` in AR-10).
pub type SharedAppState = Arc<AppState>;
