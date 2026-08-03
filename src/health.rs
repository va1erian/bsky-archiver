//! Minimal per-subsystem health tracking.
//!
//! [`app`](crate::app) updates one [`SubsystemHealth`] entry per supervised
//! background task as it starts, restarts, or keeps running; AR-10 (the web
//! UI) reads the resulting [`HealthSnapshot`] via a [`HealthReceiver`] to
//! serve `/healthz` and the config page. This module only defines the type
//! and the channel — no HTTP endpoint lives here.

use std::collections::BTreeMap;

use serde::Serialize;
use tokio::sync::watch;

/// Coarse status of one subsystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Status {
    /// Running normally.
    Connected,
    /// Not currently working (e.g. restarting after a failure, or waiting
    /// out a backoff), but expected to recover on its own.
    Degraded,
    /// Failed in a way that needs attention (e.g. bad credentials).
    // Not yet constructed: `app`'s generic supervisor can't currently tell
    // a permanent failure apart from a transient one, so every restart is
    // reported as `Degraded`. Reserved for a future refinement and for
    // AR-10 (the web UI), which reads this variant.
    #[allow(dead_code)]
    Error,
}

/// The status of one subsystem plus a short human-readable reason, if any.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SubsystemHealth {
    pub status: Status,
    pub detail: Option<String>,
}

impl SubsystemHealth {
    pub fn connected() -> Self {
        SubsystemHealth {
            status: Status::Connected,
            detail: None,
        }
    }

    pub fn degraded(detail: impl Into<String>) -> Self {
        SubsystemHealth {
            status: Status::Degraded,
            detail: Some(detail.into()),
        }
    }

    #[allow(dead_code)]
    pub fn error(detail: impl Into<String>) -> Self {
        SubsystemHealth {
            status: Status::Error,
            detail: Some(detail.into()),
        }
    }
}

/// Health of every supervised subsystem, as a single snapshot suitable for
/// serving over `/healthz`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HealthSnapshot {
    pub version: String,
    pub firehose: SubsystemHealth,
    pub rest_fallback: SubsystemHealth,
    pub likes_bookmarks: SubsystemHealth,
    pub media_downloader: SubsystemHealth,
    /// One entry per configured custom feed, keyed by the feed's slug. A feed
    /// that errors, 404s, or hits its size cap reports `Degraded` here
    /// (never `Error`, so it never fails a container healthcheck) in complete
    /// isolation from every other feed and subsystem.
    #[serde(default)]
    pub feeds: BTreeMap<String, SubsystemHealth>,
}

impl HealthSnapshot {
    /// Whether any tracked subsystem — the four fixed ones or any feed — is
    /// in the `Error` state, which is what makes `/healthz` return 503.
    pub fn any_error(&self) -> bool {
        let fixed = [
            &self.firehose,
            &self.rest_fallback,
            &self.likes_bookmarks,
            &self.media_downloader,
        ]
        .iter()
        .any(|s| s.status == Status::Error);
        fixed || self.feeds.values().any(|s| s.status == Status::Error)
    }
}

impl Default for HealthSnapshot {
    fn default() -> Self {
        let starting = SubsystemHealth::degraded("starting up");
        HealthSnapshot {
            version: env!("CARGO_PKG_VERSION").to_string(),
            firehose: starting.clone(),
            rest_fallback: starting.clone(),
            likes_bookmarks: starting.clone(),
            media_downloader: starting,
            feeds: BTreeMap::new(),
        }
    }
}

/// The sending half of the health-snapshot channel, held by [`app`](crate::app)'s
/// supervisor loops (one update per subsystem).
pub type HealthSender = watch::Sender<HealthSnapshot>;

/// The receiving half of the health-snapshot channel, held by [`crate::state::AppState`]
/// and read by the web UI (AR-10).
pub type HealthReceiver = watch::Receiver<HealthSnapshot>;

/// Creates the `watch` channel used to publish [`HealthSnapshot`] updates.
pub fn health_channel() -> (HealthSender, HealthReceiver) {
    watch::channel(HealthSnapshot::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_snapshot_starts_degraded() {
        let snapshot = HealthSnapshot::default();
        assert_eq!(snapshot.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(snapshot.firehose.status, Status::Degraded);
        assert_eq!(snapshot.rest_fallback.status, Status::Degraded);
        assert_eq!(snapshot.likes_bookmarks.status, Status::Degraded);
        assert_eq!(snapshot.media_downloader.status, Status::Degraded);
    }

    #[tokio::test]
    async fn send_modify_updates_only_targeted_field() {
        let (tx, mut rx) = health_channel();
        tx.send_modify(|s| s.firehose = SubsystemHealth::connected());
        rx.changed().await.expect("changed");
        let snapshot = rx.borrow().clone();
        assert_eq!(snapshot.firehose.status, Status::Connected);
        assert_eq!(snapshot.rest_fallback.status, Status::Degraded);
    }
}
