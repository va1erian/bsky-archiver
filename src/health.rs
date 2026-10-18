//! Minimal per-subsystem health tracking.
//!
//! [`app`](crate::app) updates one [`SubsystemHealth`] entry per supervised
//! background task as it starts, restarts, or keeps running; AR-10 (the web
//! UI) reads the resulting [`HealthSnapshot`] via a [`HealthReceiver`] to
//! serve `/healthz` and the config page. This module only defines the type
//! and the channel — no HTTP endpoint lives here.

use serde::Serialize;
use std::time::Duration;
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
    /// Intentionally not running (e.g. the Tumblr likes poller when no
    /// Tumblr credentials are configured).
    Disabled,
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

    pub fn disabled() -> Self {
        SubsystemHealth {
            status: Status::Disabled,
            detail: Some("not configured".to_string()),
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
    pub feed_poller: SubsystemHealth,
    pub likes_bookmarks: SubsystemHealth,
    pub tumblr_likes: SubsystemHealth,
    pub nightly_sweep: SubsystemHealth,
    pub media_downloader: SubsystemHealth,
}

impl Default for HealthSnapshot {
    fn default() -> Self {
        let starting = SubsystemHealth::degraded("starting up");
        HealthSnapshot {
            version: env!("CARGO_PKG_VERSION").to_string(),
            firehose: starting.clone(),
            rest_fallback: starting.clone(),
            feed_poller: starting.clone(),
            likes_bookmarks: starting.clone(),
            tumblr_likes: starting.clone(),
            nightly_sweep: starting.clone(),
            media_downloader: starting,
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

/// A cloneable handle one background task uses to report how its *work* is
/// going, as opposed to whether the task is alive (that's the supervisor's
/// job): a poller that keeps looping through rate-limited passes is alive
/// but not doing its job, so it reports [`Status::Degraded`] with a short
/// reason here and [`Status::Connected`] again once a pass succeeds.
#[derive(Clone)]
pub struct SubsystemReporter {
    tx: Option<HealthSender>,
    set: fn(&mut HealthSnapshot, SubsystemHealth),
}

impl SubsystemReporter {
    /// Builds a reporter writing into `set`'s one snapshot field.
    pub fn new(tx: HealthSender, set: fn(&mut HealthSnapshot, SubsystemHealth)) -> Self {
        SubsystemReporter { tx: Some(tx), set }
    }

    /// A reporter that discards updates: tests and callers constructed
    /// without a snapshot still get the same poller API.
    pub fn disabled() -> Self {
        SubsystemReporter {
            tx: None,
            set: |_, _| {},
        }
    }

    pub fn connected(&self) {
        self.update(SubsystemHealth::connected());
    }

    pub fn degraded(&self, detail: impl Into<String>) {
        self.update(SubsystemHealth::degraded(detail));
    }

    fn update(&self, health: SubsystemHealth) {
        if let Some(tx) = &self.tx {
            tx.send_modify(|s| (self.set)(s, health));
        }
    }
}

/// Formats a retry hint the way the dashboard's health detail shows it:
/// whole seconds under a minute, otherwise minutes, otherwise hours plus
/// remaining minutes. Shared by every subsystem's failure detail so the
/// lights all speak the same dialect.
pub(crate) fn fmt_retry_hint(hint: Duration) -> String {
    let secs = hint.as_secs();
    if secs < 60 {
        return format!("{secs}s");
    }
    let mins = secs / 60;
    if mins < 60 {
        return format!("{mins}m");
    }
    format!("{}h{}m", mins / 60, mins % 60)
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
        assert_eq!(snapshot.feed_poller.status, Status::Degraded);
        assert_eq!(snapshot.likes_bookmarks.status, Status::Degraded);
        assert_eq!(snapshot.tumblr_likes.status, Status::Degraded);
        assert_eq!(snapshot.nightly_sweep.status, Status::Degraded);
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
        assert_eq!(snapshot.feed_poller.status, Status::Degraded);
    }

    #[tokio::test]
    async fn reporter_writes_only_its_own_field() {
        let (tx, rx) = health_channel();
        let reporter = SubsystemReporter::new(tx, |s, h| s.likes_bookmarks = h);

        reporter.degraded("rate-limited; retrying in 5m");
        let snapshot = rx.borrow().clone();
        assert_eq!(snapshot.likes_bookmarks.status, Status::Degraded);
        assert_eq!(
            snapshot.likes_bookmarks.detail.as_deref(),
            Some("rate-limited; retrying in 5m")
        );
        assert_eq!(snapshot.rest_fallback.status, Status::Degraded);
        assert_eq!(
            snapshot.rest_fallback.detail.as_deref(),
            Some("starting up")
        );

        reporter.connected();
        let snapshot = rx.borrow().clone();
        assert_eq!(snapshot.likes_bookmarks.status, Status::Connected);
        assert_eq!(snapshot.likes_bookmarks.detail, None);
    }

    #[tokio::test]
    async fn disabled_reporter_discards_updates() {
        let (_tx, rx) = health_channel();
        let reporter = SubsystemReporter::disabled();
        reporter.degraded("ignored");
        reporter.connected();
        assert!(
            !rx.has_changed().expect("sender alive"),
            "no update should have been published"
        );
    }

    #[test]
    fn retry_hint_reads_in_human_units() {
        use std::time::Duration;
        assert_eq!(fmt_retry_hint(Duration::from_secs(45)), "45s");
        assert_eq!(fmt_retry_hint(Duration::from_secs(95)), "1m");
        assert_eq!(fmt_retry_hint(Duration::from_secs(600)), "10m");
        assert_eq!(fmt_retry_hint(Duration::from_secs(7685)), "2h8m");
    }
}
