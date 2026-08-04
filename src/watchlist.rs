//! The live, in-memory roster of watched sources.
//!
//! [`crate::storage`]'s `watched_sources` table is the single source of
//! truth; [`Watchlist`] is the in-process live cache that hands every
//! producer (the firehose consumer, the REST-fallback poller, and the feed
//! poller) the current set without making them re-query SQLite each tick.
//!
//! The roster is carried on a `tokio::sync::watch` channel whose value is
//! the full `Vec<crate::storage::WatchedSource>`. Producers subscribe with
//! [`Watchlist::subscribe`] and both read the current value and wake up on
//! reloads (a UI add/remove replaces the value via [`Watchlist::reload`]).
//! Because the value itself is the whole roster, a reload is atomic from a
//! consumer's point of view: they either see the old set or the new one,
//! never a partial write.

use tokio::sync::watch;

use crate::storage::{ArchiveStore, SourceKind, StorageError, WatchedSource};

/// A live snapshot of every row in `watched_sources`, shared with the
/// producers via a `watch` channel owned by [`crate::state::AppState`].
#[derive(Clone)]
pub struct Watchlist {
    tx: watch::Sender<Vec<WatchedSource>>,
    rx: watch::Receiver<Vec<WatchedSource>>,
}

impl Watchlist {
    /// Builds a roster seeded with `sources` (usually freshly read from the
    /// database at startup).
    pub fn new(sources: Vec<WatchedSource>) -> Self {
        let (tx, rx) = watch::channel(sources);
        Watchlist { tx, rx }
    }

    /// Atomically replaces the roster. Called after any
    /// `watched_sources` write (add/remove) so producers observe the change
    /// immediately; the database remains the durable source of truth.
    pub fn reload(&self, sources: Vec<WatchedSource>) {
        let _ = self.tx.send_replace(sources);
    }

    /// Reloads the roster from the database in one step. This is the only
    /// path callers should use after a write, since it guarantees the cache
    /// and the durable table can never disagree.
    pub async fn reload_from_store(&self, store: &ArchiveStore) -> Result<(), StorageError> {
        let sources = store.list_watched_sources().await?;
        self.reload(sources);
        Ok(())
    }

    /// Current roster, cheapest snapshot style.
    pub fn snapshot(&self) -> Vec<WatchedSource> {
        self.rx.borrow().clone()
    }

    /// The account sources' values (what the firehose and the REST-fallback
    /// poller operate on).
    pub fn account_values(&self) -> Vec<String> {
        self.snapshot()
            .into_iter()
            .filter(|s| s.kind == SourceKind::Account)
            .map(|s| s.value)
            .collect()
    }

    /// The feed sources' values (`at://` feed URIs the feed poller fetches).
    pub fn feed_values(&self) -> Vec<String> {
        self.snapshot()
            .into_iter()
            .filter(|s| s.kind == SourceKind::Feed)
            .map(|s| s.value)
            .collect()
    }

    /// A receiver that observes the current roster and every subsequent
    /// [`Watchlist::reload`], so a producer can both read the latest set and
    /// be woken up when it changes.
    pub fn subscribe(&self) -> watch::Receiver<Vec<WatchedSource>> {
        self.rx.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{ArchiveStore, SourceKind as Kind};

    fn source(id: i64, kind: Kind, value: &str) -> WatchedSource {
        WatchedSource {
            id,
            kind,
            value: value.to_string(),
            did: None,
            added_at: "2024-01-01T00:00:00Z".to_string(),
        }
    }

    #[tokio::test]
    async fn reload_replaces_the_roster_and_subscribers_observe_it() {
        let watchlist = Watchlist::new(vec![source(1, Kind::Account, "alice.bsky.social")]);
        let mut rx = watchlist.subscribe();

        assert_eq!(rx.borrow()[0].value, "alice.bsky.social");

        watchlist.reload(vec![
            source(1, Kind::Account, "alice.bsky.social"),
            source(
                2,
                Kind::Feed,
                "at://did:plc:alice/app.bsky.feed.generator/x",
            ),
        ]);
        rx.changed().await.expect("roster channel is alive");
        assert_eq!(rx.borrow().len(), 2);
        assert_eq!(watchlist.snapshot().len(), 2);
    }

    #[test]
    fn account_and_feed_values_are_filtered() {
        let watchlist = Watchlist::new(vec![
            source(1, Kind::Account, "alice.bsky.social"),
            source(
                2,
                Kind::Feed,
                "at://did:plc:alice/app.bsky.feed.generator/x",
            ),
            source(3, Kind::Account, "did:plc:bob"),
        ]);

        assert_eq!(
            watchlist.account_values(),
            vec!["alice.bsky.social".to_string(), "did:plc:bob".to_string()]
        );
        assert_eq!(
            watchlist.feed_values(),
            vec!["at://did:plc:alice/app.bsky.feed.generator/x".to_string()]
        );
    }

    #[tokio::test]
    async fn reload_from_store_mirrors_the_database() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            ArchiveStore::open(dir.path().join("archive"), dir.path().join("index.sqlite3"))
                .await
                .unwrap();
        store
            .add_watched_source(Kind::Account, "alice.bsky.social", Some("did:plc:alice"))
            .await
            .unwrap();

        let watchlist = Watchlist::new(Vec::new());
        watchlist.reload_from_store(&store).await.unwrap();

        let sources = watchlist.snapshot();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].value, "alice.bsky.social");
        assert_eq!(sources[0].did.as_deref(), Some("did:plc:alice"));
    }
}
