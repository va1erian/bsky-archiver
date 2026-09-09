//! Nightly deletion sweep for the likes/bookmarks archive.
//!
//! Likes and bookmarks are never on the firehose, and the REST pollers only
//! ever see the *current* view of those lists: a liked or bookmarked post
//! whose author deleted it (or whose account was deactivated) stops
//! hydrating, and nothing in the steady-state polling path notices. The
//! Jetstream delete-op path (which marks `posts.deleted_at`) only covers
//! watched accounts' own posts. This module closes that gap: once at
//! startup and then once per night, at a fixed local hour
//! ([`NightlySweeper`]), it
//!
//! 1. walks the account's entire bookmark list — every page, no dedup
//!    boundary — archiving anything the periodic poller missed, re-ranking
//!    every item with its current position in Bluesky's list (the order the
//!    Bluesky app displays), re-downloading stored media that carries a
//!    pre-fix corruption signature, and marking bookmarked posts the API
//!    reports as `notFound` (deleted upstream) with a `deleted_at`
//!    timestamp;
//! 2. walks the account's entire like list the same way;
//! 3. batch-verifies (`app.bsky.feed.getPosts`, up to
//!    [`VERIFY_BATCH_SIZE`] URIs per call) every URI already archived under
//!    the like/bookmark categories — catching deletions that happened
//!    while the service was down, or of posts no longer in either list —
//!    and marks the missing ones deleted.
//!
//! Like the firehose delete-op path, marking only touches the index: the
//! on-disk JSON record and its media are kept (the archive is the durable
//! copy), and the UI badges anything with a `deleted_at` timestamp.
//! `blockedPosts` responses are logged but never marked — a blocked post
//! still exists upstream, it is just not viewable by the authenticated
//! account.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use time::OffsetDateTime;
use tracing::{debug, info, warn};

use crate::bluesky::BlueskyClient;
use crate::pipeline::{CandidatePostSender, PostCategory};
use crate::poller::{PAGE_SIZE, PollError, archive_like_bookmark_post};
use crate::storage::{ArchiveStore, Category};

/// How many URIs to verify per `app.bsky.feed.getPosts` call — the
/// endpoint's own maximum.
pub const VERIFY_BATCH_SIZE: usize = 25;

/// How long to wait before retrying a failed sweep. A sweep that errors
/// partway (e.g. a rate-limit window or a transient 5xx) retries until it
/// completes, so one bad night can't push the next full pass a whole day
/// out.
pub const RETRY_DELAY: Duration = Duration::from_secs(15 * 60);

/// Errors from one full sweep pass.
#[derive(Debug, thiserror::Error)]
pub enum SweepError {
    #[error(transparent)]
    Bluesky(#[from] crate::bluesky::BlueskyError),
    #[error(transparent)]
    Storage(#[from] crate::storage::StorageError),
}

impl From<PollError> for SweepError {
    fn from(err: PollError) -> Self {
        match err {
            PollError::Bluesky(err) => SweepError::Bluesky(err),
            PollError::Storage(err) => SweepError::Storage(err),
        }
    }
}

/// The outcome of one full sweep pass, for logging and tests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SweepSummary {
    /// Posts newly archived by the full likes/bookmarks walks.
    pub newly_archived: usize,
    /// Archived posts newly marked deleted (first `deleted_at` write).
    pub newly_marked_deleted: usize,
}

/// Runs the nightly likes/bookmarks deletion sweep forever, at a fixed
/// local hour. Meant to be spawned as a supervised background task.
pub struct NightlySweeper {
    client: Arc<BlueskyClient>,
    store: ArchiveStore,
    sender: CandidatePostSender,
    actor: String,
    /// Local hour of day (0-23, config-validated) the sweep runs at.
    local_hour: u32,
}

impl NightlySweeper {
    pub fn new(
        client: Arc<BlueskyClient>,
        store: ArchiveStore,
        sender: CandidatePostSender,
        actor: String,
        local_hour: u32,
    ) -> Self {
        Self {
            client,
            store,
            sender,
            actor,
            local_hour,
        }
    }

    /// Runs forever: sweep once at startup (catching up on anything the
    /// previous process missed and re-ranking the whole list so the
    /// gallery's bookmarked/liked sorts are correct immediately after a
    /// deploy — not just at the next nightly hour), then sleep until the
    /// next occurrence of the configured local hour and sweep nightly. A
    /// failed sweep retries every [`RETRY_DELAY`] until it completes.
    pub async fn run(&self) {
        loop {
            loop {
                match self.sweep_once().await {
                    Ok(summary) => {
                        info!(
                            newly_archived = summary.newly_archived,
                            newly_marked_deleted = summary.newly_marked_deleted,
                            "likes/bookmarks sweep complete"
                        );
                        break;
                    }
                    Err(err) => {
                        warn!(
                            error = %err,
                            retry_secs = RETRY_DELAY.as_secs(),
                            "likes/bookmarks sweep failed; retrying"
                        );
                        tokio::time::sleep(RETRY_DELAY).await;
                    }
                }
            }

            let delay = duration_until_next_sweep(self.local_hour, local_now());
            info!(
                hour = self.local_hour,
                delay_secs = delay.as_secs(),
                "nightly likes/bookmarks sweep scheduled"
            );
            tokio::time::sleep(delay).await;
        }
    }

    /// One full sweep: walk the bookmark list, walk the like list, then
    /// batch-verify every archived like/bookmark URI. Each step commits as
    /// it goes; an `Ok` return means every step ran to completion.
    pub async fn sweep_once(&self) -> Result<SweepSummary, SweepError> {
        let mut summary = SweepSummary::default();

        let (bookmarks_archived, bookmarks_marked) = self.walk_bookmarks().await?;
        summary.newly_archived += bookmarks_archived;
        summary.newly_marked_deleted += bookmarks_marked;

        summary.newly_archived += self.walk_likes().await?;
        summary.newly_marked_deleted += self.verify_archived().await?;

        Ok(summary)
    }

    /// Walks the account's entire bookmark list (every page — no dedup
    /// boundary), archiving anything the periodic poller missed, re-ranking
    /// every item with its current position in Bluesky's list (0 = newest —
    /// this is the order the Bluesky app displays), and marking bookmarked
    /// posts the API reports as `notFound` deleted.
    async fn walk_bookmarks(&self) -> Result<(usize, usize), SweepError> {
        let mut cursor: Option<String> = None;
        let mut newly_archived = 0usize;
        let mut marked_deleted = 0usize;
        let mut rank: i64 = 0;

        loop {
            let page = self
                .client
                .get_bookmarks(cursor.as_deref(), PAGE_SIZE)
                .await?;
            if page.bookmarks.is_empty() {
                break;
            }

            for entry in &page.bookmarks {
                let position = rank;
                rank += 1;
                let Some(item) = entry.item.as_ref() else {
                    debug!(subject = %entry.subject.uri, "sweep: bookmark item missing; skipping");
                    continue;
                };
                let Some(post) = item.post() else {
                    if item.is_not_found() {
                        if let Some(created_at) = entry.created_at.as_deref() {
                            self.store
                                .set_action_at(Category::Bookmark, &entry.subject.uri, created_at)
                                .await?;
                        }
                        self.store
                            .set_action_seq(Category::Bookmark, &entry.subject.uri, position)
                            .await?;
                        if self.store.mark_post_deleted(&entry.subject.uri).await? {
                            marked_deleted += 1;
                            info!(subject = %entry.subject.uri, "sweep: bookmarked post deleted upstream; marked");
                        }
                    } else {
                        debug!(subject = %entry.subject.uri, "sweep: bookmark not resolvable (blocked?); skipping");
                    }
                    continue;
                };

                let already_archived = archive_like_bookmark_post(
                    &self.store,
                    &self.sender,
                    Category::Bookmark,
                    PostCategory::Bookmark,
                    post,
                    entry.created_at.as_deref(),
                    position,
                )
                .await?;
                if !already_archived {
                    newly_archived += 1;
                }
            }

            match page.cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        Ok((newly_archived, marked_deleted))
    }

    /// Walks the account's entire like list (every page — no dedup
    /// boundary), archiving anything the periodic poller missed and
    /// re-ranking every item with its position in Bluesky's likes list.
    async fn walk_likes(&self) -> Result<usize, SweepError> {
        let mut cursor: Option<String> = None;
        let mut newly_archived = 0usize;
        let mut rank: i64 = 0;

        loop {
            let page = self
                .client
                .get_actor_likes(&self.actor, cursor.as_deref(), PAGE_SIZE)
                .await?;
            if page.feed.is_empty() {
                break;
            }

            for entry in &page.feed {
                let position = rank;
                rank += 1;
                let already_archived = archive_like_bookmark_post(
                    &self.store,
                    &self.sender,
                    Category::Like,
                    PostCategory::Like,
                    &entry.post,
                    None,
                    position,
                )
                .await?;
                if !already_archived {
                    newly_archived += 1;
                }
            }

            match page.cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        Ok(newly_archived)
    }

    /// Batch-verifies every URI archived under the like/bookmark
    /// categories and marks the ones the API reports as not found —
    /// deleted upstream, or the author's account was deactivated — with a
    /// `deleted_at` timestamp. This is what catches deletions of posts no
    /// longer visible in either list (and deletions that happened while
    /// the service was down), including for authors not on the watch list,
    /// whom the firehose delete-op path never sees.
    async fn verify_archived(&self) -> Result<usize, SweepError> {
        let mut uris: Vec<String> = Vec::new();
        let mut seen = HashSet::new();
        for category in [Category::Bookmark, Category::Like] {
            for uri in self.store.list_archived_uris(category).await? {
                if seen.insert(uri.clone()) {
                    uris.push(uri);
                }
            }
        }

        let mut marked_deleted = 0usize;
        for chunk in uris.chunks(VERIFY_BATCH_SIZE) {
            let page = self.client.get_posts(chunk).await?;
            for missing in &page.not_found_posts {
                if self.store.mark_post_deleted(&missing.uri).await? {
                    marked_deleted += 1;
                    info!(subject = %missing.uri, "sweep: archived post deleted upstream; marked");
                }
            }
            for blocked in &page.blocked_posts {
                debug!(subject = %blocked.uri, "sweep: post not viewable (blocked?); left unmarked");
            }
        }

        Ok(marked_deleted)
    }
}

/// The wall-clock time the sweeper treats as "now". Falls back to UTC when
/// the platform can't determine the local offset (rare, multithreaded);
/// containers usually run UTC anyway, which is also why the schedule is
/// configurable rather than hardcoded.
fn local_now() -> OffsetDateTime {
    OffsetDateTime::now_local().unwrap_or_else(|_| OffsetDateTime::now_utc())
}

/// Duration from `now` until the next occurrence of `local_hour`:00 local
/// time. If `now` is exactly on the hour, the next occurrence is tomorrow
/// (the sweep that is about to run already is).
fn duration_until_next_sweep(local_hour: u32, now: OffsetDateTime) -> Duration {
    let hour = u8::try_from(local_hour).expect("config validates the sweep hour is 0-23");
    let sweep_time = time::Time::from_hms(hour, 0, 0).expect("config validates the sweep hour");
    let today = now.replace_time(sweep_time);
    let next = if today > now {
        today
    } else {
        today + time::Duration::days(1)
    };
    (next - now).unsigned_abs()
}

#[cfg(test)]
mod tests;
