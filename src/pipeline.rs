//! Shared data-acquisition contract between the post/like/bookmark producers
//! and the media downloader consumer.
//!
//! This module defines the boundary only; it contains no network/websocket/
//! HTTP code and no downloader implementation. Four later tickets are built
//! against it independently:
//!
//! - AR-5 (Jetstream firehose consumer) and AR-6 (REST polling fallback)
//!   produce [`CandidatePost`]s for [`PostCategory::Authored`], and also
//!   update/read [`ConnectionHealth`] to hand off between each other.
//! - AR-7 (likes/bookmarks poller) produces [`CandidatePost`]s for
//!   [`PostCategory::Like`] and [`PostCategory::Bookmark`].
//! - AR-8 (media downloader) consumes [`CandidatePost`]s from a
//!   [`CandidatePostReceiver`] and downloads each listed [`MediaRef`].

use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

/// Which of the three watched categories a [`CandidatePost`] belongs to.
///
/// Distinct from [`crate::storage::Category`], which names the on-disk/
/// index category (`Post`/`Like`/`Bookmark`) once an item has actually been
/// archived; `Authored` here becomes `Post` there. Whichever later ticket
/// wires a producer up to [`crate::storage::ArchiveStore`] owns that
/// mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostCategory {
    /// A media post authored by the watched account, seen on the firehose
    /// (AR-5) or via the REST-polling fallback (AR-6).
    Authored,
    /// A post the watched account liked, seen via the likes/bookmarks
    /// poller (AR-7).
    Like,
    /// A post the watched account bookmarked, seen via the likes/bookmarks
    /// poller (AR-7).
    Bookmark,
}

/// A reference to one piece of media (image or video) attached to a
/// candidate post, as declared by the post record — not yet downloaded.
/// AR-8 (media downloader) is the consumer that turns this into bytes on
/// disk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MediaRef {
    /// The CDN URL to fetch the media bytes from.
    pub cdn_url: String,
    /// The mimetype declared by the post record, if any (e.g.
    /// `image/jpeg`). Not authoritative — AR-8 should still validate what
    /// it actually downloads.
    pub declared_mime_type: Option<String>,
    /// The size in bytes declared by the post record, if any. Not
    /// authoritative — AR-8 enforces `MEDIA_MAX_BYTES` against the actual
    /// download regardless of this value.
    pub declared_size_bytes: Option<u64>,
}

/// A post-with-media handed off by a producer (AR-5/6/7) to be archived by
/// the media downloader (AR-8) and, ultimately, [`crate::storage::ArchiveStore`].
///
/// Round-trips through `serde_json` (all fields are plain owned data) so it
/// can be persisted or logged as structured JSON later without a redesign,
/// even though nothing requires that today.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CandidatePost {
    /// The post's AT URI, e.g. `at://did:plc:.../app.bsky.feed.post/abc123`.
    pub at_uri: String,
    /// The post record's CID.
    pub cid: String,
    /// DID of the post's author (the watched account for `Authored`; the
    /// original poster for `Like`/`Bookmark`).
    pub author_did: String,
    /// Which archive category this candidate belongs to.
    pub category: PostCategory,
    /// The full raw post record JSON, exactly as returned by the Bluesky
    /// API/firehose.
    pub record: serde_json::Value,
    /// Media to download and attach, in declaration order.
    pub media: Vec<MediaRef>,
}

/// The sending half of the producer -> media-downloader channel. AR-5/6/7
/// hold a clone of this and send each [`CandidatePost`] they discover.
pub type CandidatePostSender = mpsc::Sender<CandidatePost>;

/// The receiving half of the producer -> media-downloader channel. AR-8
/// owns this and drains it to process candidates.
pub type CandidatePostReceiver = mpsc::Receiver<CandidatePost>;

/// Creates the bounded channel producers (AR-5/6/7) send [`CandidatePost`]s
/// into and the media downloader (AR-8) receives them from. `buffer` is the
/// channel capacity: sends block (applying backpressure to producers) once
/// it's full.
pub fn candidate_post_channel(buffer: usize) -> (CandidatePostSender, CandidatePostReceiver) {
    mpsc::channel(buffer)
}

/// State of the Jetstream firehose connection, as tracked by the firehose
/// consumer (AR-5, the producer) and read by the REST-polling fallback
/// (AR-6, the consumer) to decide whether it should be active. The REST
/// fallback should generally stay idle while `Connected` and become active
/// while `Reconnecting`/`Disabled`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConnectionHealth {
    /// The firehose is connected and delivering events normally.
    Connected,
    /// The firehose connection dropped and AR-5 is attempting to
    /// reconnect; `since` is when that attempt began.
    Reconnecting { since: Instant },
    /// The firehose consumer is not running at all (e.g. disabled by
    /// config), so the REST fallback should assume sole responsibility.
    Disabled,
}

/// The sending half of the firehose connection-health channel. AR-5 owns
/// this and updates it as its connection state changes.
pub type ConnectionHealthSender = watch::Sender<ConnectionHealth>;

/// The receiving half of the firehose connection-health channel. AR-6
/// holds this and watches it to decide whether to poll.
pub type ConnectionHealthReceiver = watch::Receiver<ConnectionHealth>;

/// Creates the `watch` channel used to communicate [`ConnectionHealth`]
/// from the firehose consumer (AR-5) to the REST-polling fallback (AR-6).
pub fn connection_health_channel(
    initial: ConnectionHealth,
) -> (ConnectionHealthSender, ConnectionHealthReceiver) {
    watch::channel(initial)
}

/// Whether a raw post record has an image or video embed worth archiving.
/// This is the shared "is this post archive-worthy" predicate needed
/// identically by the firehose consumer (AR-5), the REST fallback (AR-6),
/// and the likes/bookmarks poller (AR-7): a post only becomes a
/// [`CandidatePost`] if this returns `true`.
///
/// Recognizes direct `app.bsky.embed.images` / `app.bsky.embed.video`
/// embeds, and media wrapped in `app.bsky.embed.recordWithMedia` (a quote
/// post with attached media). Any other, missing, or malformed embed shape
/// returns `false` rather than panicking.
pub fn has_archivable_media(record: &serde_json::Value) -> bool {
    match record.get("embed") {
        Some(embed) => embed_has_media(embed),
        None => false,
    }
}

fn embed_has_media(embed: &serde_json::Value) -> bool {
    let Some(embed_type) = embed.get("$type").and_then(|v| v.as_str()) else {
        return false;
    };

    match embed_type {
        "app.bsky.embed.images" | "app.bsky.embed.images#view" => true,
        "app.bsky.embed.video" | "app.bsky.embed.video#view" => true,
        "app.bsky.embed.recordWithMedia" | "app.bsky.embed.recordWithMedia#view" => {
            embed.get("media").map(embed_has_media).unwrap_or(false)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn image_embed_is_archivable() {
        let record = json!({
            "text": "look at this",
            "embed": {
                "$type": "app.bsky.embed.images",
                "images": [{"alt": "a cat", "image": {"ref": "bafy..."}}]
            }
        });
        assert!(has_archivable_media(&record));
    }

    #[test]
    fn video_embed_is_archivable() {
        let record = json!({
            "text": "watch this",
            "embed": {
                "$type": "app.bsky.embed.video",
                "video": {"ref": "bafy..."}
            }
        });
        assert!(has_archivable_media(&record));
    }

    #[test]
    fn record_with_media_image_is_archivable() {
        let record = json!({
            "text": "quoting with an image",
            "embed": {
                "$type": "app.bsky.embed.recordWithMedia",
                "record": {"record": {"uri": "at://...", "cid": "..."}},
                "media": {
                    "$type": "app.bsky.embed.images",
                    "images": [{"alt": "", "image": {}}]
                }
            }
        });
        assert!(has_archivable_media(&record));
    }

    #[test]
    fn record_with_media_but_no_media_images_is_not_archivable() {
        let record = json!({
            "embed": {
                "$type": "app.bsky.embed.recordWithMedia",
                "record": {"record": {"uri": "at://...", "cid": "..."}}
            }
        });
        assert!(!has_archivable_media(&record));
    }

    #[test]
    fn text_only_post_is_not_archivable() {
        let record = json!({"text": "just words, no embed"});
        assert!(!has_archivable_media(&record));
    }

    #[test]
    fn external_link_embed_is_not_archivable() {
        let record = json!({
            "text": "check this out",
            "embed": {
                "$type": "app.bsky.embed.external",
                "external": {"uri": "https://example.com", "title": "x", "description": ""}
            }
        });
        assert!(!has_archivable_media(&record));
    }

    #[test]
    fn record_only_embed_is_not_archivable() {
        let record = json!({
            "text": "quoting",
            "embed": {
                "$type": "app.bsky.embed.record",
                "record": {"uri": "at://...", "cid": "..."}
            }
        });
        assert!(!has_archivable_media(&record));
    }

    #[test]
    fn missing_type_field_returns_false_not_panic() {
        let record = json!({"embed": {"images": []}});
        assert!(!has_archivable_media(&record));
    }

    #[test]
    fn embed_as_wrong_json_shape_returns_false_not_panic() {
        for malformed in [
            json!({"embed": "not an object"}),
            json!({"embed": ["also", "not", "an", "object"]}),
            json!({"embed": 42}),
            json!({"embed": null}),
            json!({}),
            json!(null),
            json!("not even an object"),
        ] {
            assert!(!has_archivable_media(&malformed), "{malformed:?}");
        }
    }

    #[test]
    fn unknown_embed_type_returns_false_not_panic() {
        let record = json!({"embed": {"$type": "app.bsky.embed.somethingNew"}});
        assert!(!has_archivable_media(&record));
    }

    #[test]
    fn candidate_post_round_trips_through_json() {
        let candidate = CandidatePost {
            at_uri: "at://did:plc:alice/app.bsky.feed.post/1".to_string(),
            cid: "cid-1".to_string(),
            author_did: "did:plc:alice".to_string(),
            category: PostCategory::Authored,
            record: json!({"text": "hello"}),
            media: vec![MediaRef {
                cdn_url: "https://cdn.example.com/img.jpg".to_string(),
                declared_mime_type: Some("image/jpeg".to_string()),
                declared_size_bytes: Some(12345),
            }],
        };

        let serialized = serde_json::to_string(&candidate).expect("serialize");
        let deserialized: CandidatePost = serde_json::from_str(&serialized).expect("deserialize");
        assert_eq!(candidate, deserialized);
    }

    #[tokio::test]
    async fn candidate_post_channel_delivers_in_order() {
        let (tx, mut rx) = candidate_post_channel(4);
        let make = |n: u32| CandidatePost {
            at_uri: format!("at://did:plc:alice/app.bsky.feed.post/{n}"),
            cid: format!("cid-{n}"),
            author_did: "did:plc:alice".to_string(),
            category: PostCategory::Like,
            record: json!({"n": n}),
            media: vec![],
        };

        tx.send(make(1)).await.expect("send 1");
        tx.send(make(2)).await.expect("send 2");
        drop(tx);

        let first = rx.recv().await.expect("recv 1");
        let second = rx.recv().await.expect("recv 2");
        assert_eq!(first.at_uri, "at://did:plc:alice/app.bsky.feed.post/1");
        assert_eq!(second.at_uri, "at://did:plc:alice/app.bsky.feed.post/2");
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn connection_health_watch_reflects_updates() {
        let (tx, mut rx) = connection_health_channel(ConnectionHealth::Disabled);
        assert_eq!(*rx.borrow(), ConnectionHealth::Disabled);

        tx.send(ConnectionHealth::Connected).expect("send");
        rx.changed().await.expect("changed");
        assert_eq!(*rx.borrow(), ConnectionHealth::Connected);

        let since = Instant::now();
        tx.send(ConnectionHealth::Reconnecting { since })
            .expect("send");
        rx.changed().await.expect("changed");
        assert_eq!(*rx.borrow(), ConnectionHealth::Reconnecting { since });
    }
}
