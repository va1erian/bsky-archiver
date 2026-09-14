//! Tests for [`crate::telegram`]'s pure logic — key building, record
//! shaping, mime classification, and filename derivation. The MTProto
//! network layer (connect, sign-in, history iteration, chunked downloads)
//! can't be meaningfully unittested without Telegram's datacenters; the
//! module doc carries that verification checklist instead.

use super::*;

/// A recognized attachment is archivable; an empty list is not.
fn has_archivable_media(media: &[MediaDescriptor]) -> bool {
    !media.is_empty()
}

#[test]
fn message_key_is_stable_and_channel_scoped() {
    assert_eq!(message_key("durov", 42), "telegram:durov/42".to_string());
    assert_ne!(message_key("durov", 42), message_key("other", 42));
    assert_ne!(message_key("durov", 42), message_key("durov", 43));
}

#[test]
fn message_record_carries_identity_timestamp_and_media() {
    let media = vec![
        MediaDescriptor {
            kind: MediaKind::Image,
            mime_type: Some("image/jpeg".to_string()),
            declared_size_bytes: None,
        },
        MediaDescriptor {
            kind: MediaKind::Video,
            mime_type: Some("video/mp4".to_string()),
            declared_size_bytes: Some(1_234),
        },
    ];
    let record = message_record("durov", 7, "2026-09-14T12:00:00Z", "caption", &media);

    assert_eq!(record["channel"], "durov");
    assert_eq!(record["messageId"], 7);
    assert_eq!(record["createdAt"], "2026-09-14T12:00:00Z");
    assert_eq!(record["text"], "caption");
    assert_eq!(record["media"][0]["kind"], "image");
    assert_eq!(record["media"][1]["kind"], "video");
    assert_eq!(record["media"][1]["declared_size_bytes"], 1_234);
}

#[test]
fn image_mime_types_classify_as_images_and_videos_as_videos() {
    for image in [
        "image/jpeg",
        "image/png",
        "image/gif",
        "image/webp",
        "image/heic",
        "image/avif",
    ] {
        assert_eq!(mime_kind(image), Some(MediaKind::Image), "{image}");
    }
    for video in [
        "video/mp4",
        "video/quicktime",
        "video/x-matroska",
        "video/webm",
    ] {
        assert_eq!(mime_kind(video), Some(MediaKind::Video), "{video}");
    }
}

#[test]
fn unarchivable_mime_types_and_junk_are_rejected() {
    for skipped in [
        "audio/mpeg",
        "application/pdf",
        "application/octet-stream",
        "application/zip",
        "",
        "text/plain",
        // Not-quite-video types that would be a guess: keep them out.
        "image/heic2",
        "video/mp42",
    ] {
        assert_eq!(mime_kind(skipped), None, "{skipped:?}");
    }
}

#[test]
fn mime_kind_trims_parameters() {
    assert_eq!(
        mime_kind("video/mp4; charset=utf-8"),
        Some(MediaKind::Video)
    );
    assert_eq!(mime_kind("image/jpeg ;"), Some(MediaKind::Image));
}

#[test]
fn media_filename_uses_mime_extension_with_kind_fallback() {
    assert_eq!(
        media_filename(0, Some("image/png"), MediaKind::Image),
        "000.png"
    );
    assert_eq!(
        media_filename(1, Some("video/mp4"), MediaKind::Video),
        "001.mp4"
    );
    // Unknown/unrecognized mime: fall back to the kind's canonical ext.
    assert_eq!(
        media_filename(2, Some("application/octet-stream"), MediaKind::Image),
        "002.jpg"
    );
    assert_eq!(media_filename(3, None, MediaKind::Video), "003.mp4");
}

#[test]
fn media_descriptors_with_no_attachments_mean_not_archivable() {
    assert!(!has_archivable_media(&[]));
    assert!(has_archivable_media(&[MediaDescriptor {
        kind: MediaKind::Image,
        mime_type: Some("image/jpeg".to_string()),
        declared_size_bytes: None,
    }]));
}

#[test]
fn record_json_survives_a_full_round_trip() {
    // The archiver's record.json is written by serde_json straight to
    // disk; a full round trip must preserve every field the UI reads.
    let record = message_record(
        "channel",
        1,
        "2026-01-02T03:04:05Z",
        "text",
        &[MediaDescriptor {
            kind: MediaKind::Video,
            mime_type: Some("video/mp4".to_string()),
            declared_size_bytes: Some(12),
        }],
    );
    let bytes = serde_json::to_vec(&record).unwrap();
    let back: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(back["channel"], "channel");
    assert_eq!(back["messageId"], 1);
    assert_eq!(back["createdAt"], "2026-01-02T03:04:05Z");
    assert_eq!(back["media"][0]["kind"], "video");
    assert_eq!(back["media"][0]["declared_size_bytes"], 12);
}
