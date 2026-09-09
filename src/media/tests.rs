use super::*;
use crate::pipeline::{CandidatePost, candidate_post_channel};
use serde_json::json;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

async fn open_store() -> (tempfile::TempDir, ArchiveStore) {
    let dir = tempfile::tempdir().expect("create temp dir");
    let archive_dir = dir.path().join("archive");
    let database_path = dir.path().join("index.sqlite3");
    let store = ArchiveStore::open(archive_dir, database_path)
        .await
        .expect("open store");
    (dir, store)
}

fn candidate(at_uri: &str, media: Vec<MediaRef>) -> CandidatePost {
    CandidatePost {
        at_uri: at_uri.to_string(),
        cid: "cid-1".to_string(),
        author_did: "did:plc:alice".to_string(),
        category: PostCategory::Authored,
        record: json!({"text": "hello"}),
        media,
    }
}

#[tokio::test]
async fn successful_download_is_stored_alongside_the_post() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/img.jpg"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "image/jpeg")
                .set_body_bytes(b"fake-jpeg-bytes".to_vec()),
        )
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let downloader = MediaDownloader::new(store.clone(), 4, 1_000_000);
    let (tx, mut rx) = candidate_post_channel(4);

    let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
    tx.send(candidate(
        at_uri,
        vec![MediaRef {
            cdn_url: format!("{}/img.jpg", server.uri()),
            declared_mime_type: Some("image/jpeg".to_string()),
            declared_size_bytes: None,
        }],
    ))
    .await
    .unwrap();
    drop(tx);

    downloader.run(&mut rx).await;

    let record = store
        .get_post(Category::Post, at_uri)
        .await
        .unwrap()
        .expect("post should be archived");
    assert_eq!(record.media.len(), 1);
    assert_eq!(record.media[0].filename, "000.jpg");
    assert_eq!(record.media[0].content_type.as_deref(), Some("image/jpeg"));
    assert_eq!(record.media[0].size_bytes, "fake-jpeg-bytes".len() as u64);
}

#[tokio::test]
async fn content_type_mismatch_is_logged_but_not_a_hard_failure() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/img.jpg"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "image/png")
                .set_body_bytes(b"actually-a-png".to_vec()),
        )
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let downloader = MediaDownloader::new(store.clone(), 4, 1_000_000);
    let (tx, mut rx) = candidate_post_channel(4);

    let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
    tx.send(candidate(
        at_uri,
        vec![MediaRef {
            cdn_url: format!("{}/img.jpg", server.uri()),
            declared_mime_type: Some("image/jpeg".to_string()),
            declared_size_bytes: None,
        }],
    ))
    .await
    .unwrap();
    drop(tx);

    downloader.run(&mut rx).await;

    let record = store
        .get_post(Category::Post, at_uri)
        .await
        .unwrap()
        .expect("post should be archived");
    assert_eq!(record.media.len(), 1, "mismatch should not block the save");
    assert_eq!(record.media[0].filename, "000.png");
}

#[tokio::test]
async fn oversized_response_is_aborted_and_leaves_no_partial_file() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/big.jpg"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "image/jpeg")
                .set_body_bytes(vec![0u8; 1024]),
        )
        .mount(&server)
        .await;

    let (dir, store) = open_store().await;
    let downloader = MediaDownloader::new(store.clone(), 4, 100);
    let (tx, mut rx) = candidate_post_channel(4);

    let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
    tx.send(candidate(
        at_uri,
        vec![MediaRef {
            cdn_url: format!("{}/big.jpg", server.uri()),
            declared_mime_type: Some("image/jpeg".to_string()),
            declared_size_bytes: None,
        }],
    ))
    .await
    .unwrap();
    drop(tx);

    downloader.run(&mut rx).await;

    let record = store
        .get_post(Category::Post, at_uri)
        .await
        .unwrap()
        .expect("post record should still be archived");
    assert!(
        record.media.is_empty(),
        "oversized download must not be saved"
    );

    let media_dir = dir.path().join("archive");
    let has_any_media_file = walk_files(&media_dir)
        .into_iter()
        .any(|p| p.file_name().is_some_and(|n| n != "record.json"));
    assert!(
        !has_any_media_file,
        "no partial media file should be left on disk"
    );
}

fn walk_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk_files(&path));
            } else {
                out.push(path);
            }
        }
    }
    out
}

struct FlakyResponder {
    attempts: AtomicUsize,
    fail_times: usize,
}

impl Respond for FlakyResponder {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if attempt < self.fail_times {
            ResponseTemplate::new(503)
        } else {
            ResponseTemplate::new(200)
                .insert_header("content-type", "image/jpeg")
                .set_body_bytes(b"ok-after-retry".to_vec())
        }
    }
}

#[tokio::test]
async fn retries_transient_failures_then_succeeds() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/flaky.jpg"))
        .respond_with(FlakyResponder {
            attempts: AtomicUsize::new(0),
            fail_times: 2,
        })
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let downloader = MediaDownloader::new(store.clone(), 4, 1_000_000);
    let (tx, mut rx) = candidate_post_channel(4);

    let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
    tx.send(candidate(
        at_uri,
        vec![MediaRef {
            cdn_url: format!("{}/flaky.jpg", server.uri()),
            declared_mime_type: Some("image/jpeg".to_string()),
            declared_size_bytes: None,
        }],
    ))
    .await
    .unwrap();
    drop(tx);

    downloader.run(&mut rx).await;

    let record = store
        .get_post(Category::Post, at_uri)
        .await
        .unwrap()
        .expect("post should be archived");
    assert_eq!(record.media.len(), 1, "should succeed after retries");
}

#[tokio::test]
async fn exhausting_retries_gives_up_without_crashing_but_keeps_the_post() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/always-down.jpg"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let downloader = MediaDownloader::new(store.clone(), 4, 1_000_000);
    let (tx, mut rx) = candidate_post_channel(4);

    let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
    tx.send(candidate(
        at_uri,
        vec![MediaRef {
            cdn_url: format!("{}/always-down.jpg", server.uri()),
            declared_mime_type: Some("image/jpeg".to_string()),
            declared_size_bytes: None,
        }],
    ))
    .await
    .unwrap();
    drop(tx);

    // Must return normally (no panic) even though the download never
    // succeeds.
    downloader.run(&mut rx).await;

    let record = store
        .get_post(Category::Post, at_uri)
        .await
        .unwrap()
        .expect("post JSON must remain archived even when media fails permanently");
    assert!(record.media.is_empty());
}

struct TrackingResponder {
    arrivals: Mutex<Vec<Instant>>,
    delay: Duration,
}

#[derive(Clone)]
struct SharedTrackingResponder(Arc<TrackingResponder>);

impl Respond for SharedTrackingResponder {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        self.0.arrivals.lock().unwrap().push(Instant::now());
        ResponseTemplate::new(200)
            .insert_header("content-type", "image/jpeg")
            .set_body_bytes(b"bytes".to_vec())
            .set_delay(self.0.delay)
    }
}

/// Computes the maximum number of `[arrival, arrival + delay)`
/// intervals that overlap at any single point in time: the peak number
/// of requests the mock server was handling concurrently.
fn peak_overlap(arrivals: &[Instant], delay: Duration) -> usize {
    let mut events: Vec<(Instant, i32)> = Vec::new();
    for &start in arrivals {
        events.push((start, 1));
        events.push((start + delay, -1));
    }
    events.sort_by_key(|(t, kind)| (*t, *kind));

    let mut current = 0i32;
    let mut peak = 0i32;
    for (_, kind) in events {
        current += kind;
        peak = peak.max(current);
    }
    peak.max(0) as usize
}

#[tokio::test]
async fn concurrency_cap_is_respected() {
    let server = MockServer::start().await;
    let delay = Duration::from_millis(150);
    let responder = Arc::new(TrackingResponder {
        arrivals: Mutex::new(Vec::new()),
        delay,
    });

    Mock::given(method("GET"))
        .and(path("/media.jpg"))
        .respond_with(SharedTrackingResponder(responder.clone()))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    const LIMIT: usize = 2;
    let downloader = MediaDownloader::new(store.clone(), LIMIT, 1_000_000);
    let (tx, mut rx) = candidate_post_channel(16);

    for i in 0..6 {
        let at_uri = format!("at://did:plc:alice/app.bsky.feed.post/{i}");
        tx.send(candidate(
            &at_uri,
            vec![MediaRef {
                cdn_url: format!("{}/media.jpg", server.uri()),
                declared_mime_type: Some("image/jpeg".to_string()),
                declared_size_bytes: None,
            }],
        ))
        .await
        .unwrap();
    }
    drop(tx);

    downloader.run(&mut rx).await;

    let arrivals = responder.arrivals.lock().unwrap().clone();
    assert_eq!(arrivals.len(), 6);
    let peak = peak_overlap(&arrivals, delay);
    assert!(
        peak <= LIMIT,
        "observed {peak} concurrent in-flight requests, expected at most {LIMIT}"
    );
    assert!(
        peak >= 2,
        "test is vacuous unless it actually observes concurrency, got {peak}"
    );
}

#[test]
fn extension_from_mime_covers_common_media_types() {
    assert_eq!(extension_from_mime("image/jpeg"), Some("jpg"));
    assert_eq!(extension_from_mime("video/mp4"), Some("mp4"));
    assert_eq!(extension_from_mime("application/octet-stream"), None);
}

#[test]
fn hls_playlist_content_types_are_recognized() {
    assert!(is_hls_playlist_content_type(
        "application/vnd.apple.mpegurl"
    ));
    assert!(is_hls_playlist_content_type("application/x-mpegurl"));
    assert!(is_hls_playlist_content_type(
        "Application/VND.APPLE.MPEGURL; charset=utf-8"
    ));
    assert!(!is_hls_playlist_content_type("video/mp4"));
    assert!(!is_hls_playlist_content_type("image/jpeg"));
}

#[test]
fn hls_playlist_urls_are_recognized() {
    assert!(is_hls_playlist_url(
        "https://video.bsky.app/watch/did/cid/playlist.m3u8"
    ));
    assert!(is_hls_playlist_url(
        "https://cdn.example.com/a.m3u8?token=x"
    ));
    assert!(!is_hls_playlist_url("https://cdn.example.com/a.mp4"));
    assert!(!is_hls_playlist_url("https://cdn.example.com/m3u8"));
}

#[test]
fn master_playlist_variant_picks_highest_bandwidth() {
    let master = "\
#EXTM3U
#EXT-X-VERSION:7
#EXT-X-STREAM-INF:BANDWIDTH=500000
low.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2000000
high.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=1000000
mid.m3u8
";
    assert_eq!(
        master_playlist_variant(master).as_deref(),
        Some("high.m3u8")
    );
}

#[test]
fn master_playlist_detection_on_media_playlist_returns_none() {
    let media_playlist = "\
#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:4
#EXTINF:4.0,
seg0.m4s
#EXT-X-ENDLIST
";
    assert_eq!(master_playlist_variant(media_playlist), None);
}

#[test]
fn media_playlist_parses_init_and_segments_relative_to_its_url() {
    let playlist = "\
#EXTM3U
#EXT-X-VERSION:7
#EXT-X-PLAYLIST-TYPE:VOD
#EXT-X-MAP:URI=\"init.mp4\"
#EXTINF:4.00000,
segment-0.m4s
#EXTINF:4.00000,
segment-1.m4s
#EXT-X-ENDLIST
";
    let base = url::Url::parse("https://video.example.com/watch/did/cid/media.m3u8").unwrap();
    let (init, segments) = parse_media_playlist(playlist, &base).expect("parses");
    assert_eq!(
        init.map(|u| u.to_string()),
        Some("https://video.example.com/watch/did/cid/init.mp4".to_string())
    );
    assert_eq!(segments.len(), 2);
    assert_eq!(
        segments[0].to_string(),
        "https://video.example.com/watch/did/cid/segment-0.m4s"
    );
    assert_eq!(
        segments[1].to_string(),
        "https://video.example.com/watch/did/cid/segment-1.m4s"
    );
}

#[test]
fn encrypted_media_playlist_is_rejected() {
    let playlist = "\
#EXTM3U
#EXT-X-KEY:METHOD=AES-128,URI=\"https://keys.example.com/k\"
#EXTINF:4.0,
seg0.m4s
#EXT-X-ENDLIST
";
    let base = url::Url::parse("https://video.example.com/media.m3u8").unwrap();
    let err = parse_media_playlist(playlist, &base).expect_err("encrypted must be rejected");
    assert!(matches!(err, DownloadError::Unsupported(_)));
}

#[tokio::test]
async fn hls_video_is_reassembled_into_a_single_playable_mp4() {
    let server = MockServer::start().await;

    // Master playlist → one variant (highest bandwidth of two) → media
    // playlist with an fMP4 init segment and two media segments, the
    // shape Bluesky's video CDN actually serves.
    Mock::given(method("GET"))
        .and(path("/watch/did/cid/playlist.m3u8"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/vnd.apple.mpegurl")
                .set_body_string(
                    "#EXTM3U\n\
                         #EXT-X-STREAM-INF:BANDWIDTH=100000\n\
                         low.m3u8\n\
                         #EXT-X-STREAM-INF:BANDWIDTH=900000\n\
                         media.m3u8\n",
                ),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/watch/did/cid/media.m3u8"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/vnd.apple.mpegurl")
                .set_body_string(
                    "#EXTM3U\n\
                         #EXT-X-VERSION:7\n\
                         #EXT-X-MAP:URI=\"init.mp4\"\n\
                         #EXTINF:4.0,\n\
                         seg0.m4s\n\
                         #EXTINF:4.0,\n\
                         seg1.m4s\n\
                         #EXT-X-ENDLIST\n",
                ),
        )
        .mount(&server)
        .await;

    for (route, segment_body) in [
        ("/watch/did/cid/init.mp4", "init-bytes"),
        ("/watch/did/cid/seg0.m4s", "seg0-bytes"),
        ("/watch/did/cid/seg1.m4s", "seg1-bytes"),
    ] {
        Mock::given(method("GET"))
            .and(path(route))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "video/iso.segment")
                    .set_body_bytes(segment_body.as_bytes().to_vec()),
            )
            .mount(&server)
            .await;
    }

    let (_dir, store) = open_store().await;
    let downloader = MediaDownloader::new(store.clone(), 4, 1_000_000);
    let (tx, mut rx) = candidate_post_channel(4);

    let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
    tx.send(candidate(
        at_uri,
        vec![MediaRef {
            cdn_url: format!("{}/watch/did/cid/playlist.m3u8", server.uri()),
            declared_mime_type: Some("application/vnd.apple.mpegurl".to_string()),
            declared_size_bytes: None,
        }],
    ))
    .await
    .unwrap();
    drop(tx);

    downloader.run(&mut rx).await;

    let record = store
        .get_post(Category::Post, at_uri)
        .await
        .unwrap()
        .expect("post should be archived");
    assert_eq!(record.media.len(), 1);
    assert_eq!(record.media[0].filename, "000.mp4");
    assert_eq!(
        record.media[0].content_type.as_deref(),
        Some("video/mp4"),
        "reassembled video must be stored with a playable content type"
    );
    assert_eq!(
        record.media[0].size_bytes,
        ("init-bytes".len() + "seg0-bytes".len() + "seg1-bytes".len()) as u64,
        "init + every media segment, concatenated in order"
    );
}

#[tokio::test]
async fn ts_segment_hls_video_is_remuxed_into_a_playable_mp4() {
    // Bluesky's CDN serves TS segments, not fMP4: the assembled file
    // must be a real MP4 (remuxed), not raw TS in an .mp4 name.
    let server = MockServer::start().await;

    let ts = crate::remux::synthetic_test_ts();
    Mock::given(method("GET"))
        .and(path("/watch/did/cid/playlist.m3u8"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            "#EXTM3U\n\
                 #EXT-X-VERSION:3\n\
                 #EXT-X-TARGETDURATION:6\n\
                 #EXTINF:6.0,\n\
                 video0.ts\n\
                 #EXT-X-ENDLIST\n",
        ))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/watch/did/cid/video0.ts"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "video/iso.segment")
                .set_body_bytes(ts),
        )
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let downloader = MediaDownloader::new(store.clone(), 4, 5_000_000);
    let (tx, mut rx) = candidate_post_channel(4);

    let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
    tx.send(candidate(
        at_uri,
        vec![MediaRef {
            cdn_url: format!("{}/watch/did/cid/playlist.m3u8", server.uri()),
            declared_mime_type: Some("video/mp4".to_string()),
            declared_size_bytes: None,
        }],
    ))
    .await
    .unwrap();
    drop(tx);

    downloader.run(&mut rx).await;

    let record = store
        .get_post(Category::Post, at_uri)
        .await
        .unwrap()
        .expect("post should be archived");
    assert_eq!(record.media.len(), 1);
    assert_eq!(record.media[0].filename, "000.mp4");
    assert_eq!(
        record.media[0].content_type.as_deref(),
        Some("video/mp4"),
        "TS segments must be remuxed into a playable MP4"
    );

    // The stored bytes are a real MP4 (ftyp box first), not raw TS.
    let stored = store
        .read_media_head(crate::storage::Category::Post, at_uri, "000.mp4", 16)
        .await
        .unwrap()
        .expect("media file on disk");
    assert_eq!(&stored[4..8], b"ftyp");
}

#[tokio::test]
async fn hls_video_exceeding_the_size_cap_is_not_saved() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/watch/did/cid/playlist.m3u8"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            "#EXTM3U\n\
                 #EXT-X-MAP:URI=\"init.mp4\"\n\
                 #EXTINF:4.0,\n\
                 seg0.m4s\n\
                 #EXT-X-ENDLIST\n",
        ))
        .mount(&server)
        .await;

    for (route, segment_body) in [
        ("/watch/did/cid/init.mp4", vec![0u8; 60]),
        ("/watch/did/cid/seg0.m4s", vec![0u8; 60]),
    ] {
        Mock::given(method("GET"))
            .and(path(route))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(segment_body))
            .mount(&server)
            .await;
    }

    let (_dir, store) = open_store().await;
    // 100-byte cap: init alone fits, init + first segment does not.
    let downloader = MediaDownloader::new(store.clone(), 4, 100);
    let (tx, mut rx) = candidate_post_channel(4);

    let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
    tx.send(candidate(
        at_uri,
        vec![MediaRef {
            cdn_url: format!("{}/watch/did/cid/playlist.m3u8", server.uri()),
            declared_mime_type: Some("video/mp4".to_string()),
            declared_size_bytes: None,
        }],
    ))
    .await
    .unwrap();
    drop(tx);

    downloader.run(&mut rx).await;

    let record = store
        .get_post(Category::Post, at_uri)
        .await
        .unwrap()
        .expect("post record stays archived");
    assert!(record.media.is_empty(), "oversized video must not be saved");
}

#[tokio::test]
async fn encrypted_hls_video_gives_up_without_crashing_but_keeps_the_post() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/watch/did/cid/playlist.m3u8"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            "#EXTM3U\n\
                 #EXT-X-KEY:METHOD=AES-128,URI=\"https://keys.example.com/k\"\n\
                 #EXTINF:4.0,\n\
                 seg0.m4s\n\
                 #EXT-X-ENDLIST\n",
        ))
        .mount(&server)
        .await;

    let (_dir, store) = open_store().await;
    let downloader = MediaDownloader::new(store.clone(), 4, 1_000_000);
    let (tx, mut rx) = candidate_post_channel(4);

    let at_uri = "at://did:plc:alice/app.bsky.feed.post/1";
    tx.send(candidate(
        at_uri,
        vec![MediaRef {
            cdn_url: format!("{}/watch/did/cid/playlist.m3u8", server.uri()),
            declared_mime_type: None,
            declared_size_bytes: None,
        }],
    ))
    .await
    .unwrap();
    drop(tx);

    // Must return normally (no panic) even though the stream can't be
    // archived.
    downloader.run(&mut rx).await;

    let record = store
        .get_post(Category::Post, at_uri)
        .await
        .unwrap()
        .expect("post record stays archived");
    assert!(record.media.is_empty());
}

#[test]
fn extension_from_url_reads_the_path_suffix() {
    assert_eq!(
        extension_from_url("https://cdn.example.com/a/b.PNG?x=1"),
        Some("png".to_string())
    );
    assert_eq!(extension_from_url("https://cdn.example.com/noext"), None);
}

#[test]
fn base_mime_strips_parameters() {
    assert_eq!(base_mime("image/jpeg; charset=binary"), "image/jpeg");
    assert_eq!(base_mime("image/png"), "image/png");
}
