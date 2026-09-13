//! Browser e2e test: builds the real web UI (real axum router, real store,
//! real templates/static assets — no mocking in the UI layer) on an
//! ephemeral port, seeds a deterministic archive of 25 image posts through
//! the real [`ArchiveStore`], and then drives it with a real Chromium via
//! the Playwright suite in `e2e-browser/`.
//!
//! The Bluesky network is never touched and the archiving pipeline isn't
//! started at all: records and media bytes (a generated 1x1 PNG, so the
//! browser genuinely decodes every `<img>`) are seeded directly — exactly
//! what the pollers + media downloader would have produced. Pipeline
//! integration with a mocked Bluesky API is covered separately by
//! `tests/e2e.rs`.
//!
//! Skips (passes with an explanatory note) when `node` or the Playwright
//! suite is not installed; install with, in `e2e-browser/`:
//!
//! ```text
//! npm install && npx playwright install chromium
//! ```
//!
//! On CI (`.github/workflows/ci.yml`) those steps run before this test.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use serde_json::json;

use bsky_archiver::bluesky::BlueskyClient;
use bsky_archiver::config::{AppConfig, Secret};
use bsky_archiver::health::health_channel;
use bsky_archiver::state::{AppState, SharedAppState};
use bsky_archiver::storage::{ArchiveStore, Category, SourceKind};
use bsky_archiver::web;

#[tokio::test]
async fn browser_suite_passes_against_the_real_ui() {
    let dir = tempfile::tempdir().expect("tempdir");

    let store = ArchiveStore::open(dir.path().join("archive"), dir.path().join("index.sqlite3"))
        .await
        .expect("open store");
    seed_archive(&store).await;

    let (addr, server_handle) =
        spawn_server(dir.path().join("archive"), dir.path().join("index.sqlite3")).await;
    let base_url = format!("http://{addr}");
    wait_for_healthz(&base_url).await;

    // --- Drive a real browser against the real server -------------------
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let suite_dir = manifest_dir.join("e2e-browser");
    let cli = suite_dir.join("node_modules/@playwright/test/cli.js");

    if !is_node_available().await {
        println!("SKIP browser suite: node is not on PATH");
        server_handle.abort();
        return;
    }
    if !cli.is_file() {
        println!(
            "SKIP browser suite: @playwright/test not installed in {} \
             (run: npm install && npx playwright install chromium)",
            suite_dir.display()
        );
        server_handle.abort();
        return;
    }

    let output = tokio::process::Command::new("node")
        .arg(cli)
        .arg("test")
        .arg("--reporter=list")
        .current_dir(&suite_dir)
        .env("BASE_URL", &base_url)
        .env("UI_PASSWORD", "e2e-ui-password")
        .output()
        .await
        .expect("spawn node for the playwright suite");

    if !output.status.success() {
        println!(
            "--- browser suite stdout ---\n{}",
            String::from_utf8_lossy(&output.stdout)
        );
        println!(
            "--- browser suite stderr ---\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let artifacts = suite_dir.join("test-results");
        if artifacts.is_dir() {
            println!(
                "failure artifacts (traces/screenshots): {}",
                artifacts.display()
            );
        }
        panic!("browser e2e suite failed with status {:?}", output.status);
    }

    server_handle.abort();
}

/// Seeds 25 image posts (each a real 1x1 PNG so the browser actually decodes
/// every `<img>`) plus one watched source so `/config` renders its data
/// table. 25 items = 3 pages at the page_size 10 the suite paginates with.
async fn seed_archive(store: &ArchiveStore) {
    let png = tiny_png();
    for i in 0..25 {
        let at_uri = format!("at://did:plc:browser-e2e/app.bsky.feed.post/post-{i:02}");
        store
            .save_post(
                Category::Post,
                &at_uri,
                &format!("cid-{i:02}"),
                json!({
                    "$type": "app.bsky.feed.post",
                    "text": format!("an e2e browser image post {i:02}"),
                    "createdAt": "2024-06-01T00:00:00Z",
                    "author": {"did": "did:plc:browser-e2e"},
                    "embed": {
                        "$type": "app.bsky.embed.images",
                        "images": [{
                            "alt": format!("e2e browser image {i:02}"),
                            "image": {"ref": "bafy-browser-e2e"}
                        }]
                    }
                }),
            )
            .await
            .expect("save seeded post");
        store
            .save_media(
                Category::Post,
                &at_uri,
                "000.png",
                Some("image/png".to_string()),
                png.clone(),
            )
            .await
            .expect("save seeded media");
    }

    store
        .add_watched_source(
            SourceKind::Account,
            "browser-e2e.bsky.social",
            Some("did:plc:browser-e2e-watched"),
        )
        .await
        .expect("seed watched source");
}

/// Spawns the real UI router on an ephemeral loopback port and returns its
/// address plus the serve-task handle. The store is reopened here the same
/// way `app::serve` does it in production (same on-disk state, same paths).
async fn spawn_server(
    archive_dir: PathBuf,
    database_path: PathBuf,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let config = AppConfig {
        bsky_identifier: "browser-e2e.bsky.social".to_string(),
        bsky_app_password: Secret::from("browser-e2e-app-password".to_string()),
        tumblr: None,
        archive_dir,
        database_path,
        ui_password: Secret::from("e2e-ui-password".to_string()),
        ui_session_secret: Secret::from("browser-e2e-session-secret-0123456789".to_string()),
        ui_port: 0,
        poll_interval_seconds: 60,
        jetstream_url: url::Url::parse("wss://jetstream.invalid/subscribe").unwrap(),
        media_max_concurrent_downloads: 4,
        media_max_bytes: 100_000_000,
        nightly_sweep_local_hour: 3,
        tumblr_poll_interval_seconds: 300,
    };

    let store = ArchiveStore::open(config.archive_dir.clone(), config.database_path.clone())
        .await
        .expect("open store for the server");

    // No UI path exercised by the suite calls the Bluesky API (that only
    // happens on watch-list mutations), so the shared client can point at
    // an unreachable, non-routable address.
    let client = std::sync::Arc::new(BlueskyClient::new(
        url::Url::parse("https://bluesky.unreachable.invalid").unwrap(),
        config.bsky_identifier.clone(),
        Secret::from("browser-e2e-app-password".to_string()),
    ));

    let watchlist = bsky_archiver::watchlist::Watchlist::new(
        store
            .list_watched_sources()
            .await
            .expect("list watched sources"),
    );
    let (candidate_tx, _candidate_rx) = bsky_archiver::pipeline::candidate_post_channel(16);
    let state: SharedAppState = std::sync::Arc::new(AppState {
        config,
        store,
        health: health_channel().1,
        watchlist,
        bluesky: client,
        candidate_weak: bsky_archiver::pipeline::weak_from_sender(&candidate_tx),
    });

    let app = web::router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    let handle = tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .expect("serve the browser e2e UI");
    });
    (addr, handle)
}

/// The Playwright suite's first navigation assumes a live server, so poll
/// `/healthz` (a public route) until it responds or 10s elapse.
async fn wait_for_healthz(base_url: &str) {
    let client = reqwest::Client::new();
    let url = format!("{base_url}/healthz");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(resp) = client.get(&url).send().await
            && resp.status().is_success()
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "server did not come up: {url}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn is_node_available() -> bool {
    tokio::process::Command::new("node")
        .arg("--version")
        .output()
        .await
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Builds a valid 1x1 red PNG (8-bit truecolor, one stored-block IDAT) with
/// no compression library — just correctly-formed chunk framing, a deflate
/// stored block, and hand-rolled CRC32/Adler-32.
fn tiny_png() -> Vec<u8> {
    assert_eq!(crc32(b"123456789"), 0xcbf4_3926, "CRC32 self-check");

    let mut out = vec![0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
    out.extend(png_chunk(b"IHDR", &[0, 0, 0, 1, 0, 0, 0, 1, 8, 2, 0, 0, 0]));

    // IDAT = zlib wrapper around one stored deflate block carrying the
    // 4-byte scanline [filter 0, R 255, G 0, B 0].
    let scanline = [0x00, 0xFF, 0x00, 0x00];
    let mut idat = vec![0x78, 0x01]; // zlib CMF/FLG (0x7801 % 31 == 0)
    idat.push(0x01); // deflate: BFINAL=1, BTYPE=00 (stored)
    idat.extend((scanline.len() as u16).to_le_bytes());
    idat.extend((!(scanline.len() as u16)).to_le_bytes());
    idat.extend(scanline);
    idat.extend(adler32(&scanline).to_be_bytes());
    out.extend(png_chunk(b"IDAT", &idat));

    out.extend(png_chunk(b"IEND", &[]));
    out
}

fn png_chunk(kind: &[u8; 4], data: &[u8]) -> Vec<u8> {
    let mut chunk = Vec::with_capacity(12 + data.len());
    chunk.extend((data.len() as u32).to_be_bytes());
    chunk.extend(kind);
    chunk.extend(data);
    let mut crc_input = kind.to_vec();
    crc_input.extend(data);
    chunk.extend(crc32(&crc_input).to_be_bytes());
    chunk
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    crc ^ 0xFFFF_FFFF
}

fn adler32(data: &[u8]) -> u32 {
    let (mut a, mut b) = (1u32, 0u32);
    for &byte in data {
        a = (a + byte as u32) % 65_521;
        b = (b + a) % 65_521;
    }
    (b << 16) | a
}
