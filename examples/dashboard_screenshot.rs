use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let dir = tempfile::tempdir().expect("tempdir");
    let archive_dir = dir.path().join("archive");
    let database_path = dir.path().join("index.sqlite3");
    let store = bsky_archiver::storage::ArchiveStore::open(archive_dir.clone(), database_path)
        .await
        .expect("open store");

    let config = bsky_archiver::config::AppConfig {
        bsky_identifier: "test.bsky.social".to_string(),
        bsky_app_password: bsky_archiver::config::Secret::from("test".to_string()),
        archive_dir,
        database_path: dir.path().join("index.sqlite3"),
        ui_password: bsky_archiver::config::Secret::from("testpass".to_string()),
        ui_session_secret: bsky_archiver::config::Secret::from("a".repeat(64)),
        ui_port: 18081,
        poll_interval_seconds: 120,
        jetstream_url: url::Url::parse("wss://jetstream.example.com/subscribe").unwrap(),
        media_max_concurrent_downloads: 4,
        media_max_bytes: 104_857_600,
    };

    let (candidate_tx, _candidate_rx) = bsky_archiver::pipeline::candidate_post_channel(8);
    let (_health_tx, health_rx) = bsky_archiver::health::health_channel();

    let state: bsky_archiver::state::SharedAppState = Arc::new(bsky_archiver::state::AppState {
        config,
        store,
        health: health_rx,
        watchlist: bsky_archiver::watchlist::Watchlist::new(Vec::new()),
        bluesky: Arc::new(bsky_archiver::bluesky::BlueskyClient::new(
            url::Url::parse("http://localhost:1").unwrap(),
            "test.bsky.social".to_string(),
            bsky_archiver::config::Secret::from("test".to_string()),
        )),
        candidate_weak: bsky_archiver::pipeline::weak_from_sender(&candidate_tx),
    });

    let app = bsky_archiver::web::router(state);
    let addr = SocketAddr::from(([127, 0, 0, 1], 18081));
    let listener = TcpListener::bind(addr).await.unwrap();
    println!("Server listening on http://127.0.0.1:18081");
    axum::serve(listener, app).await.unwrap();
}