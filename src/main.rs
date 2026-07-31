mod bluesky;
mod config;
mod firehose;
mod media;
mod poller;
mod storage;
mod web;

use config::AppConfig;

fn main() {
    let filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let config = match AppConfig::from_env() {
        Ok(config) => config,
        Err(err) => {
            tracing::error!(error = %err, "invalid configuration");
            std::process::exit(1);
        }
    };

    tracing::info!(
        identifier = %config.bsky_identifier,
        app_password = ?config.bsky_app_password,
        watch_handles = ?config.bsky_watch_handles,
        archive_dir = %config.archive_dir.display(),
        database_path = %config.database_path.display(),
        ui_password = ?config.ui_password,
        ui_session_secret = ?config.ui_session_secret,
        ui_port = config.ui_port,
        poll_interval_seconds = config.poll_interval_seconds,
        jetstream_url = %config.jetstream_url,
        media_max_concurrent_downloads = config.media_max_concurrent_downloads,
        media_max_bytes = config.media_max_bytes,
        "starting up"
    );
}
