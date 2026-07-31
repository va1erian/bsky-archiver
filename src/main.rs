mod bluesky;
mod config;
mod firehose;
mod media;
mod poller;
mod storage;
mod web;

fn main() {
    let filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt().with_env_filter(filter).init();

    tracing::info!("starting up");
}
