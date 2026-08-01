mod app;
mod bluesky;
mod config;
mod firehose;
mod health;
mod media;
mod pipeline;
mod poller;
mod state;
mod storage;
mod web;

#[tokio::main]
async fn main() {
    let filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt().with_env_filter(filter).init();

    if let Err(err) = app::run().await {
        tracing::error!(error = %err, "fatal startup error");
        std::process::exit(1);
    }
}
