use bsky_archiver::app;

#[tokio::main]
async fn main() {
    let filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt().with_env_filter(filter).init();

    // The only subcommand: one-time interactive Telegram login, which
    // cannot be part of the unattended service loop (it needs a human to
    // type the login code — and optionally the 2FA password — once).
    let mut args = std::env::args().skip(1);
    if let Some(subcommand) = args.next() {
        if subcommand != "telegram-login" && subcommand != "--telegram-login" {
            eprintln!(
                "usage: bsky-archiver [telegram-login]\n  \
                 (run with no arguments to start the archiver service;\n   \
                  `telegram-login` performs the one-time interactive Telegram account login)"
            );
            std::process::exit(2);
        }
        if args.next().is_some() {
            eprintln!("usage: bsky-archiver telegram-login (extra arguments are not accepted)");
            std::process::exit(2);
        }
        if let Err(err) = app::run_telegram_login().await {
            tracing::error!(error = %err, "telegram-login failed");
            std::process::exit(1);
        }
        return;
    }

    if let Err(err) = app::run().await {
        tracing::error!(error = %err, "fatal startup error");
        std::process::exit(1);
    }
}
