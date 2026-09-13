//! Config view (`/config`) and the watched-sources management routes.

use axum::Form;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Redirect, Response};
use serde::Deserialize;

use super::{WebState, is_htmx_request};
use crate::poller;
use crate::storage::SourceKind;
use crate::templates;

// ---------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------

pub(super) async fn config_view(State(state): State<WebState>) -> Response {
    let config = &state.app.config;
    let redacted = "<redacted>".to_string();
    let rows = vec![
        templates::ConfigRow {
            key: "BSKY_IDENTIFIER",
            value: config.bsky_identifier.clone(),
            redacted: false,
        },
        templates::ConfigRow {
            key: "BSKY_APP_PASSWORD",
            value: redacted.clone(),
            redacted: true,
        },
        templates::ConfigRow {
            key: "TUMBLR_* (likes archiver)",
            value: if config.tumblr.is_some() {
                "enabled"
            } else {
                "disabled"
            }
            .to_string(),
            redacted: false,
        },
        templates::ConfigRow {
            key: "PIXIV_* (bookmarks archiver)",
            value: if config.pixiv_refresh_token.is_some() {
                "enabled"
            } else {
                "disabled"
            }
            .to_string(),
            redacted: false,
        },
        templates::ConfigRow {
            key: "ARCHIVE_DIR",
            value: config.archive_dir.display().to_string(),
            redacted: false,
        },
        templates::ConfigRow {
            key: "DATABASE_PATH",
            value: config.database_path.display().to_string(),
            redacted: false,
        },
        templates::ConfigRow {
            key: "UI_PASSWORD",
            value: redacted.clone(),
            redacted: true,
        },
        templates::ConfigRow {
            key: "UI_SESSION_SECRET",
            value: redacted,
            redacted: true,
        },
        templates::ConfigRow {
            key: "UI_PORT",
            value: config.ui_port.to_string(),
            redacted: false,
        },
        templates::ConfigRow {
            key: "POLL_INTERVAL_SECONDS",
            value: config.poll_interval_seconds.to_string(),
            redacted: false,
        },
        templates::ConfigRow {
            key: "JETSTREAM_URL",
            value: config.jetstream_url.to_string(),
            redacted: false,
        },
        templates::ConfigRow {
            key: "MEDIA_MAX_CONCURRENT_DOWNLOADS",
            value: config.media_max_concurrent_downloads.to_string(),
            redacted: false,
        },
        templates::ConfigRow {
            key: "MEDIA_MAX_BYTES",
            value: config.media_max_bytes.to_string(),
            redacted: false,
        },
        templates::ConfigRow {
            key: "NIGHTLY_SWEEP_LOCAL_HOUR",
            value: config.nightly_sweep_local_hour.to_string(),
            redacted: false,
        },
    ];
    let sources = match state.app.store.list_watched_sources().await {
        Ok(list) => list.iter().map(templates::source_row).collect(),
        Err(err) => {
            tracing::error!(error = %err, "failed to load watched sources");
            Vec::new()
        }
    };
    askama_axum::into_response(&templates::ConfigTemplate {
        version: templates::APP_VERSION,
        git_revision: templates::GIT_REVISION,
        build_date: templates::BUILD_DATE,
        rows,
        sources,
        error: None,
    })
}
// ---------------------------------------------------------------------
// Watched sources (UI-managed accounts + feeds, live-reloadable)
// ---------------------------------------------------------------------
//
// `POST /sources` adds an account (handle or DID — resolved via the shared
// Bluesky client before persisting) or a feed (`at://` URI — validated with
// a single `getFeed` page fetch), then reloads the shared roster so the
// firehose, REST fallback, and feed poller all pick it up immediately.
// `POST /sources/:id` removes a row the same way. Both handlers sit behind
// the session gate (see [`router`]), and every outbound call rides the
// process-wide [`crate::ratelimit::RequestLimiter`] via the shared client,
// matching how the background pollers make requests.

#[derive(Debug, Deserialize)]
pub(super) struct AddSourceForm {
    kind: String,
    value: String,
}

pub(super) async fn add_source(
    State(state): State<WebState>,
    headers: HeaderMap,
    Form(form): Form<AddSourceForm>,
) -> Response {
    let value = form.value.trim().to_string();
    let outcome = match form.kind.trim().parse::<SourceKind>() {
        Ok(SourceKind::Account) => add_account_source(&state, &value).await,
        Ok(SourceKind::Feed) => add_feed_source(&state, &value).await,
        Err(_) => Err("unknown source kind: expected 'account' or 'feed'".to_string()),
    };
    source_mutation_response(state, headers, outcome).await
}

async fn add_account_source(state: &WebState, value: &str) -> Result<(), String> {
    // A value that's already a DID is used verbatim; anything else is a
    // handle resolved via the shared (request-limiter-riding) Bluesky
    // client. An unresolvable handle is an inline error and leaves the
    // watch list unchanged.
    let did = if value.starts_with("did:") {
        value.to_string()
    } else {
        match state.app.bluesky.resolve_handle(value).await {
            Ok(did) => did,
            Err(err) => {
                tracing::warn!(handle = %value, error = %err, "failed to resolve new watch handle");
                return Err(format!(
                    "could not resolve handle {value:?} — check it and try again"
                ));
            }
        }
    };
    persist_added_source(state, SourceKind::Account, value, Some(&did)).await?;
    backfill_account(state, value);
    Ok(())
}

async fn add_feed_source(state: &WebState, value: &str) -> Result<(), String> {
    if !value.starts_with("at://") {
        return Err(format!("{value:?} is not an at:// feed URI"));
    }
    // One `getFeed` page fetch proves the URI resolves and is fetchable
    // before persisting it, so a typo or a private feed surfaces an inline
    // error instead of silently watching nothing.
    match state.app.bluesky.get_feed(value, None, 1).await {
        Ok(_) => {}
        Err(err) => {
            tracing::warn!(feed = %value, error = %err, "failed to validate new feed");
            return Err(format!(
                "could not fetch feed {value:?} — is the at:// URI correct?"
            ));
        }
    }
    persist_added_source(state, SourceKind::Feed, value, None).await?;
    backfill_feed(state, value);
    Ok(())
}

/// Persists a source and live-reloads the shared roster so every producer
/// observes the change without a restart.
async fn persist_added_source(
    state: &WebState,
    kind: SourceKind,
    value: &str,
    did: Option<&str>,
) -> Result<(), String> {
    let _id = state
        .app
        .store
        .add_watched_source(kind, value, did)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to persist watched source");
            "failed to save watch target".to_string()
        })?;
    state
        .app
        .watchlist
        .reload_from_store(&state.app.store)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to reload watch list after change");
            "failed to reload watch list".to_string()
        })
}

pub(super) async fn remove_source(
    State(state): State<WebState>,
    Path(id): Path<i64>,
    headers: HeaderMap,
) -> Response {
    let outcome = match state.app.store.remove_watched_source(id).await {
        Ok(_) => state
            .app
            .watchlist
            .reload_from_store(&state.app.store)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to reload watch list after remove");
                "failed to reload watch list".to_string()
            }),
        Err(err) => {
            tracing::error!(error = %err, "failed to remove watch source");
            Err("failed to remove watch target".to_string())
        }
    };
    source_mutation_response(state, headers, outcome).await
}

/// The shared response for a source mutation request: an htmx request gets
/// the watcher panel (with any error rendered inline inside it) swapped over
/// `#sources-panel` via outerHTML; a plain no-JS form post redirects back to
/// the config page.
async fn source_mutation_response(
    state: WebState,
    headers: HeaderMap,
    outcome: Result<(), String>,
) -> Response {
    if is_htmx_request(&headers) {
        sources_panel(state, outcome.err()).await
    } else {
        Redirect::to("/config").into_response()
    }
}

/// Renders the watcher panel from the current database roster plus an
/// optional inline error. This is the response body for both source
/// mutations and the markup the config page embeds.
async fn sources_panel(state: WebState, error: Option<String>) -> Response {
    let sources = match state.app.store.list_watched_sources().await {
        Ok(list) => list.iter().map(templates::source_row).collect::<Vec<_>>(),
        Err(err) => {
            tracing::error!(error = %err, "failed to list watched sources");
            return askama_axum::into_response(&templates::SourcesPanelTemplate {
                sources: Vec::new(),
                error: Some("failed to load watched sources".to_string()),
            });
        }
    };
    askama_axum::into_response(&templates::SourcesPanelTemplate { sources, error })
}

/// Kicks off an immediate backfill poll of a newly added account, handing
/// archive-worthy posts straight into the producer -> downloader channel
/// rather than waiting up to a full `POLL_INTERVAL_SECONDS`. Runs on the
/// shared candidate channel (upgraded from the state's weak handle, so it
/// degrades to a no-op once every producer has stopped) and rides the same
/// request limiter as the background pollers.
fn backfill_account(state: &WebState, handle: &str) {
    let Some(sender) = state.app.candidates() else {
        tracing::warn!("candidate channel closed; skipping immediate account backfill");
        return;
    };
    let client = std::sync::Arc::clone(&state.app.bluesky);
    let archive = state.app.store.clone();
    let handle = handle.to_string();
    tokio::spawn(async move {
        match poller::backfill_account_once(&client, &archive, &sender, &handle).await {
            Ok(new_count) => {
                tracing::info!(handle = %handle, new_count, "immediate account backfill completed")
            }
            Err(err) => {
                tracing::warn!(handle = %handle, error = %err, "immediate account backfill failed")
            }
        }
    });
}

/// Kicks off an immediate backfill poll of a newly added feed, mirroring
/// [`backfill_account`] for feeds.
fn backfill_feed(state: &WebState, feed_uri: &str) {
    let Some(sender) = state.app.candidates() else {
        tracing::warn!("candidate channel closed; skipping immediate feed backfill");
        return;
    };
    let client = std::sync::Arc::clone(&state.app.bluesky);
    let archive = state.app.store.clone();
    let feed_uri = feed_uri.to_string();
    tokio::spawn(async move {
        match poller::backfill_feed_once(&client, &archive, &sender, &feed_uri).await {
            Ok(new_count) => {
                tracing::info!(feed = %feed_uri, new_count, "immediate feed backfill completed")
            }
            Err(err) => {
                tracing::warn!(feed = %feed_uri, error = %err, "immediate feed backfill failed")
            }
        }
    });
}
