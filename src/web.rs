//! Web UI: axum HTTP server, password-gated session auth, and routing.
//!
//! This module owns routing, auth, and data-fetching; presentation lives
//! in [`crate::templates`] (askama templates + their view models) so this
//! file stays about *what data* each route needs, not how it's marked up.
//! Handlers are grouped into submodules by page/concern ([`assets`],
//! [`dashboard`], [`posts`], [`gallery`], [`media`], [`config`]); this root
//! module keeps the router, the session gate, the health probe, and the
//! shared error/helper vocabulary the submodules build on.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::{Form, Router};
use axum_extra::extract::cookie::{Cookie, Key, PrivateCookieJar, SameSite};
use serde::Deserialize;

use crate::health::{HealthSnapshot, Status};
use crate::state::SharedAppState;
use crate::storage::StorageError;

mod assets;
mod config;
mod dashboard;
mod gallery;
mod media;
mod posts;
#[cfg(test)]
mod tests;

const SESSION_COOKIE: &str = "bsky_archiver_session";
const SESSION_VALUE: &str = "authenticated";
const DEFAULT_PAGE_SIZE: u32 = 20;
const MAX_PAGE_SIZE: u32 = 100;

/// Router state: the shared application state plus the cookie-signing key
/// derived from `UI_SESSION_SECRET`. Kept separate from
/// [`crate::state::AppState`] so this ticket doesn't have to change what
/// AR-9 already owns.
#[derive(Clone)]
struct WebState {
    app: SharedAppState,
    key: Key,
}

impl axum::extract::FromRef<WebState> for Key {
    fn from_ref(state: &WebState) -> Self {
        state.key.clone()
    }
}

/// Builds the full `axum::Router` for the web UI: public routes
/// (`/login`, `/healthz`, static assets, PWA manifest) plus every other
/// route behind the session-auth middleware.
pub fn router(app: SharedAppState) -> Router {
    let key = Key::derive_from(app.config.ui_session_secret.expose_secret().as_bytes());
    let state = WebState { app, key };

    let protected = Router::new()
        .route("/", get(dashboard::dashboard))
        .route("/posts", get(posts::list_posts))
        .route("/posts/:id", get(posts::post_detail))
        .route("/gallery", get(gallery::gallery))
        .route("/gallery/export", get(gallery::gallery_export))
        .route("/config", get(config::config_view))
        .route("/media/:category/:id/:filename", get(media::media_file))
        .route("/sources", axum::routing::post(config::add_source))
        .route("/sources/:id", axum::routing::post(config::remove_source))
        .route("/logout", axum::routing::post(logout))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_auth));

    let public = Router::new()
        .route("/login", get(login_form).post(login_submit))
        .route("/healthz", get(healthz))
        .route("/manifest.webmanifest", get(assets::manifest))
        .route("/static/pico.min.css", get(assets::pico_css))
        .route("/static/app.css", get(assets::app_css))
        .route("/static/htmx.min.js", get(assets::htmx_js))
        .route("/static/icons/icon-192.png", get(assets::icon_192))
        .route("/static/icons/icon-512.png", get(assets::icon_512));

    public.merge(protected).with_state(state)
}

// ---------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------

async fn require_auth(
    jar: PrivateCookieJar,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    match jar.get(SESSION_COOKIE) {
        Some(cookie) if cookie.value() == SESSION_VALUE => next.run(request).await,
        _ => Redirect::to("/login").into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct LoginForm {
    password: String,
}

async fn login_form() -> Response {
    askama_axum::into_response(&crate::templates::LoginTemplate { error: None })
}

async fn login_submit(
    State(state): State<WebState>,
    jar: PrivateCookieJar,
    Form(form): Form<LoginForm>,
) -> Response {
    let expected = state.app.config.ui_password.expose_secret();
    if constant_time_eq(expected.as_bytes(), form.password.as_bytes()) {
        let cookie = Cookie::build((SESSION_COOKIE, SESSION_VALUE))
            .path("/")
            .http_only(true)
            .same_site(SameSite::Lax)
            .build();
        let jar = jar.add(cookie);
        (jar, Redirect::to("/")).into_response()
    } else {
        let body = askama_axum::into_response(&crate::templates::LoginTemplate {
            error: Some("Incorrect password"),
        });
        (StatusCode::UNAUTHORIZED, body).into_response()
    }
}

async fn logout(jar: PrivateCookieJar) -> Response {
    let jar = jar.remove(Cookie::from(SESSION_COOKIE));
    (jar, Redirect::to("/login")).into_response()
}

/// Compares two byte strings in constant time (with respect to the length
/// of `expected`), so a failed login attempt can't be used to infer how
/// many leading characters of a guess matched the real password.
fn constant_time_eq(expected: &[u8], candidate: &[u8]) -> bool {
    // Always walk the same number of bytes (the length of `expected`)
    // regardless of whether the lengths match, so a length mismatch
    // doesn't return measurably faster than a same-length mismatch.
    let mut diff: u8 = (expected.len() != candidate.len()) as u8;
    for (i, &byte) in expected.iter().enumerate() {
        diff |= byte ^ candidate.get(i).copied().unwrap_or(0);
    }
    diff == 0
}

// ---------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------

async fn healthz(State(state): State<WebState>) -> Response {
    let snapshot: HealthSnapshot = state.app.health.borrow().clone();
    let any_error = [
        &snapshot.firehose,
        &snapshot.rest_fallback,
        &snapshot.feed_poller,
        &snapshot.likes_bookmarks,
        &snapshot.nightly_sweep,
        &snapshot.media_downloader,
    ]
    .iter()
    .any(|s| s.status == Status::Error);

    let status = if any_error {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };

    (status, axum::Json(snapshot)).into_response()
}

// ---------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------

fn clamp_page_size(requested: Option<u32>) -> u32 {
    requested
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE)
}

/// Whether a request came from an `htmx` boosted link/form, in which case
/// handlers respond with just the swapped fragment instead of the full
/// page. Requests without this header (a plain link click, a browser
/// reload, or JS disabled entirely) always get the full page, which is
/// what makes pagination work with no JS at all.
///
/// History restores are the exception: htmx 4 services back/forward
/// navigation by re-fetching the URL and swapping the `[hx-history-elt]`
/// element (`<main>`) out of the response, which only exists in a full
/// page. Those requests carry `HX-Request` too, so the restore header
/// carves them back out to the full-page branch.
fn is_htmx_request(headers: &HeaderMap) -> bool {
    headers.contains_key("HX-Request") && !headers.contains_key("HX-History-Restore-Request")
}

/// Encodes an `at_uri` for use as a `/posts/:id` or `/media/.../:id/...`
/// path segment. Only non-alphanumeric characters need escaping to keep
/// the whole `at_uri` inside a single path segment (axum matches routes
/// on the raw, undecoded path, then percent-decodes each segment before
/// handing it to the handler).
pub(crate) fn encode_post_id(at_uri: &str) -> String {
    percent_encoding::utf8_percent_encode(at_uri, percent_encoding::NON_ALPHANUMERIC).to_string()
}

// ---------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------

#[derive(Debug)]
enum WebError {
    NotFound,
    BadRequest { message: String },
    Storage(StorageError),
}

impl From<StorageError> for WebError {
    fn from(err: StorageError) -> Self {
        WebError::Storage(err)
    }
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        match self {
            WebError::NotFound => (StatusCode::NOT_FOUND, "not found").into_response(),
            WebError::BadRequest { message } => (StatusCode::BAD_REQUEST, message).into_response(),
            WebError::Storage(err) => {
                tracing::error!(error = %err, "storage error serving web request");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
            }
        }
    }
}
