//! Vendored static assets for the web UI.
//!
//! Compiled straight into the binary (no build step, no CDN) so the single
//! Docker image doesn't need to ship a separate static file tree. All of
//! them are served with explicit cache lifetimes (see the constants below)
//! rather than relying on heuristic caching, which never kicks in without
//! a `Last-Modified` date.

use axum::http::header;
use axum::response::{IntoResponse, Response};

const PICO_CSS: &[u8] = include_bytes!("../../static/pico.min.css");
const APP_CSS: &[u8] = include_bytes!("../../static/app.css");
const HTMX_JS: &[u8] = include_bytes!("../../static/htmx.min.js");
const MANIFEST: &[u8] = include_bytes!("../../static/manifest.webmanifest");
const ICON_192: &[u8] = include_bytes!("../../static/icons/icon-192.png");
const ICON_512: &[u8] = include_bytes!("../../static/icons/icon-512.png");

/// Vendored assets are compiled into the binary and only change with a new
/// deploy, so they're served with explicit lifetimes instead of relying on
/// heuristic caching (which never kicks in without a `Last-Modified`).
/// Stylesheets/scripts get a day so a deploy is picked up promptly; icons
/// are effectively permanent and the manifest short, so PWA metadata
/// refreshes on the next visit.
const STATIC_CACHE_CONTROL: &str = "public, max-age=86400";
const ICON_CACHE_CONTROL: &str = "public, max-age=604800";
const MANIFEST_CACHE_CONTROL: &str = "public, max-age=3600";

fn static_response(
    content_type: &'static str,
    cache_control: &'static str,
    bytes: &'static [u8],
) -> Response {
    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, cache_control),
        ],
        bytes,
    )
        .into_response()
}

pub(super) async fn pico_css() -> Response {
    static_response("text/css; charset=utf-8", STATIC_CACHE_CONTROL, PICO_CSS)
}

pub(super) async fn app_css() -> Response {
    static_response("text/css; charset=utf-8", STATIC_CACHE_CONTROL, APP_CSS)
}

pub(super) async fn htmx_js() -> Response {
    static_response(
        "application/javascript; charset=utf-8",
        STATIC_CACHE_CONTROL,
        HTMX_JS,
    )
}

pub(super) async fn manifest() -> Response {
    static_response(
        "application/manifest+json",
        MANIFEST_CACHE_CONTROL,
        MANIFEST,
    )
}

pub(super) async fn icon_192() -> Response {
    static_response("image/png", ICON_CACHE_CONTROL, ICON_192)
}

pub(super) async fn icon_512() -> Response {
    static_response("image/png", ICON_CACHE_CONTROL, ICON_512)
}
