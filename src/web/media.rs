//! Raw media file bytes (`/media/...`) with long-lived cache headers and
//! `ETag` revalidation, so browsers can cache every image and video.

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use tokio_util::io::ReaderStream;

use super::{WebError, WebState};
use crate::storage::Category;

/// Whether a request's `If-None-Match` header matches `etag` (a quoted
/// strong ETag). Handles comma-separated candidate lists, the `*`
/// any-match form, and the `W/` weak comparison prefix. Browsers revalidate
/// a stale cached media file with this header; answering `304` is what
/// makes revalidation cheap instead of a full re-download.
fn if_none_match_matches(headers: &HeaderMap, etag: &str) -> bool {
    let header = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let weak = format!("W/{etag}");
    header.split(',').any(|candidate| {
        let candidate = candidate.trim();
        candidate == "*" || candidate == etag || candidate == weak
    })
}

pub(super) async fn media_file(
    State(state): State<WebState>,
    Path((category, id, filename)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Response, WebError> {
    let category: Category = category.parse().map_err(|_| WebError::NotFound)?;
    let file = state
        .app
        .store
        .open_media(category, &id, &filename)
        .await?
        .ok_or(WebError::NotFound)?;

    let metadata = file
        .metadata()
        .await
        .map_err(|e| WebError::Storage(e.into()))?;
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs());
    let etag = format!("\"{}-{}\"", mtime.unwrap_or(0), metadata.len());
    let etag_value = header::HeaderValue::from_str(&etag)
        .unwrap_or_else(|_| header::HeaderValue::from_static("\"\""));
    let cache_control = header::HeaderValue::from_static("public, max-age=31536000, immutable");

    // Revalidation: the cache lifetime has lapsed (or the entry was evicted
    // but its metadata kept) and the browser asks whether its copy is still
    // fresh. A matching `If-None-Match` gets the bodyless 304 — with fresh
    // lifetime headers, per RFC 9110's 304 guidance — instead of the bytes.
    if if_none_match_matches(&headers, &etag) {
        let mut response = StatusCode::NOT_MODIFIED.into_response();
        let response_headers = response.headers_mut();
        response_headers.insert(header::CACHE_CONTROL, cache_control);
        response_headers.insert(header::ETAG, etag_value);
        return Ok(response);
    }

    let mime = mime_guess::from_path(&filename).first_or_octet_stream();
    let content_type = header::HeaderValue::from_str(mime.as_ref())
        .unwrap_or_else(|_| header::HeaderValue::from_static("application/octet-stream"));

    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    let mut response = body.into_response();
    let response_headers = response.headers_mut();
    response_headers.insert(header::CONTENT_TYPE, content_type);
    response_headers.insert(header::CACHE_CONTROL, cache_control);
    // Streaming a body of unknown length would force chunked framing, which
    // some browsers treat worse than a sized response; the length is known
    // up front, so say so.
    if let Ok(length) = header::HeaderValue::from_str(&metadata.len().to_string()) {
        response_headers.insert(header::CONTENT_LENGTH, length);
    }
    response_headers.insert(header::ETAG, etag_value);
    Ok(response)
}
