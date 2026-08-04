//! View models + `askama` `Template` types for the web UI (AR-11).
//!
//! [`crate::web`] builds these from [`crate::storage`]/[`crate::health`]
//! data and renders them via `askama_axum::into_response`; this module
//! owns presentation only (excerpting, badge classes, media URLs) — never
//! storage access or auth logic.

use askama::Template;

use crate::health::{Status, SubsystemHealth};
use crate::storage::{Category, MediaSummary, PostSummary};

/// The running build's package version, surfaced on `/healthz` and in the
/// web UI footer so ops can tell which build is deployed.
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

/// How many characters of a post's text are shown in list views before
/// truncating with an ellipsis.
const MAX_EXCERPT_CHARS: usize = 220;

// ---------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Pagination {
    pub page: u32,
    pub total_pages: u32,
    pub total_items: u64,
    pub prev_href: Option<String>,
    pub next_href: Option<String>,
    pub page_links: Vec<PageLink>,
}

#[derive(Debug, Clone)]
pub struct PageLink {
    pub number: u32,
    pub href: String,
    pub current: bool,
}

/// Builds pagination view data (prev/next links plus a small window of
/// numbered page links around the current page) from a `page -> href`
/// builder, so callers can plug in whatever other query params
/// (category filter, page size) need to survive across pages.
pub fn build_pagination(
    page: u32,
    total_pages: u32,
    total_items: u64,
    href_for: impl Fn(u32) -> String,
) -> Pagination {
    let prev_href = (page > 1).then(|| href_for(page - 1));
    let next_href = (page < total_pages).then(|| href_for(page + 1));

    const WINDOW: u32 = 2;
    let page_links = if total_pages == 0 {
        Vec::new()
    } else {
        let start = page.saturating_sub(WINDOW).max(1);
        let end = page.saturating_add(WINDOW).min(total_pages);
        (start..=end)
            .map(|n| PageLink {
                number: n,
                href: href_for(n),
                current: n == page,
            })
            .collect()
    };

    Pagination {
        page,
        total_pages,
        total_items,
        prev_href,
        next_href,
        page_links,
    }
}

// ---------------------------------------------------------------------
// Shared formatting helpers
// ---------------------------------------------------------------------

pub fn category_label(category: &Category) -> &'static str {
    match category {
        Category::Post => "Post",
        Category::Like => "Like",
        Category::Bookmark => "Bookmark",
        Category::Feed(_) => "Feed",
    }
}

pub fn category_badge_class(category: &Category) -> &'static str {
    match category {
        Category::Post => "badge-post",
        Category::Like => "badge-like",
        Category::Bookmark => "badge-bookmark",
        Category::Feed(_) => "badge-feed",
    }
}

/// The URL this app serves a stored media file's raw bytes at (see
/// `crate::web`'s `/media/:category/:id/:filename` route). The category is
/// percent-encoded so a feed category (`feeds/<slug>`, which contains a `/`)
/// stays inside a single path segment; axum decodes it back before matching.
pub fn media_url(category: &Category, post_at_uri: &str, filename: &str) -> String {
    format!(
        "/media/{}/{}/{}",
        percent_encoding::utf8_percent_encode(
            &category.as_dir(),
            percent_encoding::NON_ALPHANUMERIC
        ),
        crate::web::encode_post_id(post_at_uri),
        percent_encoding::utf8_percent_encode(filename, percent_encoding::NON_ALPHANUMERIC)
    )
}

pub fn is_video_content_type(content_type: Option<&str>) -> bool {
    content_type.is_some_and(|ct| ct.starts_with("video/"))
}

/// Extracts non-empty `text` from an AT Proto record JSON value, if any.
pub fn record_text(record: &serde_json::Value) -> Option<&str> {
    record
        .get("text")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
}

/// Truncates `text` to at most `max_chars` characters (counted, not
/// bytes, so multi-byte UTF-8 is never split mid-codepoint), appending an
/// ellipsis if anything was cut.
pub fn excerpt(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let truncated: String = text.chars().take(max_chars).collect();
    format!("{truncated}\u{2026}")
}

/// The DID embedded in an `at://did:.../collection/rkey` URI, used as a
/// stand-in "author" display since summaries don't carry a resolved
/// handle.
pub fn author_did_from_at_uri(at_uri: &str) -> &str {
    at_uri
        .strip_prefix("at://")
        .and_then(|rest| rest.split('/').next())
        .unwrap_or(at_uri)
}

/// The public `bsky.app` URL for an authored post's `at_uri`, if it's
/// actually a feed post (likes/bookmarks reference someone else's post
/// the same way, so this works for all three categories).
pub fn bluesky_post_url(at_uri: &str) -> Option<String> {
    let rest = at_uri.strip_prefix("at://")?;
    let mut parts = rest.splitn(3, '/');
    let did = parts.next()?;
    let collection = parts.next()?;
    let rkey = parts.next()?;
    if collection != "app.bsky.feed.post" || rkey.is_empty() {
        return None;
    }
    Some(format!("https://bsky.app/profile/{did}/post/{rkey}"))
}

// ---------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------

pub struct SubsystemRow {
    pub name: String,
    pub status_class: &'static str,
    pub status_text: &'static str,
    pub detail: Option<String>,
}

pub fn subsystem_row(name: impl Into<String>, health: &SubsystemHealth) -> SubsystemRow {
    let (status_class, status_text) = match health.status {
        Status::Connected => ("status-connected", "Connected"),
        Status::Degraded => ("status-degraded", "Degraded"),
        Status::Error => ("status-error", "Error"),
    };
    SubsystemRow {
        name: name.into(),
        status_class,
        status_text,
        detail: health.detail.clone(),
    }
}

#[derive(Template)]
#[template(path = "dashboard.html")]
pub struct DashboardTemplate {
    pub version: &'static str,
    pub posts_count: u64,
    pub likes_count: u64,
    pub bookmarks_count: u64,
    pub health: Vec<SubsystemRow>,
    pub recent: Vec<PostRow>,
}

// ---------------------------------------------------------------------
// Posts list + detail
// ---------------------------------------------------------------------

pub struct MediaThumb {
    pub url: String,
    pub is_video: bool,
    pub alt: String,
}

pub struct PostRow {
    pub category_label: &'static str,
    pub category_badge_class: &'static str,
    pub author: String,
    pub excerpt: String,
    pub detail_href: String,
    pub indexed_at: String,
    pub media_count: u32,
    pub thumbnail: Option<MediaThumb>,
}

/// Builds a [`PostRow`] from an index-layer [`PostSummary`] plus the
/// item's text (fetched separately by the caller, best-effort, since the
/// index deliberately doesn't store full record bodies).
pub fn post_row(summary: &PostSummary, text: Option<&str>) -> PostRow {
    PostRow {
        category_label: category_label(&summary.category),
        category_badge_class: category_badge_class(&summary.category),
        author: author_did_from_at_uri(&summary.at_uri).to_string(),
        excerpt: text
            .map(|t| excerpt(t, MAX_EXCERPT_CHARS))
            .unwrap_or_default(),
        detail_href: format!("/posts/{}", crate::web::encode_post_id(&summary.at_uri)),
        indexed_at: summary.indexed_at.clone(),
        media_count: summary.media_count,
        thumbnail: summary
            .thumbnail_filename
            .as_deref()
            .map(|filename| MediaThumb {
                url: media_url(&summary.category, &summary.at_uri, filename),
                is_video: is_video_content_type(summary.thumbnail_content_type.as_deref()),
                alt: format!(
                    "Media attached to this {}",
                    category_label(&summary.category)
                ),
            }),
    }
}

pub struct CategoryOption {
    pub label: String,
    pub href: String,
    pub selected: bool,
}

#[derive(Template)]
#[template(path = "posts.html")]
pub struct PostsTemplate {
    pub rows: Vec<PostRow>,
    pub pagination: Pagination,
    pub category_options: Vec<CategoryOption>,
}

#[derive(Template)]
#[template(path = "posts_list.html")]
pub struct PostsListTemplate {
    pub rows: Vec<PostRow>,
    pub pagination: Pagination,
}

pub struct PostMedia {
    pub url: String,
    pub is_video: bool,
    pub content_type: String,
    pub alt: String,
}

#[derive(Template)]
#[template(path = "post_detail.html")]
pub struct PostDetailTemplate {
    pub category_label: &'static str,
    pub category_badge_class: &'static str,
    pub author: String,
    pub bluesky_url: Option<String>,
    pub text: Option<String>,
    pub indexed_at: String,
    pub media: Vec<PostMedia>,
    pub raw_json: String,
}

// ---------------------------------------------------------------------
// Gallery
// ---------------------------------------------------------------------

pub struct GalleryItem {
    pub thumb_url: String,
    pub full_url: String,
    pub is_video: bool,
    pub post_href: String,
    pub alt: String,
}

pub fn gallery_item(summary: &MediaSummary) -> GalleryItem {
    let url = media_url(&summary.category, &summary.post_at_uri, &summary.filename);
    GalleryItem {
        thumb_url: url.clone(),
        full_url: url,
        is_video: is_video_content_type(summary.content_type.as_deref()),
        post_href: format!(
            "/posts/{}",
            crate::web::encode_post_id(&summary.post_at_uri)
        ),
        alt: format!(
            "{} media from {}",
            category_label(&summary.category),
            author_did_from_at_uri(&summary.post_at_uri)
        ),
    }
}

/// Human-readable byte size using decimal (SI) units, e.g. `1.8 GB`, so the
/// gallery's estimate and warning read the way a user expects ("about 1.8
/// GB"). Whole bytes are shown without a decimal; larger units get one.
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1000.0 && unit < UNITS.len() - 1 {
        value /= 1000.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[0])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// The gallery's export panel: the current selection's image count and
/// size, the download link, and (over the soft threshold) a warning.
pub struct GalleryExport {
    pub image_count: u64,
    pub size_label: String,
    pub href: String,
    pub warning: Option<String>,
}

#[derive(Template)]
#[template(path = "gallery.html")]
pub struct GalleryTemplate {
    pub items: Vec<GalleryItem>,
    pub pagination: Pagination,
    pub category_options: Vec<CategoryOption>,
    pub export: GalleryExport,
}

#[derive(Template)]
#[template(path = "gallery_grid.html")]
pub struct GalleryGridTemplate {
    pub items: Vec<GalleryItem>,
    pub pagination: Pagination,
}

// ---------------------------------------------------------------------
// Login
// ---------------------------------------------------------------------

#[derive(Template)]
#[template(path = "login.html")]
pub struct LoginTemplate<'a> {
    pub error: Option<&'a str>,
}

// ---------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------

pub struct ConfigRow {
    pub key: &'static str,
    pub value: String,
    pub redacted: bool,
}

/// One configured custom feed, as shown on `/config`: its resolved URI,
/// display name, per-feed cap, and current bytes used.
pub struct ConfigFeedRow {
    pub name: String,
    pub input: String,
    pub uri: String,
    pub cap: String,
    pub used: String,
}

#[derive(Template)]
#[template(path = "config.html")]
pub struct ConfigTemplate {
    pub version: &'static str,
    pub rows: Vec<ConfigRow>,
    pub feeds: Vec<ConfigFeedRow>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn excerpt_leaves_short_text_untouched() {
        assert_eq!(excerpt("hello", 220), "hello");
    }

    #[test]
    fn excerpt_truncates_on_char_boundaries_with_ellipsis() {
        let text = "a".repeat(10);
        let result = excerpt(&text, 5);
        assert_eq!(result, format!("{}\u{2026}", "a".repeat(5)));
    }

    #[test]
    fn excerpt_never_splits_a_multibyte_codepoint() {
        let text = "é".repeat(10);
        let result = excerpt(&text, 3);
        assert_eq!(result.chars().count(), 4);
        assert!(result.starts_with("ééé"));
    }

    #[test]
    fn author_did_from_at_uri_extracts_the_did() {
        assert_eq!(
            author_did_from_at_uri("at://did:plc:alice/app.bsky.feed.post/abc"),
            "did:plc:alice"
        );
        assert_eq!(author_did_from_at_uri("not-an-at-uri"), "not-an-at-uri");
    }

    #[test]
    fn bluesky_post_url_builds_for_feed_posts_only() {
        assert_eq!(
            bluesky_post_url("at://did:plc:alice/app.bsky.feed.post/abc"),
            Some("https://bsky.app/profile/did:plc:alice/post/abc".to_string())
        );
        assert_eq!(
            bluesky_post_url("at://did:plc:alice/app.bsky.feed.like/abc"),
            None
        );
    }

    #[test]
    fn build_pagination_has_no_prev_on_first_page_no_next_on_last() {
        let p = build_pagination(1, 3, 25, |n| format!("/posts?page={n}"));
        assert!(p.prev_href.is_none());
        assert!(p.next_href.is_some());

        let last = build_pagination(3, 3, 25, |n| format!("/posts?page={n}"));
        assert!(last.prev_href.is_some());
        assert!(last.next_href.is_none());
    }

    #[test]
    fn format_bytes_uses_decimal_units() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1_500), "1.5 KB");
        assert_eq!(format_bytes(2_500_000), "2.5 MB");
        assert_eq!(format_bytes(1_800_000_000), "1.8 GB");
    }

    #[test]
    fn build_pagination_with_zero_pages_has_no_links() {
        let p = build_pagination(1, 0, 0, |n| format!("/posts?page={n}"));
        assert!(p.prev_href.is_none());
        assert!(p.next_href.is_none());
        assert!(p.page_links.is_empty());
    }
}
