//! Application configuration: loading, validation, and defaulting of all
//! `BSKY_*` / `UI_*` / `ARCHIVE_*` / `MEDIA_*` environment variables into a
//! single typed config struct used by the rest of the application.

use std::fmt;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// Names of every environment variable this application reads, in the order
/// they appear in the canonical schema. Used by tests to clear/restore
/// process env state.
#[cfg(test)]
const ALL_VARS: &[&str] = &[
    "BSKY_IDENTIFIER",
    "BSKY_APP_PASSWORD",
    "BSKY_WATCH_HANDLES",
    "BSKY_WATCH_FEEDS",
    "ARCHIVE_DIR",
    "DATABASE_PATH",
    "UI_PASSWORD",
    "UI_SESSION_SECRET",
    "UI_PORT",
    "POLL_INTERVAL_SECONDS",
    "JETSTREAM_URL",
    "MEDIA_MAX_CONCURRENT_DOWNLOADS",
    "MEDIA_MAX_BYTES",
    "FEED_MAX_BYTES",
];

mod defaults {
    pub const ARCHIVE_DIR: &str = "/data/archive";
    pub const UI_PORT: u16 = 8080;
    pub const POLL_INTERVAL_SECONDS: u64 = 120;
    pub const JETSTREAM_URL: &str = "wss://jetstream1.us-east.bsky.network/subscribe";
    pub const MEDIA_MAX_CONCURRENT_DOWNLOADS: usize = 4;
    pub const MEDIA_MAX_BYTES: u64 = 104_857_600;
    /// 2 GiB, applied per configured feed.
    pub const FEED_MAX_BYTES: u64 = 2_147_483_648;
}

/// One configured custom feed, as parsed from a `BSKY_WATCH_FEEDS` entry but
/// *before* its handle segment (if any) has been resolved to a DID. `actor`
/// is the handle-or-DID taken verbatim from the URL/URI; resolution and
/// canonical-URI/slug derivation happen at startup ([`crate::app::init`]),
/// where a handle can be turned into the stable DID the feed is keyed by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedConfig {
    /// The original entry, verbatim, for error messages and the `/config`
    /// page.
    pub input: String,
    /// The feed generator's authority: a handle (to resolve) or a DID.
    pub actor: String,
    /// The feed generator record's rkey.
    pub rkey: String,
}

/// A configured feed after startup resolution: its stable slug and canonical
/// `at://` URI (both keyed on the generator's DID + rkey, never its display
/// name), plus a best-effort human-readable name fetched once via
/// `getFeedGenerator`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedFeed {
    pub slug: String,
    pub at_uri: String,
    /// The original `BSKY_WATCH_FEEDS` entry this came from.
    pub input: String,
    /// The generator's display name, or `None` if `getFeedGenerator` failed
    /// (non-fatal — the UI falls back to the slug).
    pub display_name: Option<String>,
}

impl ResolvedFeed {
    /// The name to show for this feed in the UI: its display name if known,
    /// otherwise its slug.
    pub fn label(&self) -> &str {
        self.display_name.as_deref().unwrap_or(&self.slug)
    }
}

/// The canonical `at://` URI of a feed generator record, given its DID and
/// rkey.
pub fn feed_at_uri(did: &str, rkey: &str) -> String {
    format!("at://{did}/app.bsky.feed.generator/{rkey}")
}

/// A stable, filesystem- and URL-safe slug for a feed generator, derived
/// only from its DID + rkey (so it survives a display-name change and a
/// handle change). A short readable prefix from the rkey aids recognition; a
/// hash suffix guarantees uniqueness and keeps the result safe by
/// construction (`[a-z0-9-]` only) even for exotic DIDs/rkeys.
pub fn feed_slug(did: &str, rkey: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(did.as_bytes());
    hasher.update(b"/");
    hasher.update(rkey.as_bytes());
    let digest = hasher.finalize();
    let mut hash = String::with_capacity(16);
    for byte in &digest[..8] {
        use std::fmt::Write as _;
        let _ = write!(hash, "{byte:02x}");
    }

    let readable: String = rkey
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(24)
        .collect::<String>()
        .to_ascii_lowercase();

    if readable.is_empty() {
        format!("feed-{hash}")
    } else {
        format!("{readable}-{hash}")
    }
}

/// A secret string value (app password, UI password, session signing key).
///
/// `Debug` is redacted so secrets never end up in logs, panics, or error
/// messages produced via `{:?}` formatting. There is deliberately no
/// `Display` impl.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Exposes the plaintext secret. Callers that use this must never log,
    /// `Display`, or persist the returned value — only pass it to the API
    /// that actually needs it (e.g. an HTTP auth header, cookie signing key).
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Self {
        Secret(value)
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// A specific, readable configuration error: which variable, and what was
/// wrong with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    MissingVar(&'static str),
    InvalidValue { var: &'static str, message: String },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::MissingVar(var) => {
                write!(f, "missing required environment variable {var}")
            }
            ConfigError::InvalidValue { var, message } => {
                write!(f, "invalid value for {var}: {message}")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub bsky_identifier: String,
    pub bsky_app_password: Secret,
    pub bsky_watch_handles: Vec<String>,
    pub watch_feeds: Vec<FeedConfig>,
    pub archive_dir: PathBuf,
    pub database_path: PathBuf,
    pub ui_password: Secret,
    pub ui_session_secret: Secret,
    pub ui_port: u16,
    pub poll_interval_seconds: u64,
    pub jetstream_url: url::Url,
    pub media_max_concurrent_downloads: usize,
    pub media_max_bytes: u64,
    pub feed_max_bytes: u64,
}

impl AppConfig {
    /// Loads configuration from the process environment, applying documented
    /// defaults to optional variables and validating everything.
    ///
    /// A local `.env` file (developer convenience) is loaded first via
    /// `dotenvy`, but real environment variables always take precedence over
    /// values from `.env` (dotenvy's default behavior: it never overwrites a
    /// variable that is already set).
    pub fn from_env() -> Result<Self, ConfigError> {
        match dotenvy::dotenv() {
            Ok(_) | Err(dotenvy::Error::Io(_)) => {}
            Err(err) => {
                tracing::warn!(error = %err, ".env file present but failed to load");
            }
        }
        Self::build()
    }

    fn build() -> Result<Self, ConfigError> {
        let bsky_identifier = require_var("BSKY_IDENTIFIER")?;
        let bsky_app_password = Secret::from(require_var("BSKY_APP_PASSWORD")?);
        let ui_password = Secret::from(require_var("UI_PASSWORD")?);
        let ui_session_secret = Secret::from(require_var("UI_SESSION_SECRET")?);

        let bsky_watch_handles = match optional_var("BSKY_WATCH_HANDLES") {
            Some(raw) => parse_handles(&raw)?,
            None => vec![bsky_identifier.clone()],
        };

        let watch_feeds = match optional_var("BSKY_WATCH_FEEDS") {
            Some(raw) => parse_feeds(&raw)?,
            None => Vec::new(),
        };

        let archive_dir = optional_var("ARCHIVE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(defaults::ARCHIVE_DIR));
        validate_archive_dir(&archive_dir)?;

        let database_path = optional_var("DATABASE_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| archive_dir.join("index.sqlite3"));

        let ui_port = match optional_var("UI_PORT") {
            Some(raw) => parse_port(&raw)?,
            None => defaults::UI_PORT,
        };

        let poll_interval_seconds = match optional_var("POLL_INTERVAL_SECONDS") {
            Some(raw) => parse_positive_u64("POLL_INTERVAL_SECONDS", &raw)?,
            None => defaults::POLL_INTERVAL_SECONDS,
        };

        let jetstream_url = match optional_var("JETSTREAM_URL") {
            Some(raw) => parse_websocket_url(&raw)?,
            None => url::Url::parse(defaults::JETSTREAM_URL)
                .expect("default JETSTREAM_URL is a valid URL"),
        };

        let media_max_concurrent_downloads = match optional_var("MEDIA_MAX_CONCURRENT_DOWNLOADS") {
            Some(raw) => parse_positive_usize("MEDIA_MAX_CONCURRENT_DOWNLOADS", &raw)?,
            None => defaults::MEDIA_MAX_CONCURRENT_DOWNLOADS,
        };

        let media_max_bytes = match optional_var("MEDIA_MAX_BYTES") {
            Some(raw) => parse_positive_u64("MEDIA_MAX_BYTES", &raw)?,
            None => defaults::MEDIA_MAX_BYTES,
        };

        let feed_max_bytes = match optional_var("FEED_MAX_BYTES") {
            Some(raw) => parse_positive_u64("FEED_MAX_BYTES", &raw)?,
            None => defaults::FEED_MAX_BYTES,
        };

        Ok(AppConfig {
            bsky_identifier,
            bsky_app_password,
            bsky_watch_handles,
            watch_feeds,
            archive_dir,
            database_path,
            ui_password,
            ui_session_secret,
            ui_port,
            poll_interval_seconds,
            jetstream_url,
            media_max_concurrent_downloads,
            media_max_bytes,
            feed_max_bytes,
        })
    }
}

fn optional_var(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn require_var(name: &'static str) -> Result<String, ConfigError> {
    optional_var(name).ok_or(ConfigError::MissingVar(name))
}

fn parse_handles(raw: &str) -> Result<Vec<String>, ConfigError> {
    let handles: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    if handles.is_empty() {
        return Err(ConfigError::InvalidValue {
            var: "BSKY_WATCH_HANDLES",
            message: "must contain at least one comma-separated handle".to_string(),
        });
    }
    Ok(handles)
}

/// Parses `BSKY_WATCH_FEEDS`: a comma-separated list of `bsky.app` feed URLs
/// and/or `at://…/app.bsky.feed.generator/<rkey>` URIs. Empty entries are
/// skipped; an unparseable entry fails with a message naming it. The list may
/// legitimately be empty (feeds are opt-in), unlike watch handles.
fn parse_feeds(raw: &str) -> Result<Vec<FeedConfig>, ConfigError> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(parse_feed_entry)
        .collect()
}

/// Parses one `BSKY_WATCH_FEEDS` entry into a [`FeedConfig`]. Accepts either a
/// `bsky.app` feed URL (`https://bsky.app/profile/<handle-or-did>/feed/<rkey>`)
/// or an `at://<did>/app.bsky.feed.generator/<rkey>` URI.
fn parse_feed_entry(entry: &str) -> Result<FeedConfig, ConfigError> {
    let invalid = |message: String| ConfigError::InvalidValue {
        var: "BSKY_WATCH_FEEDS",
        message,
    };

    if let Some(rest) = entry.strip_prefix("at://") {
        let mut parts = rest.splitn(3, '/');
        let authority = parts.next().unwrap_or("");
        let collection = parts.next().unwrap_or("");
        let rkey = parts.next().unwrap_or("");
        if authority.is_empty()
            || collection != "app.bsky.feed.generator"
            || rkey.is_empty()
            || rkey.contains('/')
        {
            return Err(invalid(format!(
                "{entry:?} is not a valid feed generator URI \
                 (expected at://<did>/app.bsky.feed.generator/<rkey>)"
            )));
        }
        return Ok(FeedConfig {
            input: entry.to_string(),
            actor: authority.to_string(),
            rkey: rkey.to_string(),
        });
    }

    let url = url::Url::parse(entry)
        .map_err(|_| invalid(format!("{entry:?} is not a valid feed URL or at:// URI")))?;
    let host = url.host_str().unwrap_or("");
    if host != "bsky.app" && !host.ends_with(".bsky.app") {
        return Err(invalid(format!(
            "{entry:?} is not a bsky.app feed URL or an at:// generator URI"
        )));
    }
    let segments: Vec<&str> = url
        .path_segments()
        .map(|s| s.filter(|seg| !seg.is_empty()).collect())
        .unwrap_or_default();
    match segments.as_slice() {
        ["profile", actor, "feed", rkey] if !actor.is_empty() && !rkey.is_empty() => {
            Ok(FeedConfig {
                input: entry.to_string(),
                actor: actor.to_string(),
                rkey: rkey.to_string(),
            })
        }
        _ => Err(invalid(format!(
            "{entry:?} is not a valid bsky.app feed URL \
             (expected https://bsky.app/profile/<handle>/feed/<rkey>)"
        ))),
    }
}

fn parse_port(raw: &str) -> Result<u16, ConfigError> {
    let port: u16 = raw.parse().map_err(|_| ConfigError::InvalidValue {
        var: "UI_PORT",
        message: format!("{raw:?} is not a valid port number (0-65535)"),
    })?;
    if port == 0 {
        return Err(ConfigError::InvalidValue {
            var: "UI_PORT",
            message: "port 0 is not a valid listen port".to_string(),
        });
    }
    Ok(port)
}

fn parse_positive_u64(var: &'static str, raw: &str) -> Result<u64, ConfigError> {
    let value: u64 = raw.parse().map_err(|_| ConfigError::InvalidValue {
        var,
        message: format!("{raw:?} is not a valid non-negative integer"),
    })?;
    if value == 0 {
        return Err(ConfigError::InvalidValue {
            var,
            message: "must be greater than 0".to_string(),
        });
    }
    Ok(value)
}

fn parse_positive_usize(var: &'static str, raw: &str) -> Result<usize, ConfigError> {
    let value: usize = raw.parse().map_err(|_| ConfigError::InvalidValue {
        var,
        message: format!("{raw:?} is not a valid non-negative integer"),
    })?;
    if value == 0 {
        return Err(ConfigError::InvalidValue {
            var,
            message: "must be greater than 0".to_string(),
        });
    }
    Ok(value)
}

fn parse_websocket_url(raw: &str) -> Result<url::Url, ConfigError> {
    let url = url::Url::parse(raw).map_err(|err| ConfigError::InvalidValue {
        var: "JETSTREAM_URL",
        message: format!("{raw:?} is not a valid URL: {err}"),
    })?;
    match url.scheme() {
        "ws" | "wss" => Ok(url),
        other => Err(ConfigError::InvalidValue {
            var: "JETSTREAM_URL",
            message: format!("scheme {other:?} is not a websocket scheme (expected ws/wss)"),
        }),
    }
}

fn validate_archive_dir(path: &Path) -> Result<(), ConfigError> {
    std::fs::create_dir_all(path).map_err(|err| ConfigError::InvalidValue {
        var: "ARCHIVE_DIR",
        message: format!("cannot create directory {}: {err}", path.display()),
    })?;

    let probe = path.join(".bsky-archiver-write-test");
    std::fs::write(&probe, b"ok").map_err(|err| ConfigError::InvalidValue {
        var: "ARCHIVE_DIR",
        message: format!("directory {} is not writable: {err}", path.display()),
    })?;
    let _ = std::fs::remove_file(&probe);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Environment variables are global process state. `cargo test` runs
    // tests in parallel threads by default, so two tests setting/clearing
    // `BSKY_IDENTIFIER` etc. at the same time would race and could observe
    // each other's values. `ENV_LOCK` serializes any test that touches env
    // vars, and `EnvGuard` snapshots + restores the prior value of every
    // variable it touches (including clearing every other known var, so a
    // stray variable set in the outer environment can't leak into a test).
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
        saved: Vec<(&'static str, Option<String>)>,
    }

    impl EnvGuard {
        /// Clears every known config var, then applies `overrides` on top.
        fn new(overrides: &[(&'static str, &str)]) -> Self {
            let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let saved = ALL_VARS
                .iter()
                .map(|&name| (name, std::env::var(name).ok()))
                .collect();

            for &name in ALL_VARS {
                // SAFETY: serialized by ENV_LOCK for the lifetime of this guard.
                unsafe { std::env::remove_var(name) };
            }
            for &(name, value) in overrides {
                // SAFETY: serialized by ENV_LOCK for the lifetime of this guard.
                unsafe { std::env::set_var(name, value) };
            }

            EnvGuard { _lock: lock, saved }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (name, value) in &self.saved {
                match value {
                    // SAFETY: serialized by ENV_LOCK for the lifetime of this guard.
                    Some(v) => unsafe { std::env::set_var(name, v) },
                    None => unsafe { std::env::remove_var(name) },
                }
            }
        }
    }

    fn temp_dir(label: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "bsky-archiver-config-test-{label}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        dir
    }

    fn required_vars(archive_dir: &Path) -> Vec<(&'static str, String)> {
        vec![
            ("BSKY_IDENTIFIER", "alice.bsky.social".to_string()),
            ("BSKY_APP_PASSWORD", "app-password-secret".to_string()),
            ("UI_PASSWORD", "ui-password-secret".to_string()),
            ("UI_SESSION_SECRET", "session-secret".to_string()),
            ("ARCHIVE_DIR", archive_dir.to_string_lossy().into_owned()),
        ]
    }

    #[test]
    fn defaults_applied_when_optional_vars_missing() {
        let dir = temp_dir("defaults");
        let required = required_vars(&dir);
        let overrides: Vec<(&'static str, &str)> =
            required.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let _guard = EnvGuard::new(&overrides);

        let config = AppConfig::from_env().expect("config should load with only required vars");

        assert_eq!(config.bsky_identifier, "alice.bsky.social");
        assert_eq!(config.bsky_watch_handles, vec!["alice.bsky.social"]);
        assert_eq!(config.database_path, dir.join("index.sqlite3"));
        assert_eq!(config.ui_port, defaults::UI_PORT);
        assert_eq!(
            config.poll_interval_seconds,
            defaults::POLL_INTERVAL_SECONDS
        );
        assert_eq!(config.jetstream_url.as_str(), defaults::JETSTREAM_URL);
        assert_eq!(
            config.media_max_concurrent_downloads,
            defaults::MEDIA_MAX_CONCURRENT_DOWNLOADS
        );
        assert_eq!(config.media_max_bytes, defaults::MEDIA_MAX_BYTES);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_required_var_produces_specific_error() {
        let dir = temp_dir("missing-required");
        let mut required = required_vars(&dir);
        required.retain(|(k, _)| *k != "BSKY_APP_PASSWORD");
        let overrides: Vec<(&'static str, &str)> =
            required.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let _guard = EnvGuard::new(&overrides);

        let err = AppConfig::from_env().expect_err("missing required var should fail");
        assert_eq!(err, ConfigError::MissingVar("BSKY_APP_PASSWORD"));
        assert_eq!(
            err.to_string(),
            "missing required environment variable BSKY_APP_PASSWORD"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn invalid_ui_port_produces_specific_error() {
        let dir = temp_dir("invalid-port");
        let required = required_vars(&dir);
        let mut overrides: Vec<(&'static str, &str)> =
            required.iter().map(|(k, v)| (*k, v.as_str())).collect();
        overrides.push(("UI_PORT", "not-a-port"));
        let _guard = EnvGuard::new(&overrides);

        let err = AppConfig::from_env().expect_err("invalid UI_PORT should fail");
        match err {
            ConfigError::InvalidValue { var, .. } => assert_eq!(var, "UI_PORT"),
            other => panic!("expected InvalidValue, got {other:?}"),
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn zero_ui_port_is_invalid() {
        let dir = temp_dir("zero-port");
        let required = required_vars(&dir);
        let mut overrides: Vec<(&'static str, &str)> =
            required.iter().map(|(k, v)| (*k, v.as_str())).collect();
        overrides.push(("UI_PORT", "0"));
        let _guard = EnvGuard::new(&overrides);

        let err = AppConfig::from_env().expect_err("port 0 should fail");
        match err {
            ConfigError::InvalidValue { var, .. } => assert_eq!(var, "UI_PORT"),
            other => panic!("expected InvalidValue, got {other:?}"),
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn invalid_poll_interval_produces_specific_error() {
        let dir = temp_dir("invalid-poll-interval");
        let required = required_vars(&dir);
        let mut overrides: Vec<(&'static str, &str)> =
            required.iter().map(|(k, v)| (*k, v.as_str())).collect();
        overrides.push(("POLL_INTERVAL_SECONDS", "0"));
        let _guard = EnvGuard::new(&overrides);

        let err = AppConfig::from_env().expect_err("zero poll interval should fail");
        match err {
            ConfigError::InvalidValue { var, .. } => assert_eq!(var, "POLL_INTERVAL_SECONDS"),
            other => panic!("expected InvalidValue, got {other:?}"),
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn invalid_media_max_concurrent_downloads_produces_specific_error() {
        let dir = temp_dir("invalid-media-concurrency");
        let required = required_vars(&dir);
        let mut overrides: Vec<(&'static str, &str)> =
            required.iter().map(|(k, v)| (*k, v.as_str())).collect();
        overrides.push(("MEDIA_MAX_CONCURRENT_DOWNLOADS", "-1"));
        let _guard = EnvGuard::new(&overrides);

        let err = AppConfig::from_env().expect_err("negative concurrency should fail");
        match err {
            ConfigError::InvalidValue { var, .. } => {
                assert_eq!(var, "MEDIA_MAX_CONCURRENT_DOWNLOADS")
            }
            other => panic!("expected InvalidValue, got {other:?}"),
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn invalid_media_max_bytes_produces_specific_error() {
        let dir = temp_dir("invalid-media-bytes");
        let required = required_vars(&dir);
        let mut overrides: Vec<(&'static str, &str)> =
            required.iter().map(|(k, v)| (*k, v.as_str())).collect();
        overrides.push(("MEDIA_MAX_BYTES", "huge"));
        let _guard = EnvGuard::new(&overrides);

        let err = AppConfig::from_env().expect_err("non-numeric media bytes should fail");
        match err {
            ConfigError::InvalidValue { var, .. } => assert_eq!(var, "MEDIA_MAX_BYTES"),
            other => panic!("expected InvalidValue, got {other:?}"),
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn invalid_jetstream_url_produces_specific_error() {
        let dir = temp_dir("invalid-jetstream-url");
        let required = required_vars(&dir);
        let mut overrides: Vec<(&'static str, &str)> =
            required.iter().map(|(k, v)| (*k, v.as_str())).collect();
        overrides.push(("JETSTREAM_URL", "not a url"));
        let _guard = EnvGuard::new(&overrides);

        let err = AppConfig::from_env().expect_err("malformed JETSTREAM_URL should fail");
        match err {
            ConfigError::InvalidValue { var, .. } => assert_eq!(var, "JETSTREAM_URL"),
            other => panic!("expected InvalidValue, got {other:?}"),
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn non_websocket_jetstream_url_is_invalid() {
        let dir = temp_dir("http-jetstream-url");
        let required = required_vars(&dir);
        let mut overrides: Vec<(&'static str, &str)> =
            required.iter().map(|(k, v)| (*k, v.as_str())).collect();
        overrides.push(("JETSTREAM_URL", "https://example.com/subscribe"));
        let _guard = EnvGuard::new(&overrides);

        let err = AppConfig::from_env().expect_err("http(s) JETSTREAM_URL should fail");
        match err {
            ConfigError::InvalidValue { var, .. } => assert_eq!(var, "JETSTREAM_URL"),
            other => panic!("expected InvalidValue, got {other:?}"),
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn blank_watch_handles_is_invalid() {
        let dir = temp_dir("blank-watch-handles");
        let required = required_vars(&dir);
        let mut overrides: Vec<(&'static str, &str)> =
            required.iter().map(|(k, v)| (*k, v.as_str())).collect();
        overrides.push(("BSKY_WATCH_HANDLES", " , , "));
        let _guard = EnvGuard::new(&overrides);

        let err = AppConfig::from_env().expect_err("blank handle list should fail");
        match err {
            ConfigError::InvalidValue { var, .. } => assert_eq!(var, "BSKY_WATCH_HANDLES"),
            other => panic!("expected InvalidValue, got {other:?}"),
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn archive_dir_not_creatable_produces_specific_error() {
        // Create a plain file, then ask for an ARCHIVE_DIR *inside* that
        // file's path: `create_dir_all` cannot possibly succeed, since a
        // path component that must be a directory is actually a file.
        let blocking_file = temp_dir("archive-dir-blocker-file");
        std::fs::write(&blocking_file, b"not a directory").unwrap();
        let impossible_dir = blocking_file.join("archive");

        let required = required_vars(&impossible_dir);
        let overrides: Vec<(&'static str, &str)> =
            required.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let _guard = EnvGuard::new(&overrides);

        let err = AppConfig::from_env().expect_err("uncreatable ARCHIVE_DIR should fail");
        match err {
            ConfigError::InvalidValue { var, .. } => assert_eq!(var, "ARCHIVE_DIR"),
            other => panic!("expected InvalidValue, got {other:?}"),
        }

        let _ = std::fs::remove_file(&blocking_file);
    }

    #[test]
    fn custom_watch_handles_are_parsed_and_trimmed() {
        let dir = temp_dir("custom-watch-handles");
        let required = required_vars(&dir);
        let mut overrides: Vec<(&'static str, &str)> =
            required.iter().map(|(k, v)| (*k, v.as_str())).collect();
        overrides.push((
            "BSKY_WATCH_HANDLES",
            "alice.bsky.social, bob.bsky.social ,carol.bsky.social",
        ));
        let _guard = EnvGuard::new(&overrides);

        let config = AppConfig::from_env().expect("config should load");
        assert_eq!(
            config.bsky_watch_handles,
            vec!["alice.bsky.social", "bob.bsky.social", "carol.bsky.social"]
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn feed_urls_and_at_uris_parse() {
        let url = parse_feed_entry("https://bsky.app/profile/goose.art/feed/aaaf2pqeodmpy")
            .expect("bsky.app feed URL");
        assert_eq!(url.actor, "goose.art");
        assert_eq!(url.rkey, "aaaf2pqeodmpy");

        let url_did = parse_feed_entry("https://bsky.app/profile/did:plc:abc123/feed/whats-hot")
            .expect("bsky.app feed URL with a DID actor");
        assert_eq!(url_did.actor, "did:plc:abc123");
        assert_eq!(url_did.rkey, "whats-hot");

        let at = parse_feed_entry("at://did:plc:xyz789/app.bsky.feed.generator/cats")
            .expect("at:// generator URI");
        assert_eq!(at.actor, "did:plc:xyz789");
        assert_eq!(at.rkey, "cats");
    }

    #[test]
    fn invalid_feed_entries_are_rejected_naming_the_entry() {
        for bad in [
            "not a url at all",
            "https://example.com/profile/goose.art/feed/x", // wrong host
            "https://bsky.app/profile/goose.art",           // missing feed/rkey
            "https://bsky.app/profile/goose.art/feed/",     // empty rkey
            "at://did:plc:xyz/app.bsky.feed.post/x",        // wrong collection
            "at://did:plc:xyz/app.bsky.feed.generator/",    // empty rkey
            "at:///app.bsky.feed.generator/x",              // empty authority
        ] {
            let err = parse_feed_entry(bad).expect_err(bad);
            match err {
                ConfigError::InvalidValue { var, message } => {
                    assert_eq!(var, "BSKY_WATCH_FEEDS");
                    assert!(
                        message.contains(bad),
                        "message should name the offending entry: {message}"
                    );
                }
                other => panic!("expected InvalidValue, got {other:?}"),
            }
        }
    }

    #[test]
    fn feed_slug_is_stable_and_unique_and_safe() {
        let a = feed_slug("did:plc:abc", "aaaf2pqeodmpy");
        // Stable across repeated derivation (a display-name change never
        // touches the inputs, so the slug can't change).
        assert_eq!(a, feed_slug("did:plc:abc", "aaaf2pqeodmpy"));
        // A different DID or rkey yields a different slug.
        assert_ne!(a, feed_slug("did:plc:def", "aaaf2pqeodmpy"));
        assert_ne!(a, feed_slug("did:plc:abc", "different"));
        // Safe by construction: only URL/path-safe characters.
        assert!(a.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'));

        // An rkey with unsafe characters still produces a safe slug.
        let weird = feed_slug("did:web:example.com:8443", "../../etc/passwd");
        assert!(weird.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'));
        assert!(!weird.contains('/'));
        assert!(!weird.contains('.'));
    }

    #[test]
    fn feed_defaults_and_multiple_entries() {
        let dir = temp_dir("feeds-config");
        let required = required_vars(&dir);
        let mut overrides: Vec<(&'static str, &str)> =
            required.iter().map(|(k, v)| (*k, v.as_str())).collect();
        overrides.push((
            "BSKY_WATCH_FEEDS",
            "https://bsky.app/profile/goose.art/feed/aaaf2pqeodmpy, \
             at://did:plc:xyz/app.bsky.feed.generator/cats",
        ));
        let _guard = EnvGuard::new(&overrides);

        let config = AppConfig::from_env().expect("config loads with feeds");
        assert_eq!(config.watch_feeds.len(), 2);
        assert_eq!(config.watch_feeds[0].actor, "goose.art");
        assert_eq!(config.watch_feeds[1].rkey, "cats");
        // FEED_MAX_BYTES defaults to 2 GiB.
        assert_eq!(config.feed_max_bytes, 2_147_483_648);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn no_feeds_configured_is_valid_and_empty() {
        let dir = temp_dir("no-feeds");
        let required = required_vars(&dir);
        let overrides: Vec<(&'static str, &str)> =
            required.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let _guard = EnvGuard::new(&overrides);

        let config = AppConfig::from_env().expect("config loads without feeds");
        assert!(config.watch_feeds.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn secrets_are_redacted_in_debug_output() {
        let secret = Secret::from("super-secret-value".to_string());
        let debug_output = format!("{secret:?}");
        assert_eq!(debug_output, "<redacted>");
        assert!(!debug_output.contains("super-secret-value"));
        assert_eq!(secret.expose_secret(), "super-secret-value");
    }

    #[test]
    fn app_config_debug_output_redacts_all_secrets() {
        let dir = temp_dir("debug-redaction");
        let required = required_vars(&dir);
        let overrides: Vec<(&'static str, &str)> =
            required.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let _guard = EnvGuard::new(&overrides);

        let config = AppConfig::from_env().expect("config should load");
        let debug_output = format!("{config:?}");

        assert!(!debug_output.contains("app-password-secret"));
        assert!(!debug_output.contains("ui-password-secret"));
        assert!(!debug_output.contains("session-secret"));
        assert_eq!(debug_output.matches("<redacted>").count(), 3);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
