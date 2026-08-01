//! Application configuration: loading, validation, and defaulting of all
//! `BSKY_*` / `UI_*` / `ARCHIVE_*` / `MEDIA_*` environment variables into a
//! single typed config struct used by the rest of the application.

use std::fmt;
use std::path::{Path, PathBuf};

/// Names of every environment variable this application reads, in the order
/// they appear in the canonical schema. Used by tests to clear/restore
/// process env state.
#[cfg(test)]
const ALL_VARS: &[&str] = &[
    "BSKY_IDENTIFIER",
    "BSKY_APP_PASSWORD",
    "BSKY_WATCH_HANDLES",
    "ARCHIVE_DIR",
    "DATABASE_PATH",
    "UI_PASSWORD",
    "UI_SESSION_SECRET",
    "UI_PORT",
    "POLL_INTERVAL_SECONDS",
    "JETSTREAM_URL",
    "MEDIA_MAX_CONCURRENT_DOWNLOADS",
    "MEDIA_MAX_BYTES",
];

mod defaults {
    pub const ARCHIVE_DIR: &str = "/data/archive";
    pub const UI_PORT: u16 = 8080;
    pub const POLL_INTERVAL_SECONDS: u64 = 120;
    pub const JETSTREAM_URL: &str = "wss://jetstream1.us-east.bsky.network/subscribe";
    pub const MEDIA_MAX_CONCURRENT_DOWNLOADS: usize = 4;
    pub const MEDIA_MAX_BYTES: u64 = 104_857_600;
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
    pub archive_dir: PathBuf,
    pub database_path: PathBuf,
    pub ui_password: Secret,
    pub ui_session_secret: Secret,
    pub ui_port: u16,
    pub poll_interval_seconds: u64,
    pub jetstream_url: url::Url,
    pub media_max_concurrent_downloads: usize,
    pub media_max_bytes: u64,
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

        Ok(AppConfig {
            bsky_identifier,
            bsky_app_password,
            bsky_watch_handles,
            archive_dir,
            database_path,
            ui_password,
            ui_session_secret,
            ui_port,
            poll_interval_seconds,
            jetstream_url,
            media_max_concurrent_downloads,
            media_max_bytes,
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
