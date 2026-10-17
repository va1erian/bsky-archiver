//! Session state behind [`super::BlueskyClient`]: the token pair a login or
//! refresh yields, its persistence across restarts, and the cooldown that
//! keeps a rate-limited `createSession` from being hammered.
//!
//! Bluesky allows only a handful of `createSession` logins per account per
//! day (10 on `bsky.social`), while an access token lives about two hours. A
//! client that logs in again on every expiry or restart therefore exhausts
//! the budget and locks itself out for up to a day. So the client logs in
//! once, renews with `refreshSession` (which has no such budget), and keeps
//! the token pair on disk so a restart resumes the same session.

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tracing::warn;

/// How long to stop calling `createSession` after a 429 that carried no
/// retry hint.
const DEFAULT_LOGIN_COOLDOWN: Duration = Duration::from_secs(5 * 60);

/// Upper bound on a hinted login cooldown, so a bogus header can't disable
/// logins indefinitely. Matches the longest rate-limit window Bluesky uses.
const MAX_LOGIN_COOLDOWN: Duration = Duration::from_secs(24 * 60 * 60);

/// The token pair and account DID returned by `createSession` and
/// `refreshSession` (their responses share this shape; extra fields are
/// ignored). Deliberately not `Debug`, so tokens can't leak into logs.
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Session {
    #[serde(rename = "accessJwt")]
    pub access_jwt: String,
    #[serde(rename = "refreshJwt")]
    pub refresh_jwt: String,
    pub did: String,
}

/// The on-disk form of a [`Session`], tagged with the identifier it was
/// issued to so a changed `BSKY_IDENTIFIER` never resumes another
/// account's session.
#[derive(Serialize, Deserialize)]
struct PersistedSession {
    identifier: String,
    #[serde(flatten)]
    session: Session,
}

/// Where the client keeps its session between restarts.
pub(super) struct SessionFile {
    path: PathBuf,
    identifier: String,
}

impl SessionFile {
    pub fn new(path: PathBuf, identifier: String) -> Self {
        Self { path, identifier }
    }

    /// The saved session, if there is a readable one for this identifier.
    /// An unreadable file is treated as absent (the client just logs in).
    pub fn load(&self) -> Option<Session> {
        let raw = match std::fs::read_to_string(&self.path) {
            Ok(raw) => raw,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return None,
            Err(err) => {
                warn!(error = %err, "could not read the saved bluesky session; ignoring it");
                return None;
            }
        };
        match serde_json::from_str::<PersistedSession>(&raw) {
            Ok(saved) if saved.identifier == self.identifier => Some(saved.session),
            Ok(_) => None,
            Err(_) => {
                warn!("the saved bluesky session is malformed; ignoring it");
                None
            }
        }
    }

    /// Saves `session` atomically (temp file + rename), readable only by
    /// the owner on Unix. Failure is logged, never fatal: the session still
    /// works in memory, a restart would just need a fresh login.
    pub async fn save(&self, session: &Session) {
        let saved = PersistedSession {
            identifier: self.identifier.clone(),
            session: session.clone(),
        };
        let path = self.path.clone();
        let result = tokio::task::spawn_blocking(move || write_private(&path, &saved)).await;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!(error = %err, "could not save the bluesky session"),
            Err(err) => warn!(error = %err, "saving the bluesky session was interrupted"),
        }
    }
}

/// Writes `saved` to `path` atomically: a uniquely named sibling temp file
/// (owner-only where the platform supports it), fsynced and renamed into
/// place, so a reader never sees a torn token file and a crashed write
/// leaves only an orphaned temp file. Mirrors `storage::atomic_write`.
fn write_private(path: &std::path::Path, saved: &PersistedSession) -> std::io::Result<()> {
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    let body = serde_json::to_vec(saved)?;
    let dir = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    std::fs::create_dir_all(dir)?;
    let unique = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    let tmp = dir.join(format!(".tmp-{}-{nanos}-{unique}", std::process::id()));

    let write_result = (|| {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&tmp)?;
        file.write_all(&body)?;
        file.sync_all()
    })();

    if let Err(err) = write_result {
        let _ = std::fs::remove_file(&tmp);
        return Err(err);
    }

    std::fs::rename(&tmp, path)
}

/// A shared cooldown on `createSession`: once Bluesky answers a login with
/// 429, every caller (pollers, sweep, web UI) fails fast until the window
/// reopens instead of each spending another request against the limit. The
/// cooldown is in memory, so a restart during a window spends one login
/// re-discovering the limit; that is a deliberate trade against persisting
/// rate-limit state.
#[derive(Default)]
pub(super) struct LoginGate {
    blocked_until: Mutex<Option<Instant>>,
}

impl LoginGate {
    /// How much longer logins are blocked, or `None` when they are allowed.
    pub fn remaining(&self) -> Option<Duration> {
        let mut blocked_until = self.blocked_until.lock().expect("login gate poisoned");
        let until = (*blocked_until)?;
        match until.checked_duration_since(Instant::now()) {
            Some(remaining) if !remaining.is_zero() => Some(remaining),
            _ => {
                *blocked_until = None;
                None
            }
        }
    }

    /// Blocks logins for the server's `hint` (or a default without one).
    pub fn block(&self, hint: Option<Duration>) {
        let cooldown = hint
            .unwrap_or(DEFAULT_LOGIN_COOLDOWN)
            .min(MAX_LOGIN_COOLDOWN);
        *self.blocked_until.lock().expect("login gate poisoned") = Some(Instant::now() + cooldown);
    }

    pub fn clear(&self) {
        *self.blocked_until.lock().expect("login gate poisoned") = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(access: &str) -> Session {
        Session {
            access_jwt: access.to_string(),
            refresh_jwt: "refresh".to_string(),
            did: "did:plc:alice".to_string(),
        }
    }

    #[tokio::test]
    async fn session_file_round_trips_for_the_same_identifier() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.json");
        let file = SessionFile::new(path.clone(), "alice.bsky.social".to_string());
        assert!(file.load().is_none(), "nothing saved yet");

        file.save(&session("access-1")).await;
        let loaded = file.load().expect("saved session loads");
        assert_eq!(loaded.access_jwt, "access-1");
        assert_eq!(loaded.refresh_jwt, "refresh");
        assert_eq!(loaded.did, "did:plc:alice");
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .filter(|name| name != std::ffi::OsStr::new("session.json"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "no temp file left behind: {leftovers:?}"
        );

        let other = SessionFile::new(path, "bob.bsky.social".to_string());
        assert!(
            other.load().is_none(),
            "another identifier must not resume it"
        );
    }

    #[test]
    fn malformed_session_file_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.json");
        std::fs::write(&path, "not json").unwrap();
        let file = SessionFile::new(path, "alice.bsky.social".to_string());
        assert!(file.load().is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn session_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.json");
        SessionFile::new(path.clone(), "alice.bsky.social".to_string())
            .save(&session("access-1"))
            .await;
        let mode = std::fs::metadata(path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn login_gate_blocks_for_the_hint_and_clears() {
        let gate = LoginGate::default();
        assert!(gate.remaining().is_none());

        gate.block(Some(Duration::from_secs(120)));
        let remaining = gate.remaining().expect("gate is closed");
        assert!(remaining > Duration::from_secs(100) && remaining <= Duration::from_secs(120));

        gate.clear();
        assert!(gate.remaining().is_none());
    }

    #[test]
    fn login_gate_defaults_and_caps_the_cooldown() {
        let gate = LoginGate::default();
        gate.block(None);
        assert!(gate.remaining().unwrap() <= DEFAULT_LOGIN_COOLDOWN);

        gate.block(Some(Duration::from_secs(10 * 24 * 60 * 60)));
        assert!(gate.remaining().unwrap() <= MAX_LOGIN_COOLDOWN);
    }

    #[test]
    fn login_gate_reopens_once_the_window_passes() {
        let gate = LoginGate::default();
        gate.block(Some(Duration::from_millis(1)));
        std::thread::sleep(Duration::from_millis(20));
        assert!(gate.remaining().is_none());
    }
}
