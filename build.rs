//! Captures build metadata (git revision + UTC build date) into the
//! binary so the web UI footer can show which build is deployed.
//!
//! Both values are emitted as `cargo:rustc-env` vars and read via
//! `option_env!` in `src/templates.rs`; when git isn't available (building
//! outside a checkout, or a source tarball) the revision falls back to
//! "unknown" rather than failing the build.

use std::path::Path;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_default();

    // Re-run when the checked-out commit could have changed (a new commit
    // moves either HEAD or a branch ref it points at). Best-effort: a
    // missing .git (tarball/docker context without it) just means the
    // revision is captured on the builds that do have it.
    for watched in [".git/HEAD", ".git/refs"] {
        if let Some(path) = non_empty_path(&manifest_dir, watched) {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }

    let revision = git_revision(&manifest_dir).unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=BSKY_ARCHIVER_GIT_SHA={revision}");
    println!(
        "cargo:rustc-env=BSKY_ARCHIVER_BUILD_DATE={}",
        build_date_utc()
    );
}

fn non_empty_path(base: &str, relative: &str) -> Option<std::path::PathBuf> {
    if base.is_empty() {
        return None;
    }
    let path = Path::new(base).join(relative);
    path.exists().then_some(path)
}

/// The short hash of the current commit, or `None` when git is unavailable
/// or the directory isn't a repository.
fn git_revision(manifest_dir: &str) -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .current_dir(manifest_dir)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let revision = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!revision.is_empty()).then_some(revision)
}

/// The current UTC time formatted as `YYYY-MM-DD HH:MM`. Computed from the
/// Unix epoch with the standard days-to-civil-date algorithm so `build.rs`
/// needs no external dependencies.
fn build_date_utc() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02} UTC",
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60
    )
}

/// Converts a count of days since 1970-01-01 to a (year, month, day) civil
/// date (Howard Hinnant's `civil_from_days` algorithm, public domain).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = (z - era * 146_097) as u64;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era as i64 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}
