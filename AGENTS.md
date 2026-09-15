# AGENTS.md — guidance for coding agents working in this repo

bsky-archiver is a Rust daemon + web UI that watches Bluesky sources and archives
posts/likes/bookmarks/feeds (plus optional Tumblr likes and Telegram channel media)
to disk as JSON records + downloaded media, with a SQLite query index on top.
See `README.md` for the full module map and environment variable reference.

## Workflow

- Always start in a private git worktree created from a fresh `origin/main`.
- Rebase onto `origin/main` before submitting; never create merge commits.
- Agents may commit their own work, but ensure the code is properly reviewed
  before committing. Never commit `.env`, `app-err.log`, `app-out.log`,
  archive/, or `target/` output.
- Keep documentation up to date with any change you make (README, module map).
- Verify the application still works before you consider the task done.

## Validation

CI runs (and so should you, before submitting a PR):

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo build --release
cargo test
```

Browser e2e (needs Node + Playwright):

```sh
cd e2e-browser && npm ci && npx playwright install chromium
cargo test --test browser
```

Local run: `cargo run` with a `.env` at the repo root (required:
`BSKY_IDENTIFIER`, `BSKY_APP_PASSWORD`, `UI_PASSWORD`, `UI_SESSION_SECRET`).
Tests never touch the real Bluesky/Telegram network — REST is mocked with
`wiremock`, Jetstream with an in-process mock websocket server.

## Code

- Match existing style; Rust edition 2024; `cargo fmt` and `clippy -D warnings`
  must pass.
- Keep code clean, efficient, readable, and not over-commented. Split large
  files into multiple modules rather than letting them grow.
- The web UI must stay responsive, elegant, usable, and fast — changes there
  should degrade gracefully and not slow down common pages.

## Invariants

- The on-disk JSON archive under `ARCHIVE_DIR` is the source of truth; the
  SQLite index is disposable/rebuildable. Never delete or rewrite archived
  records/media; deletes are modeled by setting `deleted_at` only.
- Configuration lives in the `config` module (`AppConfig`); the service fails
  fast (non-zero exit) on invalid config or bad credentials. No parallel config
  paths.
- Secrets (app passwords, OAuth tokens, session secrets) must never be logged
  or appear in error messages — CI greps logs for the password.
- The watch list lives in the SQLite `watched_sources` table, managed via the
  UI (`/config`, `/sources`) — never via env vars.
- Background tasks in `app::serve` are individually supervised (restart with
  backoff on panic); keep the fail-fast startup vs. supervised-runtime split.
- Cargo.lock pins `glass_pumpkin` to 2.0.0-rc0 (see note in `Cargo.toml`);
  restore with `cargo update -p glass_pumpkin --precise 2.0.0-rc0` if lost.
- `.symphony/` is agent tooling, unrelated to the app; don't touch it in
  application changes.
