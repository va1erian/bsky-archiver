# bsky-archiver

A self-hosted daemon + web UI that watches one Bluesky account and archives:

1. That account's own posts which contain media (images or video).
2. That account's likes.
3. That account's bookmarks.

For each archived item, the full post record is stored as JSON and any attached
images/video are downloaded and stored alongside it. A small web UI lets a human
browse the archive (post list + detail), view a gallery of archived media —
filterable by category (posts / likes / bookmarks) and downloadable as a single
zip of every image in the current selection — and see the active (non-secret)
configuration.

Real-time capture of the watched account's own posts uses a Jetstream firehose
subscription; a REST-polling path fully substitutes for it whenever the firehose
connection is unavailable. Likes and bookmarks aren't available on the firehose at
all, so they're always fetched via periodic REST polling. The application ships as a
single Docker image, fully configured by environment variables, and is intended to
run under Cosmos Cloud (a Docker Compose-style host) or plain `docker compose`.

## Architecture: module map

| Module | Owns |
| --- | --- |
| `config` | Loading, defaulting, and validating every environment variable into one typed `AppConfig`. Startup fails fast (non-zero exit) on anything invalid. |
| `bluesky` | The `com.atproto.*` / `app.bsky.*` REST (XRPC) client: session auth (with automatic re-login on a 401), `getAuthorFeed`, `getActorLikes`, `getBookmarks`, and handle resolution. |
| `firehose` | The Jetstream websocket consumer: real-time capture of the watched account's authored posts with media, filtered by DID, with reconnect/backoff and a persisted cursor so a restart resumes roughly where it left off. |
| `poller` | The REST-polling fallback for authored posts (active whenever the firehose is down) and the periodic likes/bookmarks poller. Both use adaptive intervals with jittered exponential backoff. |
| `pipeline` | The shared `CandidatePost` channel and the `has_archivable_media` predicate connecting every producer (firehose, REST fallback, likes/bookmarks poller) to the one consumer (the media downloader). |
| `media` | Concurrency-limited, size-capped media downloading: streams each file, aborts if it exceeds `MEDIA_MAX_BYTES`, retries transient failures, and never leaves a partial file on disk. |
| `storage` | The on-disk JSON archive (source of truth) plus the SQLite query index built on top of it. The index is fully rebuildable from disk (`reindex`) and is rebuilt automatically on startup if missing. |
| `ratelimit` | The shared backoff/circuit-breaker policy and the process-wide inflight-request cap used by the pollers, the Bluesky client, and the media downloader. |
| `health` | Per-subsystem health tracking (`Connected` / `Degraded` / `Error`), read by `/healthz` and the dashboard. |
| `state` | `AppState`, the shared handle (config + storage + health) passed to every request handler and background task. |
| `web` | The `axum` HTTP server: routing, password-gated session auth, and the pages/JSON surface backing the UI. |
| `templates` | `askama` templates and their view models (kept separate from `web` so that module stays about *what data* each route needs, not how it's marked up). |
| `app` | Startup sequencing (open storage, authenticate, resolve watched handles — failing fast on any error) and supervised orchestration of every background task (firehose, REST fallback, likes/bookmarks poller, media downloader, web server) plus graceful shutdown. |

Every background task in `app::serve` is independently supervised: a panic or
unexpected exit in the firehose consumer, REST fallback poller, likes/bookmarks
poller, or media downloader is logged and restarted with exponential backoff, rather
than taking down the rest of the service. The only failures that stop the process
outright are genuine startup-validation failures (invalid config, bad Bluesky
credentials, an unresolvable watched handle, an unwritable database path) — those are
surfaced immediately, with a non-zero exit, so a misconfigured deployment fails
visibly instead of running in a broken state.

## Local development

Requires a recent stable Rust toolchain (see `Cargo.toml`'s `edition`).

At minimum, `BSKY_IDENTIFIER`, `BSKY_APP_PASSWORD`, `UI_PASSWORD`, and
`UI_SESSION_SECRET` must be set (see the full reference below). The simplest way
locally is a `.env` file in the repo root (already in `.gitignore`, so it's never
committed) — it's loaded automatically via `dotenvy` on startup, with real
environment variables always taking precedence over `.env` values:

```sh
cat > .env <<'EOF'
BSKY_IDENTIFIER=your-handle.bsky.social
BSKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx
UI_PASSWORD=some-local-password
UI_SESSION_SECRET=a-long-random-string-32-chars-or-more
ARCHIVE_DIR=./archive
EOF
cargo run
```

Use a dedicated Bluesky [app password](https://bsky.app/settings/app-passwords) for
local development, never your main account password.

Before committing, run the same checks CI runs:

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

`cargo test` runs the full suite: unit tests in every module (using `wiremock` for
anything that talks to the Bluesky REST API, and an in-process mock websocket server
for anything that talks to Jetstream — the real Bluesky network is never contacted
from a test) plus an end-to-end test (`tests/e2e.rs`) that drives a real `AppState`,
real on-disk storage, and a real `axum` router through the full pipeline: an authored
post with media, a like, and a bookmark, each archived end to end and then verified
through the actual web UI routes (list, detail, gallery, raw media bytes, and the
gallery's zip export at `GET /gallery/export`).

The crate is structured as a library (`src/lib.rs`) with a thin binary shim
(`src/main.rs`) specifically so `tests/e2e.rs` can exercise real internal types
rather than re-implementing them.

## Deploying

The application ships as a single Docker image (see the multi-stage `Dockerfile` in
this repo) and is configured entirely via environment variables — no config file
editing is required. A ready-to-use `docker-compose.yml` example is included; copy
it, fill in the secrets, and run:

```sh
docker compose up -d
```

This is also directly usable as a Cosmos Cloud app definition (Cosmos Cloud consumes
Docker Compose-style service definitions).

### Environment variables

**Required:**

| Variable | Description |
| --- | --- |
| `BSKY_IDENTIFIER` | Handle or DID used to log in to Bluesky. |
| `BSKY_APP_PASSWORD` | Bluesky [app password](https://bsky.app/settings/app-passwords) (secret — not your main account password). |
| `UI_PASSWORD` | Shared password that gates the web UI (secret). |
| `UI_SESSION_SECRET` | Secret key used to sign the web UI's session cookies. Use a long, random value (32+ characters). |

**Optional (defaults shown):**

| Variable | Default | Description |
| --- | --- | --- |
| `BSKY_WATCH_HANDLES` | `BSKY_IDENTIFIER` alone | Comma-separated list of handles/DIDs to watch for authored posts with media. |
| `ARCHIVE_DIR` | `/data/archive` | Root directory for archived JSON records and media. **Must persist across restarts and upgrades** (mount a volume here). |
| `DATABASE_PATH` | `<ARCHIVE_DIR>/index.sqlite3` | SQLite index file path. Safe to delete — it is rebuilt automatically from `ARCHIVE_DIR` on next startup — but persisting it avoids a slow reindex. |
| `UI_PORT` | `8080` | Port the web UI listens on inside the container. |
| `POLL_INTERVAL_SECONDS` | `120` | Baseline interval for likes/bookmarks polling and the REST fallback path (adaptive, with jittered exponential backoff on errors). |
| `JETSTREAM_URL` | `wss://jetstream1.us-east.bsky.network/subscribe` | Jetstream websocket endpoint for real-time firehose consumption. Override to point at a self-hosted Jetstream instance. |
| `MEDIA_MAX_CONCURRENT_DOWNLOADS` | `4` | Cap on simultaneous media downloads. |
| `MEDIA_MAX_BYTES` | `104857600` (100 MiB) | Per-file download size safety cap. |
| `RUST_LOG` | `info` | Standard `tracing`/`tracing-subscriber` filter string. |

Every variable above is the single canonical source of a given setting: none of it is
duplicated or re-declared elsewhere, and the active (non-secret) values can always be
inspected at runtime on the web UI's `/config` page.

### Persistent volumes

Only one path needs to survive container restarts and image upgrades:

- **`ARCHIVE_DIR`** (default `/data/archive`) — the source of truth: every archived
  post's JSON record, its downloaded media, and (unless `DATABASE_PATH` points
  elsewhere) the SQLite query index all live under here. The example
  `docker-compose.yml` mounts a named volume at this path. If the SQLite index is
  ever lost or corrupted, the app rebuilds it automatically from the JSON/media on
  disk on next startup — the archive itself is always the durable copy.

The container itself is stateless and disposable; upgrading is just pulling a new
image and recreating the container against the same volume.

### Health checks

The image defines a `HEALTHCHECK` that polls `GET /healthz` (no auth required) and
reports unhealthy if any background subsystem (firehose, REST fallback,
likes/bookmarks poller, media downloader) is in an error state. The process also
fails fast and exits non-zero on startup if configuration is invalid or Bluesky
authentication fails, so a misconfigured deployment is immediately visible in
container logs/exit status rather than hanging.

### First deploy, step by step

1. Create a dedicated Bluesky [app password](https://bsky.app/settings/app-passwords)
   for the account you want to watch/archive from — do not use your main account
   password.
2. Copy `docker-compose.yml`, fill in `BSKY_IDENTIFIER`, `BSKY_APP_PASSWORD`,
   `UI_PASSWORD`, and a random `UI_SESSION_SECRET` (32+ characters).
3. `docker compose up -d`, then check `docker compose logs -f` — on a bad credential
   or config the container exits non-zero immediately with a `fatal startup error`
   log line, rather than starting up broken.
4. Visit `http://<host>:8080` (or whatever `UI_PORT` is set to), log in with
   `UI_PASSWORD`, and confirm the dashboard shows every subsystem as `Connected`
   (allow a few seconds after startup for the first poll/firehose connection).
5. Confirm `ARCHIVE_DIR`'s volume is durable in your deployment environment (e.g. a
   named Docker volume, not an ephemeral container filesystem) — it is the only
   state that must survive an upgrade or restart.

## The `.symphony/` directory

`.symphony/` configures the Symphony agent daemon — an autonomous coding-agent that
works this repository's own GitHub issues (`.symphony/WORKFLOW.md` holds its tracker
config and per-ticket prompt). It's
purely development tooling for maintaining this project — it is **not** part of the
archiver application, plays no role at runtime, and is safe to ignore or delete if
you're not running that daemon yourself.

Note that `.symphony/Dockerfile` is unrelated to the `Dockerfile` at the repo root:
the top-level one builds the archiver's production image (see [Deploying](#deploying)),
while `.symphony/Dockerfile` builds the sandboxed image the coding agent runs inside
while working a single ticket.
