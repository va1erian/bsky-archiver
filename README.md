# bsky-archiver

A self-hosted Bluesky watcher/archiver: backs up a watched account's authored posts
with media, likes, and bookmarks (JSON + downloaded images/video) and serves a small
web UI to browse the archive.

This repository is being built incrementally by an automated coding-agent pipeline
(Symphony) working through the tickets in `../issues/` relative to this repo. See
`../WORKFLOW.md` for the orchestration config.

Project status and usage docs will be filled in as the pipeline progresses (see the
final "docs and e2e" ticket).

## Development

Before committing, run the same checks CI runs:

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

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
