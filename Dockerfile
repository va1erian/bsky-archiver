# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Builder: full Rust toolchain, compiles a release binary.
#
# Templates (askama) and static assets (CSS/htmx) are embedded into the
# binary at compile time (`include_bytes!` / askama's own template
# inclusion), so nothing from `templates/` or `static/` needs to be copied
# into the runtime stage below - only the compiled binary does.
# ---------------------------------------------------------------------------
FROM rust:1.90-bookworm AS builder

WORKDIR /build

# Cache dependency compilation separately from application source so that
# source-only changes don't invalidate the (slow) dependency build layer.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && cargo build --release \
    && rm -rf src

COPY src ./src
COPY templates ./templates
COPY static ./static
# Force cargo to notice main.rs changed since the dummy build above.
RUN touch src/main.rs && cargo build --release

# ---------------------------------------------------------------------------
# Runtime: minimal glibc base with just the binary + CA certs + curl (for
# the HEALTHCHECK below). debian:bookworm-slim is used instead of a
# distroless base to keep the image debuggable (shell, package manager).
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system --gid 1000 bsky-archiver \
    && useradd --system --uid 1000 --gid bsky-archiver --home-dir /nonexistent --no-create-home bsky-archiver

COPY --from=builder /build/target/release/bsky-archiver /usr/local/bin/bsky-archiver

# Default ARCHIVE_DIR (see canonical env var schema); overridable via the
# ARCHIVE_DIR env var, in which case this directory is simply unused.
RUN mkdir -p /data/archive && chown -R bsky-archiver:bsky-archiver /data

USER bsky-archiver
WORKDIR /

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -fsS "http://127.0.0.1:${UI_PORT:-8080}/healthz" || exit 1

ENTRYPOINT ["/usr/local/bin/bsky-archiver"]
