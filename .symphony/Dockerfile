# Per-ticket agent image for this project: symphony-base plus a full Rust toolchain,
# since this project's tickets build/test/clippy a Cargo workspace. Not to be confused
# with the project's own top-level Dockerfile (the application's own production
# image) -- this one only ever runs Symphony's per-ticket coding-agent containers, via
# `workspace.docker.image: bsky-archiver-agent:latest` in .symphony/WORKFLOW.md.
FROM symphony-base:latest

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential pkg-config libssl-dev curl \
    && rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --default-toolchain stable --profile default \
    && rustup component add rustfmt clippy \
    # Installed as root during the build, but containers run as uid 1000 at runtime
    # (workspace.docker.user: "1000:1000" -- see symphony-base's own Dockerfile for
    # why: `claude` refuses bypassPermissions as root). cargo needs to write to its
    # own registry cache under $CARGO_HOME, so that uid needs write access here too,
    # not just read.
    && chmod -R a+rwX /usr/local/cargo /usr/local/rustup
