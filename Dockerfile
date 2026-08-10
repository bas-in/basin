# Phase 5.31.B — multi-stage production Dockerfile for basin-server.
#
# Builder : rust:${RUST_VERSION}-slim-bookworm
#           RUST_VERSION must be >= the workspace `rust-version` in Cargo.toml,
#           which is the *resolved graph's* floor, not a preference: cranelift
#           0.134 (via wasmtime 47, via basin-fn) declares rust-version 1.94 and
#           vortex-error 0.71 declares 1.91, and cargo hard-errors below either.
#           This was pinned at 1.85 while the lockfile needed 1.92, so every
#           Docker Smoke run failed with
#             error: rustc 1.85.1 is not supported by the following packages:
#               vortex-error@0.71.0 requires rustc 1.91.0
#           Bump this together with Cargo.toml's rust-version; the msrv-floor
#           CI job fails closed if they drift apart again.
#           bookworm (not trixie) so the builder's glibc is not newer than the
#           runtime stage's — a trixie builder produces a binary bookworm-slim
#           cannot load.
# Runtime : debian:bookworm-slim     (~30 MB base + glibc + libgcc + netcat;
#                                     distroless/cc was preferred but has no
#                                     shell/netcat for the HEALTHCHECK TCP probe)
#
# Expected final image size: ~60-80 MB (well under the 100 MB CI limit).
#
# ── ENV var corrections vs the Phase 5.31.B spec (verified against main.rs) ──
#   Real var         Spec var (incorrect)   Notes
#   BASIN_BIND       BASIN_BIND_ADDR        reads BASIN_BIND; default 127.0.0.1:5433
#   BASIN_DATA_DIR   BASIN_DATA_DIR         ✓ matches
#   BASIN_STORAGE_BACKEND  BASIN_OBJECT_STORE  reads BASIN_STORAGE_BACKEND; values: local|s3|tigris
#   BASIN_PROJECTS   (not in spec)          "user=*" provisions a project at startup
#                                           set to "basin=*" to match smoke PGUSER=basin

# ─── Stage 1: builder ────────────────────────────────────────────────────────
ARG RUST_VERSION=1.94
FROM rust:${RUST_VERSION}-slim-bookworm AS builder

# Native build deps:
#   clang/cmake — required by pg_query crate (libpg_query, C build)
#   pkg-config + libssl-dev — required by reqwest (rustls uses ring, but some
#                              deps link openssl for feature detection)
RUN apt-get update && apt-get install -y --no-install-recommends \
        clang \
        cmake \
        pkg-config \
        libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy the full workspace. The dependency layer is warm for incremental CI
# builds via Docker BuildKit cache (--cache-from type=gha in the workflow).
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY services/ services/
COPY tests/ tests/

# Build the release binary. Package name and [[bin]] name are both "basin-server"
# (verified in services/basin-server/Cargo.toml).
RUN cargo build --release -p basin-server

# ─── Stage 2: runtime ────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# Install only the shared libraries the binary actually needs at runtime:
#   ca-certificates — S3/HTTPS object store connections
#   netcat-openbsd  — nc(1) for the HEALTHCHECK TCP probe
# glibc and libgcc are already in bookworm-slim.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Create the data directory; it will be owned by nobody (uid 65534).
RUN mkdir -p /var/basin && chown 65534:65534 /var/basin

COPY --from=builder /build/target/release/basin-server /usr/local/bin/basin-server

# Drop privileges — run as nobody.
USER 65534

# ── Environment defaults ─────────────────────────────────────────────────────
# BASIN_BIND:             bind all interfaces on the pgwire port.
# BASIN_DATA_DIR:         writable data root inside the container.
# BASIN_STORAGE_BACKEND:  "local" uses BASIN_DATA_DIR; no external store needed.
# BASIN_PROJECTS:         provision project for user "basin" on first boot.
#                         Matches smoke test PGUSER=basin; "basin=*" allocates
#                         a fresh ULID project ID at startup.
ENV BASIN_BIND="0.0.0.0:5432" \
    BASIN_DATA_DIR="/var/basin" \
    BASIN_STORAGE_BACKEND="local" \
    BASIN_PROJECTS="basin=*"

VOLUME ["/var/basin"]

EXPOSE 5432

# TCP connect probe — nc exits 0 if the port accepts a connection.
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD ["nc", "-z", "127.0.0.1", "5432"]

ENTRYPOINT ["/usr/local/bin/basin-server"]
