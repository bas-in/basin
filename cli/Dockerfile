# Dockerfile — minimal OCI image for the basin CLI.
#
# Built by goreleaser at release time (see `dockers:` block in
# `.goreleaser.yaml`). Goreleaser builds the `basin` binary via the
# top-level `builds:` block and then COPYs the appropriate per-arch
# binary into this image, one image per (linux/amd64, linux/arm64).
#
# Why distroless/static:
#   - basin is a Go binary built with `CGO_ENABLED=0` (per `.goreleaser.yaml`
#     `builds.env`), so there's no glibc/musl dependency.
#   - distroless/static is ~2.5 MB, contains tzdata + ca-certificates
#     only — no shell, no package manager, no extra attack surface.
#   - The `nonroot` tag runs as UID 65532 by default.
#
# Usage on the operator side:
#   docker run --rm ghcr.io/vul-os/basin:latest version
#   docker run --rm -v ~/.config/basin:/home/nonroot/.config/basin \
#     ghcr.io/vul-os/basin:latest projects list

FROM gcr.io/distroless/static-debian12:nonroot

# Goreleaser drops the `basin` binary into the build context root before
# invoking docker build, so this is a flat COPY — no per-arch sub-dirs.
COPY basin /usr/local/bin/basin

# Run as the non-root user baked into the image (UID 65532).
USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/basin"]
