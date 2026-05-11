#!/usr/bin/env sh
# basin — installer script.
#
# Detects the host OS + arch, downloads the matching tarball from the
# basin-cli GitHub release, verifies the SHA256 against the release's
# `checksums.txt`, and installs the `basin` binary into either
# `$PREFIX/bin` (when PREFIX is set) or, when running as a normal user,
# `$HOME/.local/bin`. Use `sudo sh install.sh` if you want a system-
# wide install under `/usr/local/bin` — the script picks `$PREFIX/bin`
# automatically when run as root.
#
# Usage:
#   curl -fsSL https://basin.run/install.sh | sh
#
# Once install.basin.run resolves, the canonical URL flips to:
#   curl -fsSL https://install.basin.run/cli | sh
# (same script, different vanity host).
#
# Honours overrides:
#   BASIN_VERSION   — pin to a specific tag (default: latest release)
#   BASIN_PREFIX    — install root (defaults: /usr/local when root, ~/.local otherwise)
#   BASIN_OS        — override OS auto-detect
#   BASIN_ARCH      — override arch auto-detect
#   BASIN_REPO      — override GitHub repo (default: bas-in/basin-cli)
#   BASIN_NO_VERIFY — set to 1 to skip checksum verification (don't do this)
#
# POSIX sh only. No bash-isms. The script is meant to run on Linux,
# macOS, and inside WSL with the same code path.

set -eu

# ── repo + version ──────────────────────────────────────────────────
REPO="${BASIN_REPO:-bas-in/basin-cli}"
VERSION="${BASIN_VERSION:-}"

# ── OS detect ───────────────────────────────────────────────────────
detect_os() {
  if [ -n "${BASIN_OS:-}" ]; then
    printf '%s\n' "$BASIN_OS"; return 0
  fi
  case "$(uname -s)" in
    Linux)  printf 'linux\n' ;;
    Darwin) printf 'darwin\n' ;;
    MINGW*|MSYS*|CYGWIN*) printf 'windows\n' ;;
    *) printf 'unsupported os: %s\n' "$(uname -s)" >&2; return 1 ;;
  esac
}

# ── arch detect ─────────────────────────────────────────────────────
detect_arch() {
  if [ -n "${BASIN_ARCH:-}" ]; then
    printf '%s\n' "$BASIN_ARCH"; return 0
  fi
  case "$(uname -m)" in
    x86_64|amd64) printf 'amd64\n' ;;
    aarch64|arm64) printf 'arm64\n' ;;
    *) printf 'unsupported arch: %s\n' "$(uname -m)" >&2; return 1 ;;
  esac
}

# ── PREFIX detect ───────────────────────────────────────────────────
detect_prefix() {
  if [ -n "${BASIN_PREFIX:-}" ]; then
    printf '%s\n' "$BASIN_PREFIX"; return 0
  fi
  if [ "$(id -u)" = "0" ]; then
    printf '/usr/local\n'
  else
    printf '%s/.local\n' "$HOME"
  fi
}

# ── tool checks ─────────────────────────────────────────────────────
require() {
  command -v "$1" >/dev/null 2>&1 || {
    printf 'install.sh: required tool missing: %s\n' "$1" >&2
    return 1
  }
}

# Use curl when present, fall back to wget. Same args.
fetch() {
  url="$1"; out="$2"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL --retry 3 -o "$out" "$url"
  elif command -v wget >/dev/null 2>&1; then
    wget -q -O "$out" "$url"
  else
    printf 'install.sh: neither curl nor wget on PATH\n' >&2
    return 1
  fi
}

# Cross-platform SHA256. macOS ships `shasum`, Linux ships `sha256sum`.
sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    printf 'install.sh: neither sha256sum nor shasum on PATH\n' >&2
    return 1
  fi
}

resolve_version() {
  if [ -n "$VERSION" ]; then
    printf '%s\n' "$VERSION"; return 0
  fi
  api="https://api.github.com/repos/${REPO}/releases/latest"
  tmp="$(mktemp)"
  fetch "$api" "$tmp"
  # Extract the tag_name without pulling in jq. POSIX sed.
  tag="$(sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$tmp" | head -n1)"
  rm -f "$tmp"
  if [ -z "$tag" ]; then
    printf 'install.sh: could not resolve latest release tag from %s\n' "$api" >&2
    return 1
  fi
  printf '%s\n' "$tag"
}

main() {
  os="$(detect_os)"
  arch="$(detect_arch)"
  prefix="$(detect_prefix)"
  version="$(resolve_version)"

  # GoReleaser default archive name: basin_<version-no-leading-v>_<os>_<arch>.<ext>
  ver_nov="${version#v}"
  case "$os" in
    windows) ext="zip" ;;
    *)       ext="tar.gz" ;;
  esac
  archive="basin_${ver_nov}_${os}_${arch}.${ext}"
  base="https://github.com/${REPO}/releases/download/${version}"
  url="${base}/${archive}"
  sums="${base}/checksums.txt"

  tmpdir="$(mktemp -d)"
  trap 'rm -rf "$tmpdir"' EXIT

  printf 'install.sh: fetching %s\n' "$archive"
  fetch "$url" "$tmpdir/$archive"

  if [ -z "${BASIN_NO_VERIFY:-}" ]; then
    printf 'install.sh: verifying checksum\n'
    fetch "$sums" "$tmpdir/checksums.txt"
    expected="$(grep -E "[[:space:]]${archive}\$" "$tmpdir/checksums.txt" | awk '{print $1}')"
    if [ -z "$expected" ]; then
      printf 'install.sh: %s not listed in checksums.txt (bad release?)\n' "$archive" >&2
      exit 1
    fi
    actual="$(sha256 "$tmpdir/$archive")"
    if [ "$expected" != "$actual" ]; then
      printf 'install.sh: checksum mismatch — expected %s got %s\n' "$expected" "$actual" >&2
      exit 1
    fi
  fi

  # Extract.
  cd "$tmpdir"
  case "$ext" in
    tar.gz) tar -xzf "$archive" ;;
    zip)    require unzip; unzip -q "$archive" ;;
  esac

  bin="basin"
  [ "$os" = "windows" ] && bin="basin.exe"
  if [ ! -f "$bin" ]; then
    printf 'install.sh: binary "%s" not found inside %s\n' "$bin" "$archive" >&2
    exit 1
  fi
  chmod +x "$bin"

  dest="${prefix}/bin"
  mkdir -p "$dest"
  install_path="${dest}/${bin}"
  mv "$bin" "$install_path"

  printf 'install.sh: installed %s -> %s\n' "$bin" "$install_path"

  # PATH hint when the user installed into ~/.local/bin and it's not on PATH.
  case ":$PATH:" in
    *":$dest:"*) ;;
    *) printf 'install.sh: note — %s is not on PATH. Add this to your shell rc:\n  export PATH="%s:$PATH"\n' "$dest" "$dest" ;;
  esac
}

main "$@"
