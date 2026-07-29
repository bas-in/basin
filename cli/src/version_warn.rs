//! version_warn — CLI↔cloud support-window warning + self-update notice.
//!
//! Wires the pure semver helpers, the HTTP fetches, and the on-disk
//! cache into side-effecting calls invoked from `main`'s dispatch before
//! each subcommand. Both always return; callers never propagate errors.

use std::io::Write;

use crate::client::{version, Client};
use crate::config::{
    cli_release_stale, cloud_version_stale, lookup_cli_release, lookup_cloud_version,
    upsert_cli_release, upsert_cloud_version,
};
use crate::fetch::{default_github_releases_url, fetch_latest_cli_release};
use crate::global::GlobalFlags;
use crate::version_check::{in_support_window, parse_semver};

const UPDATE_HINT: &str =
    "       update via `brew upgrade basin` or `go install github.com/bas-in/basin-cli@latest`.";

fn unstamped(v: &str) -> bool {
    v.is_empty() || v == "dev"
}

/// warn_if_version_out_of_window emits the support-window warning to
/// stderr when applicable.
pub fn warn_if_version_out_of_window(g: &GlobalFlags) {
    warn_if_version_out_of_window_to(g, &mut std::io::stderr());
}

/// warn_if_version_out_of_window_to is the test-friendly inner.
pub fn warn_if_version_out_of_window_to<W: Write>(g: &GlobalFlags, w: &mut W) {
    if g.quiet || unstamped(version()) {
        return;
    }
    let Ok(cli_ver) = parse_semver(version()) else {
        return;
    };
    let cloud_str = resolve_cloud_version_string(g);
    if unstamped(&cloud_str) {
        return;
    }
    let Ok(cloud_ver) = parse_semver(&cloud_str) else {
        return;
    };
    if in_support_window(cli_ver, cloud_ver) {
        return;
    }
    let _ = writeln!(
        w,
        "basin: cli {cli_ver} is outside the cloud's two-minor support window (cloud {cloud_ver}).\n{UPDATE_HINT}"
    );
}

/// warn_if_self_update_available emits a "newer release available"
/// notice when the latest GitHub release is ≥1 minor ahead.
pub fn warn_if_self_update_available(g: &GlobalFlags) {
    warn_if_self_update_available_to(g, &mut std::io::stderr());
}

/// warn_if_self_update_available_to is the test-friendly inner.
pub fn warn_if_self_update_available_to<W: Write>(g: &GlobalFlags, w: &mut W) {
    if g.quiet || unstamped(version()) {
        return;
    }
    let Ok(cli_ver) = parse_semver(version()) else {
        return;
    };
    let (tag, html_url) = resolve_latest_cli_release();
    if tag.is_empty() {
        return;
    }
    let Ok(latest) = parse_semver(&tag) else {
        return;
    };
    if latest.major < cli_ver.major {
        return;
    }
    if latest.major == cli_ver.major && latest.minor <= cli_ver.minor {
        return;
    }
    if html_url.is_empty() {
        let _ = writeln!(
            w,
            "basin: newer release available ({latest}; running {cli_ver}).\n{UPDATE_HINT}"
        );
    } else {
        let _ = writeln!(
            w,
            "basin: newer release available ({latest}; running {cli_ver}). {html_url}\n{UPDATE_HINT}"
        );
    }
}

/// resolve_latest_cli_release returns (tag, html_url) from the on-disk
/// cache when fresh, else a single network fetch. Errors collapse to
/// ("", "") so the caller skips the notice.
fn resolve_latest_cli_release() -> (String, String) {
    if let Some(entry) = lookup_cli_release() {
        if !cli_release_stale(&entry) {
            return (entry.tag_name, entry.html_url);
        }
    }
    match fetch_latest_cli_release(&default_github_releases_url()) {
        Ok(rel) => {
            let _ = upsert_cli_release(&rel);
            (rel.tag_name, rel.html_url)
        }
        Err(_) => (String::new(), String::new()),
    }
}

/// resolve_cloud_version_string returns the cloud's version for the
/// active org slot, cache-first. Errors collapse to "".
fn resolve_cloud_version_string(g: &GlobalFlags) -> String {
    if let Some(entry) = lookup_cloud_version(&g.org_slug, &g.api_url) {
        if !cloud_version_stale(&entry) {
            return entry.version;
        }
    }
    let client = Client::new(&g.api_url, &g.token);
    match client.fetch_version() {
        Ok(cv) => {
            let _ = upsert_cloud_version(&g.org_slug, &cv, &g.api_url);
            cv.version
        }
        Err(_) => String::new(),
    }
}
