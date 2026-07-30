//! fetch — the two soft-fail network probes feeding the version-warning
//! path: `GET /v1/version` (cloud support window) and GitHub's
//! releases/latest (self-update check). Both treat 404 / transport
//! errors as "compatibility unknown" — never abort a command.

use std::time::Duration;

use reqwest::Method;

use crate::client::{unwrap_envelope, version, Client};
use crate::error::{as_api_error, msg, CliResult};
use crate::types::{CLIRelease, CloudVersion};

/// Sentinel message for a cloud older than the `/v1/version` endpoint
/// (404). Callers compare via [`is_version_endpoint_absent`].
const VERSION_ENDPOINT_ABSENT: &str = "cloud: /v1/version endpoint absent";
/// Sentinel message for a repo with no published release yet (404).
const CLI_RELEASE_ENDPOINT_ABSENT: &str = "github: no published release for basin-cli";

pub fn is_version_endpoint_absent(err: &(dyn std::error::Error + 'static)) -> bool {
    err.to_string() == VERSION_ENDPOINT_ABSENT
}

pub fn is_cli_release_endpoint_absent(err: &(dyn std::error::Error + 'static)) -> bool {
    err.to_string() == CLI_RELEASE_ENDPOINT_ABSENT
}

impl Client {
    /// fetch_version calls `GET /v1/version` with a short 5s timeout so
    /// a sick cloud can't hang `basin login`. Collapses a 404 to the
    /// [`VERSION_ENDPOINT_ABSENT`] sentinel.
    pub fn fetch_version(&self) -> CliResult<CloudVersion> {
        match self.do_json_timeout::<CloudVersion>(
            Method::GET,
            "/v1/version",
            None,
            Duration::from_secs(5),
        ) {
            Ok(v) => Ok(v),
            Err(e) => {
                if let Some(ae) = as_api_error(e.as_ref()) {
                    if ae.http_status == 404 {
                        return Err(msg(VERSION_ENDPOINT_ABSENT));
                    }
                }
                Err(e)
            }
        }
    }
}

/// Tag prefix the CLI's own releases carry. The repo publishes two independent
/// tag series — `v*` for the engine (basin-server, basinctl) and `cli-v*` for
/// this binary (see `.github/workflows/release.yml` and `cli-release.yml`) — so
/// the CLI must never compare its version against whatever tag happens to be
/// newest overall.
pub const CLI_TAG_PREFIX: &str = "cli-v";

/// default_github_releases_url is the endpoint queried by
/// [`fetch_latest_cli_release`]. Overridable via `$BASIN_GITHUB_RELEASES_URL`
/// so tests can point at a local stub without real DNS / TLS.
///
/// This used to point at `bas-in/basin-cli/releases/latest`, a repository that
/// does not exist (`gh api repos/bas-in/basin-cli` → 404) — the CLI moved into
/// the `vul-os/basin` monorepo under `cli/`. Every self-update probe therefore
/// 404'd, collapsed to the ENDPOINT_ABSENT sentinel, and the notice could never
/// fire. It is the LIST endpoint rather than `/latest` on purpose: `/latest`
/// returns the newest release across both tag series, so a CLI at 0.1.0 would
/// have been told to upgrade to the engine's 0.1.9.
pub fn default_github_releases_url() -> String {
    std::env::var("BASIN_GITHUB_RELEASES_URL")
        .unwrap_or_else(|_| "https://api.github.com/repos/vul-os/basin/releases?per_page=100".into())
}

/// Pick the newest `cli-v*` release from a GitHub releases-list response.
///
/// GitHub returns the list newest-first, so the first matching entry wins. The
/// `cli-v` prefix is stripped so the caller gets a bare semver to parse.
/// Returns `None` when the list holds no CLI release — which the caller treats
/// as "unknown", never as "you are up to date".
pub fn newest_cli_release(list: &[CLIRelease]) -> Option<CLIRelease> {
    list.iter()
        .find(|r| r.tag_name.starts_with(CLI_TAG_PREFIX))
        .map(|r| CLIRelease {
            tag_name: r.tag_name[CLI_TAG_PREFIX.len()..].to_string(),
            html_url: r.html_url.clone(),
        })
}

/// fetch_latest_cli_release queries GitHub's public Releases API and returns
/// the newest `cli-v*` release. 5s timeout; 404 and "no CLI release in the
/// list" both collapse to the [`CLI_RELEASE_ENDPOINT_ABSENT`] sentinel; any
/// transport error propagates for the caller to silently skip.
///
/// Accepts either a list response (the default endpoint) or a single-object
/// response, so an operator pointing `$BASIN_GITHUB_RELEASES_URL` at a
/// `/releases/latest` mirror still works.
pub fn fetch_latest_cli_release(url: &str) -> CliResult<CLIRelease> {
    let http = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let resp = http
        .get(url)
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", format!("basin-cli/{}", version()))
        .send()?;
    let status = resp.status();
    if status.as_u16() == 404 {
        return Err(msg(CLI_RELEASE_ENDPOINT_ABSENT));
    }
    if !status.is_success() {
        return Err(msg(format!(
            "github: HTTP {} fetching latest release",
            status.as_u16()
        )));
    }
    let text = resp.text()?;
    // List first (the default endpoint); fall back to a single object.
    if let Ok(list) = unwrap_envelope::<Vec<CLIRelease>>(&text) {
        return newest_cli_release(&list).ok_or_else(|| msg(CLI_RELEASE_ENDPOINT_ABSENT));
    }
    let single = unwrap_envelope::<CLIRelease>(&text)?;
    // A single-object mirror may or may not carry the prefix; strip it if so,
    // and refuse an engine `v*` tag rather than compare against it.
    if let Some(rest) = single.tag_name.strip_prefix(CLI_TAG_PREFIX) {
        return Ok(CLIRelease {
            tag_name: rest.to_string(),
            html_url: single.html_url,
        });
    }
    if single.tag_name.starts_with('v') {
        return Err(msg(CLI_RELEASE_ENDPOINT_ABSENT));
    }
    Ok(single)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rel(tag: &str) -> CLIRelease {
        CLIRelease {
            tag_name: tag.to_string(),
            html_url: format!("https://github.com/vul-os/basin/releases/tag/{tag}"),
        }
    }

    #[test]
    fn default_url_targets_the_repo_that_actually_exists() {
        // `bas-in/basin-cli` is a 404. Pinned so the self-update probe cannot
        // silently regress to a dead repo again — a 404 collapses to "unknown"
        // and the notice never fires, which looks identical to "up to date".
        let url = default_github_releases_url();
        assert!(
            url.contains("vul-os/basin"),
            "self-update probe must target vul-os/basin, got {url}"
        );
        assert!(
            !url.contains("bas-in"),
            "bas-in/* repos do not exist; got {url}"
        );
        // The list endpoint, not /latest: /latest spans both tag series.
        assert!(
            url.contains("/releases?") || url.contains("/releases/tags/"),
            "must query the releases LIST so cli-v* can be selected, got {url}"
        );
    }

    #[test]
    fn newest_cli_release_skips_engine_tags() {
        // The engine's v0.1.9 is newer and first in the list. Selecting it
        // would tell a cli at 0.1.0 to "upgrade" to an engine release.
        let list = vec![rel("v0.1.9"), rel("cli-v0.2.0"), rel("v0.1.8"), rel("cli-v0.1.0")];
        let got = newest_cli_release(&list).expect("a cli release is present");
        assert_eq!(got.tag_name, "0.2.0", "prefix must be stripped");
        assert!(got.html_url.ends_with("cli-v0.2.0"));
    }

    #[test]
    fn newest_cli_release_is_none_when_only_engine_tags() {
        // Must be None, not the newest engine tag. The caller treats None as
        // "unknown" and stays silent; returning something here would emit a
        // wrong upgrade notice on every invocation.
        let list = vec![rel("v0.1.9"), rel("v0.1.8")];
        assert!(newest_cli_release(&list).is_none());
    }

    #[test]
    fn newest_cli_release_is_none_on_empty_list() {
        assert!(newest_cli_release(&[]).is_none());
    }
}
