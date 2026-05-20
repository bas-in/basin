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

/// default_github_releases_url is the endpoint queried by
/// [`fetch_latest_cli_release`]. Overridable via `$BASIN_GITHUB_RELEASES_URL`
/// so tests can point at a local stub without real DNS / TLS.
pub fn default_github_releases_url() -> String {
    std::env::var("BASIN_GITHUB_RELEASES_URL")
        .unwrap_or_else(|_| "https://api.github.com/repos/bas-in/basin-cli/releases/latest".into())
}

/// fetch_latest_cli_release queries GitHub's public Releases API for the
/// most recent release of basin-cli. 5s timeout; 404 collapses to the
/// [`CLI_RELEASE_ENDPOINT_ABSENT`] sentinel; any transport error
/// propagates for the caller to silently skip.
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
    unwrap_envelope::<CLIRelease>(&text)
}
