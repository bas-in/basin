//! config — read/write the on-disk config file (`config.json`) plus the
//! directory-scoped working-project file (`./basin/config.toml`).
//!
//! Path resolution follows XDG: `$XDG_CONFIG_HOME/basin/config.json`,
//! else `$HOME/.config/basin/config.json`, else (Windows) `%APPDATA%`.
//! The file is mode 0600; reads refuse a group/world-readable token file.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{msg, CliResult};
use crate::types::{CLIRelease, CloudVersion};

/// CLOUD_VERSION_TTL is the cache window before the warning path
/// refetches `/v1/version`.
pub const CLOUD_VERSION_TTL: Duration = Duration::from_secs(6 * 60 * 60);
/// CLI_RELEASE_TTL is the cache window before the self-update warning
/// refetches GitHub's Releases API.
pub const CLI_RELEASE_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// ConfigFile is the on-disk representation. `tokens` is keyed by org
/// slug; `default_token` is the catch-all when --org isn't set.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ConfigFile {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub default_token: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub api_url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub default_org: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tokens: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub cloud_versions: BTreeMap<String, CachedCloudVersion>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub telemetry: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cli_release: Option<CachedCLIRelease>,
}

/// CachedCloudVersion is one cached `/v1/version` response plus the
/// api_url + last-checked stamp (so a reconfigure invalidates it).
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CachedCloudVersion {
    pub version: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub commit: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub go: String,
    pub api_url: String,
    pub last_checked_at: DateTime<Utc>,
}

/// CachedCLIRelease is the most-recent GitHub Releases response. One
/// slot total — the release is global, not per-org.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CachedCLIRelease {
    pub tag_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub html_url: String,
    pub last_checked_at: DateTime<Utc>,
}

/// config_dir resolves the directory holding config.json.
pub fn config_dir() -> CliResult<PathBuf> {
    if let Ok(v) = std::env::var("XDG_CONFIG_HOME") {
        if !v.is_empty() {
            return Ok(PathBuf::from(v).join("basin"));
        }
    }
    if let Some(h) = home_dir() {
        return Ok(h.join(".config").join("basin"));
    }
    #[cfg(windows)]
    if let Ok(v) = std::env::var("APPDATA") {
        if !v.is_empty() {
            return Ok(PathBuf::from(v).join("basin"));
        }
    }
    Err(msg("could not determine config dir (set XDG_CONFIG_HOME or HOME)"))
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .filter(|h| !h.is_empty())
        .map(PathBuf::from)
}

/// config_path is the full file path including the basename.
pub fn config_path() -> CliResult<PathBuf> {
    Ok(config_dir()?.join("config.json"))
}

/// read_config_file loads the on-disk config. A missing file returns
/// `Ok(None)`. A group/world-readable file errors so we never silently
/// honour a leaked token.
pub fn read_config_file() -> CliResult<Option<ConfigFile>> {
    let p = config_path()?;
    let meta = match std::fs::metadata(&p) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(Box::new(e)),
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if meta.permissions().mode() & 0o077 != 0 {
            return Err(msg(format!(
                "refusing to read {}: file is group- or world-readable (chmod 600)",
                p.display()
            )));
        }
    }
    let _ = &meta;
    let b = std::fs::read_to_string(&p)?;
    let cf: ConfigFile =
        serde_json::from_str(&b).map_err(|e| msg(format!("parse {}: {e}", p.display())))?;
    Ok(Some(cf))
}

/// write_config_file atomically writes the config with mode 0600 (write
/// to temp + rename so a Ctrl-C can't leave a half-written file).
pub fn write_config_file(cf: &ConfigFile) -> CliResult<()> {
    let dir = config_dir()?;
    std::fs::create_dir_all(&dir)?;
    let p = dir.join("config.json");
    let b = serde_json::to_string_pretty(cf)?;
    let tmp = p.with_extension("json.tmp");
    std::fs::write(&tmp, b.as_bytes())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    if let Err(e) = std::fs::rename(&tmp, &p) {
        let _ = std::fs::remove_file(&tmp);
        return Err(Box::new(e));
    }
    Ok(())
}

/// upsert_token stamps a token onto the per-org map (when org_slug is
/// non-empty) or default_token. Returns the resulting config + path.
pub fn upsert_token(org_slug: &str, token: &str) -> CliResult<(ConfigFile, PathBuf)> {
    let mut cf = read_config_file().unwrap_or(None).unwrap_or_default();
    if org_slug.is_empty() {
        cf.default_token = token.to_string();
    } else {
        cf.tokens.insert(org_slug.to_string(), token.to_string());
    }
    write_config_file(&cf)?;
    Ok((cf, config_path()?))
}

/// remove_token wipes the per-org entry or default_token. "" clears
/// default_token only; "*" wipes everything. Idempotent.
pub fn remove_token(org_slug: &str) -> CliResult<()> {
    let mut cf = match read_config_file() {
        Ok(Some(cf)) => cf,
        _ => return Ok(()),
    };
    match org_slug {
        "" => cf.default_token = String::new(),
        "*" => {
            cf.default_token = String::new();
            cf.tokens.clear();
        }
        s => {
            cf.tokens.remove(s);
        }
    }
    write_config_file(&cf)
}

// ── cloud-version cache ────────────────────────────────────────────

/// upsert_cloud_version records the most-recent `/v1/version` response
/// for `org_slug` (empty = default slot), stamping LastCheckedAt to now.
pub fn upsert_cloud_version(org_slug: &str, cv: &CloudVersion, api_url: &str) -> CliResult<()> {
    let mut cf = read_config_file().unwrap_or(None).unwrap_or_default();
    cf.cloud_versions.insert(
        org_slug.to_string(),
        CachedCloudVersion {
            version: cv.version.clone(),
            commit: cv.commit.clone(),
            go: cv.go.clone(),
            api_url: api_url.to_string(),
            last_checked_at: Utc::now(),
        },
    );
    write_config_file(&cf)
}

/// lookup_cloud_version reads the cached entry for `org_slug`. Returns
/// `Some` only when an entry exists AND its api_url matches the active
/// one (a reconfigured CLI forces a refetch).
pub fn lookup_cloud_version(org_slug: &str, api_url: &str) -> Option<CachedCloudVersion> {
    let cf = read_config_file().ok().flatten()?;
    let entry = cf.cloud_versions.get(org_slug)?;
    if entry.api_url != api_url {
        return None;
    }
    Some(entry.clone())
}

/// cloud_version_stale reports whether `entry` is older than the TTL.
pub fn cloud_version_stale(entry: &CachedCloudVersion) -> bool {
    age(entry.last_checked_at) > CLOUD_VERSION_TTL
}

// ── cli-release cache ──────────────────────────────────────────────

/// upsert_cli_release records the most-recent GitHub Releases response.
pub fn upsert_cli_release(rel: &CLIRelease) -> CliResult<()> {
    let mut cf = read_config_file().unwrap_or(None).unwrap_or_default();
    cf.cli_release = Some(CachedCLIRelease {
        tag_name: rel.tag_name.clone(),
        html_url: rel.html_url.clone(),
        last_checked_at: Utc::now(),
    });
    write_config_file(&cf)
}

/// lookup_cli_release reads the cached entry (None on miss / empty tag).
pub fn lookup_cli_release() -> Option<CachedCLIRelease> {
    let cf = read_config_file().ok().flatten()?;
    let rel = cf.cli_release?;
    if rel.tag_name.is_empty() {
        return None;
    }
    Some(rel)
}

/// cli_release_stale reports whether `entry` is older than the TTL.
pub fn cli_release_stale(entry: &CachedCLIRelease) -> bool {
    age(entry.last_checked_at) > CLI_RELEASE_TTL
}

/// age returns the elapsed wall-clock since `t`; a future / zero stamp
/// reads as "very old" so the caller refetches.
fn age(t: DateTime<Utc>) -> Duration {
    let secs = (Utc::now() - t).num_seconds();
    if secs <= 0 {
        // Either zero-value (epoch) or clock skew — treat as stale.
        if t.timestamp() == 0 {
            return Duration::from_secs(u64::MAX / 2);
        }
        return Duration::ZERO;
    }
    Duration::from_secs(secs as u64)
}

// ── directory-scoped working project ───────────────────────────────

/// WorkingProject is the in-memory shape of `./basin/config.toml`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WorkingProject {
    pub project_ref: String,
    pub default_branch: String,
    pub engine_version_pin: String,
}

/// WORKING_PROJECT_FILE is the path (from the project root) of the
/// directory-scoped config.
pub const WORKING_PROJECT_FILE: &str = "basin/config.toml";

/// load_working_project walks up from `cwd` looking for
/// basin/config.toml. Returns `Ok(None)` when not found anywhere.
pub fn load_working_project(cwd: &std::path::Path) -> CliResult<Option<WorkingProject>> {
    let mut dir = cwd.to_path_buf();
    loop {
        let p = dir.join(WORKING_PROJECT_FILE);
        match std::fs::read_to_string(&p) {
            Ok(b) => {
                let wp = parse_working_project_toml(&b)
                    .map_err(|e| msg(format!("parse {}: {e}", p.display())))?;
                return Ok(Some(wp));
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => match dir.parent() {
                Some(parent) if parent != dir => dir = parent.to_path_buf(),
                _ => return Ok(None),
            },
            Err(e) => return Err(Box::new(e)),
        }
    }
}

/// save_working_project writes `wp` to `<cwd>/basin/config.toml`
/// atomically with mode 0644.
pub fn save_working_project(cwd: &std::path::Path, wp: &WorkingProject) -> CliResult<()> {
    let dir = cwd.join("basin");
    std::fs::create_dir_all(&dir)?;
    let p = dir.join("config.toml");
    let tmp = p.with_extension("toml.tmp");
    std::fs::write(&tmp, marshal_working_project_toml(wp).as_bytes())?;
    if let Err(e) = std::fs::rename(&tmp, &p) {
        let _ = std::fs::remove_file(&tmp);
        return Err(Box::new(e));
    }
    Ok(())
}

/// parse_working_project_toml is a hand-rolled key=value parser handling
/// only the three keys WorkingProject needs; unknown keys are ignored.
pub fn parse_working_project_toml(b: &str) -> CliResult<WorkingProject> {
    let mut wp = WorkingProject::default();
    for (i, raw) in b.split('\n').enumerate() {
        let mut line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(idx) = line.find(" #") {
            line = line[..idx].trim();
        }
        let eq = line
            .find('=')
            .ok_or_else(|| msg(format!("line {}: expected key=value, got {:?}", i + 1, raw)))?;
        let key = line[..eq].trim();
        let mut val = line[eq + 1..].trim();
        if val.len() >= 2 {
            let bytes = val.as_bytes();
            let (f, l) = (bytes[0], bytes[bytes.len() - 1]);
            if (f == b'"' && l == b'"') || (f == b'\'' && l == b'\'') {
                val = &val[1..val.len() - 1];
            }
        }
        match key {
            "project_ref" => wp.project_ref = val.to_string(),
            "default_branch" => wp.default_branch = val.to_string(),
            "engine_version_pin" => wp.engine_version_pin = val.to_string(),
            _ => {}
        }
    }
    Ok(wp)
}

/// marshal_working_project_toml serialises `wp`; empty fields omitted.
pub fn marshal_working_project_toml(wp: &WorkingProject) -> String {
    let mut s = String::from("# basin project config — managed by the basin CLI\n");
    if !wp.project_ref.is_empty() {
        s.push_str(&format!("project_ref = {}\n", wp.project_ref));
    }
    if !wp.default_branch.is_empty() {
        s.push_str(&format!("default_branch = {}\n", wp.default_branch));
    }
    if !wp.engine_version_pin.is_empty() {
        s.push_str(&format!("engine_version_pin = {}\n", wp.engine_version_pin));
    }
    s
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_false(b: &bool) -> bool {
    !*b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn working_project_round_trip() {
        let wp = WorkingProject {
            project_ref: "myproj".into(),
            default_branch: "main".into(),
            engine_version_pin: String::new(),
        };
        let toml = marshal_working_project_toml(&wp);
        let parsed = parse_working_project_toml(&toml).unwrap();
        assert_eq!(parsed, wp);
    }

    #[test]
    fn working_project_ignores_unknown_and_comments() {
        let toml = "# header\nproject_ref = \"p1\"\nunknown_key = 5\ndefault_branch = dev # inline\n";
        let wp = parse_working_project_toml(toml).unwrap();
        assert_eq!(wp.project_ref, "p1");
        assert_eq!(wp.default_branch, "dev");
    }
}
