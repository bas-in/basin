//! telemetry — opt-in anonymous usage pings. Default OFF; flipped via
//! `basin config set telemetry on`. When ON, every command POSTs a
//! minimal body to `{api_url}/v1/cli/telemetry` after the handler
//! returns. No PII, no args, no env. Soft-fails on any error — a missing
//! route or sick endpoint never pollutes command output.

use std::time::Duration;

use serde_json::json;

use crate::client::version;
use crate::config::read_config_file;
use crate::global::GlobalFlags;

const TELEMETRY_ENDPOINT: &str = "/v1/cli/telemetry";

fn os_arch() -> String {
    format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH)
}

/// emit_telemetry is the side-effect entry point called from `run`. All
/// paths return silently — never bubble errors through the CLI exit.
pub fn emit_telemetry(g: &GlobalFlags, cmd: &str, dur: Duration, ok: bool) {
    let cf = match read_config_file() {
        Ok(Some(cf)) if cf.telemetry => cf,
        _ => return, // default OFF: missing file, parse error, or flag-not-set
    };
    let _ = cf;
    let body = json!({
        "cmd": cmd,
        "duration_ms": dur.as_millis() as u64,
        "version": version(),
        "os": os_arch(),
        "ok": ok,
    });
    let url = format!("{}{}", g.api_url, TELEMETRY_ENDPOINT);
    if let Ok(client) = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
    {
        let _ = client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("User-Agent", format!("basin-cli/{}", version()))
            .json(&body)
            .send();
    }
}
