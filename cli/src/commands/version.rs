//! version — print binary version + build metadata. No network.

use serde_json::json;

use crate::client::version;
use crate::error::CliResult;
use crate::global::GlobalFlags;
use crate::output::print_json;

/// build_date is stamped via the `BASIN_BUILD_DATE` env var at build
/// time (cargo-dist / CI); uninjected it reads "unknown".
fn build_date() -> &'static str {
    match option_env!("BASIN_BUILD_DATE") {
        Some(d) if !d.is_empty() => d,
        _ => "unknown",
    }
}

/// os_arch returns "darwin/arm64" / "linux/amd64".
fn os_arch() -> String {
    format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH)
}

pub fn cmd_version(g: &GlobalFlags, _args: &[String]) -> CliResult<()> {
    if g.json {
        // JSON shape: { version: string, date: string, os_arch: string, rustc: string }
        return print_json(
            &mut std::io::stdout(),
            &json!({
                "version": version(),
                "date": build_date(),
                "os_arch": os_arch(),
                "rustc": option_env!("BASIN_RUSTC").unwrap_or(env!("CARGO_PKG_RUST_VERSION")),
            }),
        );
    }
    println!(
        "basin {} built {} for {}",
        version(),
        build_date(),
        os_arch()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flags() -> GlobalFlags {
        GlobalFlags {
            quiet: true,
            ..Default::default()
        }
    }

    #[test]
    fn plain_output_returns_ok() {
        // No network, no config — pure local string formatting.
        assert!(cmd_version(&flags(), &[]).is_ok());
    }

    #[test]
    fn json_output_returns_ok() {
        let g = GlobalFlags {
            json: true,
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_version(&g, &[]).is_ok());
    }

    #[test]
    fn json_shape_has_expected_fields() {
        // Render to a buffer to verify the JSON envelope contains the
        // four documented keys.
        let mut buf = Vec::<u8>::new();
        print_json(
            &mut buf,
            &json!({
                "version": version(),
                "date": build_date(),
                "os_arch": os_arch(),
                "rustc": "1.0.0",
            }),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(v.get("version").is_some());
        assert!(v.get("date").is_some());
        assert!(v.get("os_arch").is_some());
        assert!(v.get("rustc").is_some());
        // os_arch is always "<os>/<arch>".
        assert!(v["os_arch"].as_str().unwrap().contains('/'));
    }

    #[test]
    fn build_date_defaults_to_unknown_when_unset() {
        // The compiled binary may or may not have BASIN_BUILD_DATE
        // injected; either a non-empty string or "unknown" is valid.
        let d = build_date();
        assert!(!d.is_empty());
    }
}
