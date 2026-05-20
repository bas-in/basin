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
    println!("basin {} built {} for {}", version(), build_date(), os_arch());
    Ok(())
}
