//! members — STUB. Pending Rust port from the Go `cmd_members.go`.
//! Replace this body with the real implementation + a `#[cfg(test)]`
//! test module. Do not edit `commands/mod.rs`; the dispatch entry is
//! already wired.

use crate::error::{msg, CliResult};
use crate::global::GlobalFlags;

pub fn cmd_members(_g: &GlobalFlags, _args: &[String]) -> CliResult<()> {
    Err(msg("basin members: not yet ported to the Rust CLI"))
}
