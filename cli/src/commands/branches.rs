//! branches — STUB. Pending Rust port from the Go `cmd_branches.go`.
//! Replace this body with the real implementation + a `#[cfg(test)]`
//! test module. Do not edit `commands/mod.rs`; the dispatch entry is
//! already wired.

use crate::error::{msg, CliResult};
use crate::global::GlobalFlags;

pub fn cmd_branches(_g: &GlobalFlags, _args: &[String]) -> CliResult<()> {
    Err(msg("basin branches: not yet ported to the Rust CLI"))
}
