//! tokens — STUB. Pending Rust port from the Go `cmd_tokens.go`.
//! Replace this body with the real implementation + a `#[cfg(test)]`
//! test module. Do not edit `commands/mod.rs`; the dispatch entry is
//! already wired.

use crate::error::{msg, CliResult};
use crate::global::GlobalFlags;

pub fn cmd_tokens(_g: &GlobalFlags, _args: &[String]) -> CliResult<()> {
    Err(msg("basin tokens: not yet ported to the Rust CLI"))
}
