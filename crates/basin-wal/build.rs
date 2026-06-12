//! Code generation for the raft transport (multi-node commit 5).
//!
//! Compiles `proto/raft.proto` into `$OUT_DIR/basin.raft.v1.rs` with
//! `tonic-build` (which drives `prost-build`). The generated module is
//! `include!`d from `src/raft_net/proto.rs`. We build BOTH the client and
//! the server stubs: this crate hosts the tonic service (server side) and
//! implements `RaftNetworkFactory` over generated clients (client side).
//!
//! Codegen is feature-gated on `raft-net` so the default `basin-wal` build
//! (and every crate that only needs the disk WAL) does not require `protoc`.
//! When the feature is off, `build.rs` is a no-op and emits no `OUT_DIR`
//! file — `src/raft_net` is `#[cfg(feature = "raft-net")]` and never compiled.
//!
//! `tonic-build` 0.12 vendors `protoc` via the `prost`/`protoc-bin-vendored`
//! path on most targets; if a system `protoc` is required and missing the
//! build fails loudly with a clear message rather than silently degrading.

fn main() {
    // Only run codegen when the network transport is actually being built.
    if std::env::var_os("CARGO_FEATURE_RAFT_NET").is_none() {
        return;
    }

    println!("cargo:rerun-if-changed=proto/raft.proto");

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        // Keep the generated file name stable and explicit for the
        // `include!` in src/raft_net/proto.rs.
        .compile_protos(&["proto/raft.proto"], &["proto"])
        .expect("compile raft.proto with tonic-build");
}
