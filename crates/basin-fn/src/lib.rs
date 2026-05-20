//! `basin-fn` — Wasm component-model host ABI (Phase 5.11.W1).
//!
//! Gives Wasm functions a **host-import interface** on the
//! WASI Preview 2 / component model so any guest language targets one ABI.
//!
//! ## Four host imports
//!
//! | Interface | Status | Wire-up |
//! |---|---|---|
//! | `basin:fn/query` | **Fully wired** | executes SQL through the mock/real engine session |
//! | `basin:fn/log` | **Fully wired** | emits via `tracing` at the requested level |
//! | `basin:fn/http` | **Wired** | `HttpSend` facade → real `basin_net` adapter or `MockHttpClient` |
//! | `basin:fn/secret` | **Wired** | `SecretStore` facade → `MockSecretStore` / catalog + `EncryptionProvider` |
//!
//! ## Component model approach
//!
//! * WIT definitions live in `crates/basin-fn/wit/basin-fn.wit`.
//! * `wasmtime::component::bindgen!` generates host-side Rust traits from the WIT.
//! * [`FunctionHost`] implements those traits and is threaded into the
//!   component [`Linker`].
//! * [`ComponentHarness`] instantiates a component, wires the four imports,
//!   and calls the exported `run` entrypoint.
//!
//! ## Feature gate
//!
//! Per ADR 0018, everything that pulls wasmtime/wasmtime-wasi lives behind
//! the `component-model` Cargo feature (default on). `--no-default-features`
//! drops the runtime; `cargo check --workspace` stays clean in both modes.

#![forbid(unsafe_code)]

// Re-export the WIT path for downstream consumers that want to embed it.
pub const WIT_BASIN_FN: &str = include_str!("../wit/basin-fn.wit");

#[cfg(feature = "component-model")]
pub mod harness;
#[cfg(feature = "component-model")]
pub mod host;
#[cfg(feature = "component-model")]
pub mod engine;
#[cfg(feature = "component-model")]
pub mod handler;
#[cfg(feature = "component-model")]
pub mod governance;

#[cfg(feature = "component-model")]
pub use harness::ComponentHarness;
#[cfg(feature = "component-model")]
pub use host::{FunctionCallContext, FunctionHost};
#[cfg(feature = "component-model")]
pub use engine::{
    FnHttpRequest, FnHttpResponse, HttpSend, MockHttpClient,
    MockSecretStore, QueryExecutor, SecretStore, StubSecretStore,
};
#[cfg(feature = "component-model")]
pub use handler::{FunctionStore, HandlerHarness, HandlerRequest, HandlerResponse};

/// Re-export of the generated WIT bindings for use in integration tests.
/// Consumers use `basin_fn::harness_bindings::basin::functions::{query, …}`.
#[cfg(feature = "component-model")]
pub use harness as harness_bindings;
