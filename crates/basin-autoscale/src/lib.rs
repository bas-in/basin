//! `basin-autoscale` — engine shard autoscale controller library.
//!
//! # Overview
//!
//! This crate implements the decision logic for the engine-level shard
//! autoscaler. It is intentionally separate from the `basin-autoscale`
//! *service* binary so the policy can be unit-tested without a running
//! engine and so future cloud tooling can embed the same policy logic.
//!
//! ## What this is NOT
//!
//! The Fly Machines autoscaler (`backend-rs/src/bin/autoscaler.rs`) manages
//! the cloud *infrastructure* layer — spinning Fly VMs up/down based on
//! project count. This crate is the *engine* layer: reading per-project OTEL
//! usage counters and deciding when to split or merge hash-bucket shards
//! inside a single engine instance.
//!
//! ## Design
//!
//! `Scaler` is a pure-function decision engine:
//!
//! ```text
//! tick N:  evaluate(&self, project_id, &ProjectCounters) -> ScaleDecision
//! ```
//!
//! The caller drives the tick loop (the service binary does so on a
//! configurable interval). `Scaler` holds no async handles; all state is
//! in-memory and trivially serialisable.
//!
//! ### Hysteresis
//!
//! A `SplitShard` decision is only emitted after `hysteresis_window`
//! *consecutive* ticks where `cpu_ms` exceeds `threshold_cpu_pct` of the
//! configurable budget. This prevents flapping on momentary spikes.
//!
//! A `MergeShards` decision is emitted when `cpu_ms` drops below
//! `merge_threshold_cpu_pct` for `hysteresis_window` consecutive cool ticks
//! AND the cooldown since the last split has elapsed.
//!
//! ### Shard count bounds
//!
//! `Scaler` respects `min_shards` and `max_shards` per project. When the
//! shard count is at `max_shards` a hot project gets `Hold`; when at
//! `min_shards` a cold project gets `Hold` instead of `MergeShards`.
//!
//! [`scaler`] contains the full implementation.

#![forbid(unsafe_code)]

pub mod scaler;

pub use scaler::{ScaleDecision, Scaler, ScalerConfig, ProjectSnapshot};
