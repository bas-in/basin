//! INTERVAL round-trips to disk and back on BOTH storage formats, via the
//! DataFusion read path (the owned engine is off in this binary; its variant
//! lives in `interval_storage_round_trip_owned.rs`).
//!
//! See `interval_common/mod.rs` for what is being proven and why.

mod interval_common;

#[tokio::test]
async fn interval_round_trips_through_vortex() {
    // No WITH clause — Vortex is the default format, and the default is what
    // the fallback-histogram probe writes.
    interval_common::round_trip_through_disk("", ".vortex").await;
}

#[tokio::test]
async fn interval_round_trips_through_parquet() {
    interval_common::round_trip_through_disk(" WITH (basin.file_format='parquet')", ".parquet")
        .await;
}
