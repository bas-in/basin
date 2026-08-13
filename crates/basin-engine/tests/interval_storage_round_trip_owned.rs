//! The INTERVAL round-trip again, with `BASIN_OWNED_ENGINE=1` — the
//! configuration `fallback_histogram` measures, and the one in which the
//! interval column first has to reach disk at all.
//!
//! This is a SEPARATE test binary from `interval_storage_round_trip.rs`
//! because `BASIN_OWNED_ENGINE` is process-global: setting it inside one test
//! would silently change the configuration a sibling test believes it is
//! measuring.
//!
//! See `interval_common/mod.rs` for what is being proven and why.

mod interval_common;

fn owned_engine_on() {
    std::env::set_var("BASIN_OWNED_ENGINE", "1");
}

#[tokio::test]
async fn interval_round_trips_through_vortex_owned() {
    owned_engine_on();
    interval_common::round_trip_through_disk("", ".vortex").await;
}

#[tokio::test]
async fn interval_round_trips_through_parquet_owned() {
    owned_engine_on();
    interval_common::round_trip_through_disk(" WITH (basin.file_format='parquet')", ".parquet")
        .await;
}
