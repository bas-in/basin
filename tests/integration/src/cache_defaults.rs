//! Shared helpers that every integration test uses to wire up sensible
//! cache defaults on its `StorageConfig`.
//!
//! Background: until Phase 5.7 A1/A2 shipped, `StorageConfig::disk_cache`
//! and `StorageConfig::page_cache` defaulted to `None`. Every existing
//! benchmark card was therefore measuring the no-cache path even though
//! the real production server runs with both caches on. The integration
//! suite has been updated to enable both caches by default — these
//! helpers centralise the "make a fresh tempdir, spin it into a
//! `DiskCacheConfig`, attach a 1 GiB page cache" boilerplate so each
//! card stays a one-liner.
//!
//! The handful of tests that *must* keep caches off (the cache cards
//! themselves: `viability_disk_cache`, `viability_page_cache`,
//! `viability_perf_stack`) construct their `StorageConfig` literally
//! and don't call into here.
//!
//! Implementation note on lifetimes: the tempdir is created with
//! [`tempfile::TempDir::new`] and then leaked via
//! [`tempfile::TempDir::keep`]. Cleanup happens when the test
//! process exits — which for `cargo test` is right after the test
//! finishes, so the on-disk footprint is bounded by the duration
//! of the suite. Using `keep` lets us return owned config values
//! without forcing every call site to thread a `TempDir` guard
//! through its locals.

use basin_storage::{DiskCacheConfig, PageCacheConfig};
use tempfile::TempDir;

/// 1 GiB on-disk budget. Every benchmark working set we currently
/// drive is well under this; the cap exists only to keep the cache
/// from growing without bound on a runaway test.
pub const DEFAULT_TEST_DISK_CACHE_BYTES: u64 = 1024 * 1024 * 1024;

/// 256 MiB in-RAM page-cache budget. Generous enough that the
/// working set for every existing card fits without eviction.
pub const DEFAULT_TEST_PAGE_CACHE_BYTES: u64 = 256 * 1024 * 1024;

/// `Some(DiskCacheConfig)` rooted at a fresh per-call tempdir. The
/// tempdir is leaked (its path persists until the test process exits)
/// so callers can drop the returned value without it being reaped
/// out from under the [`Storage`](basin_storage::Storage) that holds the cache.
pub fn default_test_disk_cache() -> Option<DiskCacheConfig> {
    let dir = TempDir::new().expect("create disk-cache tempdir");
    let path = dir.keep();
    Some(DiskCacheConfig::new(path, DEFAULT_TEST_DISK_CACHE_BYTES))
}

/// `Some(PageCacheConfig)` with a 256 MiB budget — large enough that
/// no benchmark card we currently ship triggers eviction.
pub fn default_test_page_cache() -> Option<PageCacheConfig> {
    Some(PageCacheConfig::new(DEFAULT_TEST_PAGE_CACHE_BYTES))
}
