//! Atomic counters surfaced via [`SessionPool::stats`].

use std::sync::atomic::{AtomicU64, Ordering};

/// Live counters maintained by the pool. Exposed as a snapshot via
/// [`PoolStats`](crate::PoolStats); callers should treat the snapshot as a
/// point-in-time observation rather than a transactional read across fields.
#[derive(Default, Debug)]
pub(crate) struct AtomicStats {
    pub(crate) hits: AtomicU64,
    pub(crate) misses: AtomicU64,
    pub(crate) evictions: AtomicU64,
}

impl AtomicStats {
    pub(crate) fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.evictions.load(Ordering::Relaxed),
        )
    }
}

/// Public snapshot of pool counters.
///
/// `resident_sessions` is the total number of sessions held by the pool
/// (in-use plus idle). `resident_per_tenant` is the maximum number of
/// resident sessions for any single tenant — the pressure-tracking number
/// for the per-tenant cap.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PoolStats {
    pub resident_sessions: usize,
    pub resident_per_tenant: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}
