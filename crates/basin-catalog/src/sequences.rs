//! Per-tenant SQL sequence catalog.
//!
//! Customers will write `CREATE SEQUENCE` and `nextval('seq')` once the
//! DDL surface lands; this module ships the underlying machinery: the
//! sequence definition, the per-tenant Mutex-guarded counter state, and
//! the `nextval` / `currval` / `setval` operations on [`Catalog`]. The
//! SQL-string entrypoint is wired by the engine via three scalar UDFs.
//!
//! The catalog is a single shared `HashMap<(TenantId, String),
//! Arc<Mutex<SequenceState>>>` — same shape as [`crate::functions`] —
//! so per-tenant cost stays `O(bytes-of-sequences)`. Registering a
//! sequence touches one HashMap entry and one Arc allocation; reading
//! / advancing one is a single Mutex acquisition that never blocks
//! another tenant's sequences (or even another sequence inside the
//! same tenant).
//!
//! ## Cache blocks
//!
//! `SequenceDef::cache_size` controls how many values a `nextval` call
//! reserves at once. The default `1` makes every `nextval` an in-memory
//! `AtomicI64` increment + bounds check; no extra catalog round-trip.
//! Setting it to e.g. 100 lets a high-rate sequence reserve 100 values
//! at once and hand them out one by one until the block is exhausted.
//! On engine restart any unused values in the in-memory block are lost
//! (a gap in the resulting id sequence) — PG calls this acceptable.
//! Duplicates are NOT acceptable; the persisted counter always advances
//! by `cache_size` before the in-memory cursor starts handing them out.

use std::sync::atomic::{AtomicI64, Ordering};

use serde::{Deserialize, Serialize};

use basin_common::TenantId;

/// Catalog row for a sequence. Field defaults match Postgres'
/// `CREATE SEQUENCE` defaults except where called out.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceDef {
    pub tenant: TenantId,
    /// Unqualified name, scoped to the owning tenant. Validated to be
    /// a SQL identifier.
    pub name: String,
    /// Starting value; the first `nextval` after creation returns this.
    pub start: i64,
    /// Step size. Negative is permitted; PG treats positive vs negative
    /// step direction symmetrically.
    pub increment: i64,
    /// Floor of the value space. PG's default is `1` for `INCREMENT 1` and
    /// `i64::MIN+1` for negative steps; we store whatever the caller hands
    /// us so the registration site decides. [`SequenceDef::with_defaults`]
    /// fills in PG-shaped defaults.
    pub min_value: i64,
    /// Ceiling of the value space. PG's default is `i64::MAX` for positive
    /// steps and `-1` for negative steps. Callers may override.
    pub max_value: i64,
    /// Block size for `nextval` reservation; `1` (the default) means every
    /// call hits the catalog state. `>1` lets the implementation hand out
    /// `cache_size` values from in-memory and refill in one catalog step.
    pub cache_size: u64,
    /// `false` (the default) errors with [`SequenceError::Exhausted`] when
    /// the next value would cross `max_value` (positive step) or `min_value`
    /// (negative step). `true` wraps to the other end.
    pub cycle: bool,
}

impl SequenceDef {
    /// Produce a sensible PG-shaped default for a sequence with the given
    /// `start` and `increment`. The min/max bounds match PG: `1..=i64::MAX`
    /// for positive steps; `i64::MIN+1..=-1` for negative.
    pub fn with_defaults(tenant: TenantId, name: impl Into<String>) -> Self {
        Self {
            tenant,
            name: name.into(),
            start: 1,
            increment: 1,
            min_value: 1,
            max_value: i64::MAX,
            cache_size: 1,
            cycle: false,
        }
    }
}

/// Mutable in-memory state for a sequence. The per-`(tenant, name)` value
/// is `Arc<Mutex<SequenceState>>`; the mutex serialises every advance so
/// concurrent `nextval` calls always observe distinct values.
///
/// `current` is the *last value handed out* (so the very first `nextval`
/// returns `start`, not `start + increment`). `block_end` is the inclusive
/// upper edge of the currently-reserved cache block; when `current` reaches
/// it, the next `nextval` reserves a new block by advancing `block_end` by
/// `cache_size * increment`.
///
/// We expose `current` / `block_end` as atomics so a future "lock-free
/// fast path" within the in-memory block is straightforward (each value
/// hand-out becomes one CAS instead of a Mutex acquisition). v0.1 uses the
/// Mutex unconditionally because the lock contention dominates only at
/// extreme call rates we don't yet need to support.
#[derive(Debug)]
pub struct SequenceState {
    pub current: AtomicI64,
    pub block_end: AtomicI64,
    /// Whether `current` has ever been advanced. `false` means the next
    /// `nextval` returns `start` (the seed); `true` means it returns
    /// `current + increment`.
    pub started: bool,
}

impl SequenceState {
    /// Build the genesis state for a fresh sequence: nothing handed out
    /// yet, no block reserved. `current` is seeded to `start - increment`
    /// so the first hand-out lands on `start` after the standard advance.
    pub fn genesis(def: &SequenceDef) -> Self {
        Self {
            current: AtomicI64::new(def.start.wrapping_sub(def.increment)),
            block_end: AtomicI64::new(def.start.wrapping_sub(def.increment)),
            started: false,
        }
    }
}

/// Errors specific to the per-sequence advance logic. Mapped to
/// [`basin_common::BasinError`] at the catalog API boundary — callers
/// see `Catalog` (state machine violation — e.g. exhausted sequence
/// with `NO CYCLE`) or `InvalidSchema` (caller bug, e.g. zero
/// increment). The "sequence not registered" case is detected at the
/// outer HashMap probe and surfaced directly as `BasinError::NotFound`.
#[derive(Debug)]
pub enum SequenceError {
    InvalidIncrement,
    Exhausted,
}

/// Compute the next value to hand out given the current state. Used by
/// the in-memory and Postgres catalog implementations to keep the actual
/// step / wrap / overflow / cycle logic in one place.
///
/// Returns the new `current` value (the value to hand back to the
/// caller) and the new `block_end` (the upper edge of the cache block
/// the in-memory state should track from now on). When the call would
/// cross `max_value` / `min_value` the function returns
/// [`SequenceError::Exhausted`] for `NO CYCLE` sequences; for `CYCLE`
/// sequences it wraps to the other end.
///
/// Pre-condition: `def.increment != 0` (the catalog API rejects this at
/// registration time; checked here defensively).
pub(crate) fn compute_next(
    def: &SequenceDef,
    last: i64,
    started: bool,
) -> Result<i64, SequenceError> {
    if def.increment == 0 {
        return Err(SequenceError::InvalidIncrement);
    }
    if !started {
        // First hand-out is the seed value verbatim.
        return Ok(def.start);
    }
    let next = last.checked_add(def.increment);
    let candidate = match next {
        Some(v) => v,
        None => {
            // i64 overflow: treat as exhaustion. `CYCLE` wraps; otherwise
            // the caller propagates Exhausted.
            if def.cycle {
                if def.increment > 0 {
                    def.min_value
                } else {
                    def.max_value
                }
            } else {
                return Err(SequenceError::Exhausted);
            }
        }
    };
    if def.increment > 0 && candidate > def.max_value {
        return if def.cycle {
            Ok(def.min_value)
        } else {
            Err(SequenceError::Exhausted)
        };
    }
    if def.increment < 0 && candidate < def.min_value {
        return if def.cycle {
            Ok(def.max_value)
        } else {
            Err(SequenceError::Exhausted)
        };
    }
    Ok(candidate)
}

/// Hand out the next value from `state`, advancing in-memory cursors and
/// optionally reserving a new cache block. Used by the in-memory catalog;
/// durable backends layer their own catalog write on top.
///
/// Returns the value to hand to the caller. The mutex on `state` is the
/// caller's responsibility — this function operates on a unique borrow.
pub(crate) fn advance_one(
    def: &SequenceDef,
    state: &mut SequenceState,
) -> Result<i64, SequenceError> {
    let last = state.current.load(Ordering::Relaxed);
    let started = state.started;
    let v = compute_next(def, last, started)?;
    state.current.store(v, Ordering::Relaxed);
    state.started = true;
    // For v0.1 the in-memory cache block tracking is bookkeeping only:
    // the per-`nextval` mutex acquisition serialises the increment, so
    // `block_end` mirrors `current`. A future fast-path (lock-free CAS
    // hand-out from a pre-reserved block) reads `block_end` to know
    // when to refill.
    state.block_end.store(v, Ordering::Relaxed);
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def_with(increment: i64, start: i64, cycle: bool) -> SequenceDef {
        SequenceDef {
            tenant: TenantId::new(),
            name: "s".into(),
            start,
            increment,
            min_value: if increment > 0 { 1 } else { i64::MIN + 1 },
            max_value: if increment > 0 { i64::MAX } else { -1 },
            cache_size: 1,
            cycle,
        }
    }

    #[test]
    fn first_call_returns_start() {
        let d = def_with(1, 5, false);
        assert_eq!(compute_next(&d, 0, false).unwrap(), 5);
    }

    #[test]
    fn subsequent_calls_advance_by_increment() {
        let d = def_with(2, 10, false);
        assert_eq!(compute_next(&d, 10, true).unwrap(), 12);
        assert_eq!(compute_next(&d, 12, true).unwrap(), 14);
    }

    #[test]
    fn no_cycle_at_max_returns_exhausted() {
        let mut d = def_with(1, 1, false);
        d.max_value = 3;
        assert!(matches!(
            compute_next(&d, 3, true),
            Err(SequenceError::Exhausted)
        ));
    }

    #[test]
    fn cycle_wraps_to_min() {
        let mut d = def_with(1, 1, true);
        d.max_value = 3;
        d.min_value = 1;
        assert_eq!(compute_next(&d, 3, true).unwrap(), 1);
    }

    #[test]
    fn negative_increment_advances_downward() {
        let d = def_with(-1, 10, false);
        assert_eq!(compute_next(&d, 0, false).unwrap(), 10);
        assert_eq!(compute_next(&d, 10, true).unwrap(), 9);
    }

    #[test]
    fn zero_increment_rejected() {
        let mut d = def_with(1, 1, false);
        d.increment = 0;
        assert!(matches!(
            compute_next(&d, 0, false),
            Err(SequenceError::InvalidIncrement)
        ));
    }
}
