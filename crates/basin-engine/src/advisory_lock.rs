//! In-process PostgreSQL-faithful advisory-lock subsystem (BUG #138).
//!
//! ## What this replaces
//!
//! Previously `pg_advisory_lock` / `pg_advisory_unlock` were registered as
//! `VoidNullArgUdf` (return NULL, do nothing) and `pg_try_advisory_lock` as a
//! `SimpleConstBoolUdf` that *always* returned `true`. The xact-scoped
//! variants and `pg_advisory_unlock_all` were not registered at all. Net
//! effect: applications using advisory locks as distributed mutexes (job
//! queues, cron singletons, leader election) got **no mutual exclusion** and
//! were silently told every lock attempt succeeded.
//!
//! ## Scope
//!
//! Basin is a single-node engine. Cross-node advisory locks are explicitly
//! OUT of scope; we implement *node-local* correctness, which is exactly what
//! the common single-instance job-queue / cron-singleton pattern needs and
//! what the test suite exercises. The lock table is process-global (one
//! `Mutex<HashMap>` per process), shared by every session in the process.
//!
//! ## PG semantics implemented
//!
//! * Keyspace: a single `bigint` key, OR a `(int4, int4)` pair packed into an
//!   `i64` exactly as PostgreSQL does: `((classid as u32 as i64) << 32) |
//!   (objid as u32 as i64)`. `(int4,int4)` and the equivalent packed `bigint`
//!   address the *same* lock.
//! * Ownership: a lock is owned by a *session* (we mint a unique 64-bit owner
//!   token per [`crate::session::SessionState`]). A different session sees the
//!   lock as held.
//! * Reentrancy: the same owner can take the same key multiple times; PG
//!   tracks a per-owner reference count and the lock is only released when the
//!   count reaches zero (one `unlock` per `lock`). Session-scoped and
//!   xact-scoped acquisitions of the same key by the same owner accumulate
//!   into the same per-owner count, matching PG.
//! * `pg_try_advisory_lock(bigint) -> bool`: non-blocking; `true` if acquired
//!   (or already owned — bumps the count), `false` if held by another session.
//! * `pg_advisory_lock(bigint) -> void`: see the blocking-deviation note.
//! * `pg_advisory_unlock(bigint) -> bool`: `true` if this session held it
//!   (decrements the count, releasing at zero), `false` otherwise. Unlocking
//!   a key not held by this session returns `false` (PG also emits a WARNING;
//!   we just return `false`, which is the contract callers test on).
//! * `pg_advisory_xact_lock(bigint) -> void` /
//!   `pg_try_advisory_xact_lock(bigint) -> bool`: same acquisition rules but
//!   the acquisition is recorded as xact-scoped and auto-released at the end
//!   of the current transaction (COMMIT *or* ROLLBACK), with no manual unlock
//!   possible (PG: `pg_advisory_unlock` cannot release an xact lock).
//! * `pg_advisory_unlock_all() -> void`: releases every *session-scoped* lock
//!   held by this session (PG also leaves xact locks to txn-end; so do we).
//! * Session end: every lock still held by the ending session — session- and
//!   xact-scoped alike — is released (wired via `Drop for SessionState`).
//!
//! ## Deviation from PG: the blocking `pg_advisory_lock`
//!
//! True indefinite blocking inside DataFusion's synchronous scalar-UDF
//! `invoke_with_args` path is not available: a UDF cannot `.await`, cannot
//! park the executor thread without risking a Tokio worker-pool stall, and
//! the engine has no fairness/queueing primitive wired through the planner.
//! Faking it (returning immediately while pretending we blocked) would
//! re-introduce the silent-no-mutual-exclusion bug. Instead, the blocking
//! variants (`pg_advisory_lock`, `pg_advisory_xact_lock`) perform a
//! **bounded spin-with-backoff retry** (`ADVISORY_BLOCK_ATTEMPTS` attempts,
//! ~`ADVISORY_BLOCK_SLEEP` between them, std thread sleep). If the lock is
//! still held by another session after the bound elapses, the function
//! returns an explicit error (SQLSTATE 55P03 `lock_not_available`-style) so
//! the caller is *never* falsely told it holds the lock. For the common
//! single-contender test/job pattern the lock is free immediately and the
//! function returns instantly. This deviation (bounded wait + error vs.
//! indefinite block) is documented precisely here and does not compromise
//! mutual exclusion — it only changes how a long-contended blocking call
//! terminates.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use std::any::Any;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

/// Number of retry attempts the blocking variants make before giving up.
const ADVISORY_BLOCK_ATTEMPTS: u32 = 200;
/// Sleep between blocking-variant retry attempts.
const ADVISORY_BLOCK_SLEEP: std::time::Duration = std::time::Duration::from_millis(5);

/// Monotonic source of per-session owner tokens. Starts at 1 so that 0 can
/// stand in for "no owner" if ever needed.
static OWNER_SEQ: AtomicU64 = AtomicU64::new(1);

/// One entry in the process-global advisory-lock table: the owner that holds
/// the key and how many outstanding acquisitions it has (PG reference count).
#[derive(Debug)]
struct LockHolder {
    owner: u64,
    /// Total outstanding acquisitions by `owner` (session- + xact-scoped).
    count: u32,
}

/// Process-global advisory-lock table. Keyed by the `i64` lock key. Single
/// `Mutex` — advisory-lock traffic is low-frequency control-plane, not a hot
/// path, so a coarse lock is the right tradeoff and keeps the semantics
/// trivially correct.
static LOCK_TABLE: Mutex<Option<HashMap<i64, LockHolder>>> = Mutex::new(None);

fn with_table<R>(f: impl FnOnce(&mut HashMap<i64, LockHolder>) -> R) -> R {
    let mut guard = LOCK_TABLE.lock().expect("advisory LOCK_TABLE poisoned");
    let map = guard.get_or_insert_with(HashMap::new);
    f(map)
}

/// Pack a `(classid, objid)` int4 pair into the single i64 keyspace exactly
/// as PostgreSQL does: high 32 bits = classid, low 32 bits = objid, each
/// reinterpreted through `u32` first so sign is preserved bit-for-bit.
pub(crate) fn pack_two_int4(classid: i32, objid: i32) -> i64 {
    (((classid as u32 as u64) << 32) | (objid as u32 as u64)) as i64
}

/// Outcome of an acquisition attempt.
#[derive(Debug, PartialEq, Eq)]
enum AcquireOutcome {
    /// Acquired (newly, or re-entered by the same owner — count bumped).
    Acquired,
    /// Held by a *different* owner — caller could not acquire.
    Contended,
}

/// Try to acquire `key` for `owner` exactly once. Non-blocking.
fn try_acquire(key: i64, owner: u64) -> AcquireOutcome {
    with_table(|t| match t.get_mut(&key) {
        Some(h) if h.owner == owner => {
            h.count += 1;
            AcquireOutcome::Acquired
        }
        Some(_) => AcquireOutcome::Contended,
        None => {
            t.insert(key, LockHolder { owner, count: 1 });
            AcquireOutcome::Acquired
        }
    })
}

/// Release one acquisition of `key` by `owner`. Returns `true` if `owner`
/// held at least one acquisition of `key` (and one was released), `false`
/// otherwise. The entry is removed entirely when its count reaches zero.
fn release_one(key: i64, owner: u64) -> bool {
    with_table(|t| match t.get_mut(&key) {
        Some(h) if h.owner == owner => {
            h.count -= 1;
            if h.count == 0 {
                t.remove(&key);
            }
            true
        }
        _ => false,
    })
}

/// Forcibly drop *all* `count` acquisitions of `key` held by `owner` (used
/// for xact-end and session-end bulk release). No-op if not held by `owner`.
fn release_all_of(key: i64, owner: u64) {
    with_table(|t| {
        if let Some(h) = t.get(&key) {
            if h.owner == owner {
                t.remove(&key);
            }
        }
    });
}

/// Per-session advisory-lock bookkeeping. One instance per
/// [`crate::session::SessionState`], shared (via `Arc`) with the UDF closures.
///
/// `held` maps key -> (session_scoped_count, xact_scoped_count). The two
/// counts together equal the per-owner count in the global table. Splitting
/// them lets `pg_advisory_unlock` only touch session-scoped acquisitions
/// (PG: cannot manually unlock an xact lock) and lets txn-end release only
/// the xact-scoped portion.
#[derive(Debug)]
pub(crate) struct AdvisorySessionLocks {
    /// Unique owner token for this session.
    owner: u64,
    held: Mutex<HashMap<i64, HeldCounts>>,
}

#[derive(Debug, Default, Clone, Copy)]
struct HeldCounts {
    session: u32,
    xact: u32,
}

impl Default for AdvisorySessionLocks {
    fn default() -> Self {
        Self::new()
    }
}

impl AdvisorySessionLocks {
    pub(crate) fn new() -> Self {
        Self {
            owner: OWNER_SEQ.fetch_add(1, Ordering::Relaxed),
            held: Mutex::new(HashMap::new()),
        }
    }

    fn note_acquired(&self, key: i64, xact: bool) {
        let mut h = self.held.lock().expect("AdvisorySessionLocks poisoned");
        let e = h.entry(key).or_default();
        if xact {
            e.xact += 1;
        } else {
            e.session += 1;
        }
    }

    /// Record a session-scoped unlock. Returns `true` if a session-scoped
    /// acquisition existed to release.
    fn note_session_unlock(&self, key: i64) -> bool {
        let mut h = self.held.lock().expect("AdvisorySessionLocks poisoned");
        match h.get_mut(&key) {
            Some(c) if c.session > 0 => {
                c.session -= 1;
                if c.session == 0 && c.xact == 0 {
                    h.remove(&key);
                }
                true
            }
            _ => false,
        }
    }

    /// Try to take `key` (non-blocking). `xact` selects scope. Returns the
    /// boolean PG `pg_try_advisory_*` result.
    pub(crate) fn try_lock(&self, key: i64, xact: bool) -> bool {
        match try_acquire(key, self.owner) {
            AcquireOutcome::Acquired => {
                self.note_acquired(key, xact);
                true
            }
            AcquireOutcome::Contended => false,
        }
    }

    /// Bounded-blocking acquire (see module deviation note). `Ok(())` once
    /// held; `Err` if still contended after the bound.
    pub(crate) fn block_lock(&self, key: i64, xact: bool) -> Result<(), String> {
        for attempt in 0..ADVISORY_BLOCK_ATTEMPTS {
            if self.try_lock(key, xact) {
                return Ok(());
            }
            if attempt + 1 < ADVISORY_BLOCK_ATTEMPTS {
                std::thread::sleep(ADVISORY_BLOCK_SLEEP);
            }
        }
        Err(format!(
            "advisory lock {key} is held by another session; \
             Basin's pg_advisory_lock waits a bounded time then errors \
             (single-node, see advisory_lock.rs deviation note) rather than \
             blocking indefinitely or falsely reporting success"
        ))
    }

    /// `pg_advisory_unlock`: release one session-scoped acquisition.
    pub(crate) fn session_unlock(&self, key: i64) -> bool {
        if self.note_session_unlock(key) {
            // Global table mirrors only the released acquisition.
            release_one(key, self.owner);
            true
        } else {
            false
        }
    }

    /// `pg_advisory_unlock_all`: release every session-scoped acquisition
    /// this session holds. Xact-scoped acquisitions are left for txn-end
    /// (matches PG).
    pub(crate) fn unlock_all_session(&self) {
        let to_release: Vec<(i64, u32)> = {
            let h = self.held.lock().expect("AdvisorySessionLocks poisoned");
            h.iter()
                .filter(|(_, c)| c.session > 0)
                .map(|(k, c)| (*k, c.session))
                .collect()
        };
        for (key, n) in to_release {
            for _ in 0..n {
                self.note_session_unlock(key);
                release_one(key, self.owner);
            }
        }
    }

    /// Release every xact-scoped acquisition (called at txn COMMIT/ROLLBACK).
    pub(crate) fn release_xact(&self) {
        let keys: Vec<(i64, u32)> = {
            let h = self.held.lock().expect("AdvisorySessionLocks poisoned");
            h.iter()
                .filter(|(_, c)| c.xact > 0)
                .map(|(k, c)| (*k, c.xact))
                .collect()
        };
        if keys.is_empty() {
            return;
        }
        let mut h = self.held.lock().expect("AdvisorySessionLocks poisoned");
        for (key, xn) in keys {
            // Decrement the global table by exactly the xact portion.
            with_table(|t| {
                if let Some(holder) = t.get_mut(&key) {
                    if holder.owner == self.owner {
                        holder.count = holder.count.saturating_sub(xn);
                        if holder.count == 0 {
                            t.remove(&key);
                        }
                    }
                }
            });
            if let Some(c) = h.get_mut(&key) {
                c.xact = 0;
                if c.session == 0 {
                    h.remove(&key);
                }
            }
        }
    }

    /// Release *everything* this session holds (called at session end).
    pub(crate) fn release_all_on_session_end(&self) {
        let keys: Vec<i64> = {
            let h = self.held.lock().expect("AdvisorySessionLocks poisoned");
            h.keys().copied().collect()
        };
        for key in keys {
            release_all_of(key, self.owner);
        }
        self.held
            .lock()
            .expect("AdvisorySessionLocks poisoned")
            .clear();
    }
}

// ---------------------------------------------------------------------------
// Argument decoding: a single bigint key OR an (int4,int4) pair.
// ---------------------------------------------------------------------------

fn array_i64_at(arr: &ArrayRef, i: usize) -> Option<i64> {
    if arr.is_null(i) {
        return None;
    }
    match arr.data_type() {
        DataType::Int64 => arr
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .map(|a| a.value(i)),
        DataType::Int32 => arr
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .map(|a| a.value(i) as i64),
        DataType::UInt32 => arr
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt32Array>()
            .map(|a| a.value(i) as i64),
        DataType::UInt64 => arr
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .map(|a| a.value(i) as i64),
        _ => None,
    }
}

/// Decode the lock key for row `i` from the UDF args. Supports `(bigint)`,
/// `(int4)`, and the two-argument `(int4,int4)` / `(int8,int8)` packing.
fn decode_key(arrs: &[ArrayRef], i: usize) -> Option<i64> {
    match arrs.len() {
        1 => array_i64_at(&arrs[0], i),
        2 => {
            let hi = array_i64_at(&arrs[0], i)?;
            let lo = array_i64_at(&arrs[1], i)?;
            Some(pack_two_int4(hi as i32, lo as i32))
        }
        _ => None,
    }
}

fn args_to_arrays(args: &ScalarFunctionArgs) -> DFResult<(Vec<ArrayRef>, usize)> {
    let n = args
        .args
        .iter()
        .filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1);
    let mut arrs = Vec::with_capacity(args.args.len());
    for a in &args.args {
        arrs.push(a.clone().into_array(n)?);
    }
    Ok((arrs, n))
}

fn advisory_signature() -> Signature {
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Int32]),
            TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Int32, DataType::Int32]),
        ],
        Volatility::Volatile,
    )
}

// ---------------------------------------------------------------------------
// UDF: pg_try_advisory_lock / pg_try_advisory_xact_lock  -> bool
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct TryAdvisoryLockUdf {
    name: String,
    xact: bool,
    locks: Arc<AdvisorySessionLocks>,
    signature: Signature,
}

impl PartialEq for TryAdvisoryLockUdf {
    fn eq(&self, o: &Self) -> bool {
        self.name == o.name && self.xact == o.xact && Arc::ptr_eq(&self.locks, &o.locks)
    }
}
impl Eq for TryAdvisoryLockUdf {}
impl std::hash::Hash for TryAdvisoryLockUdf {
    fn hash<H: std::hash::Hasher>(&self, s: &mut H) {
        self.name.hash(s);
        self.xact.hash(s);
    }
}

impl ScalarUDFImpl for TryAdvisoryLockUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _a: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (arrs, n) = args_to_arrays(&args)?;
        let mut out: Vec<Option<bool>> = Vec::with_capacity(n);
        for i in 0..n {
            match decode_key(&arrs, i) {
                Some(key) => out.push(Some(self.locks.try_lock(key, self.xact))),
                None => out.push(None),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(out))))
    }
}

// ---------------------------------------------------------------------------
// UDF: pg_advisory_lock / pg_advisory_xact_lock  -> void (NULL)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct AdvisoryLockUdf {
    name: String,
    xact: bool,
    locks: Arc<AdvisorySessionLocks>,
    signature: Signature,
}

impl PartialEq for AdvisoryLockUdf {
    fn eq(&self, o: &Self) -> bool {
        self.name == o.name && self.xact == o.xact && Arc::ptr_eq(&self.locks, &o.locks)
    }
}
impl Eq for AdvisoryLockUdf {}
impl std::hash::Hash for AdvisoryLockUdf {
    fn hash<H: std::hash::Hasher>(&self, s: &mut H) {
        self.name.hash(s);
        self.xact.hash(s);
    }
}

impl ScalarUDFImpl for AdvisoryLockUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _a: &[DataType]) -> DFResult<DataType> {
        // PG `pg_advisory_lock` returns void; Basin surfaces void as NULL.
        Ok(DataType::Null)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (arrs, n) = args_to_arrays(&args)?;
        for i in 0..n {
            if let Some(key) = decode_key(&arrs, i) {
                self.locks
                    .block_lock(key, self.xact)
                    .map_err(DataFusionError::Execution)?;
            }
        }
        Ok(ColumnarValue::Scalar(ScalarValue::Null))
    }
}

// ---------------------------------------------------------------------------
// UDF: pg_advisory_unlock  -> bool
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct AdvisoryUnlockUdf {
    locks: Arc<AdvisorySessionLocks>,
    signature: Signature,
}

impl PartialEq for AdvisoryUnlockUdf {
    fn eq(&self, o: &Self) -> bool {
        Arc::ptr_eq(&self.locks, &o.locks)
    }
}
impl Eq for AdvisoryUnlockUdf {}
impl std::hash::Hash for AdvisoryUnlockUdf {
    fn hash<H: std::hash::Hasher>(&self, _s: &mut H) {}
}

impl ScalarUDFImpl for AdvisoryUnlockUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "pg_advisory_unlock"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _a: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (arrs, n) = args_to_arrays(&args)?;
        let mut out: Vec<Option<bool>> = Vec::with_capacity(n);
        for i in 0..n {
            match decode_key(&arrs, i) {
                Some(key) => out.push(Some(self.locks.session_unlock(key))),
                None => out.push(None),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(out))))
    }
}

// ---------------------------------------------------------------------------
// UDF: pg_advisory_unlock_all  -> void (NULL)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct AdvisoryUnlockAllUdf {
    locks: Arc<AdvisorySessionLocks>,
    signature: Signature,
}

impl PartialEq for AdvisoryUnlockAllUdf {
    fn eq(&self, o: &Self) -> bool {
        Arc::ptr_eq(&self.locks, &o.locks)
    }
}
impl Eq for AdvisoryUnlockAllUdf {}
impl std::hash::Hash for AdvisoryUnlockAllUdf {
    fn hash<H: std::hash::Hasher>(&self, _s: &mut H) {}
}

impl ScalarUDFImpl for AdvisoryUnlockAllUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "pg_advisory_unlock_all"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _a: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Null)
    }
    #[allow(deprecated)]
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        self.locks.unlock_all_session();
        Ok(ColumnarValue::Scalar(ScalarValue::Null))
    }
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

/// Register the per-session advisory-lock UDFs on `ctx`. Called from
/// `session::open` *after* the stateless cache is installed, so these
/// session-aware implementations overwrite the (now-removed) stub names.
pub(crate) fn register_advisory_lock_udfs(ctx: &SessionContext, locks: Arc<AdvisorySessionLocks>) {
    let sig = advisory_signature();

    ctx.register_udf(ScalarUDF::from(TryAdvisoryLockUdf {
        name: "pg_try_advisory_lock".into(),
        xact: false,
        locks: locks.clone(),
        signature: sig.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(TryAdvisoryLockUdf {
        name: "pg_try_advisory_xact_lock".into(),
        xact: true,
        locks: locks.clone(),
        signature: sig.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(AdvisoryLockUdf {
        name: "pg_advisory_lock".into(),
        xact: false,
        locks: locks.clone(),
        signature: sig.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(AdvisoryLockUdf {
        name: "pg_advisory_xact_lock".into(),
        xact: true,
        locks: locks.clone(),
        signature: sig.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(AdvisoryUnlockUdf {
        locks: locks.clone(),
        signature: sig.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(AdvisoryUnlockAllUdf {
        locks,
        signature: Signature::nullary(Volatility::Volatile),
    }));
}

// ===========================================================================
// Tests — two simulated sessions over the process-global table.
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_lock_excludes_other_session() {
        let a = AdvisorySessionLocks::new();
        let b = AdvisorySessionLocks::new();
        let key = 0x1380_0001_i64; // unique to this test

        // A acquires.
        assert!(a.try_lock(key, false));
        // B cannot — held by A.
        assert!(!b.try_lock(key, false));
        // A releases.
        assert!(a.session_unlock(key));
        // B now succeeds.
        assert!(b.try_lock(key, false));
        assert!(b.session_unlock(key));
    }

    #[test]
    fn reentrant_same_owner_needs_matching_unlocks() {
        let a = AdvisorySessionLocks::new();
        let b = AdvisorySessionLocks::new();
        let key = 0x1380_0002_i64;

        assert!(a.try_lock(key, false));
        assert!(a.try_lock(key, false)); // re-entered: count == 2
                                         // Still held by A after one unlock.
        assert!(a.session_unlock(key));
        assert!(!b.try_lock(key, false));
        // Second unlock fully releases.
        assert!(a.session_unlock(key));
        assert!(b.try_lock(key, false));
        assert!(b.session_unlock(key));
    }

    #[test]
    fn unlock_unheld_key_returns_false() {
        let a = AdvisorySessionLocks::new();
        assert!(!a.session_unlock(0x1380_0003_i64));
    }

    #[test]
    fn xact_lock_auto_releases_on_txn_end() {
        let a = AdvisorySessionLocks::new();
        let b = AdvisorySessionLocks::new();
        let key = 0x1380_0004_i64;

        // A takes an xact-scoped lock; B is excluded.
        assert!(a.try_lock(key, true));
        assert!(!b.try_lock(key, true));
        // Cannot be manually unlocked (it's xact-scoped, no session count).
        assert!(!a.session_unlock(key));
        // Transaction ends -> auto-release.
        a.release_xact();
        // B can now take it.
        assert!(b.try_lock(key, true));
        b.release_xact();
    }

    #[test]
    fn two_int4_form_keys_equivalently_to_packed_bigint() {
        let a = AdvisorySessionLocks::new();
        let b = AdvisorySessionLocks::new();
        let classid: i32 = 0x1380;
        let objid: i32 = 0x0005;
        let packed = pack_two_int4(classid, objid);

        // A takes the lock via the packed bigint.
        assert!(a.try_lock(packed, false));
        // B trying the *same* key via the (int4,int4) packing is excluded.
        let b_key = pack_two_int4(classid, objid);
        assert_eq!(b_key, packed);
        assert!(!b.try_lock(b_key, false));
        a.session_unlock(packed);
        assert!(b.try_lock(b_key, false));
        b.session_unlock(b_key);
    }

    #[test]
    fn negative_int4_pair_packs_like_pg() {
        // PG packs through u32; -1,-1 -> 0xFFFFFFFFFFFFFFFF as i64 == -1.
        assert_eq!(pack_two_int4(-1, -1), -1_i64);
        // (1, 0) -> 1<<32.
        assert_eq!(pack_two_int4(1, 0), 1_i64 << 32);
    }

    #[test]
    fn session_end_releases_everything() {
        let a = AdvisorySessionLocks::new();
        let b = AdvisorySessionLocks::new();
        let k1 = 0x1380_0006_i64;
        let k2 = 0x1380_0007_i64;

        assert!(a.try_lock(k1, false));
        assert!(a.try_lock(k2, true));
        assert!(!b.try_lock(k1, false));
        assert!(!b.try_lock(k2, false));

        // Session A ends.
        a.release_all_on_session_end();

        assert!(b.try_lock(k1, false));
        assert!(b.try_lock(k2, false));
        b.session_unlock(k1);
        b.session_unlock(k2);
    }

    #[test]
    fn unlock_all_releases_session_but_not_xact() {
        let a = AdvisorySessionLocks::new();
        let b = AdvisorySessionLocks::new();
        let ks = 0x1380_0008_i64;
        let kx = 0x1380_0009_i64;

        assert!(a.try_lock(ks, false));
        assert!(a.try_lock(kx, true));
        a.unlock_all_session();

        // Session-scoped released.
        assert!(b.try_lock(ks, false));
        b.session_unlock(ks);
        // Xact-scoped still held until txn end.
        assert!(!b.try_lock(kx, false));
        a.release_xact();
        assert!(b.try_lock(kx, false));
        b.session_unlock(kx);
    }

    #[test]
    fn block_lock_returns_err_when_contended() {
        // Sanity: a held key makes the bounded blocking variant give up with
        // an error rather than ever returning Ok (no fake mutual exclusion).
        // Keep the bound effect cheap by relying on the documented constants;
        // this still completes well under a second.
        let a = AdvisorySessionLocks::new();
        let b = AdvisorySessionLocks::new();
        let key = 0x1380_000A_i64;
        assert!(a.try_lock(key, false));
        let err = b.block_lock(key, false).unwrap_err();
        assert!(err.contains("held by another session"));
        a.session_unlock(key);
        // Now uncontended -> immediate Ok.
        assert!(b.block_lock(key, false).is_ok());
        b.session_unlock(key);
    }
}
