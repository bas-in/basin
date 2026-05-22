//! In-process PostgreSQL-faithful advisory-lock subsystem (BUG #138 + ADR 0026).
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
//! ## ADR 0026 — blocking with lock_timeout
//!
//! Per ADR 0026, Basin uses optimistic concurrency for row writes (conflicts
//! surface as SQLSTATE 40001 at commit). Advisory locks are the *only*
//! operations that genuinely block in Basin. This module implements a real
//! keyed wait queue: a second session requesting a held key spins (using
//! `basin_shard::lock_wait::bounded_lock_wait_sync`) until the holder
//! releases or `lock_timeout` fires. On timeout the function returns
//! `BasinError::LockNotAvailable` → SQLSTATE 55P03.
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
//! * `pg_advisory_lock(bigint) -> void`: blocks until acquired or
//!   `lock_timeout` fires (→ SQLSTATE 55P03 via `BasinError::LockNotAvailable`).
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
//! ## lock_timeout enforcement (ADR 0026)
//!
//! The blocking variants use `basin_shard::lock_wait::bounded_lock_wait_sync`
//! with the session's `lock_timeout` GUC value as the deadline. On expiry
//! the function returns `BasinError::LockNotAvailable` → SQLSTATE 55P03. A
//! `None` lock_timeout means wait indefinitely (until the lock is released).
//!
//! ## LockRegistry integration (Phase 5.23.D)
//!
//! Granted advisory locks are surfaced in the `LockRegistry` so they appear
//! in `pg_locks`. A `LockHandle` is held for the life of the advisory lock and
//! dropped on release (RAII). Waiting entries (granted=false) are registered
//! before blocking and replaced by granted=true on success.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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

use basin_common::BasinError;
use basin_shard::lock_wait::bounded_lock_wait_sync;

/// Retry cadence for the blocking variants. 2 ms gives low latency for the
/// common case (held for < 10 ms) while not hammering the CPU on long waits.
const ADVISORY_RETRY_INTERVAL: Duration = Duration::from_millis(2);

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
///
/// `lock_timeout` mirrors the session's GUC: set by `SET lock_timeout = …`
/// and read by the blocking acquisition path.
///
/// `registry` and `project_id` allow granted advisory locks to appear in
/// `pg_locks` (Phase 5.23.D). `session_pid` is the synthetic backend pid.
/// These are set once at session-open time via `set_registry`.
#[derive(Debug)]
pub(crate) struct AdvisorySessionLocks {
    /// Unique owner token for this session.
    owner: u64,
    held: Mutex<HashMap<i64, HeldEntry>>,
    /// Per-session lock_timeout GUC. `None` = disabled (wait indefinitely).
    lock_timeout: Mutex<Option<Duration>>,
    /// LockRegistry for pg_locks observability. Set once via `set_registry`.
    registry: std::sync::OnceLock<basin_shard::LockRegistry>,
    /// ProjectId for LockRegistry scoping.
    project_id: std::sync::OnceLock<basin_common::ProjectId>,
    /// Synthetic backend pid for LockEntry.
    session_pid: std::sync::atomic::AtomicI32,
}

/// Per-key tracking for a single session.
#[derive(Debug)]
struct HeldEntry {
    session: u32,
    xact: u32,
    /// RAII LockHandle(s) for pg_locks. One handle per total acquisition
    /// (we keep only a single handle and drop it when count reaches zero).
    _registry_handle: Option<basin_shard::LockHandle>,
}

impl HeldEntry {
    fn total(&self) -> u32 {
        self.session + self.xact
    }
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
            lock_timeout: Mutex::new(None),
            registry: std::sync::OnceLock::new(),
            project_id: std::sync::OnceLock::new(),
            session_pid: std::sync::atomic::AtomicI32::new(0),
        }
    }

    /// Wire the LockRegistry and project context so granted locks appear in
    /// `pg_locks`. Called from `session::open` after the registry + pid are
    /// known. Must be called at most once (OnceLock semantics).
    pub(crate) fn set_registry(
        &self,
        registry: basin_shard::LockRegistry,
        project_id: basin_common::ProjectId,
        session_pid: i32,
    ) {
        let _ = self.registry.set(registry);
        let _ = self.project_id.set(project_id);
        self.session_pid
            .store(session_pid, std::sync::atomic::Ordering::Relaxed);
    }

    /// Update the session's `lock_timeout`. Called by `SET lock_timeout = …`.
    pub(crate) fn set_lock_timeout(&self, d: Option<Duration>) {
        *self.lock_timeout.lock().expect("lock_timeout poisoned") = d;
    }

    /// Read the current `lock_timeout`.
    pub(crate) fn get_lock_timeout(&self) -> Option<Duration> {
        *self.lock_timeout.lock().expect("lock_timeout poisoned")
    }

    fn note_acquired(&self, key: i64, xact: bool) {
        let pid = self.session_pid.load(std::sync::atomic::Ordering::Relaxed);
        let registry_handle = match (self.registry.get(), self.project_id.get()) {
            (Some(reg), Some(proj)) => {
                let entry = basin_shard::LockEntry::advisory_lock(pid, key, true);
                Some(reg.acquire(proj, entry))
            }
            _ => None,
        };
        let mut h = self.held.lock().expect("AdvisorySessionLocks poisoned");
        let e = h.entry(key).or_insert_with(|| HeldEntry {
            session: 0,
            xact: 0,
            _registry_handle: registry_handle,
        });
        if xact {
            e.xact += 1;
        } else {
            e.session += 1;
        }
        // If the entry already existed (reentrant), we don't add a new
        // registry handle — the existing one covers the combined hold.
    }

    /// Record a session-scoped unlock. Returns `true` if a session-scoped
    /// acquisition existed to release.
    fn note_session_unlock(&self, key: i64) -> bool {
        let mut h = self.held.lock().expect("AdvisorySessionLocks poisoned");
        match h.get_mut(&key) {
            Some(c) if c.session > 0 => {
                c.session -= 1;
                if c.total() == 0 {
                    h.remove(&key); // drops _registry_handle → pg_locks entry removed
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

    /// Blocking acquire with `lock_timeout` enforcement (ADR 0026).
    ///
    /// Spins via `bounded_lock_wait_sync` until the lock is free or the
    /// session's `lock_timeout` elapses. On expiry returns
    /// `BasinError::LockNotAvailable` → SQLSTATE 55P03.
    ///
    /// The waiter entry (`granted=false`) is registered in `LockRegistry`
    /// before blocking and replaced by `granted=true` on success.
    pub(crate) fn block_lock(&self, key: i64, xact: bool) -> Result<(), BasinError> {
        let pid = self.session_pid.load(std::sync::atomic::Ordering::Relaxed);
        // Register a waiting entry in pg_locks so observers see the contention.
        let _wait_handle: Option<basin_shard::LockHandle> =
            match (self.registry.get(), self.project_id.get()) {
                (Some(reg), Some(proj)) => {
                    let entry = basin_shard::LockEntry::advisory_lock(pid, key, false);
                    Some(reg.acquire(proj, entry))
                }
                _ => None,
            };

        let owner = self.owner;
        let acquired = bounded_lock_wait_sync(
            move || try_acquire(key, owner) == AcquireOutcome::Acquired,
            self.get_lock_timeout(),
            ADVISORY_RETRY_INTERVAL,
        );

        // _wait_handle is dropped here — removes the waiting entry from pg_locks.
        drop(_wait_handle);

        if acquired {
            self.note_acquired(key, xact);
            Ok(())
        } else {
            Err(BasinError::LockNotAvailable(format!(
                "canceling statement due to lock timeout on advisory lock {key}"
            )))
        }
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
                    h.remove(&key); // drops _registry_handle
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
        // Clear held map — this drops all _registry_handle entries.
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
//
// Blocking variant: waits until the lock is free or lock_timeout elapses.
// On timeout: returns SQLSTATE 55P03 via DataFusionError::Execution wrapping
// BasinError::LockNotAvailable (the router maps this to "55P03").
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
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
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
    fn block_lock_honors_lock_timeout_and_errors() {
        // When the lock is held and lock_timeout is set, block_lock must
        // return LockNotAvailable within the timeout window — never falsely
        // claiming success.
        let a = AdvisorySessionLocks::new();
        let b = AdvisorySessionLocks::new();
        let key = 0x1380_000A_i64;
        assert!(a.try_lock(key, false));

        // B has a 50 ms lock_timeout.
        b.set_lock_timeout(Some(Duration::from_millis(50)));
        let t0 = std::time::Instant::now();
        let err = b.block_lock(key, false).expect_err("should time out");
        let elapsed = t0.elapsed();
        // Must not take much longer than the timeout.
        assert!(
            elapsed < Duration::from_millis(500),
            "block_lock took too long: {elapsed:?}"
        );
        assert!(
            matches!(err, BasinError::LockNotAvailable(_)),
            "unexpected error: {err:?}"
        );

        a.session_unlock(key);
        // Now uncontended -> immediate Ok.
        assert!(b.block_lock(key, false).is_ok());
        b.session_unlock(key);
    }

    #[test]
    fn block_lock_succeeds_after_holder_releases() {
        // B waits with a generous timeout; A releases mid-wait → B acquires.
        let a = AdvisorySessionLocks::new();
        let b = AdvisorySessionLocks::new();
        let key = 0x1380_000B_i64;
        assert!(a.try_lock(key, false));

        let a_arc = Arc::new(a);
        let a_clone = a_arc.clone();

        // Release A's lock in a background thread after 30 ms.
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            a_clone.session_unlock(key);
        });

        b.set_lock_timeout(Some(Duration::from_millis(500)));
        b.block_lock(key, false).expect("B must acquire after A releases");
        b.session_unlock(key);
    }

    #[test]
    fn lock_timeout_none_waits_until_free() {
        // With no lock_timeout, block_lock must wait however long needed.
        let a = AdvisorySessionLocks::new();
        let b = AdvisorySessionLocks::new();
        let key = 0x1380_000C_i64;
        assert!(a.try_lock(key, false));

        let a_arc = Arc::new(a);
        let a_clone = a_arc.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            a_clone.session_unlock(key);
        });

        // No timeout — b.lock_timeout is None (wait indefinitely).
        b.block_lock(key, false).expect("must acquire once A releases");
        b.session_unlock(key);
    }
}
