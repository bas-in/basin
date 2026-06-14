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
//! keyed wait queue: a second session requesting a held key spins (in
//! [`AdvisorySessionLocks::wait_loop`], a bounded spin-with-backoff) until the
//! holder releases or the effective lock timeout fires. On timeout the function
//! returns `BasinError::LockNotAvailable` → SQLSTATE 55P03. A cyclic wait is
//! broken promptly by the wait-for-graph detector (see "Deadlock detection"
//! below) with `BasinError::DeadlockDetected` → SQLSTATE 40P01.
//!
//! ## Project-scoped keyspace
//!
//! Basin is multi-project: every session belongs to exactly one project. PG's
//! advisory-lock keyspace is per-database, so Basin's must be per-project — two
//! different projects taking the *same* numeric key (e.g. Prisma's migration
//! lock `72707369`) must **not** contend. The process-global table is therefore
//! keyed by `(project_ulid, i64_key)`, not the raw `i64`. The project is wired
//! into each [`AdvisorySessionLocks`] at session-open time via
//! [`AdvisorySessionLocks::set_registry`]; until then locks fall back to a
//! reserved "no project" namespace (`0`) which only affects unit tests that do
//! not set a project.
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
//!   address the *same* lock. The numeric key is further qualified by the
//!   owning project (see "Project-scoped keyspace" above).
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
//! * `pg_advisory_lock(bigint) -> void`: blocks until acquired or the
//!   effective lock timeout fires (→ SQLSTATE 55P03 via
//!   `BasinError::LockNotAvailable`).
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
//! The blocking variants spin in [`AdvisorySessionLocks::wait_loop`] with the
//! *effective* timeout as the deadline. The effective timeout is the
//! session's `lock_timeout` GUC if set, else the process default from the
//! `BASIN_ADVISORY_LOCK_TIMEOUT_SECS` environment variable (default 60s). A
//! value of `0` for either disables the timeout (wait indefinitely until the
//! lock is released), matching PG's `lock_timeout = 0` semantics. On expiry
//! the function returns `BasinError::LockNotAvailable` → SQLSTATE 55P03.
//!
//! ## Deadlock detection (wait-for graph)
//!
//! Basin builds a process-global **wait-for graph** so cyclic waits are broken
//! immediately with SQLSTATE 40P01 (`deadlock_detected`) instead of every
//! member of the cycle stalling until its `lock_timeout` fires (55P03). This
//! closes the documented divergence from PG's prompt deadlock reporting.
//!
//! * **Graph structure.** Nodes are *owner tokens* (one per session). An edge
//!   `A → B` means "session A is currently blocked waiting for a key that
//!   session B holds". The edge set is stored as `HashMap<owner, WaitEdge>`
//!   keyed by the *waiter*: a session executes statements serially, so a given
//!   session has at most one outstanding `block_lock` and therefore at most one
//!   out-edge at a time. A single owner can however be the *holder* of many
//!   keys, so it can be the target of many in-edges. The holder of the waited-on
//!   key is read live from `LOCK_TABLE` at detection time, so edges track holder
//!   hand-offs without per-release mutation.
//! * **Check cadence.** Cycle detection runs (a) once when the wait begins,
//!   immediately after the first failed acquire, and (b) on every retry tick of
//!   the wait loop (the `ADVISORY_RETRY_INTERVAL`, 2 ms) — i.e. "periodically
//!   while waiting". Detection is therefore prompt (sub-10 ms in practice),
//!   nowhere near the 60 s timeout. The graph is tiny (bounded by the number of
//!   *currently blocked* sessions), so a plain walk per check is far cheaper
//!   than any incremental scheme — simplicity over cleverness.
//! * **Cycle detection.** The wait-for graph is *functional* (each waiter has
//!   out-degree ≤ 1), so detection is a single linear chase from the waiter:
//!   follow "waiter → live holder of its key → that holder's own out-edge (if
//!   it too is waiting)" until the chain reaches a non-waiter (no deadlock) or
//!   revisits a node already on the path (cycle).
//! * **Victim policy.** Basin aborts the **newest** waiter in the cycle — the
//!   one with the highest owner token (owner tokens are minted from a monotonic
//!   counter, so "highest token" == "most recently opened session among the
//!   cycle members"). Every waiter in the cycle runs the same walk and computes
//!   the same max-owner victim deterministically, so exactly one is chosen
//!   without coordination. This diverges from PostgreSQL, which picks the
//!   victim by an internal *cost* heuristic (it prefers to abort the
//!   transaction that has done the least work / is cheapest to roll back).
//!   Basin has no per-session work accounting at this layer, so it uses the
//!   simplest deterministic rule that guarantees a single victim; the surviving
//!   waiters then make progress as the victim's transaction unwinds and releases
//!   its locks. The victim's `block_lock` returns `BasinError::DeadlockDetected`
//!   → SQLSTATE 40P01; its already-held locks are released by the normal error
//!   unwind (xact-end / session-end / explicit unlock paths are unchanged).
//! * **Edge lifecycle.** An edge is inserted right before the wait loop and
//!   removed the instant the wait ends — acquired, timed out, *or* aborted as a
//!   deadlock victim — via a `WaitEdgeGuard` RAII drop. No code path can leave a
//!   stale edge: even a panic in the wait loop drops the guard.
//! * **Self-deadlock.** A session waiting on a key it *already holds* is a PG
//!   no-op: re-entrancy means the acquire succeeds immediately (the owner check
//!   in `try_acquire` bumps the count), so `block_lock` never enters the wait
//!   loop and no self-edge is ever created. The walk additionally stops if a
//!   key's holder is the waiter itself, so a self-cycle is never reported.
//! * **Shared keyspace.** Session-scoped and xact-scoped acquisitions share the
//!   same `LOCK_TABLE`, so the wait-for graph covers both uniformly — an xact
//!   lock held by B is just as much an edge target as a session lock.
//!
//! ## Divergence from PostgreSQL
//!
//! * **Victim selection.** PG breaks a deadlock using a cost-based victim
//!   choice; Basin always aborts the newest waiter (highest owner token) in the
//!   cycle. Both guarantee forward progress; only *which* statement loses
//!   differs. See "Victim policy" above.
//! * **Granularity.** PG's detector covers all lock types (row, table, advisory)
//!   under one graph and fires after `deadlock_timeout` (1 s default). Basin's
//!   graph covers *advisory* locks only — they are the sole operations that
//!   genuinely block in Basin (ADR 0026; row writes use optimistic concurrency
//!   and surface 40001 at commit) — and it checks on every 2 ms retry tick, so
//!   it reports faster than PG's 1 s timer for this lock class.
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

/// Retry cadence for the blocking variants. 2 ms gives low latency for the
/// common case (held for < 10 ms) while not hammering the CPU on long waits.
const ADVISORY_RETRY_INTERVAL: Duration = Duration::from_millis(2);

/// Default lock-wait timeout when neither the `lock_timeout` GUC nor the
/// `BASIN_ADVISORY_LOCK_TIMEOUT_SECS` env var is set. Generous so that the
/// common case (Prisma's migration lock, briefly contended cron singletons)
/// always acquires, while a genuine cyclic wait eventually surfaces 55P03
/// instead of hanging the connection forever.
const DEFAULT_ADVISORY_TIMEOUT_SECS: u64 = 60;

/// The process default lock-wait timeout, read once from the environment.
///
/// `BASIN_ADVISORY_LOCK_TIMEOUT_SECS` may be set to:
/// * a positive integer N — wait up to N seconds, then 55P03;
/// * `0` — wait indefinitely (no default timeout; matches PG `lock_timeout=0`);
/// * unset / unparseable — fall back to [`DEFAULT_ADVISORY_TIMEOUT_SECS`].
fn process_default_timeout() -> Option<Duration> {
    static DEFAULT_TIMEOUT: std::sync::OnceLock<Option<Duration>> = std::sync::OnceLock::new();
    *DEFAULT_TIMEOUT.get_or_init(|| {
        let secs = std::env::var("BASIN_ADVISORY_LOCK_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_ADVISORY_TIMEOUT_SECS);
        if secs == 0 {
            None
        } else {
            Some(Duration::from_secs(secs))
        }
    })
}

/// Monotonic source of per-session owner tokens. Starts at 1 so that 0 can
/// stand in for "no owner" if ever needed.
static OWNER_SEQ: AtomicU64 = AtomicU64::new(1);

/// Process-global lock-table key: `(project_ulid, numeric_key)`. The project
/// ulid (a `u128`) namespaces the numeric key so two projects taking the same
/// `i64` do not contend. A project ulid of `0` is the reserved "no project"
/// namespace used by unit tests that never call `set_registry`.
type ScopedKey = (u128, i64);

/// One entry in the process-global advisory-lock table: the owner that holds
/// the key and how many outstanding acquisitions it has (PG reference count).
#[derive(Debug)]
struct LockHolder {
    owner: u64,
    /// Total outstanding acquisitions by `owner` (session- + xact-scoped).
    count: u32,
}

/// Process-global advisory-lock table. Keyed by `(project_ulid, i64)`. Single
/// `Mutex` — advisory-lock traffic is low-frequency control-plane, not a hot
/// path, so a coarse lock is the right tradeoff and keeps the semantics
/// trivially correct.
static LOCK_TABLE: Mutex<Option<HashMap<ScopedKey, LockHolder>>> = Mutex::new(None);

fn with_table<R>(f: impl FnOnce(&mut HashMap<ScopedKey, LockHolder>) -> R) -> R {
    let mut guard = LOCK_TABLE.lock().expect("advisory LOCK_TABLE poisoned");
    let map = guard.get_or_insert_with(HashMap::new);
    f(map)
}

// ---------------------------------------------------------------------------
// Wait-for graph (deadlock detection).
//
// One edge per *currently blocked* waiter, keyed by the waiter's owner token.
// `WaitEdge::key` is the scoped key the waiter is blocked on; the *holder* of
// that key is resolved live from LOCK_TABLE during detection so the graph
// follows holder hand-offs without us mutating edges on every release. `abort`
// is the victim signal: the wait loop polls it and bails out with
// DeadlockDetected when the detector elects this waiter as the victim.
// ---------------------------------------------------------------------------

/// A single wait-for edge: "owner (the map key) is blocked on `key`".
struct WaitEdge {
    /// The scoped key this waiter is blocked on. The holder is read live.
    key: ScopedKey,
    /// Set to `true` by the detector when this waiter is elected the victim.
    abort: Arc<std::sync::atomic::AtomicBool>,
}

/// Process-global wait-for graph: waiter-owner -> the edge it is blocked on.
/// Guarded by its own mutex (separate from `LOCK_TABLE`) so detection never
/// holds the lock-table mutex while doing graph work; the walk briefly locks
/// `LOCK_TABLE` to resolve each edge's live holder, then releases it. The two
/// mutexes are never held simultaneously, so there is no lock-ordering hazard.
static WAIT_GRAPH: Mutex<Option<HashMap<u64, WaitEdge>>> = Mutex::new(None);

fn with_graph<R>(f: impl FnOnce(&mut HashMap<u64, WaitEdge>) -> R) -> R {
    let mut guard = WAIT_GRAPH.lock().expect("advisory WAIT_GRAPH poisoned");
    let map = guard.get_or_insert_with(HashMap::new);
    f(map)
}

/// Resolve the current holder owner of `key`, if any. Used by detection to walk
/// from a waiter to the session it is blocked on.
fn holder_of(key: ScopedKey) -> Option<u64> {
    with_table(|t| t.get(&key).map(|h| h.owner))
}

/// RAII guard that removes a waiter's edge from `WAIT_GRAPH` when the wait ends
/// (acquired, timed out, or aborted as a deadlock victim) — including on panic.
struct WaitEdgeGuard {
    owner: u64,
}

impl Drop for WaitEdgeGuard {
    fn drop(&mut self) {
        with_graph(|g| {
            g.remove(&self.owner);
        });
    }
}

/// Run cycle detection over the wait-for graph and, if a cycle through `start`
/// exists, return the elected victim (the highest owner token among the
/// cycle's members — the newest session). Returns `None` when no cycle involves
/// `start`.
///
/// The graph is *functional* (each waiter has out-degree ≤ 1), so a single
/// linear chase suffices: from each node follow the live holder of the key it
/// waits on; the chain ends at a non-waiter (no cycle) or revisits a node
/// already on the path (cycle). A key whose holder is the walker itself
/// (re-entrant self-hold — which cannot actually happen, but we guard anyway)
/// ends the chain without reporting a deadlock.
fn detect_cycle_victim(start: u64) -> Option<u64> {
    // Snapshot the waiter edges (owner -> waited-on key) under the graph lock,
    // then resolve holders without holding it, to keep lock ordering simple.
    let edges: HashMap<u64, ScopedKey> =
        with_graph(|g| g.iter().map(|(o, e)| (*o, e.key)).collect());

    let mut path: Vec<u64> = Vec::new();
    let mut on_path: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut cur = start;
    loop {
        if !on_path.insert(cur) {
            // Revisited `cur`: the cycle is the suffix of `path` from the first
            // occurrence of `cur` onward; the victim is its highest owner token.
            let from = path.iter().position(|&n| n == cur).unwrap_or(0);
            return path[from..].iter().copied().max();
        }
        path.push(cur);

        // Where is `cur` blocked? If it is not a waiter, the chain ends.
        let key = match edges.get(&cur) {
            Some(k) => *k,
            None => return None,
        };
        // Who holds that key right now?
        let holder = match holder_of(key) {
            Some(h) => h,
            None => return None, // key just got released — no deadlock here
        };
        if holder == cur {
            return None; // self-hold (re-entrant) — not a deadlock
        }
        cur = holder;
    }
}

/// Set the abort flag of `victim`'s wait edge, if it is still waiting.
fn signal_victim(victim: u64) {
    with_graph(|g| {
        if let Some(e) = g.get(&victim) {
            e.abort.store(true, std::sync::atomic::Ordering::Release);
        }
    });
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
fn try_acquire(key: ScopedKey, owner: u64) -> AcquireOutcome {
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
fn release_one(key: ScopedKey, owner: u64) -> bool {
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
fn release_all_of(key: ScopedKey, owner: u64) {
    with_table(|t| {
        if let Some(h) = t.get(&key) {
            if h.owner == owner {
                t.remove(&key);
            }
        }
    });
}

/// Result of [`AdvisorySessionLocks::wait_loop`].
enum WaitOutcome {
    /// The lock was acquired.
    Acquired,
    /// The effective lock timeout fired before acquisition (→ 55P03).
    TimedOut,
    /// This waiter was elected the victim of a detected deadlock (→ 40P01).
    Deadlock,
}

impl WaitOutcome {
    /// Run cycle detection from `owner`. If a cycle exists, signal the elected
    /// victim's abort flag; if `owner` *is* the victim, return
    /// [`WaitOutcome::Deadlock`] so the caller exits immediately. Returns `None`
    /// when no deadlock involves `owner`, or when another cycle member is the
    /// victim (that member's own loop then observes its abort flag and bails).
    fn check_deadlock(owner: u64, abort: &std::sync::atomic::AtomicBool) -> Option<WaitOutcome> {
        match detect_cycle_victim(owner) {
            Some(victim) if victim == owner => {
                // We are the victim — abort right here (the flag is set too,
                // harmlessly).
                abort.store(true, std::sync::atomic::Ordering::Release);
                Some(WaitOutcome::Deadlock)
            }
            Some(victim) => {
                // Another waiter in the cycle is the victim — signal it. Its
                // wait loop polls `abort` each tick and exits with Deadlock,
                // which frees the lock(s) we are waiting on.
                signal_victim(victim);
                None
            }
            None => None,
        }
    }
}

/// Per-session advisory-lock bookkeeping. One instance per
/// [`crate::session::SessionState`], shared (via `Arc`) with the UDF closures.
///
/// `held` maps key -> (session_scoped_count, xact_scoped_count). The two
/// counts together equal the per-owner count in the global table. Splitting
/// them lets `pg_advisory_unlock` only touch session-scoped acquisitions
/// (PG: cannot manually unlock an xact lock) and lets txn-end release only
/// the xact-scoped portion. `held` is keyed by the bare numeric `i64`: every
/// session belongs to a single project, so there is no in-session collision.
///
/// `lock_timeout` mirrors the session's GUC: set by `SET lock_timeout = …`
/// and read by the blocking acquisition path. When unset the effective
/// timeout falls back to the process default (env-driven).
///
/// `registry`, `project_id`, and `project_ns` allow granted advisory locks to
/// appear in `pg_locks` (Phase 5.23.D) and to namespace the global keyspace by
/// project. `session_pid` is the synthetic backend pid. These are set once at
/// session-open time via `set_registry`.
#[derive(Debug)]
pub(crate) struct AdvisorySessionLocks {
    /// Unique owner token for this session.
    owner: u64,
    held: Mutex<HashMap<i64, HeldEntry>>,
    /// Per-session lock_timeout GUC. `None` = not set by SET (falls back to
    /// the process default); `Some(Duration::ZERO)` = explicitly disabled.
    lock_timeout: Mutex<Option<Duration>>,
    /// LockRegistry for pg_locks observability. Set once via `set_registry`.
    registry: std::sync::OnceLock<basin_shard::LockRegistry>,
    /// ProjectId for LockRegistry scoping.
    project_id: std::sync::OnceLock<basin_common::ProjectId>,
    /// Project ULID as a `u128`, used to namespace the global lock keyspace.
    /// `0` until `set_registry` is called (the reserved "no project" namespace).
    project_ns: std::sync::atomic::AtomicU64,
    /// High 64 bits of the project ULID namespace (the ULID is 128-bit).
    project_ns_hi: std::sync::atomic::AtomicU64,
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
            project_ns: std::sync::atomic::AtomicU64::new(0),
            project_ns_hi: std::sync::atomic::AtomicU64::new(0),
            session_pid: std::sync::atomic::AtomicI32::new(0),
        }
    }

    /// Wire the LockRegistry and project context so granted locks appear in
    /// `pg_locks` and the global keyspace is namespaced by project. Called
    /// from `session::open` after the registry + pid are known. Must be called
    /// at most once (OnceLock semantics for the registry/project handles).
    pub(crate) fn set_registry(
        &self,
        registry: basin_shard::LockRegistry,
        project_id: basin_common::ProjectId,
        session_pid: i32,
    ) {
        // Record the project ULID as a 128-bit namespace (split across two
        // atomics) *before* publishing the project_id handle, so any concurrent
        // reader that observes the handle also observes the namespace.
        let ulid: u128 = project_id.as_ulid().into();
        self.project_ns_hi
            .store((ulid >> 64) as u64, std::sync::atomic::Ordering::Relaxed);
        self.project_ns
            .store(ulid as u64, std::sync::atomic::Ordering::Relaxed);
        let _ = self.registry.set(registry);
        let _ = self.project_id.set(project_id);
        self.session_pid
            .store(session_pid, std::sync::atomic::Ordering::Relaxed);
    }

    /// The project namespace as a `u128` (`0` until `set_registry`).
    fn project_namespace(&self) -> u128 {
        let lo = self.project_ns.load(std::sync::atomic::Ordering::Relaxed) as u128;
        let hi = self.project_ns_hi.load(std::sync::atomic::Ordering::Relaxed) as u128;
        (hi << 64) | lo
    }

    /// Qualify a bare numeric key with this session's project namespace.
    fn scoped(&self, key: i64) -> ScopedKey {
        (self.project_namespace(), key)
    }

    /// Update the session's `lock_timeout`. Called by `SET lock_timeout = …`.
    pub(crate) fn set_lock_timeout(&self, d: Option<Duration>) {
        *self.lock_timeout.lock().expect("lock_timeout poisoned") = d;
    }

    /// Read the GUC value verbatim (without env fallback). Used by tests and
    /// by `SHOW lock_timeout`.
    pub(crate) fn get_lock_timeout(&self) -> Option<Duration> {
        *self.lock_timeout.lock().expect("lock_timeout poisoned")
    }

    /// The *effective* lock-wait deadline: the session GUC if it was set,
    /// else the process default from `BASIN_ADVISORY_LOCK_TIMEOUT_SECS`.
    /// `Some(Duration::ZERO)` (an explicit `SET lock_timeout = 0`) means wait
    /// forever, so it collapses to `None` here, matching PG.
    fn effective_timeout(&self) -> Option<Duration> {
        match self.get_lock_timeout() {
            Some(d) if d.is_zero() => None,
            Some(d) => Some(d),
            None => process_default_timeout(),
        }
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
        match try_acquire(self.scoped(key), self.owner) {
            AcquireOutcome::Acquired => {
                self.note_acquired(key, xact);
                true
            }
            AcquireOutcome::Contended => false,
        }
    }

    /// Blocking acquire with effective-timeout enforcement (ADR 0026) and
    /// wait-for-graph deadlock detection.
    ///
    /// Spins in [`Self::wait_loop`] until the lock is free, the effective
    /// timeout elapses (→ `BasinError::LockNotAvailable` / 55P03), or this
    /// waiter is elected the victim of a detected deadlock cycle (→
    /// `BasinError::DeadlockDetected` / 40P01). A re-entrant self-acquire
    /// returns immediately and never enters the wait loop or the graph.
    ///
    /// The waiter entry (`granted=false`) is registered in `LockRegistry`
    /// before blocking and replaced by `granted=true` on success.
    pub(crate) fn block_lock(&self, key: i64, xact: bool) -> Result<(), BasinError> {
        let owner = self.owner;
        let scoped = self.scoped(key);

        // Fast path: an immediate, uncontended acquire (covers the re-entrant
        // self-lock case too — try_acquire bumps the count for the same owner)
        // never enters the wait loop and never touches the wait-for graph.
        if try_acquire(scoped, owner) == AcquireOutcome::Acquired {
            self.note_acquired(key, xact);
            return Ok(());
        }

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

        // Register this waiter's out-edge in the wait-for graph. The guard
        // removes it the instant the wait ends (any exit path, including panic).
        let abort = Arc::new(std::sync::atomic::AtomicBool::new(false));
        with_graph(|g| {
            g.insert(
                owner,
                WaitEdge {
                    key: scoped,
                    abort: abort.clone(),
                },
            );
        });
        let _edge_guard = WaitEdgeGuard { owner };

        let outcome = self.wait_loop(scoped, owner, &abort);

        // _wait_handle and _edge_guard are dropped here — removes the waiting
        // entry from pg_locks and the edge from the wait-for graph.
        drop(_wait_handle);

        match outcome {
            WaitOutcome::Acquired => {
                self.note_acquired(key, xact);
                Ok(())
            }
            WaitOutcome::TimedOut => Err(BasinError::LockNotAvailable(format!(
                "canceling statement due to lock timeout on advisory lock {key}"
            ))),
            WaitOutcome::Deadlock => Err(BasinError::DeadlockDetected(format!(
                "deadlock detected waiting for advisory lock {key}"
            ))),
        }
    }

    /// The bounded wait loop with per-tick deadlock detection. Spins on
    /// `try_acquire` until the lock is free, the effective timeout fires, or the
    /// detector elects this waiter as a deadlock victim. Runs cycle detection
    /// once up front (right after the first failed acquire) and again on every
    /// retry tick — see the module header's "Check cadence".
    fn wait_loop(
        &self,
        scoped: ScopedKey,
        owner: u64,
        abort: &std::sync::atomic::AtomicBool,
    ) -> WaitOutcome {
        use std::time::Instant;
        let deadline = self.effective_timeout().map(|d| Instant::now() + d);

        // Check for a deadlock immediately: the edge we just inserted may have
        // closed a cycle. If so, fire the victim before we even sleep.
        if let Some(o) = WaitOutcome::check_deadlock(owner, abort) {
            return o;
        }

        loop {
            // Victim signal (set by *another* waiter's detection run, or our own).
            if abort.load(std::sync::atomic::Ordering::Acquire) {
                return WaitOutcome::Deadlock;
            }
            // Deadline check before sleeping.
            if let Some(dl) = deadline {
                if Instant::now() >= dl {
                    return WaitOutcome::TimedOut;
                }
            }

            let sleep_dur = if let Some(dl) = deadline {
                dl.saturating_duration_since(Instant::now())
                    .min(ADVISORY_RETRY_INTERVAL)
            } else {
                ADVISORY_RETRY_INTERVAL
            };
            std::thread::sleep(sleep_dur);

            if try_acquire(scoped, owner) == AcquireOutcome::Acquired {
                return WaitOutcome::Acquired;
            }

            // Per-tick cycle detection.
            if let Some(o) = WaitOutcome::check_deadlock(owner, abort) {
                return o;
            }

            if let Some(dl) = deadline {
                if Instant::now() >= dl {
                    return WaitOutcome::TimedOut;
                }
            }
        }
    }

    /// `pg_advisory_unlock`: release one session-scoped acquisition.
    pub(crate) fn session_unlock(&self, key: i64) -> bool {
        if self.note_session_unlock(key) {
            // Global table mirrors only the released acquisition.
            release_one(self.scoped(key), self.owner);
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
                release_one(self.scoped(key), self.owner);
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
            let scoped = self.scoped(key);
            with_table(|t| {
                if let Some(holder) = t.get_mut(&scoped) {
                    if holder.owner == self.owner {
                        holder.count = holder.count.saturating_sub(xn);
                        if holder.count == 0 {
                            t.remove(&scoped);
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
            release_all_of(self.scoped(key), self.owner);
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
// Blocking variant: waits until the lock is free or the effective timeout
// elapses. On timeout: returns SQLSTATE 55P03 via DataFusionError::External
// wrapping BasinError::LockNotAvailable (the router maps this to "55P03").
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

    /// Mint a session whose project namespace is a fixed value, so unit tests
    /// can assert project isolation without standing up an Engine. We reuse
    /// the public `set_registry` path indirectly by poking the atomics; here a
    /// tiny helper sets only the namespace (registry/pid stay unset, which is
    /// fine for the table-level semantics these tests exercise).
    fn session_in_project(ns: u128) -> AdvisorySessionLocks {
        let s = AdvisorySessionLocks::new();
        s.project_ns_hi
            .store((ns >> 64) as u64, std::sync::atomic::Ordering::Relaxed);
        s.project_ns
            .store(ns as u64, std::sync::atomic::Ordering::Relaxed);
        s
    }

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
    fn lock_timeout_zero_waits_until_free() {
        // An explicit `SET lock_timeout = 0` must wait however long needed
        // (collapses to "no deadline" in effective_timeout), even though the
        // process default would otherwise apply.
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

        // Explicit zero timeout — wait indefinitely.
        b.set_lock_timeout(Some(Duration::ZERO));
        assert_eq!(b.effective_timeout(), None);
        b.block_lock(key, false).expect("must acquire once A releases");
        b.session_unlock(key);
    }

    #[test]
    fn effective_timeout_falls_back_to_process_default() {
        // With no GUC set, the effective timeout is the process default, which
        // is some finite value (default 60s unless the env overrides it to 0).
        let s = AdvisorySessionLocks::new();
        assert_eq!(s.get_lock_timeout(), None, "GUC starts unset");
        // The process default is finite in the normal test environment.
        // (If BASIN_ADVISORY_LOCK_TIMEOUT_SECS=0 in CI, this would be None;
        // assert only that effective_timeout mirrors process_default_timeout.)
        assert_eq!(s.effective_timeout(), process_default_timeout());
    }

    #[test]
    fn same_key_isolated_across_projects() {
        // The canonical multi-project invariant: two sessions in *different*
        // projects taking the same numeric key must NOT contend.
        let p1 = session_in_project(0x1111_1111_1111_1111_2222_2222_2222_2222);
        let p2 = session_in_project(0x3333_3333_3333_3333_4444_4444_4444_4444);
        let key = 72_707_369_i64; // Prisma's migration lock key

        // Both acquire the same numeric key concurrently.
        assert!(p1.try_lock(key, false));
        assert!(p2.try_lock(key, false), "different project must not contend");

        // Within p1, a *second* session in the same project IS excluded.
        let p1b = session_in_project(0x1111_1111_1111_1111_2222_2222_2222_2222);
        assert!(!p1b.try_lock(key, false), "same project, same key → held");

        p1.session_unlock(key);
        p2.session_unlock(key);
        // After p1 releases, p1b can take it.
        assert!(p1b.try_lock(key, false));
        p1b.session_unlock(key);
    }

    // -----------------------------------------------------------------------
    // Deadlock detection (wait-for graph).
    // -----------------------------------------------------------------------

    /// 2-session ABBA deadlock: A holds k1 then blocks on k2; B holds k2 then
    /// blocks on k1. The detector must abort the newest waiter (higher owner
    /// token) with DeadlockDetected promptly (far under the timeout). After the
    /// victim's transaction unwinds — modelled here by releasing its locks, as
    /// the executor's error path does — the surviving waiter ACQUIRES.
    ///
    /// Because the victim is *deterministic* (highest owner token), the test
    /// knows up front which waiter will lose, joins it first to observe the
    /// prompt 40P01, sheds its locks, then joins the winner.
    #[test]
    fn abba_deadlock_aborts_one_and_lets_other_proceed() {
        // `a` is minted first, `b` second → b.owner > a.owner → B is the victim.
        let a = Arc::new(AdvisorySessionLocks::new());
        let b = Arc::new(AdvisorySessionLocks::new());
        assert!(b.owner > a.owner, "B must be the newest → the victim");
        let k1 = 0x1380_1000_i64;
        let k2 = 0x1380_1001_i64;

        // A holds k1, B holds k2 (no contention yet).
        assert!(a.try_lock(k1, false));
        assert!(b.try_lock(k2, false));

        // Generous timeouts so a *timeout* would take 10s — if detection failed
        // the prompt-detection assertion below catches it as a slow failure.
        a.set_lock_timeout(Some(Duration::from_secs(10)));
        b.set_lock_timeout(Some(Duration::from_secs(10)));

        let a2 = a.clone();
        let b2 = b.clone();
        let t0 = std::time::Instant::now();

        // A blocks on k2 (held by B); B blocks on k1 (held by A) → ABBA cycle.
        let ah = std::thread::spawn(move || a2.block_lock(k2, false));
        let bh = std::thread::spawn(move || b2.block_lock(k1, false));

        // The victim (B) returns promptly with 40P01.
        let rb = bh.join().unwrap();
        let victim_elapsed = t0.elapsed();
        assert!(
            victim_elapsed < Duration::from_secs(2),
            "deadlock victim must abort promptly (not via 10s timeout): {victim_elapsed:?}"
        );
        assert!(
            matches!(rb, Err(BasinError::DeadlockDetected(_))),
            "newest waiter B must be the 40P01 victim: {rb:?}"
        );

        // B's transaction unwinds → it releases everything it held (k2). The
        // executor error path does this via xact/session cleanup; we model it
        // directly here.
        b.release_all_on_session_end();

        // Now A's wait on k2 can complete: A ACQUIRES.
        let ra = ah.join().unwrap();
        assert!(ra.is_ok(), "surviving waiter A must acquire after B aborts: {ra:?}");

        a.release_all_on_session_end();
    }

    /// 3-session cycle A→B→C→A. The newest waiter (C, minted last) is the sole
    /// victim and aborts promptly with 40P01; breaking the cycle then lets the
    /// remaining acyclic chain drain (C aborts → B acquires k3 → A acquires k2)
    /// as each upstream session's transaction unwinds.
    #[test]
    fn three_session_cycle_breaks_with_one_victim() {
        let a = Arc::new(AdvisorySessionLocks::new());
        let b = Arc::new(AdvisorySessionLocks::new());
        let c = Arc::new(AdvisorySessionLocks::new());
        assert!(
            c.owner > a.owner && c.owner > b.owner,
            "C must be the newest → the victim"
        );
        let k1 = 0x1380_1010_i64;
        let k2 = 0x1380_1011_i64;
        let k3 = 0x1380_1012_i64;

        // A holds k1, B holds k2, C holds k3.
        assert!(a.try_lock(k1, false));
        assert!(b.try_lock(k2, false));
        assert!(c.try_lock(k3, false));

        for s in [&a, &b, &c] {
            s.set_lock_timeout(Some(Duration::from_secs(10)));
        }

        let (a2, b2, c2) = (a.clone(), b.clone(), c.clone());
        let t0 = std::time::Instant::now();
        // A waits on k2 (B), B waits on k3 (C), C waits on k1 (A): a 3-cycle.
        let ah = std::thread::spawn(move || a2.block_lock(k2, false));
        let bh = std::thread::spawn(move || b2.block_lock(k3, false));
        let ch = std::thread::spawn(move || c2.block_lock(k1, false));

        // C (newest) is the deterministic victim and returns promptly.
        let rc = ch.join().unwrap();
        let victim_elapsed = t0.elapsed();
        assert!(
            victim_elapsed < Duration::from_secs(3),
            "3-cycle victim must abort promptly: {victim_elapsed:?}"
        );
        assert!(
            matches!(rc, Err(BasinError::DeadlockDetected(_))),
            "newest waiter C must be the sole 40P01 victim: {rc:?}"
        );

        // C's txn unwinds → releases k3. The chain is now acyclic: B→A only.
        c.release_all_on_session_end();
        // B was waiting on k3 → now acquires.
        let rb = bh.join().unwrap();
        assert!(rb.is_ok(), "B acquires k3 after C aborts: {rb:?}");

        // B's txn unwinds → releases k2 (and k3). A was waiting on k2 → acquires.
        b.release_all_on_session_end();
        let ra = ah.join().unwrap();
        assert!(ra.is_ok(), "A acquires k2 after B unwinds: {ra:?}");

        a.release_all_on_session_end();
    }

    /// Self-deadlock: a session waiting on a key it already holds is a PG no-op
    /// (re-entrancy). `block_lock` must return Ok immediately, never enter the
    /// wait loop, and never create a self-edge.
    #[test]
    fn self_lock_is_reentrant_not_a_deadlock() {
        let a = AdvisorySessionLocks::new();
        let key = 0x1380_1020_i64;
        assert!(a.try_lock(key, false));

        let t0 = std::time::Instant::now();
        // Re-acquire the same key: must succeed immediately via re-entrancy.
        a.block_lock(key, false)
            .expect("re-entrant self-acquire must succeed, not deadlock");
        assert!(
            t0.elapsed() < Duration::from_millis(200),
            "self-acquire must be immediate"
        );
        // No edge was left behind.
        with_graph(|g| assert!(!g.contains_key(&a.owner), "no self-edge expected"));

        // Two acquisitions outstanding → two unlocks to release.
        assert!(a.session_unlock(key));
        assert!(a.session_unlock(key));
        assert!(!a.session_unlock(key));
    }

    /// No false positive: a non-cyclic wait *chain* (C waits on B waits on A,
    /// A holds and never waits) must wait normally and time out, never reporting
    /// a spurious deadlock.
    #[test]
    fn acyclic_chain_does_not_false_positive() {
        let a = Arc::new(AdvisorySessionLocks::new());
        let b = Arc::new(AdvisorySessionLocks::new());
        let c = Arc::new(AdvisorySessionLocks::new());
        let k1 = 0x1380_1030_i64;
        let k2 = 0x1380_1031_i64;

        // A holds k1 (terminal — A never waits). B holds k2 and waits on k1.
        // C waits on k2. Chain: C → B → A. No cycle.
        assert!(a.try_lock(k1, false));
        assert!(b.try_lock(k2, false));

        // Short timeouts: B and C should hit 55P03 (lock timeout), NOT 40P01.
        b.set_lock_timeout(Some(Duration::from_millis(150)));
        c.set_lock_timeout(Some(Duration::from_millis(150)));

        let b2 = b.clone();
        let c2 = c.clone();
        let bh = std::thread::spawn(move || b2.block_lock(k1, false)); // B → A
        let ch = std::thread::spawn(move || c2.block_lock(k2, false)); // C → B

        let rb = bh.join().unwrap();
        let rc = ch.join().unwrap();

        assert!(
            matches!(rb, Err(BasinError::LockNotAvailable(_))),
            "acyclic waiter B must time out (55P03), not deadlock: {rb:?}"
        );
        assert!(
            matches!(rc, Err(BasinError::LockNotAvailable(_))),
            "acyclic waiter C must time out (55P03), not deadlock: {rc:?}"
        );

        a.release_all_on_session_end();
        b.release_all_on_session_end();
        c.release_all_on_session_end();
    }

    /// The victim's *other* locks are released properly after the deadlock
    /// error: the caller's normal unwind (here `release_all_on_session_end`,
    /// mirroring session drop) frees everything, so a third session can take
    /// the victim's keys afterward.
    #[test]
    fn victim_other_locks_released_after_error() {
        // Mint B first, A second, so A is the newest → A is the victim.
        let b = Arc::new(AdvisorySessionLocks::new());
        let a = Arc::new(AdvisorySessionLocks::new());
        assert!(a.owner > b.owner, "A must be newest → the victim");
        let k1 = 0x1380_1040_i64; // held by B
        let k2 = 0x1380_1041_i64; // held by A (the soon-to-be victim)
        let extra = 0x1380_1042_i64; // an *unrelated* lock A also holds

        assert!(b.try_lock(k1, false));
        assert!(a.try_lock(k2, false));
        assert!(a.try_lock(extra, false));

        a.set_lock_timeout(Some(Duration::from_secs(10)));
        b.set_lock_timeout(Some(Duration::from_secs(10)));

        let a2 = a.clone();
        let b2 = b.clone();
        // A waits on k1 (B), B waits on k2 (A) → cycle; A (newest) is victim.
        let ah = std::thread::spawn(move || a2.block_lock(k1, false));
        let bh = std::thread::spawn(move || b2.block_lock(k2, false));

        // A (newest) is the deterministic victim and returns promptly.
        let ra = ah.join().unwrap();
        assert!(
            matches!(ra, Err(BasinError::DeadlockDetected(_))),
            "A (newest) must be the victim: {ra:?}"
        );

        // A's transaction unwinds — session end releases ALL its locks,
        // including the unrelated `extra` it held before the deadlock, and the
        // contested k2. That frees B (waiting on k2) to acquire.
        a.release_all_on_session_end();
        let rb = bh.join().unwrap();
        assert!(rb.is_ok(), "B acquires k2 after victim A unwinds: {rb:?}");
        b.release_all_on_session_end();

        // A third session can now take A's former keys (all released).
        let c = AdvisorySessionLocks::new();
        assert!(c.try_lock(k2, false), "victim's k2 must be free");
        assert!(c.try_lock(extra, false), "victim's unrelated lock must be free");
        assert!(c.try_lock(k1, false), "B's k1 must be free after its cleanup");
        c.session_unlock(k2);
        c.session_unlock(extra);
        c.session_unlock(k1);
    }

    /// Stress: N sessions take lock pairs in a strict ascending key order, so by
    /// construction the wait-for graph is acyclic. The detector must never fire
    /// a false 40P01 and the run must not hang.
    #[test]
    fn stress_acyclic_ordering_no_false_deadlock() {
        const N: usize = 12;
        // A pool of keys; every session locks two of them in ascending order,
        // which is the classic deadlock-avoidance discipline — so no cycle can
        // form regardless of interleaving.
        let base = 0x1380_1100_i64;

        let mut handles = Vec::new();
        for i in 0..N {
            let lo = base + (i as i64 % 5);
            let hi = base + 5 + (i as i64 % 5); // strictly greater than `lo`
            handles.push(std::thread::spawn(move || {
                let s = AdvisorySessionLocks::new();
                // Modest timeout: contention is fine, but no wait should be a
                // deadlock, so any 40P01 here is a false-positive bug.
                s.set_lock_timeout(Some(Duration::from_secs(5)));
                let r1 = s.block_lock(lo, false);
                let r2 = s.block_lock(hi, false);
                s.release_all_on_session_end();
                (r1, r2)
            }));
        }

        let t0 = std::time::Instant::now();
        for h in handles {
            let (r1, r2) = h.join().unwrap();
            for r in [r1, r2] {
                assert!(
                    !matches!(r, Err(BasinError::DeadlockDetected(_))),
                    "ascending-order locking must never deadlock: {r:?}"
                );
            }
        }
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "stress run must not hang"
        );
    }
}
