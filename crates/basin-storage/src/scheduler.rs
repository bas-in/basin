//! Per-project fair-share I/O scheduler over a shared global RPC budget.
//!
//! Earliest-deadline-first (EDF) with a per-project fairness cap.
//! Activates the v0.3 fix described in ADR 0008. Point reads (5 ms
//! deadline) consistently beat bulk scans (1 s deadline); the
//! consecutive-dispatch cap prevents one project from monopolising via
//! deadline=now flooding.
//!
//! ## Deadline-budget formula
//!
//! Each request's deadline is a function of its priority class only:
//!
//! ```text
//! deadline = now + match priority {
//!     Priority::High => HIGH_PRIORITY_DEADLINE,  // 5 ms
//!     Priority::Low  => LOW_PRIORITY_DEADLINE,   // 1 s
//! }
//! ```
//!
//! Priority is derived at the `ProjectScopedStore` boundary (see
//! `concurrency.rs`). It is derived from operation *class* and from whether the
//! caller is background work — NOT from range size:
//!
//! - `head` / `list` / `delete` / `copy` / foreground `get_opts` / `get_range`
//!   → **High** (point-shaped; query footers stay snappy).
//! - `put` / `put_multipart`, and any read issued under `is_background_io()`
//!   (compaction / merge) → **Low** (bulk-shaped). Demoting background reads is
//!   what removes the #78 priority inversion where compaction I/O out-ranked
//!   live ingest writes.
//!
//! An earlier design flipped High→Low on a range-size threshold
//! (`PRIORITY_RANGE_BYTES_THRESHOLD`, 256 KiB). That constant is gone: nothing
//! read it, and describing a rule the code does not implement is worse than
//! describing nothing.
//!
//! ADR 0008 doesn't fully specify the deadline-budget formula. We use a
//! priority-class formula (rather than a recent-throughput one) because
//! the workload signal we have is *operation shape*, not historical
//! project volume: a quiet project issuing a 1 GB scan should still
//! receive the bulk deadline, and a noisy project issuing a point lookup
//! should still receive the point deadline. Recent-throughput
//! prioritisation collapses to the same outcome in steady state (a
//! project doing many bulk ops naturally accumulates bulk-class
//! deadlines) without the bookkeeping. Per-project rate counters are
//! still maintained on `ProjectStatsAtomics` so `Storage::project_stats`
//! exposes them for the noisy-project detector at the engine layer.
//!
//! Per-project cost is O(bytes): two VecDeques + counters. Heap entries
//! are 24 bytes. No per-project tasks. Idle projects are evicted.
//!
//! Liveness: each request awaits a `oneshot::Receiver`. The dispatcher
//! is driven inline from `acquire` and `Permit::Drop` — no background
//! task. The per-project semaphore in `ProjectScopedStore` is acquired
//! BEFORE we enqueue, so its liveness floor still bounds each project.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use basin_common::ProjectId;
use tokio::sync::oneshot;

/// Floor for the High-priority partition of the global budget. ADR 0008
/// explains why <4 deadlocks the Parquet reader's range fan-out: the
/// builder may have 2-3 concurrent range fetches queued (footer +
/// column-chunk + page-index) and a smaller floor stalls forward
/// progress under back-pressure. The High pool is sized at
/// `max(MIN_HIGH_BUDGET, total / 4)` so it scales with the configured
/// total while never dropping below this hard floor.
pub const MIN_HIGH_BUDGET: usize = 4;

/// Compute the default total in-flight RPC budget across all projects.
/// The previous fixed cap of 4 was a deadlock floor; under the C=64
/// concurrent-reader workload the queueing wall became the dominant
/// latency wall (demand ≈ 192 vs cap 4 → ~50× p50 explosion). Scaling
/// with available CPU keeps the cap aligned with the workload while
/// still leaving plenty of head-room above the ADR 0008 floor.
///
/// Overridable via `BASIN_STORAGE_GLOBAL_BUDGET` (read at `Storage::new`).
pub fn default_global_budget() -> usize {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    std::cmp::max(32, 4 * cpus)
}

/// Back-compat constant retained for the small number of callers (mostly
/// tests) that opted into the historical floor explicitly. New code
/// should call [`default_global_budget`].
pub const DEFAULT_GLOBAL_BUDGET: usize = 4;

/// Deadlines per priority class. High wins the heap by ~200x.
const HIGH_PRIORITY_DEADLINE: Duration = Duration::from_millis(5);
const LOW_PRIORITY_DEADLINE: Duration = Duration::from_millis(1000);

/// Yielding project's new deadline = max(other_head, self) + epsilon.
const FAIRNESS_YIELD_EPSILON: Duration = Duration::from_micros(1);

/// Max consecutive dispatches by the same project before yielding to
/// another project with queued work. Load-bearing fairness invariant:
/// without this, a project flooding the heap with deadline=now starves
/// other projects.
const CONSECUTIVE_DISPATCH_CAP: u8 = 2;

/// Priority class. High = point reads / HEADs / footer fetches (5ms
/// deadline). Low = bulk scans / large range reads / PUTs (1s).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum Priority {
    High,
    Low,
}

impl Priority {
    fn deadline_budget(self) -> Duration {
        match self {
            Priority::High => HIGH_PRIORITY_DEADLINE,
            Priority::Low => LOW_PRIORITY_DEADLINE,
        }
    }
}

/// Public lock-free view of one project's live I/O accounting. Used by
/// the engine layer for noisy-project detection.
#[derive(Debug, Clone, Default)]
pub struct ProjectIoStats {
    pub in_flight: u64,
    pub queue_depth_high: u64,
    pub queue_depth_low: u64,
    /// Arrivals in the most recent ~1s window (resets when stale).
    pub recent_rpcs: u64,
    /// Total RPCs ever issued by this project (monotonic).
    pub total_rpcs: u64,
}

#[derive(Debug)]
pub(crate) struct ProjectStatsAtomics {
    in_flight: AtomicU64,
    queue_depth_high: AtomicU64,
    queue_depth_low: AtomicU64,
    recent_rpcs: AtomicU64,
    total_rpcs: AtomicU64,
}

impl ProjectStatsAtomics {
    fn new() -> Self {
        Self {
            in_flight: AtomicU64::new(0),
            queue_depth_high: AtomicU64::new(0),
            queue_depth_low: AtomicU64::new(0),
            recent_rpcs: AtomicU64::new(0),
            total_rpcs: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> ProjectIoStats {
        ProjectIoStats {
            in_flight: self.in_flight.load(Ordering::Relaxed),
            queue_depth_high: self.queue_depth_high.load(Ordering::Relaxed),
            queue_depth_low: self.queue_depth_low.load(Ordering::Relaxed),
            recent_rpcs: self.recent_rpcs.load(Ordering::Relaxed),
            total_rpcs: self.total_rpcs.load(Ordering::Relaxed),
        }
    }
}

struct Waiter {
    tx: oneshot::Sender<Permit>,
}

/// Per-project queue. Waiter deadline = enqueue time + priority budget.
struct ProjectQueue {
    high: VecDeque<DeadlinedWaiter>,
    low: VecDeque<DeadlinedWaiter>,
    in_flight: u64,
    /// Consecutive dispatches without yielding. Reset on project switch.
    consecutive_dispatches: u8,
    /// Set when this project has a (possibly stale) entry in the heap.
    in_heap: bool,
    stats: Arc<ProjectStatsAtomics>,
}

struct DeadlinedWaiter {
    waiter: Waiter,
    deadline: Instant,
}

impl ProjectQueue {
    fn new(stats: Arc<ProjectStatsAtomics>) -> Self {
        Self {
            high: VecDeque::new(),
            low: VecDeque::new(),
            in_flight: 0,
            consecutive_dispatches: 0,
            in_heap: false,
            stats,
        }
    }

    /// Pop next waiter, restricted to the given priority lane. Used when
    /// one priority's pool is exhausted but the other still has slots.
    fn pop_next_with_priority(&mut self, p: Priority) -> Option<DeadlinedWaiter> {
        match p {
            Priority::High => {
                let w = self.high.pop_front()?;
                self.stats.queue_depth_high.fetch_sub(1, Ordering::Relaxed);
                Some(w)
            }
            Priority::Low => {
                let w = self.low.pop_front()?;
                self.stats.queue_depth_low.fetch_sub(1, Ordering::Relaxed);
                Some(w)
            }
        }
    }


    /// Earliest deadline across queued waiters; None if queue is empty.
    fn head_deadline(&self) -> Option<Instant> {
        match (self.high.front(), self.low.front()) {
            (Some(h), Some(l)) => Some(h.deadline.min(l.deadline)),
            (Some(h), None) => Some(h.deadline),
            (None, Some(l)) => Some(l.deadline),
            (None, None) => None,
        }
    }

    fn is_idle(&self) -> bool {
        self.high.is_empty() && self.low.is_empty() && self.in_flight == 0
    }

    fn is_queue_empty(&self) -> bool {
        self.high.is_empty() && self.low.is_empty()
    }
}

/// One heap entry per active project. Stale entries (deadline differs
/// from project's current `head_deadline()`) are skipped on pop.
/// Wrapped in `Reverse` to make `BinaryHeap` a min-heap on deadline.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct DeadlineEntry {
    deadline: Instant,
    seq: u64,
    project: ProjectId,
}

impl Ord for DeadlineEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| self.seq.cmp(&other.seq))
            .then_with(|| self.project.cmp(&other.project))
    }
}

impl PartialOrd for DeadlineEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Fair-share I/O scheduler. One per `Storage`. Cheap to clone (Arc inside).
#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<SchedulerInner>,
}

struct SchedulerInner {
    global_budget: usize,
    state: Mutex<State>,
    /// Per-project atomic stats. Read lock-free by `Storage::project_stats`.
    stats: Mutex<HashMap<ProjectId, Arc<ProjectStatsAtomics>>>,
}

/// Per-priority partition of the global budget. The pools are
/// independent: a saturated Low pool cannot starve High, and vice
/// versa. ADR 0008 still applies — `high_available` is sized at
/// startup with a floor of [`MIN_HIGH_BUDGET`] so the parquet reader's
/// range fan-out can't deadlock waiting for a sibling fetch to land.
struct State {
    high_available: usize,
    low_available: usize,
    queues: HashMap<ProjectId, ProjectQueue>,
    heap: BinaryHeap<Reverse<DeadlineEntry>>,
    /// Per-project rate window start (recent_rpcs reset boundary).
    rate_window_start: HashMap<ProjectId, Instant>,
    next_seq: u64,
    last_dispatched_project: Option<ProjectId>,
}

/// RAII permit. Drop releases the global slot.
pub(crate) struct Permit {
    sched: Arc<SchedulerInner>,
    project: ProjectId,
    /// Which pool to return the slot to on drop.
    priority: Priority,
    /// false if the receiver was dropped before the permit landed.
    held: bool,
}

impl Drop for Permit {
    fn drop(&mut self) {
        if !self.held {
            return;
        }
        let mut state = self.sched.state.lock().expect("scheduler state poisoned");
        match self.priority {
            Priority::High => state.high_available += 1,
            Priority::Low => state.low_available += 1,
        }
        if let Some(q) = state.queues.get_mut(&self.project) {
            if q.in_flight > 0 {
                q.in_flight -= 1;
                self.sched.stats_for(&self.project, |s| {
                    s.in_flight.fetch_sub(1, Ordering::Relaxed);
                });
            }
            if q.is_idle() {
                state.queues.remove(&self.project);
                state.rate_window_start.remove(&self.project);
            }
        }
        drop(state);
        // Wake the next-deadline waiter; without this, parked oneshots
        // never resume — only new acquire() callers run try_dispatch.
        Scheduler {
            inner: self.sched.clone(),
        }
        .try_dispatch();
    }
}

impl Scheduler {
    /// Compute the split between High and Low pools.
    ///
    /// - For small budgets (≤ MIN_HIGH_BUDGET * 2) we collapse to a
    ///   single shared pool by putting *all* budget into High; Low
    ///   waiters then dispatch only when High is idle. This preserves
    ///   the ADR 0008 deadlock floor on tiny budgets (the legacy
    ///   single-pool shape) and keeps the existing 1-2-4 budget tests
    ///   meaningful.
    /// - For larger budgets we partition: High gets at least
    ///   [`MIN_HIGH_BUDGET`] (or 25% of total — whichever is larger),
    ///   Low gets the remainder. So with the default 32-slot total we
    ///   land at High = 8 / Low = 24, which is well above the C=64
    ///   queueing wall on point reads while leaving bulk operations
    ///   plenty of headroom.
    fn split_budget(global_budget: usize) -> (usize, usize) {
        if global_budget <= MIN_HIGH_BUDGET * 2 {
            return (global_budget, 0);
        }
        let high = std::cmp::max(MIN_HIGH_BUDGET, global_budget / 4).min(global_budget);
        let low = global_budget - high;
        (high, low)
    }

    pub fn new(global_budget: usize) -> Self {
        let (high_available, low_available) = Self::split_budget(global_budget);
        Self {
            inner: Arc::new(SchedulerInner {
                global_budget,
                state: Mutex::new(State {
                    high_available,
                    low_available,
                    queues: HashMap::new(),
                    heap: BinaryHeap::new(),
                    rate_window_start: HashMap::new(),
                    next_seq: 0,
                    last_dispatched_project: None,
                }),
                stats: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Enqueue and await dispatch. Returns a `Permit` that must be held
    /// for the underlying RPC's lifetime. Cancellation-safe.
    pub(crate) async fn acquire(&self, project: ProjectId, priority: Priority) -> Permit {
        let (tx, rx) = oneshot::channel();
        let deadline = Instant::now() + priority.deadline_budget();
        let dw = DeadlinedWaiter {
            waiter: Waiter { tx },
            deadline,
        };

        {
            let stats = self.inner.ensure_stats(&project);
            let mut state = self.inner.state.lock().expect("scheduler state poisoned");

            // Recent-rpcs single-second rate window.
            let now = Instant::now();
            let win_start = state.rate_window_start.entry(project).or_insert(now);
            if now.duration_since(*win_start).as_secs_f64() >= 1.0 {
                *win_start = now;
                stats.recent_rpcs.store(0, Ordering::Relaxed);
            }
            stats.recent_rpcs.fetch_add(1, Ordering::Relaxed);
            stats.total_rpcs.fetch_add(1, Ordering::Relaxed);

            let head_deadline = {
                let q = state
                    .queues
                    .entry(project)
                    .or_insert_with(|| ProjectQueue::new(stats.clone()));
                match priority {
                    Priority::High => {
                        q.high.push_back(dw);
                        stats.queue_depth_high.fetch_add(1, Ordering::Relaxed);
                    }
                    Priority::Low => {
                        q.low.push_back(dw);
                        stats.queue_depth_low.fetch_add(1, Ordering::Relaxed);
                    }
                }
                q.in_heap = true;
                q.head_deadline().expect("just pushed")
            };
            // Always push a fresh entry stamped with the current head
            // deadline. Older entries become stale and are skipped on
            // pop — cheaper than tracking a single-entry invariant.
            let seq = state.next_seq;
            state.next_seq = state.next_seq.wrapping_add(1);
            state.heap.push(Reverse(DeadlineEntry {
                deadline: head_deadline,
                seq,
                project,
            }));
        }

        self.try_dispatch();
        rx.await.expect("dispatcher dropped waiter")
    }

    /// Lock-free read of one project's live stats. Returns zeros if the
    /// project has never used the scheduler.
    pub fn project_stats(&self, project: &ProjectId) -> ProjectIoStats {
        let stats = self.inner.stats.lock().expect("stats map poisoned");
        match stats.get(project) {
            Some(s) => s.snapshot(),
            None => ProjectIoStats::default(),
        }
    }

    /// Dispatch as many waiters as we have budget for. Holds the state
    /// lock only for the short bookkeeping window — never across an
    /// await — so this is safe to call from the request hot path.
    fn try_dispatch(&self) {
        loop {
            let (waiter, project, priority) = {
                let mut state = self.inner.state.lock().expect("scheduler state poisoned");
                if state.high_available == 0 && state.low_available == 0 {
                    return;
                }
                match self.pick_next(&mut state) {
                    Some(p) => p,
                    None => return,
                }
            };

            // Hand the permit to the waiter. If the receiver was
            // dropped (caller cancelled), reclaim immediately via
            // Permit::Drop.
            let permit = Permit {
                sched: self.inner.clone(),
                project,
                priority,
                held: true,
            };
            if let Err(returned) = waiter.tx.send(permit) {
                drop(returned);
            }
        }
    }

    /// Pop the earliest-deadline project honoring the fairness cap.
    /// On success: the matching pool (`high_available` or
    /// `low_available`) is decremented, in_flight incremented, and
    /// consecutive_dispatches updated. Returns None if nothing is
    /// dispatchable in the matching pool.
    fn pick_next(&self, state: &mut State) -> Option<(Waiter, ProjectId, Priority)> {
        // Defensive bound: stale entries are dropped without re-push;
        // fairness yields re-push at most once per encounter.
        let mut sweep_budget = state.heap.len().saturating_mul(2) + 8;
        // Track projects we've already inspected this sweep that had no
        // eligible waiter (pool exhausted on the only queued lane).
        // We re-push their heap entry once and then skip on re-encounter
        // so we don't spin.
        let mut skipped: std::collections::HashSet<ProjectId> = std::collections::HashSet::new();
        while sweep_budget > 0 {
            sweep_budget -= 1;

            let Reverse(top) = state.heap.pop()?;

            // Stale check: the heap entry must match the project's
            // current head_deadline. If it doesn't, the project has
            // either drained or has a newer (earlier) entry pushed.
            let (matches, head_dl) = match state.queues.get(&top.project) {
                Some(q) => match q.head_deadline() {
                    Some(hd) => (hd == top.deadline, Some(hd)),
                    None => (false, None),
                },
                None => (false, None),
            };
            if !matches {
                if head_dl.is_none() {
                    if let Some(q) = state.queues.get_mut(&top.project) {
                        q.in_heap = false;
                    }
                }
                continue;
            }

            // Fairness yield: if this project hit the consecutive cap
            // and any other project has queued work, bump our deadlines
            // past theirs and re-push.
            let consecutive = state
                .queues
                .get(&top.project)
                .map(|q| q.consecutive_dispatches)
                .unwrap_or(0);
            if consecutive >= CONSECUTIVE_DISPATCH_CAP {
                let next_other = state
                    .queues
                    .iter()
                    .filter(|(t, q)| **t != top.project && !q.is_queue_empty())
                    .filter_map(|(_, q)| q.head_deadline())
                    .min();
                if let Some(other_dl) = next_other {
                    let target = (other_dl + FAIRNESS_YIELD_EPSILON)
                        .max(top.deadline + FAIRNESS_YIELD_EPSILON);
                    if let Some(q) = state.queues.get_mut(&top.project) {
                        let shift = target.saturating_duration_since(top.deadline);
                        for w in q.high.iter_mut() {
                            w.deadline += shift;
                        }
                        for w in q.low.iter_mut() {
                            w.deadline += shift;
                        }
                        q.consecutive_dispatches = 0;
                        q.in_heap = true;
                    }
                    let seq = state.next_seq;
                    state.next_seq = state.next_seq.wrapping_add(1);
                    state.heap.push(Reverse(DeadlineEntry {
                        deadline: target,
                        seq,
                        project: top.project,
                    }));
                    continue;
                }
                // Sole project — clear streak, dispatch.
                if let Some(q) = state.queues.get_mut(&top.project) {
                    q.consecutive_dispatches = 0;
                }
            }

            // Decide which lane to pop from (Priority::High = High
            // queue; Priority::Low = Low queue) and which pool to
            // charge. Normal case: lane == pool. Single-pool fallback:
            // when the configured budget is small enough that
            // `low_available == 0` at construction
            // (`global_budget ≤ MIN_HIGH_BUDGET*2`, see
            // [`Scheduler::split_budget`]), Low waiters borrow from the
            // High pool. We tag the borrowed permit's priority as
            // Priority::High so it returns to the right pool on drop —
            // accounting still balances.
            let small_budget_single_pool = self.inner.global_budget <= MIN_HIGH_BUDGET * 2;
            let (lane, pool): (Option<Priority>, Option<Priority>) = {
                let q = state.queues.get(&top.project)?;
                let has_high = !q.high.is_empty();
                let has_low = !q.low.is_empty();
                if has_high && state.high_available > 0 {
                    (Some(Priority::High), Some(Priority::High))
                } else if has_low && state.low_available > 0 {
                    (Some(Priority::Low), Some(Priority::Low))
                } else if has_low && small_budget_single_pool && state.high_available > 0 {
                    // Borrow High budget for Low; permit returns to High pool.
                    (Some(Priority::Low), Some(Priority::High))
                } else {
                    (None, None)
                }
            };
            let chosen_priority = pool;

            let Some(chosen_priority) = chosen_priority else {
                // No pool can serve this project right now. Re-push the
                // heap entry once and try the next project. If we
                // re-encounter the same project later in this sweep,
                // there's nothing dispatchable across the heap → bail
                // out to avoid an infinite re-push loop.
                if !skipped.insert(top.project) {
                    // Already re-pushed this project once in this sweep.
                    // The remaining heap is either stale or also
                    // blocked; no forward progress possible right now.
                    // Re-push the current entry so it's reconsidered
                    // when a permit returns.
                    if let Some(d) = state
                        .queues
                        .get(&top.project)
                        .and_then(|q| q.head_deadline())
                    {
                        let seq = state.next_seq;
                        state.next_seq = state.next_seq.wrapping_add(1);
                        state.heap.push(Reverse(DeadlineEntry {
                            deadline: d,
                            seq,
                            project: top.project,
                        }));
                        if let Some(q) = state.queues.get_mut(&top.project) {
                            q.in_heap = true;
                        }
                    }
                    return None;
                }
                if let Some(d) = state
                    .queues
                    .get(&top.project)
                    .and_then(|q| q.head_deadline())
                {
                    let seq = state.next_seq;
                    state.next_seq = state.next_seq.wrapping_add(1);
                    state.heap.push(Reverse(DeadlineEntry {
                        deadline: d,
                        seq,
                        project: top.project,
                    }));
                    if let Some(q) = state.queues.get_mut(&top.project) {
                        q.in_heap = true;
                    }
                }
                continue;
            };

            // Pop the waiter from the chosen lane.
            let lane = lane.expect("lane set whenever pool is set");
            let (waiter, queue_drained) = {
                let Some(q) = state.queues.get_mut(&top.project) else {
                    continue;
                };
                let Some(w) = q.pop_next_with_priority(lane) else {
                    continue;
                };
                q.in_flight += 1;
                q.stats.in_flight.fetch_add(1, Ordering::Relaxed);
                let drained = q.is_queue_empty();
                if drained {
                    q.in_heap = false;
                }
                (w, drained)
            };

            // If more remain, re-push a fresh entry for the new head.
            if !queue_drained {
                if let Some(d) = state
                    .queues
                    .get(&top.project)
                    .and_then(|q| q.head_deadline())
                {
                    let seq = state.next_seq;
                    state.next_seq = state.next_seq.wrapping_add(1);
                    state.heap.push(Reverse(DeadlineEntry {
                        deadline: d,
                        seq,
                        project: top.project,
                    }));
                    if let Some(q) = state.queues.get_mut(&top.project) {
                        q.in_heap = true;
                    }
                }
            }

            // Streak bookkeeping.
            let same = state.last_dispatched_project == Some(top.project);
            if let Some(q) = state.queues.get_mut(&top.project) {
                q.consecutive_dispatches = if same {
                    q.consecutive_dispatches.saturating_add(1)
                } else {
                    1
                };
            }
            if !same {
                if let Some(prev) = state.last_dispatched_project {
                    if let Some(prev_q) = state.queues.get_mut(&prev) {
                        prev_q.consecutive_dispatches = 0;
                    }
                }
            }
            state.last_dispatched_project = Some(top.project);
            match chosen_priority {
                Priority::High => state.high_available -= 1,
                Priority::Low => state.low_available -= 1,
            }
            return Some((waiter.waiter, top.project, chosen_priority));
        }
        None
    }

    /// Snapshot of the available budget per pool. Only used for tests.
    #[cfg(test)]
    fn pool_snapshot(&self) -> (usize, usize) {
        let s = self.inner.state.lock().expect("scheduler state poisoned");
        (s.high_available, s.low_available)
    }
}

impl SchedulerInner {
    fn ensure_stats(&self, project: &ProjectId) -> Arc<ProjectStatsAtomics> {
        let mut map = self.stats.lock().expect("stats map poisoned");
        map.entry(*project)
            .or_insert_with(|| Arc::new(ProjectStatsAtomics::new()))
            .clone()
    }

    fn stats_for<F: FnOnce(&ProjectStatsAtomics)>(&self, project: &ProjectId, f: F) {
        let map = self.stats.lock().expect("stats map poisoned");
        if let Some(s) = map.get(project) {
            f(s);
        }
    }
}

impl std::fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler")
            .field("global_budget", &self.inner.global_budget)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    #[tokio::test]
    async fn budget_caps_concurrency() {
        let s = Scheduler::new(2);
        let project = ProjectId::new();
        let max = Arc::new(AtomicUsize::new(0));
        let cur = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];
        for _ in 0..10 {
            let s = s.clone();
            let t = project;
            let max = max.clone();
            let cur = cur.clone();
            handles.push(tokio::spawn(async move {
                let _p = s.acquire(t, Priority::Low).await;
                let now = cur.fetch_add(1, Ordering::Relaxed) + 1;
                max.fetch_max(now, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(20)).await;
                cur.fetch_sub(1, Ordering::Relaxed);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert!(
            max.load(Ordering::Relaxed) <= 2,
            "max in-flight exceeded budget: {}",
            max.load(Ordering::Relaxed)
        );
    }

    /// Fairness cap: A pre-loads 5 Low requests, B arrives later with
    /// a single Low. Pure EDF would serve all 5 As first (A's earlier
    /// deadlines). The cap forces a yield after 2 consecutive A wins,
    /// so B dispatches by index <= 2.
    #[tokio::test]
    async fn fair_share_does_not_starve() {
        let s = Scheduler::new(1);
        let a = ProjectId::new();
        let b = ProjectId::new();
        let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));

        let mut handles = vec![];
        for _ in 0..5 {
            let s = s.clone();
            let order = order.clone();
            handles.push(tokio::spawn(async move {
                let _p = s.acquire(a, Priority::Low).await;
                order.lock().unwrap().push("A");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }));
        }
        // Give A a head start so it's all enqueued before B arrives.
        tokio::time::sleep(Duration::from_millis(5)).await;
        let s2 = s.clone();
        let order2 = order.clone();
        handles.push(tokio::spawn(async move {
            let _p = s2.acquire(b, Priority::Low).await;
            order2.lock().unwrap().push("B");
        }));
        for h in handles {
            h.await.unwrap();
        }
        let o = order.lock().unwrap();
        assert_eq!(o[0], "A", "first should be A: {:?}", *o);
        let b_idx = o.iter().position(|x| *x == "B").unwrap();
        // Cap is 2 → B must run at index <= 2 (after at most 2 A wins).
        // Pure EDF without cap would put B at index 5.
        assert!(
            b_idx <= (CONSECUTIVE_DISPATCH_CAP as usize),
            "fairness cap violated, B at idx {}: {:?}",
            b_idx,
            *o
        );
    }

    /// Within a single project, High waiters jump ahead of queued Lows.
    #[tokio::test]
    async fn high_priority_jumps_within_project() {
        let s = Scheduler::new(1);
        let t = ProjectId::new();
        let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let blocker = s.acquire(t, Priority::Low).await;
        let mut handles = vec![];
        for label in ["L1", "L2", "L3"].iter() {
            let s = s.clone();
            let order = order.clone();
            let label = *label;
            handles.push(tokio::spawn(async move {
                let _p = s.acquire(t, Priority::Low).await;
                order.lock().unwrap().push(label);
            }));
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        let s2 = s.clone();
        let order2 = order.clone();
        handles.push(tokio::spawn(async move {
            let _p = s2.acquire(t, Priority::High).await;
            order2.lock().unwrap().push("H");
        }));
        tokio::time::sleep(Duration::from_millis(5)).await;
        drop(blocker);
        for h in handles {
            h.await.unwrap();
        }
        let o = order.lock().unwrap();
        assert_eq!(o[0], "H", "high priority should jump: {:?}", *o);
    }

    #[tokio::test]
    async fn idle_project_evicted() {
        let s = Scheduler::new(4);
        let t = ProjectId::new();
        drop(s.acquire(t, Priority::Low).await);
        assert!(!s.inner.state.lock().unwrap().queues.contains_key(&t));
    }

    #[tokio::test]
    async fn cancellation_releases_slot() {
        let s = Scheduler::new(1);
        let t = ProjectId::new();
        let _hold = s.acquire(t, Priority::Low).await;
        let s2 = s.clone();
        let waiter = tokio::spawn(async move {
            let _p = s2.acquire(t, Priority::Low).await;
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
        waiter.abort();
        drop(_hold);
        tokio::time::timeout(Duration::from_millis(200), s.acquire(t, Priority::Low))
            .await
            .expect("scheduler leaked a permit through cancellation");
    }

    #[tokio::test]
    async fn stats_reflect_live_state() {
        let s = Scheduler::new(1);
        let t = ProjectId::new();
        let h = s.acquire(t, Priority::Low).await;
        let s2 = s.clone();
        let bg = tokio::spawn(async move {
            let _p = s2.acquire(t, Priority::Low).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
        let stats = s.project_stats(&t);
        assert_eq!(stats.in_flight, 1, "{:?}", stats);
        assert_eq!(stats.queue_depth_low, 1, "{:?}", stats);
        drop(h);
        bg.await.unwrap();
    }

    /// EDF across projects: noisy queues 100 Low (1s deadlines), quiet
    /// arrives with one High (5ms). Quiet must dispatch first.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn earliest_deadline_first_across_projects() {
        let s = Scheduler::new(1);
        let noisy = ProjectId::new();
        let quiet = ProjectId::new();
        let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let q_started = Arc::new(tokio::sync::Notify::new());

        // Hold permit so the queue builds up before Q enqueues.
        let blocker = s.acquire(noisy, Priority::Low).await;

        let mut handles = vec![];
        for _ in 0..100 {
            let s = s.clone();
            let order = order.clone();
            handles.push(tokio::spawn(async move {
                let _p = s.acquire(noisy, Priority::Low).await;
                order.lock().unwrap().push("N");
                tokio::time::sleep(Duration::from_millis(1)).await;
            }));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;

        let s2 = s.clone();
        let order2 = order.clone();
        let qs = q_started.clone();
        handles.push(tokio::spawn(async move {
            qs.notify_one();
            let _p = s2.acquire(quiet, Priority::High).await;
            order2.lock().unwrap().push("Q");
        }));
        q_started.notified().await;
        tokio::time::sleep(Duration::from_millis(5)).await;

        drop(blocker);

        for h in handles {
            h.await.unwrap();
        }
        let o = order.lock().unwrap();
        let q_idx = o.iter().position(|x| *x == "Q").unwrap();
        assert_eq!(q_idx, 0, "EDF failed: quiet at {} of {}", q_idx, o.len());
    }

    /// Liveness: 4 noisy scan workers (Low) + 1 quiet (High point reads).
    /// Each "RPC" = 2ms sleep holding the permit. Quiet's p99 must not
    /// blow up under noise — EDF puts High at the head of the heap.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn quiet_project_p99_under_noise() {
        let s = Scheduler::new(4);
        let noisy_projects = [
            ProjectId::new(),
            ProjectId::new(),
            ProjectId::new(),
            ProjectId::new(),
        ];
        let quiet = ProjectId::new();

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut noisy_handles = vec![];
        for project in noisy_projects {
            let s = s.clone();
            let stop = stop.clone();
            noisy_handles.push(tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    let p = s.acquire(project, Priority::Low).await;
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    drop(p);
                }
            }));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut quiet_latencies = Vec::with_capacity(50);
        for _ in 0..50 {
            let start = Instant::now();
            let p = s.acquire(quiet, Priority::High).await;
            tokio::time::sleep(Duration::from_millis(2)).await;
            drop(p);
            quiet_latencies.push(start.elapsed());
        }

        stop.store(true, Ordering::Relaxed);
        for h in noisy_handles {
            let _ = tokio::time::timeout(Duration::from_millis(200), h).await;
        }

        quiet_latencies.sort();
        let p50 = quiet_latencies[quiet_latencies.len() / 2];
        let p99 = quiet_latencies[(quiet_latencies.len() * 99) / 100];
        eprintln!("quiet p50={:?} p99={:?}", p50, p99);
        assert!(
            p99 < Duration::from_millis(50),
            "quiet p99 too high: p50={:?} p99={:?}",
            p50,
            p99
        );
    }

    /// Budget split: large totals partition into High + Low pools that
    /// respect the ADR 0008 floor and total to the configured budget.
    #[test]
    fn split_budget_partitions_with_high_floor() {
        // Tiny budget → single pool (all into High).
        assert_eq!(Scheduler::split_budget(1), (1, 0));
        assert_eq!(Scheduler::split_budget(4), (4, 0));
        assert_eq!(Scheduler::split_budget(8), (8, 0));
        // Default range: 32 → high=8, low=24 (high floor MIN_HIGH_BUDGET=4,
        // 25 % of 32 = 8 wins).
        let (h, l) = Scheduler::split_budget(32);
        assert!(h >= MIN_HIGH_BUDGET, "high {h} below floor");
        assert_eq!(h + l, 32, "split must sum to total");
        // Huge budget: high ≈ 25 %.
        let (h, l) = Scheduler::split_budget(256);
        assert!(h >= MIN_HIGH_BUDGET);
        assert_eq!(h + l, 256);
        assert!(h >= 64 && h <= 96, "expected ~25% high, got {h}/{l}");
    }

    /// With a partitioned budget (default = 32), Low traffic saturating
    /// its pool MUST NOT starve a High permit — the dedicated High pool
    /// retains the ADR 0008 floor. Regression for the C=64 cliff that
    /// motivated the split: a 192-deep Low queue used to take ~237 ms
    /// for a single High point read; with the split it stays in the µs
    /// range (no contention on the High pool at all).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn split_pool_high_unblocked_by_low_saturation() {
        let total = 32; // matches default_global_budget on small CI hosts
        let s = Scheduler::new(total);
        let (h0, l0) = s.pool_snapshot();
        assert!(h0 >= MIN_HIGH_BUDGET, "high floor not respected: {h0}");
        assert!(l0 > 0, "low pool must be nonzero at total={total}");

        let noisy = ProjectId::new();
        // Saturate Low: hold all low_available permits + many queued.
        let mut blockers = Vec::with_capacity(l0);
        for _ in 0..l0 {
            blockers.push(s.acquire(noisy, Priority::Low).await);
        }
        let mut bg = Vec::new();
        for _ in 0..200 {
            let s = s.clone();
            bg.push(tokio::spawn(async move {
                let _p = s.acquire(noisy, Priority::Low).await;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;

        let quiet = ProjectId::new();
        let start = Instant::now();
        let _high = s.acquire(quiet, Priority::High).await;
        let elapsed = start.elapsed();
        // Must be fast — the High pool was never touched by the Low
        // flood. Generous bound (50 ms) tolerates CI scheduling noise
        // but catches a 200 ms+ regression cleanly.
        assert!(
            elapsed < Duration::from_millis(50),
            "split pool failed: High permit took {elapsed:?} despite saturated Low pool"
        );

        // Cleanup so the bg tasks can drain.
        drop(blockers);
        for h in bg {
            let _ = tokio::time::timeout(Duration::from_millis(500), h).await;
        }
    }

    /// Round-trip: each priority's permit returns to the right pool on
    /// drop so total capacity is conserved across many acquire/drop cycles.
    #[tokio::test]
    async fn split_pool_capacity_conserved() {
        let s = Scheduler::new(32);
        let (h0, l0) = s.pool_snapshot();
        let p = ProjectId::new();
        for _ in 0..16 {
            let h = s.acquire(p, Priority::High).await;
            drop(h);
            let l = s.acquire(p, Priority::Low).await;
            drop(l);
        }
        // Give the dispatcher a moment to settle any tail wakeups.
        tokio::time::sleep(Duration::from_millis(5)).await;
        let (h1, l1) = s.pool_snapshot();
        assert_eq!((h0, l0), (h1, l1), "pool capacity leaked");
    }
}
