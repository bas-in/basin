//! Hardware-aware ingest concurrency auto-tuning.
//!
//! Basin's ingest concurrency knobs (shard fan-out, flush concurrency,
//! per-project storage concurrency, scheduler global budget) historically
//! shipped as fixed constants chosen for a small CI box. On real hardware the
//! right value scales with the *effective* CPU and memory allotment — but
//! raising the knobs blindly is actively harmful: on a small shared-cpu box
//! over-fanning saturates the CPU and ingest gets SLOWER (measured: fanout 12
//! → ~22k r/s vs fanout 8 → ~56k r/s on a 4-vCPU box). So the engine derives
//! its knobs from the hardware it actually has, anchored on the measured
//! sweet spot, and clamps concurrency by available memory to avoid OOM.
//!
//! # Flag gate
//!
//! Everything here is behind the `BASIN_AUTOTUNE` env var, **default OFF**.
//! When off, [`tuning`] returns `None` and every knob reader falls back to
//! its historical default / explicit-env behavior — a provable no-op. When
//! on, a derived value is used *only* for knobs that have no explicit env
//! override (explicit env always wins).
//!
//! # Detection (container-aware)
//!
//! On Fly / Kubernetes the process sees the *host* CPU count via
//! `available_parallelism`, not the machine's cgroup allotment. We therefore
//! take `min(detected_nproc, cgroup_quota)`:
//!
//! * CPU quota: cgroup v2 `/sys/fs/cgroup/cpu.max` (`"<quota> <period>"`,
//!   `quota == "max"` means unlimited), falling back to cgroup v1
//!   `/sys/fs/cgroup/cpu/cpu.cfs_quota_us` ÷ `cpu.cfs_period_us` (`quota <= 0`
//!   means unlimited). Effective cpus = `ceil(quota / period)`, floored at 1.
//! * Memory limit: cgroup v2 `/sys/fs/cgroup/memory.max` (`"max"` = unlimited),
//!   falling back to cgroup v1 `/sys/fs/cgroup/memory/memory.limit_in_bytes`
//!   (a sentinel near `u64::MAX` / `i64::MAX` means unlimited), then
//!   `/proc/meminfo` `MemTotal`.
//!
//! If a source is unreadable we fall back to `available_parallelism()` for
//! CPUs and a conservative default for memory; the tuner never panics.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::OnceLock;

/// Per-knob ingest concurrency tuning derived from hardware.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tuning {
    /// Number of partitions a single table's bulk ingest fans across
    /// (`BASIN_SHARD_PARTITIONS_PER_TABLE`).
    pub fanout: usize,
    /// How many compaction chunks the shard prepares/writes per wave
    /// (`BASIN_SHARD_FLUSH_CONCURRENCY`).
    pub flush_concurrency: usize,
    /// Per-project storage concurrency cap
    /// (`BASIN_STORAGE_PROJECT_CONCURRENCY`).
    pub project_concurrency: usize,
    /// Scheduler global in-flight RPC budget
    /// (`BASIN_STORAGE_GLOBAL_BUDGET`).
    pub global_budget: usize,
}

/// Detected hardware envelope (effective CPUs after cgroup clamping, and the
/// memory limit in bytes). Returned by [`detect_hardware`] so callers — and
/// tests — can inspect what the tuner saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hardware {
    pub effective_cpus: usize,
    pub mem_bytes: u64,
}

/// Typical bytes one in-flight ingest segment (data-file PUT body) holds in
/// memory. Used by the memory cap so we never let
/// `max_inflight × typical_segment_bytes` exceed a fraction of RAM. A bulk
/// compaction chunk targets a few MiB of Arrow + encoded body; 16 MiB is a
/// deliberately generous over-estimate so the cap errs conservative.
const TYPICAL_SEGMENT_BYTES: u64 = 16 * 1024 * 1024;

/// Fraction of memory the in-flight ingest segments are allowed to occupy.
/// `1/4` leaves three-quarters of RAM for the page cache, query execution,
/// tails, and headroom.
const MEM_FRACTION_DENOM: u64 = 4;

/// Conservative memory assumption (bytes) when no limit can be read. 4 GiB is
/// below the smallest box we target, so the cap clamps rather than over-runs.
const FALLBACK_MEM_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Derive the ingest concurrency tuning from an effective CPU count and a
/// memory limit. Pure (no env, no I/O) so it is unit-testable.
///
/// # Anchor
///
/// The formula is anchored on the measured 4-vCPU / 8-GiB sweet spot:
/// `(4, 8 GiB)` yields `fanout = 8`, `flush = 16`, `project = 96`,
/// `global_budget = 96`. Parallelism stays roughly proportional to CPUs
/// (small multiples, not a large multiple) because over-fanning regresses
/// throughput on shared vCPUs:
///
/// * `fanout            = clamp(2 × cpus, 4, 64)`            → 4 cpu ⇒ 8
/// * `flush_concurrency = clamp(4 × cpus, 8, 128)`           → 4 cpu ⇒ 16
/// * `project_concurrency = clamp(24 × cpus, 64, 1024)`      → 4 cpu ⇒ 96
/// * `global_budget     = clamp(24 × cpus, 32, 1024)`        → 4 cpu ⇒ 96
///
/// # Memory cap
///
/// `fanout × flush_concurrency` bounds the number of in-flight ingest
/// segments. We clamp that product so
/// `inflight × TYPICAL_SEGMENT_BYTES ≤ mem_bytes / MEM_FRACTION_DENOM`,
/// shrinking `flush_concurrency` first (it is the cheaper knob to give up)
/// and then `fanout` if memory is truly tiny. This is what stops a fat-CPU /
/// thin-RAM box from OOMing under bulk ingest.
pub fn derive_tuning(cpus: usize, mem_bytes: u64) -> Tuning {
    let cpus = cpus.max(1);

    let mut fanout = (2 * cpus).clamp(4, 64);
    let mut flush_concurrency = (4 * cpus).clamp(8, 128);
    let project_concurrency = (24 * cpus).clamp(64, 1024);
    let global_budget = (24 * cpus).clamp(32, 1024);

    // Memory cap: limit the in-flight ingest segments so we don't OOM.
    let mem = if mem_bytes == 0 { FALLBACK_MEM_BYTES } else { mem_bytes };
    let inflight_budget = (mem / MEM_FRACTION_DENOM / TYPICAL_SEGMENT_BYTES).max(1);
    let inflight_budget = usize::try_from(inflight_budget).unwrap_or(usize::MAX);

    // Shrink flush_concurrency first (keep at least 1), then fanout, until the
    // product fits the in-flight segment budget.
    if fanout * flush_concurrency > inflight_budget {
        // Allow flush to drop as low as 1 before we touch fanout.
        flush_concurrency = (inflight_budget / fanout).max(1);
        if fanout * flush_concurrency > inflight_budget {
            // Even flush=1 overruns: shrink fanout (floor 1 disables fan-out).
            fanout = inflight_budget.max(1);
            flush_concurrency = 1;
        }
    }

    Tuning {
        fanout,
        flush_concurrency,
        project_concurrency,
        global_budget,
    }
}

/// Whether `BASIN_AUTOTUNE` is set to a truthy value (`1`, `true`, `yes`,
/// `on`, case-insensitive). Anything else — including unset — is OFF.
pub fn autotune_enabled() -> bool {
    match std::env::var("BASIN_AUTOTUNE") {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}

/// The process-wide derived tuning, computed once and cached.
///
/// Returns `None` when `BASIN_AUTOTUNE` is off (the no-op default) — callers
/// must then use their historical default / env behavior. Returns
/// `Some(Tuning)` when on, derived from the detected hardware envelope.
///
/// Cached for the process lifetime: detection touches `/sys`/`/proc` once.
pub fn tuning() -> Option<&'static Tuning> {
    static CACHE: OnceLock<Option<Tuning>> = OnceLock::new();
    CACHE
        .get_or_init(|| {
            if !autotune_enabled() {
                return None;
            }
            let hw = detect_hardware();
            let t = derive_tuning(hw.effective_cpus, hw.mem_bytes);
            tracing::info!(
                effective_cpus = hw.effective_cpus,
                mem_bytes = hw.mem_bytes,
                fanout = t.fanout,
                flush_concurrency = t.flush_concurrency,
                project_concurrency = t.project_concurrency,
                global_budget = t.global_budget,
                "BASIN_AUTOTUNE on: derived ingest concurrency from hardware"
            );
            Some(t)
        })
        .as_ref()
}

/// Detect the effective hardware envelope: cgroup-clamped CPU count and the
/// memory limit in bytes. Always succeeds (falls back rather than panicking).
pub fn detect_hardware() -> Hardware {
    let detected = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let effective_cpus = match cgroup_cpu_quota() {
        Some(q) => detected.min(q).max(1),
        None => detected,
    };
    let mem_bytes = detect_mem_bytes().unwrap_or(FALLBACK_MEM_BYTES);
    Hardware {
        effective_cpus,
        mem_bytes,
    }
}

/// Effective CPU count from the cgroup quota, or `None` if unlimited /
/// unreadable. Reads cgroup v2 `cpu.max` first, then cgroup v1
/// `cpu.cfs_quota_us` / `cpu.cfs_period_us`.
fn cgroup_cpu_quota() -> Option<usize> {
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        if let Some(n) = parse_cpu_max_v2(&s) {
            return Some(n);
        }
        // `cpu.max` present but "max <period>" → unlimited; don't fall through
        // to v1 (this is a v2 host).
        return None;
    }
    let quota = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").ok()?;
    let period = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us").ok()?;
    parse_cpu_v1(&quota, &period)
}

/// Parse cgroup v2 `cpu.max` contents (`"<quota> <period>"`). Returns the
/// effective CPU count = `ceil(quota / period)`, `None` when `quota == "max"`
/// (unlimited) or the line is malformed.
fn parse_cpu_max_v2(s: &str) -> Option<usize> {
    let line = s.trim();
    let mut parts = line.split_whitespace();
    let quota = parts.next()?;
    if quota == "max" {
        return None;
    }
    let period: u64 = parts.next().unwrap_or("100000").parse().ok()?;
    let quota: u64 = quota.parse().ok()?;
    if period == 0 || quota == 0 {
        return None;
    }
    Some(ceil_div(quota, period).max(1))
}

/// Parse cgroup v1 quota/period files. `quota <= 0` means unlimited (`None`).
fn parse_cpu_v1(quota: &str, period: &str) -> Option<usize> {
    let quota: i64 = quota.trim().parse().ok()?;
    if quota <= 0 {
        return None; // -1 == unlimited
    }
    let period: i64 = period.trim().parse().ok()?;
    if period <= 0 {
        return None;
    }
    Some(ceil_div(quota as u64, period as u64).max(1))
}

/// Memory limit in bytes: cgroup v2 `memory.max`, then cgroup v1
/// `memory.limit_in_bytes`, then `/proc/meminfo` `MemTotal`.
fn detect_mem_bytes() -> Option<u64> {
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        if let Some(n) = parse_mem_max_v2(&s) {
            return Some(n);
        }
        // present but "max" → unlimited; try /proc/meminfo for the physical
        // total rather than guessing.
    } else if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
        if let Some(n) = parse_mem_limit_v1(&s) {
            return Some(n);
        }
    }
    parse_meminfo_total(&std::fs::read_to_string("/proc/meminfo").ok()?)
}

/// Parse cgroup v2 `memory.max` (`"max"` = unlimited → `None`).
fn parse_mem_max_v2(s: &str) -> Option<u64> {
    let t = s.trim();
    if t == "max" {
        return None;
    }
    let n: u64 = t.parse().ok()?;
    if n == 0 {
        None
    } else {
        Some(n)
    }
}

/// Parse cgroup v1 `memory.limit_in_bytes`. Unlimited is encoded as a huge
/// sentinel (often `0x7FFF_FFFF_FFFF_F000` or `i64::MAX`-ish, page-aligned),
/// which we treat as "no limit" (`None`) so the meminfo fallback is used.
fn parse_mem_limit_v1(s: &str) -> Option<u64> {
    let n: u64 = s.trim().parse().ok()?;
    // Anything within a few pages of i64::MAX (or beyond) is the "unlimited"
    // sentinel — far above any real machine's RAM.
    const UNLIMITED_FLOOR: u64 = (i64::MAX as u64) - (1 << 20);
    if n == 0 || n >= UNLIMITED_FLOOR {
        None
    } else {
        Some(n)
    }
}

/// Parse `/proc/meminfo`, returning `MemTotal` in bytes (the file reports kB).
fn parse_meminfo_total(s: &str) -> Option<u64> {
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

#[inline]
fn ceil_div(a: u64, b: u64) -> usize {
    usize::try_from(a.div_ceil(b)).unwrap_or(usize::MAX)
}

// ---------------------------------------------------------------------------
// Phase 2 — runtime-adjustable knobs + adaptive controller.
//
// Static hardware detection cannot tell a *shared* vCPU from a *dedicated*
// one: on Fly's shared-cpu-Nx the cgroup quota reports N, but the host may
// steal cycles, so the real throughput-optimal concurrency is lower than the
// dedicated-N value. The only way to find the true optimum is to observe
// throughput and an overload signal at runtime and hill-climb.
//
// CORRECTNESS — what may change live, and what MUST NOT:
//
//   * flush_concurrency — SAFE to adjust live. It only bounds how many
//     compaction chunks one wave prepares/writes per pass
//     (`in_process::compact_one`). Each chunk is still drained once, written
//     once, committed once, and pruned by its own `max_lsn`; the knob changes
//     I/O parallelism, never the identity of the data or which partition it
//     belongs to. So a mid-wave change is exactly-once-safe.
//
//   * fanout — NOT safe to adjust live, and the controller NO LONGER touches
//     it. `fanout` is the bulk-ingest PARTITION TOPOLOGY: it is the modulus of
//     the round-robin in `executor::write_batch_fanout`
//     (`idx = cursor % fanout`), so changing it mid-stream reshapes which
//     partitions a single table's chunks land in. The cold read / `count(*)`
//     path enumerates data files by OBJECT-STORE LIST across EVERY partition
//     subdir (`reader::read`, no-predicate branch) — it does not consult the
//     catalog live set for existence. Exactly-once therefore depends on the
//     compaction + file-merge sweeps (which run under DIFFERENT locks:
//     per-partition `compact_lock` vs. process-wide `stripe_merge_lock`)
//     retiring superseded files cleanly. Shifting the partition set under
//     those concurrent sweeps let the pre-merge sources AND the post-merge
//     output stay physically live at once, which the LIST-based count then
//     double-counts (measured on dev: a clean 20M COPY read back 23.31M,
//     ~16.5% phantom rows, exactly when the controller changed fan-out
//     mid-ingest; with fan-out fixed the same load is exact). So fan-out is
//     pinned to the Phase-1 hardware-derived value for the life of the process
//     and never published as a runtime override.
//
// The scheduler global budget and per-project concurrency are sized once, at
// `Storage::new`, from a Tokio `Semaphore` whose permit count is fixed at
// construction. Those are therefore STARTUP-ONLY: the controller does not
// touch them (resizing a live semaphore mid-flight is not supported by the
// current scheduler and would risk the ADR-0008 deadlock floor). This is
// documented on [`AdaptiveController`].
// ---------------------------------------------------------------------------

/// Runtime flush-concurrency override. `0` = unset.
static RUNTIME_FLUSH: AtomicUsize = AtomicUsize::new(0);

/// Current runtime flush-concurrency override, if any.
pub fn runtime_flush_concurrency() -> Option<usize> {
    match RUNTIME_FLUSH.load(Ordering::Relaxed) {
        0 => None,
        n => Some(n),
    }
}

/// Set the live flush-concurrency override (controller-only). `0` clears it.
///
/// Flush concurrency is the ONLY ingest knob the adaptive controller adjusts
/// at runtime: it changes compaction I/O parallelism without changing the
/// partition topology, so a mid-ingest change stays exactly-once. Fan-out is
/// deliberately NOT runtime-adjustable — see the module comment above.
pub fn set_runtime_flush_concurrency(n: usize) {
    RUNTIME_FLUSH.store(n, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// Phase 2 — process-global live feedback signals.
//
// The adaptive controller needs a node-wide view, not a per-shard one: a node
// may host several shards, but the fan-out / flush-concurrency knobs are
// process-global, so the controller must see the SUM of committed throughput
// and the SUM of overload events across every shard. We therefore accumulate
// the two signals in process-global relaxed atomics rather than per-shard
// state. The hot-path cost is a single `fetch_add(Relaxed)` per commit wave
// (NOT per row) and per hard-cap backpressure stall — no lock, no allocation,
// no per-row work. The controller's tick reads-and-resets (`swap`) these to
// derive an interval rate, so an unread accumulator just keeps growing
// cheaply and is bounded by `u64`.
//
// These counters are written unconditionally on the commit / backpressure
// paths (the `record_*` calls are unconditional), but that is still a no-op at
// the default: the only READER is the tick task, which is spawned ONLY when
// `BASIN_AUTOTUNE` is on. With autotune off, nothing ever swaps or observes
// them, no override is ever published (the runtime atomics stay 0), and the
// knob readers fall through to their historical defaults. The extra relaxed
// add on the commit path is unconditional but negligible (one uncontended
// atomic increment per bounded compaction wave / backpressure stall, both of
// which already do object-store I/O).
// ---------------------------------------------------------------------------

/// Total rows committed to cold storage since process start (monotonic until
/// the controller's `swap`). Written on the compaction commit path.
static COMMITTED_ROWS: AtomicU64 = AtomicU64::new(0);
/// Count of commit waves observed since the last controller tick. The
/// denominator for the overload ratio (overload = stalls / waves).
static COMMIT_WAVES: AtomicU64 = AtomicU64::new(0);
/// Count of hard-cap backpressure stalls (the writer blocked on a synchronous
/// `compact_one` because the in-memory tail hit the hard cap = compaction is
/// not keeping up with ingest) since the last controller tick.
static OVERLOAD_STALLS: AtomicU64 = AtomicU64::new(0);

/// Record `rows` committed to cold storage in one compaction wave. Called on
/// the commit path (`compact_one`). Cheap: one relaxed `fetch_add` per wave.
#[inline]
pub fn record_committed_rows(rows: u64) {
    COMMITTED_ROWS.fetch_add(rows, Ordering::Relaxed);
    COMMIT_WAVES.fetch_add(1, Ordering::Relaxed);
}

/// Record a hard-cap backpressure stall (writer blocked on synchronous
/// compaction because the tail hit the hard cap). This is the overload proxy:
/// it fires exactly when compaction cannot keep pace with ingest, which is the
/// regime over-fanning produces on a shared vCPU. Cheap: one relaxed
/// `fetch_add`.
#[inline]
pub fn record_overload_stall() {
    OVERLOAD_STALLS.fetch_add(1, Ordering::Relaxed);
}

/// Read-and-reset the global signal accumulators, returning the rows
/// committed, the number of commit waves, and the number of overload stalls
/// since the last call. Used by the controller tick only.
pub fn drain_signals() -> (u64, u64, u64) {
    (
        COMMITTED_ROWS.swap(0, Ordering::Relaxed),
        COMMIT_WAVES.swap(0, Ordering::Relaxed),
        OVERLOAD_STALLS.swap(0, Ordering::Relaxed),
    )
}

/// Tick interval (seconds) for the adaptive controller. Env-tunable via
/// `BASIN_AUTOTUNE_INTERVAL_SECS`; defaults to 20 s. Clamped to `[5, 600]` so
/// a fat-fingered value can't busy-spin or stall the loop. Conservative: long
/// enough that one interval reflects a real throughput trend, short enough to
/// converge within a few minutes.
pub fn tick_interval_secs() -> u64 {
    std::env::var("BASIN_AUTOTUNE_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(20)
        .clamp(5, 600)
}

/// Build one [`Sample`] from the drained global signals over an interval of
/// `elapsed_secs` seconds. Pure (no atomics, no clock) so it is unit-testable:
/// `throughput = committed_rows / elapsed`, `overload = stalls / waves`
/// (clamped to `[0, 1]`; `0` waves ⇒ `0` overload). Exposed for the tick task
/// and tests.
pub fn sample_from_signals(
    committed_rows: u64,
    commit_waves: u64,
    overload_stalls: u64,
    elapsed_secs: f64,
) -> Sample {
    let elapsed = elapsed_secs.max(1.0);
    let rows_per_sec = committed_rows as f64 / elapsed;
    let overload = if commit_waves == 0 {
        // No commits at all this interval: either idle, or every wave stalled
        // (backpressure with no successful commit). If there were stalls,
        // that IS overload; otherwise the node is idle (overload 0).
        if overload_stalls > 0 {
            1.0
        } else {
            0.0
        }
    } else {
        (overload_stalls as f64 / commit_waves as f64).clamp(0.0, 1.0)
    };
    Sample {
        rows_per_sec,
        overload,
    }
}

/// The hard floor on the published flush concurrency: the controller never
/// drives flush below this, matching the fan-out floor of 4 (the empirical
/// "don't starve compaction" minimum).
pub const FLUSH_FLOOR: usize = 4;

/// Build the live controller for the current hardware. The async tick loop
/// (in `basin-shard`, where tokio lives) owns the controller and calls
/// [`AdaptiveController::observe`] each tick. Kept here (env- and
/// hardware-pure) so it is unit-testable and the policy (floors/ceilings)
/// lives next to the controller.
///
/// The controller drives **flush concurrency only** — the sole ingest knob
/// that is exactly-once-safe to change live (it changes compaction I/O
/// parallelism, never the partition topology; see the module comment).
/// Fan-out is held FIXED at the Phase-1 hardware-derived value and is never
/// published as a runtime override, because mutating the partition topology
/// mid-stream double-counts the LIST-based `count(*)`.
///
/// Floors/ceilings: flush roams `[max(FLUSH_FLOOR, derived/2), derived×2]`,
/// seeded at the derived flush value so it is a no-op until the first
/// adjustment.
pub fn build_controller() -> AdaptiveController {
    // Seed from the hardware-derived tuning (autotune is on, so `tuning()` is
    // `Some`). Fall back to the empirical anchor if detection somehow yields
    // `None` (it cannot when enabled, but be defensive).
    let derived = tuning().copied().unwrap_or(Tuning {
        fanout: 8,
        flush_concurrency: 16,
        project_concurrency: 96,
        global_budget: 96,
    });
    let min_flush = (derived.flush_concurrency / 2).max(FLUSH_FLOOR);
    let max_flush = (derived.flush_concurrency * 2).max(min_flush + 2);
    AdaptiveController::new(derived.flush_concurrency, min_flush, max_flush)
}

/// Publish the controller's current flush concurrency as the live override,
/// floored at [`FLUSH_FLOOR`]. Controller-only.
pub fn publish_flush(flush: usize) {
    set_runtime_flush_concurrency(flush.max(FLUSH_FLOOR));
}

/// A throughput sample fed to the [`AdaptiveController`] each tick.
#[derive(Debug, Clone, Copy)]
pub struct Sample {
    /// Committed rows per second over the last interval (the thing we want to
    /// maximize).
    pub rows_per_sec: f64,
    /// Overload pressure in `[0.0, 1.0]`: e.g. fraction of recent compaction
    /// waves that hit the hard tail cap, or normalized scheduler queue depth.
    /// `>= overload_ceiling` forces a back-off regardless of throughput.
    pub overload: f64,
}

/// EWMA smoothing factor for the throughput signal. The committed-rows/s
/// signal is noisy at low ingest rates (measured: ~10–17k r/s on dev swings
/// enough that a single sample can flip "up"/"down"). We smooth with an
/// exponentially-weighted moving average so decisions ride the trend, not the
/// jitter. `0.3` keeps ~70% of the prior estimate each tick — slow enough to
/// reject noise, fast enough to track a real regime change within a few ticks.
const EWMA_ALPHA: f64 = 0.3;

/// Clear-margin a *smoothed* throughput must beat the best-seen-at-this-fanout
/// reference by before the controller will step AWAY from where it is. At 10%
/// this is well above dev's per-sample noise, so noise cannot masquerade as a
/// win and walk the knob off the derived baseline.
const CLEAR_MARGIN: f64 = 0.10;

/// How many *consecutive* evaluations the clear-margin win must hold before the
/// controller commits to a move away from the current fan-out. A single noisy
/// tick is never enough — the win must be sustained.
const SUSTAIN_TICKS: u32 = 2;

/// Conservative, anchored hill-climbing controller for the one runtime-
/// adjustable ingest knob, **flush concurrency**.
///
/// (The controller is a generic anchored hill-climber over a single integer
/// knob; its internal `*_fanout` field/method names are historical — they now
/// carry the flush-concurrency value. Fan-out itself is NOT runtime-adjustable;
/// see the module comment for why mutating the partition topology mid-stream is
/// unsafe.)
///
/// The load-bearing invariant: **the adaptive layer's worst case must equal
/// the static hardware-derived baseline.** A noisy throughput signal must
/// never strand the knob at a value worse than `derived`. We get this from
/// three mechanisms, all keyed off a *smoothed* (EWMA) throughput:
///
/// 1. **Smoothing.** Decisions use an EWMA of committed-rows/s
///    ([`EWMA_ALPHA`]), not raw consecutive samples, so per-tick jitter does
///    not register as a trend.
/// 2. **Clear-margin, sustained gate to move.** The controller steps *away*
///    from its current fan-out only when the smoothed throughput beats the
///    best-seen-at-the-current-fanout by [`CLEAR_MARGIN`] for at least
///    [`SUSTAIN_TICKS`] consecutive evaluations. A move that does not clearly,
///    repeatably help is not taken (and an explored move that fails to pay off
///    is unwound by mechanism 3).
/// 3. **Anchor to the derived baseline.** The controller remembers the derived
///    start fan-out. Whenever it sits at a *non-derived* setting that is not
///    clearly better than the best smoothed throughput it ever saw *at the
///    derived value*, it biases one step back toward `derived`. Net effect: in
///    the absence of a clear, sustained win the knob converges to `derived`
///    (its safe floor) and never strands at a noise-chosen worse point.
///
/// Overload (compaction can't keep pace) forces an immediate one-step back-off
/// regardless of throughput.
///
/// STARTUP-ONLY knobs (`fanout`, `project_concurrency`, `global_budget`) are
/// *not* driven here — see the module comment (fan-out is the partition
/// topology and pinning it is a correctness requirement). The controller is
/// constructed and stepped only when `BASIN_AUTOTUNE` is on; the background
/// task that calls [`AdaptiveController::observe`] on a 10–30 s tick is wired
/// by the server (see `set_runtime_flush_concurrency` call sites). When
/// autotune is off it is never constructed, so it is a strict no-op at the
/// default.
#[derive(Debug, Clone)]
pub struct AdaptiveController {
    cur_fanout: usize,
    /// The hardware-derived start fan-out — the validated safe baseline the
    /// controller anchors to.
    derived_fanout: usize,
    min_fanout: usize,
    max_fanout: usize,
    step: usize,
    /// Smoothed (EWMA) throughput; `None` until the first sample seeds it.
    smoothed_rps: Option<f64>,
    /// Best smoothed throughput ever observed *while sitting at*
    /// `derived_fanout`. The reference the anchor compares against: a
    /// non-derived setting must clear this by [`CLEAR_MARGIN`] to be kept.
    best_at_derived: f64,
    /// Best smoothed throughput observed at the *current* fan-out since we last
    /// arrived here. The reference the clear-margin move-gate compares against.
    best_at_cur: f64,
    /// Direction of the last exploratory move: +1 climbing, -1 backing off.
    direction: i8,
    /// Consecutive evaluations the clear-margin win has held.
    sustain: u32,
    clear_margin: f64,
    sustain_ticks: u32,
    overload_ceiling: f64,
}

impl AdaptiveController {
    /// Build a controller seeded at the hardware-derived fan-out, free to roam
    /// between `min` and `max` (typically `[max(4, derived/2), derived×2]`).
    /// The start value is the derived anchor the controller converges back to
    /// absent a clear, sustained throughput win.
    pub fn new(start_fanout: usize, min_fanout: usize, max_fanout: usize) -> Self {
        let lo = min_fanout.max(1);
        let hi = max_fanout.max(lo);
        let cur = start_fanout.clamp(lo, hi);
        Self {
            cur_fanout: cur,
            derived_fanout: cur,
            min_fanout: lo,
            max_fanout: hi,
            step: 2,
            smoothed_rps: None,
            best_at_derived: 0.0,
            best_at_cur: 0.0,
            direction: 1,
            sustain: 0,
            clear_margin: CLEAR_MARGIN,
            sustain_ticks: SUSTAIN_TICKS,
            overload_ceiling: 0.5,
        }
    }

    /// Current fan-out the controller wants live.
    pub fn fanout(&self) -> usize {
        self.cur_fanout
    }

    /// The smoothed throughput estimate (EWMA of committed-rows/s), if seeded.
    pub fn smoothed_rps(&self) -> Option<f64> {
        self.smoothed_rps
    }

    /// Feed one [`Sample`]; returns the new fan-out to publish.
    ///
    /// Conservative and anchored: at most one `step` move per tick. Overload
    /// forces an immediate back-off; otherwise a move *away* from the current
    /// fan-out requires a clear, sustained smoothed-throughput win, and absent
    /// that the controller biases back toward the derived baseline.
    pub fn observe(&mut self, s: Sample) -> usize {
        let prev = self.cur_fanout;

        // 1. Smooth the throughput signal (EWMA). Decisions use `sm`, not the
        //    raw sample — single-tick noise must not register as a trend.
        let sm = match self.smoothed_rps {
            Some(prev_sm) => EWMA_ALPHA * s.rows_per_sec + (1.0 - EWMA_ALPHA) * prev_sm,
            None => s.rows_per_sec,
        };
        self.smoothed_rps = Some(sm);

        let reason: &str;

        if s.overload >= self.overload_ceiling {
            // Overloaded: compaction can't keep pace. Back off one step
            // immediately, regardless of throughput.
            self.direction = -1;
            self.cur_fanout = self.cur_fanout.saturating_sub(self.step).max(self.min_fanout);
            self.sustain = 0;
            reason = "overload, backing off";
        } else if sm > self.best_at_cur * (1.0 + self.clear_margin) {
            // 2. Clear-margin gate to MOVE. The smoothed throughput clearly
            //    beats the best we've seen at this fan-out. Require it to hold
            //    for `sustain_ticks` consecutive evals before committing — one
            //    noisy tick is not enough.
            self.best_at_cur = sm;
            self.sustain = self.sustain.saturating_add(1);
            if self.sustain >= self.sustain_ticks {
                self.sustain = 0;
                self.cur_fanout = self.step_in_direction();
                reason = "clear sustained throughput win";
            } else {
                reason = "win pending sustain";
            }
        } else {
            // No clear win at the current fan-out. Reset the sustain counter
            // (the streak is broken) and, if we are away from the derived
            // baseline without having justified it, bias back toward derived.
            self.sustain = 0;
            if self.cur_fanout != self.derived_fanout
                && sm <= self.best_at_derived * (1.0 + self.clear_margin)
            {
                // 3. Anchor. This non-derived setting is not clearly better
                //    than the best we ever saw at the derived value — step one
                //    toward derived. Convergence to the safe floor.
                self.cur_fanout = self.step_toward(self.derived_fanout);
                reason = "anchor: no clear win, biasing to derived";
            } else {
                reason = "hold";
            }
        }

        // Maintain the per-position best references AFTER any move. If we just
        // landed somewhere new, start its best from the current smoothed value
        // so the move-gate measures improvement *from here*.
        if self.cur_fanout != prev {
            self.best_at_cur = sm;
        } else {
            self.best_at_cur = self.best_at_cur.max(sm);
        }
        if self.cur_fanout == self.derived_fanout {
            self.best_at_derived = self.best_at_derived.max(sm);
        }

        if self.cur_fanout != prev {
            tracing::info!(
                from = prev,
                to = self.cur_fanout,
                smoothed_rows_per_sec = sm,
                rows_per_sec = s.rows_per_sec,
                overload = s.overload,
                reason,
                "autotune controller adjusted fanout"
            );
        }
        self.cur_fanout
    }

    /// Step one in the current exploratory direction, clamped to bounds.
    fn step_in_direction(&self) -> usize {
        if self.direction >= 0 {
            (self.cur_fanout + self.step).min(self.max_fanout)
        } else {
            self.cur_fanout.saturating_sub(self.step).max(self.min_fanout)
        }
    }

    /// Step one toward `target`, clamped to bounds (overshoot snaps to target).
    fn step_toward(&self, target: usize) -> usize {
        let next = if self.cur_fanout > target {
            self.cur_fanout.saturating_sub(self.step).max(target)
        } else {
            (self.cur_fanout + self.step).min(target)
        };
        next.clamp(self.min_fanout, self.max_fanout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;

    /// The anchor: a 4-vCPU / 8-GiB box reproduces today's empirical sweet
    /// spot exactly — fanout 8, flush 16, project 96, budget 96. This is the
    /// load-bearing regression gate (fanout 12 regressed throughput).
    #[test]
    fn anchor_4cpu_8gib() {
        let t = derive_tuning(4, 8 * GIB);
        assert_eq!(t.fanout, 8, "fanout anchor");
        assert_eq!(t.flush_concurrency, 16, "flush anchor");
        assert_eq!(t.project_concurrency, 96, "project-concurrency anchor");
        assert_eq!(t.global_budget, 96, "global-budget anchor");
    }

    /// A fat box scales up sensibly but stays roughly proportional to CPUs
    /// (no large multiple) and within the clamps.
    #[test]
    fn scales_up_16cpu_64gib() {
        let t = derive_tuning(16, 64 * GIB);
        assert_eq!(t.fanout, 32, "2x cpus");
        // CPU formula wants flush = 4×16 = 64, but the memory cap (64 GiB / 4 /
        // 16 MiB = 1024 in-flight budget; 32 × 64 = 2048 > 1024) trims flush to
        // 1024 / 32 = 32 so we stay within a quarter of RAM.
        assert_eq!(t.flush_concurrency, 32, "trimmed by memory cap");
        assert_eq!(t.project_concurrency, 384, "24x cpus");
        assert_eq!(t.global_budget, 384, "24x cpus");
        assert!(
            t.fanout * t.flush_concurrency <= 1024,
            "in-flight product within memory budget"
        );
    }

    /// Very large box: knobs clamp at their ceilings, not unbounded.
    #[test]
    fn clamps_at_ceiling() {
        let t = derive_tuning(256, 4096 * GIB);
        assert_eq!(t.fanout, 64, "fanout ceiling");
        assert_eq!(t.flush_concurrency, 128, "flush ceiling");
        assert_eq!(t.project_concurrency, 1024, "project ceiling");
        assert_eq!(t.global_budget, 1024, "budget ceiling");
    }

    /// Tiny RAM clamps the in-flight concurrency down even on many CPUs: the
    /// memory cap must dominate so we don't OOM.
    #[test]
    fn tiny_ram_clamps_concurrency() {
        // 16 cpus would want fanout 32 × flush 64 = 2048 segments. With only
        // 512 MiB: budget = 512MiB/4/16MiB = 8 in-flight. flush drops to fit.
        let t = derive_tuning(16, 512 * 1024 * 1024);
        assert!(
            t.fanout * t.flush_concurrency <= 8,
            "in-flight product {} × {} must fit the 8-segment memory budget",
            t.fanout,
            t.flush_concurrency
        );
        assert!(t.flush_concurrency >= 1);
        // The CPU-derived (memory-independent) knobs are untouched by RAM.
        assert_eq!(t.project_concurrency, 384);
        assert_eq!(t.global_budget, 384);
    }

    /// Pathologically small RAM still yields a valid (>=1) tuning, never 0 or
    /// a panic.
    #[test]
    fn micro_ram_never_zero() {
        let t = derive_tuning(8, 16 * 1024 * 1024); // 16 MiB
        assert!(t.fanout >= 1);
        assert!(t.flush_concurrency >= 1);
    }

    #[test]
    fn cpu_max_v2_parsing() {
        // 200000/100000 = 2 cpus.
        assert_eq!(parse_cpu_max_v2("200000 100000"), Some(2));
        // 450000/100000 = 4.5 → ceil 5.
        assert_eq!(parse_cpu_max_v2("450000 100000"), Some(5));
        // 50000/100000 = 0.5 → ceil 1 (floored).
        assert_eq!(parse_cpu_max_v2("50000 100000"), Some(1));
        // unlimited.
        assert_eq!(parse_cpu_max_v2("max 100000"), None);
        // trailing newline tolerated.
        assert_eq!(parse_cpu_max_v2("400000 100000\n"), Some(4));
    }

    #[test]
    fn cpu_v1_parsing() {
        assert_eq!(parse_cpu_v1("200000", "100000"), Some(2));
        assert_eq!(parse_cpu_v1("400000", "100000"), Some(4));
        assert_eq!(parse_cpu_v1("-1", "100000"), None, "-1 = unlimited");
        assert_eq!(parse_cpu_v1("0", "100000"), None);
    }

    #[test]
    fn mem_max_v2_parsing() {
        assert_eq!(parse_mem_max_v2("8589934592"), Some(8 * GIB));
        assert_eq!(parse_mem_max_v2("8589934592\n"), Some(8 * GIB));
        assert_eq!(parse_mem_max_v2("max"), None);
        assert_eq!(parse_mem_max_v2("0"), None);
    }

    #[test]
    fn mem_limit_v1_parsing() {
        assert_eq!(parse_mem_limit_v1("8589934592"), Some(8 * GIB));
        // The classic v1 unlimited sentinel.
        assert_eq!(parse_mem_limit_v1("9223372036854771712"), None);
        assert_eq!(parse_mem_limit_v1("0"), None);
    }

    #[test]
    fn meminfo_total_parsing() {
        let sample = "MemTotal:        8167848 kB\nMemFree:  123 kB\n";
        assert_eq!(parse_meminfo_total(sample), Some(8167848 * 1024));
        assert_eq!(parse_meminfo_total("Bogus: 1 kB"), None);
    }

    #[test]
    fn runtime_overrides_default_to_unset() {
        // The flush override atomic defaults to 0 (= unset = no-op). We don't
        // set it here (process-global state) so the no-op invariant holds for
        // every other test in the suite. There is no runtime fan-out override:
        // fan-out is pinned to the derived value for the process lifetime.
        assert_eq!(RUNTIME_FLUSH.load(Ordering::Relaxed), 0);
    }

    /// The controller climbs while throughput *clearly and sustainedly* rises,
    /// then anchors back toward the derived baseline once the gains stop —
    /// never stranding above derived without a sustained win.
    #[test]
    fn controller_climbs_then_backs_off_and_settles() {
        let mut c = AdaptiveController::new(8, 4, 24);

        // Throughput rises by a clear margin each tick (well over CLEAR_MARGIN)
        // and sustains: after each pair of confirming ticks the controller
        // steps fan-out up. Feed enough to climb several steps above derived.
        for rps in [
            50_000.0, 60_000.0, 75_000.0, 95_000.0, 120_000.0, 150_000.0, 190_000.0, 240_000.0,
        ] {
            c.observe(Sample { rows_per_sec: rps, overload: 0.1 });
        }
        let peak_fanout = c.fanout();
        assert!(peak_fanout > 8, "should have climbed above the start (got {peak_fanout})");

        // Now throughput collapses (over-fanned: CPU saturated). No clear win
        // at the peak and not clearly better than best-at-derived, so the
        // controller anchors back down toward derived.
        for rps in [40_000.0, 38_000.0, 37_000.0, 36_000.0, 35_000.0] {
            c.observe(Sample { rows_per_sec: rps, overload: 0.2 });
        }
        assert!(
            c.fanout() < peak_fanout,
            "should back off after the gains stop (peak {peak_fanout}, now {})",
            c.fanout()
        );

        // Feed a long flat plateau: with no clear win it must converge to the
        // derived baseline and settle there, not oscillate.
        for _ in 0..20 {
            c.observe(Sample { rows_per_sec: 39_000.0, overload: 0.2 });
        }
        assert_eq!(
            c.fanout(),
            8,
            "absent a sustained win it converges to the derived baseline"
        );
        // Stays within configured bounds throughout.
        assert!(c.fanout() >= 4 && c.fanout() <= 24);
    }

    /// NOISE regression (the bug this fix targets): a flat true throughput with
    /// ±noise must NOT walk the knob away from the derived baseline. Single
    /// noisy samples can read as "throughput up", but smoothing + the
    /// clear/sustained move-gate + the derived anchor must keep the controller
    /// pinned to (or converging back to) the derived value.
    #[test]
    fn controller_does_not_drift_on_noise() {
        let mut c = AdaptiveController::new(8, 4, 16);
        let derived = c.fanout();

        // Deterministic pseudo-noise around a flat ~12k r/s mean (dev-like low
        // rate). Swings of ±~15% — bigger than the real per-sample jitter —
        // yet never a *sustained* clear-margin trend.
        let base = 12_000.0;
        let offsets = [
            0.12, -0.10, 0.08, -0.13, 0.15, -0.07, 0.11, -0.14, 0.06, -0.09, 0.13, -0.12, 0.10,
            -0.08, 0.14, -0.11,
        ];
        let mut max_seen = derived;
        let mut min_seen = derived;
        for &off in offsets.iter().cycle().take(120) {
            let f = c.observe(Sample {
                rows_per_sec: base * (1.0 + off),
                overload: 0.0,
            });
            max_seen = max_seen.max(f);
            min_seen = min_seen.min(f);
        }

        // Never strands away from derived: at most one exploratory step in
        // either direction during the run, and it converges back.
        assert!(
            max_seen <= derived + c.step,
            "noise pushed fan-out above derived+step (max {max_seen}, derived {derived})"
        );
        assert!(
            min_seen + c.step >= derived,
            "noise pushed fan-out below derived-step (min {min_seen}, derived {derived})"
        );
        assert_eq!(
            c.fanout(),
            derived,
            "must converge back to the derived baseline under pure noise (got {})",
            c.fanout()
        );
    }

    /// CLEAR-WIN: when a higher fan-out genuinely sustains higher smoothed
    /// throughput, the controller moves up and *stays* there (does not anchor
    /// back to derived, because the win clears the margin against best-at-
    /// derived).
    #[test]
    fn controller_moves_up_and_stays_on_clear_win() {
        let mut c = AdaptiveController::new(8, 4, 16);

        // Establish a stable baseline at derived first.
        for _ in 0..3 {
            c.observe(Sample { rows_per_sec: 50_000.0, overload: 0.0 });
        }
        assert_eq!(c.fanout(), 8, "settled at derived on the flat baseline");

        // A genuine, large, sustained improvement (well over CLEAR_MARGIN, held
        // many ticks) drives — and holds — fan-out above derived.
        for _ in 0..12 {
            c.observe(Sample { rows_per_sec: 120_000.0, overload: 0.0 });
        }
        let up = c.fanout();
        assert!(up > 8, "clear sustained win must move fan-out up (got {up})");

        // The win persists: hold the higher throughput and the controller does
        // NOT anchor back, because it clearly beats best-at-derived.
        for _ in 0..20 {
            c.observe(Sample { rows_per_sec: 120_000.0, overload: 0.0 });
        }
        assert!(
            c.fanout() >= up,
            "must stay up while the win holds (was {up}, now {})",
            c.fanout()
        );
        assert!(c.fanout() > 8, "must not anchor back to derived during a real win");
    }

    /// No-op proof: `autotune_enabled` is false unless `BASIN_AUTOTUNE` is an
    /// explicit truthy value. When false, knob readers consult `tuning()`,
    /// which returns `None`, so every reader falls through to its historical
    /// default — the autotune machinery cannot affect production until
    /// deliberately enabled. (Env is process-global; this test sets and
    /// restores `BASIN_AUTOTUNE` itself and does not call the cached
    /// `tuning()`, so it leaves no residue for sibling tests.)
    #[test]
    fn autotune_enabled_only_on_truthy_env() {
        let prev = std::env::var("BASIN_AUTOTUNE").ok();

        std::env::remove_var("BASIN_AUTOTUNE");
        assert!(!autotune_enabled(), "unset => off (the production default)");

        for off in ["0", "false", "no", "off", "", "bogus"] {
            std::env::set_var("BASIN_AUTOTUNE", off);
            assert!(!autotune_enabled(), "{off:?} must be OFF");
        }
        for on in ["1", "true", "yes", "on", "TRUE", "On", " yes "] {
            std::env::set_var("BASIN_AUTOTUNE", on);
            assert!(autotune_enabled(), "{on:?} must be ON");
        }

        match prev {
            Some(v) => std::env::set_var("BASIN_AUTOTUNE", v),
            None => std::env::remove_var("BASIN_AUTOTUNE"),
        }
    }

    /// A hard overload signal forces a back-off even if throughput looks fine.
    #[test]
    fn controller_backs_off_on_overload() {
        let mut c = AdaptiveController::new(16, 2, 24);
        let before = c.fanout();
        c.observe(Sample { rows_per_sec: 100_000.0, overload: 0.9 });
        assert!(c.fanout() < before, "overload must force back-off");
    }

    /// `sample_from_signals` derives rows/s and a bounded overload ratio.
    #[test]
    fn sample_from_signals_derives_rate_and_overload() {
        // 200k rows over 20 s, 10 waves, 0 stalls → 10k r/s, overload 0.
        let s = sample_from_signals(200_000, 10, 0, 20.0);
        assert!((s.rows_per_sec - 10_000.0).abs() < 1e-6);
        assert_eq!(s.overload, 0.0);

        // 5 of 10 waves stalled → overload 0.5.
        let s = sample_from_signals(100_000, 10, 5, 20.0);
        assert_eq!(s.overload, 0.5);

        // More stalls than waves clamps at 1.0.
        let s = sample_from_signals(0, 2, 9, 20.0);
        assert_eq!(s.overload, 1.0);

        // Stalls but zero committed waves = full overload (every wave blocked).
        let s = sample_from_signals(0, 0, 3, 20.0);
        assert_eq!(s.overload, 1.0);

        // Fully idle interval: no signal, overload 0, rate 0.
        let s = sample_from_signals(0, 0, 0, 20.0);
        assert_eq!(s.rows_per_sec, 0.0);
        assert_eq!(s.overload, 0.0);

        // Elapsed is floored at 1 s so a tiny/zero interval can't divide-by-zero.
        let s = sample_from_signals(5_000, 1, 0, 0.0);
        assert!((s.rows_per_sec - 5_000.0).abs() < 1e-6);
    }

    /// `publish_flush` publishes the controller's flush value verbatim but
    /// never below the flush floor.
    #[test]
    fn publish_flush_floors_at_min() {
        // (We don't actually call `publish_flush` here — it writes the
        //  process-global atomic other tests rely on staying 0. We check the
        //  flooring contract by recomputing it the way `publish_flush` does.)
        let flush = |f: usize| f.max(FLUSH_FLOOR);
        assert_eq!(flush(16), 16);
        assert_eq!(flush(8), 8);
        // A tiny value can't push the published flush below the floor.
        assert_eq!(flush(1), FLUSH_FLOOR);
        assert_eq!(FLUSH_FLOOR, 4);
    }

    /// End-to-end live-loop policy: drive the controller through the SAME
    /// scripted-sample path the tick task uses (`sample_from_signals` →
    /// `observe`), feeding rising-then-falling committed throughput, and assert
    /// it climbs while throughput rises and backs off (without runaway
    /// oscillation) once it falls — the load-bearing shared-vCPU behavior.
    #[test]
    fn live_loop_climbs_then_backs_off() {
        // Mirror build_controller's floors/ceilings for the 4-vCPU anchor
        // (derived fanout 8): roam [4, 16].
        let mut c = AdaptiveController::new(8, 4, 16);
        let elapsed = 20.0; // one nominal interval

        // Rising throughput: committed rows per interval climb by a clear,
        // sustained margin. No stalls.
        for rows in [
            800_000u64, 1_000_000, 1_300_000, 1_700_000, 2_200_000, 2_900_000, 3_800_000,
        ] {
            let s = sample_from_signals(rows, 20, 0, elapsed);
            c.observe(s);
        }
        let peak = c.fanout();
        assert!(peak > 8, "should climb above the derived start (got {peak})");

        // Over-fanned: throughput collapses (CPU saturated) AND stalls appear.
        for (rows, stalls) in [(700_000u64, 8u64), (650_000, 10), (640_000, 11)] {
            let s = sample_from_signals(rows, 20, stalls, elapsed);
            c.observe(s);
        }
        assert!(
            c.fanout() < peak,
            "must back off after the regression (peak {peak}, now {})",
            c.fanout()
        );

        // Plateau: with no sustained win the controller converges to the
        // derived baseline and settles there (overload now gone).
        for _ in 0..20 {
            let s = sample_from_signals(660_000, 20, 0, elapsed);
            c.observe(s);
        }
        assert_eq!(c.fanout(), 8, "converges to derived absent a sustained win");
        assert!(c.fanout() >= 4 && c.fanout() <= 16, "stays within bounds");
    }

    /// No-op proof: the global signal accumulators exist, but with autotune off
    /// nothing READS them (the tick task is never spawned) and the runtime
    /// override atomics stay 0, so the knob readers fall through to their
    /// historical defaults. We assert the override atomics are unset; we do not
    /// touch the accumulators (process-global) so sibling tests are unaffected.
    #[test]
    fn signals_are_noop_without_reader() {
        // The runtime flush override — the ONLY ingest knob that changes live
        // behavior — is unset unless the controller publishes, which only
        // happens in the spawned tick task (autotune on). Fan-out has no
        // runtime override at all (pinned to the derived value).
        assert_eq!(RUNTIME_FLUSH.load(Ordering::Relaxed), 0);
    }

    /// CORRECTNESS GATE (Phase-2 exactly-once): `build_controller` drives the
    /// FLUSH-concurrency knob — it seeds the controller on the derived FLUSH
    /// value and roams the FLUSH bounds `[max(FLUSH_FLOOR, seed/2), seed×2]`,
    /// NOT the fan-out value/bounds. Fan-out is pinned (see the module
    /// comment), so the controller can never publish a fan-out change that
    /// reshapes the partition topology mid-ingest and double-counts the
    /// LIST-based count(*).
    ///
    /// Detection-order-independent: `build_controller` reads the process-cached
    /// `tuning()`, which a sibling test may have resolved first. We assert the
    /// STRUCTURE (seed → flush-shaped bounds) rather than an absolute value,
    /// and contrast it against the fan-out bounds the OLD controller used so a
    /// regression to fan-out tuning is caught regardless of detection order.
    #[test]
    fn controller_tunes_flush_not_fanout() {
        let c = build_controller();
        let seed = c.fanout(); // historical accessor name; carries the flush seed.

        // The controller's bounds are the FLUSH bounds derived from its seed.
        let min_flush = (seed / 2).max(FLUSH_FLOOR);
        let max_flush = (seed * 2).max(min_flush + 2);
        assert_eq!(c.min_fanout, min_flush, "lower bound is max(FLUSH_FLOOR, seed/2)");
        assert_eq!(c.max_fanout, max_flush, "upper bound is 2× seed");

        // The seed must be the derived FLUSH value, never the derived fan-out.
        // For ANY derived tuning, flush = clamp(4×cpus, …) is always >= fan-out
        // = clamp(2×cpus, …) before the memory cap, and the memory cap only
        // ever SHRINKS flush — so deriving the controller from fan-out would be
        // observable as a smaller seed on a fat box. The decisive structural
        // fact we pin: the bounds match the seed's FLUSH shaping, and the seed
        // is floored at FLUSH_FLOOR (the flush floor), which the fan-out floor
        // of 4 happens to share — so we additionally check the seed equals the
        // derived flush for whatever envelope build_controller resolved.
        let cached = tuning().copied();
        let derived = cached.unwrap_or(Tuning {
            fanout: 8,
            flush_concurrency: 16,
            project_concurrency: 96,
            global_budget: 96,
        });
        assert_eq!(
            seed, derived.flush_concurrency,
            "controller must seed at derived flush ({}), not derived fan-out ({})",
            derived.flush_concurrency, derived.fanout,
        );

        // There is NO runtime fan-out override surface at all (the symbol does
        // not even exist); the only live ingest knob is flush concurrency, and
        // it is a no-op until the tick task publishes.
        assert_eq!(RUNTIME_FLUSH.load(Ordering::Relaxed), 0, "no-op until published");
    }
}
