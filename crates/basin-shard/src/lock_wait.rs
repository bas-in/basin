//! Bounded lock-wait primitive for the `lock_timeout` GUC (Phase 5.28.B).
//!
//! ## Why a separate primitive?
//!
//! The engine's `pg_advisory_lock` path already implements a bounded spin-with-
//! backoff inside a synchronous UDF. `lock_wait.rs` generalises the concept to
//! an **async** context where the waiter cooperates with the Tokio runtime:
//! rather than a `thread::sleep` spin (which blocks a worker thread), the async
//! variant yields between attempts using `tokio::time::sleep`. This makes it
//! safe to call from within the async executor path without pinning a worker.
//!
//! ## Usage
//!
//! ```ignore
//! use basin_shard::lock_wait::bounded_lock_wait;
//! use std::time::Duration;
//!
//! // `try_acquire` returns `true` when the caller got the lock.
//! let acquired = bounded_lock_wait(
//!     || my_try_lock(),           // non-blocking attempt
//!     Some(Duration::from_millis(500)),  // lock_timeout (None = wait forever)
//!     Duration::from_millis(5),   // retry cadence
//! )
//! .await;
//! ```
//!
//! When `lock_timeout` is `Some(d)` and the deadline elapses without acquiring
//! the lock, `bounded_lock_wait` returns `false`. The caller maps this to
//! `BasinError::LockNotAvailable` (SQLSTATE 55P03).

use std::time::{Duration, Instant};

/// Attempt to acquire a lock by calling `try_acquire` repeatedly until it
/// returns `true`, `lock_timeout` expires, or (when `lock_timeout` is `None`)
/// the caller cancels the future.
///
/// Returns `true` when the lock was acquired, `false` when the timeout fired.
///
/// - `try_acquire`: a non-blocking closure — must NOT block the thread.
/// - `lock_timeout`: `None` means wait indefinitely; `Some(d)` caps the wait.
/// - `retry_interval`: how long to sleep between retries. Short values (1–5 ms)
///   reduce latency at the cost of slightly higher CPU; for `lock_timeout`
///   testing, 1 ms is appropriate.
pub async fn bounded_lock_wait<F>(
    mut try_acquire: F,
    lock_timeout: Option<Duration>,
    retry_interval: Duration,
) -> bool
where
    F: FnMut() -> bool,
{
    // Immediate attempt before we even look at the clock.
    if try_acquire() {
        return true;
    }

    let deadline = lock_timeout.map(|d| Instant::now() + d);

    loop {
        // Check deadline before sleeping.
        if let Some(dl) = deadline {
            if Instant::now() >= dl {
                return false;
            }
        }

        // Yield to the runtime for `retry_interval` (or until the deadline,
        // whichever is sooner).
        let sleep_dur = if let Some(dl) = deadline {
            let remaining = dl.saturating_duration_since(Instant::now());
            remaining.min(retry_interval)
        } else {
            retry_interval
        };

        tokio::time::sleep(sleep_dur).await;

        if try_acquire() {
            return true;
        }

        // Re-check deadline after the attempt.
        if let Some(dl) = deadline {
            if Instant::now() >= dl {
                return false;
            }
        }
    }
}

/// Synchronous variant of [`bounded_lock_wait`] for contexts where `await` is
/// unavailable (e.g. inside DataFusion UDFs). Uses `std::thread::sleep`.
/// Prefer the async variant when calling from async code.
///
/// Returns `true` when acquired, `false` on timeout.
pub fn bounded_lock_wait_sync<F>(
    mut try_acquire: F,
    lock_timeout: Option<Duration>,
    retry_interval: Duration,
) -> bool
where
    F: FnMut() -> bool,
{
    if try_acquire() {
        return true;
    }

    let deadline = lock_timeout.map(|d| Instant::now() + d);

    loop {
        if let Some(dl) = deadline {
            if Instant::now() >= dl {
                return false;
            }
        }

        let sleep_dur = if let Some(dl) = deadline {
            let remaining = dl.saturating_duration_since(Instant::now());
            remaining.min(retry_interval)
        } else {
            retry_interval
        };

        std::thread::sleep(sleep_dur);

        if try_acquire() {
            return true;
        }

        if let Some(dl) = deadline {
            if Instant::now() >= dl {
                return false;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn acquires_immediately_when_free() {
        // Lock is "free" on the first try.
        let acquired = bounded_lock_wait(
            || true,
            Some(Duration::from_millis(100)),
            Duration::from_millis(5),
        )
        .await;
        assert!(acquired, "must acquire when lock is immediately free");
    }

    #[tokio::test]
    async fn times_out_when_lock_never_free() {
        // Lock is always held by someone else.
        let acquired = bounded_lock_wait(
            || false,
            Some(Duration::from_millis(50)),
            Duration::from_millis(5),
        )
        .await;
        assert!(!acquired, "must time out when lock is never free");
    }

    #[tokio::test]
    async fn acquires_after_brief_contention() {
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();
        // Lock becomes free after 3 attempts.
        let acquired = bounded_lock_wait(
            move || {
                let n = c.fetch_add(1, Ordering::Relaxed);
                n >= 3
            },
            Some(Duration::from_millis(200)),
            Duration::from_millis(10),
        )
        .await;
        assert!(acquired, "must acquire once contention clears");
    }

    #[tokio::test]
    async fn no_timeout_waits_until_free() {
        let counter = Arc::new(AtomicU32::new(0));
        let c = counter.clone();
        // Becomes free after 2 attempts; no timeout — must still complete.
        let acquired = bounded_lock_wait(
            move || {
                let n = c.fetch_add(1, Ordering::Relaxed);
                n >= 2
            },
            None, // no timeout
            Duration::from_millis(5),
        )
        .await;
        assert!(acquired);
    }

    #[test]
    fn sync_variant_acquires_immediately() {
        let acquired = bounded_lock_wait_sync(
            || true,
            Some(Duration::from_millis(100)),
            Duration::from_millis(5),
        );
        assert!(acquired);
    }

    #[test]
    fn sync_variant_times_out() {
        let acquired = bounded_lock_wait_sync(
            || false,
            Some(Duration::from_millis(30)),
            Duration::from_millis(5),
        );
        assert!(!acquired);
    }
}
