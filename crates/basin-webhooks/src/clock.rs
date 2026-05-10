//! Clock injection for tests.
//!
//! Production paths use [`SystemClock`] (`chrono::Utc::now`); tests use
//! [`TestClock`] to fast-forward the wall clock without sleeping. The
//! sink, the queue (via `enqueue_at`), and the worker all consume the
//! same clock so a fast-forward in a test moves all three forwards
//! atomically.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};

/// Source of time. Send + Sync + 'static so it can be passed through
/// `Arc<dyn Clock>`.
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> DateTime<Utc>;
}

/// Real wall-clock. Always-`Utc::now`.
#[derive(Clone, Copy, Default, Debug)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Manually-advanced clock. Lock-free reads via [`AtomicI64`] of unix
/// milliseconds. Cheap to clone (`Arc` inside).
#[derive(Clone, Debug)]
pub struct TestClock {
    ms: Arc<AtomicI64>,
}

impl TestClock {
    pub fn new(t: DateTime<Utc>) -> Self {
        Self {
            ms: Arc::new(AtomicI64::new(t.timestamp_millis())),
        }
    }

    pub fn set(&self, t: DateTime<Utc>) {
        self.ms.store(t.timestamp_millis(), Ordering::Relaxed);
    }

    pub fn advance(&self, d: chrono::Duration) {
        self.ms.fetch_add(d.num_milliseconds(), Ordering::Relaxed);
    }
}

impl Clock for TestClock {
    fn now(&self) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp_millis(self.ms.load(Ordering::Relaxed))
            .unwrap_or_else(Utc::now)
    }
}
