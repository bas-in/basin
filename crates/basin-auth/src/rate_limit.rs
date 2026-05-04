//! Per-key rate limiting on top of `governor`.
//!
//! One limiter per (logical bucket, key). Keys are arbitrary strings — the
//! flow modules pass either an IP, an email, or a `(ip, email)` composite.
//!
//! The default quota lives at `AuthConfig::rate_limit_per_ip_per_min`; we
//! expose a small builder so tests can dial it up or down.

use std::num::NonZeroU32;

use basin_common::{BasinError, Result};
use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};

/// Per-key limiter. `RateLimiter::keyed::<String>` tracks one bucket per key.
pub struct PerKey {
    inner: RateLimiter<String, governor::state::keyed::DefaultKeyedStateStore<String>, DefaultClock>,
    label: &'static str,
}

impl PerKey {
    pub fn per_minute(per_min: u32, label: &'static str) -> Self {
        // `per_minute(N)` would mean "1 every 60/N seconds with burst N".
        // governor's API takes the burst rate via `Quota::per_minute`.
        let n = NonZeroU32::new(per_min.max(1)).expect("max(1) is >0");
        let quota = Quota::per_minute(n);
        Self {
            inner: RateLimiter::keyed(quota),
            label,
        }
    }

    /// Returns Err if the key is over quota. The error message names the
    /// limiter for log triage.
    pub fn check(&self, key: &str) -> Result<()> {
        match self.inner.check_key(&key.to_owned()) {
            Ok(_) => Ok(()),
            Err(_) => Err(BasinError::internal(format!(
                "rate limited ({label})",
                label = self.label
            ))),
        }
    }
}

/// Single-bucket limiter for cases where the key is implicit.
pub struct Single {
    inner: RateLimiter<NotKeyed, InMemoryState, DefaultClock>,
    label: &'static str,
}

impl Single {
    #[allow(dead_code)]
    pub fn per_minute(per_min: u32, label: &'static str) -> Self {
        let n = NonZeroU32::new(per_min.max(1)).expect("max(1) is >0");
        Self {
            inner: RateLimiter::direct(Quota::per_minute(n)),
            label,
        }
    }

    #[allow(dead_code)]
    pub fn check(&self) -> Result<()> {
        match self.inner.check() {
            Ok(_) => Ok(()),
            Err(_) => Err(BasinError::internal(format!(
                "rate limited ({label})",
                label = self.label
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_key_limits() {
        let lim = PerKey::per_minute(2, "test");
        assert!(lim.check("a").is_ok());
        assert!(lim.check("a").is_ok());
        assert!(lim.check("a").is_err(), "third hit must be denied");
        // Different key has its own bucket.
        assert!(lim.check("b").is_ok());
    }
}
