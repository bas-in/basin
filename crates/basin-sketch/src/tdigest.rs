//! t-digest sketch — hoisted from `basin-engine` Phase 5.14.B1.
//!
//! ## Algorithm
//! - Merging t-digest with a fixed compression factor `delta = 100`
//!   (≈ 200 centroids worst-case). Standard accuracy guarantee: ≤ 1 % error
//!   on uniformly distributed data for p ∈ (0.01, 0.99).
//! - Centroids are sorted by mean and cumulatively merged when
//!   `size_limit(k, delta)` is exceeded (standard Dunning 2019 formulation).
//! - Serialization: `n` centroids encoded as 16 bytes each (mean: f64 LE,
//!   weight: f64 LE) prefixed by an 8-byte little-endian count.
//!
//! Wire format is identical to the inline `TDigest` that was in
//! `basin-engine/src/approx_percentile.rs`; existing UDF state bytes
//! deserialise without any migration.

/// Compression parameter. Higher = more centroids = more accurate but slower.
const DELTA: f64 = 100.0;

/// A single t-digest centroid: mean and total weight.
#[derive(Clone, Debug, PartialEq)]
pub struct Centroid {
    pub mean: f64,
    pub weight: f64,
}

/// Compact t-digest representation.
#[derive(Clone, Debug)]
pub struct TDigest {
    /// Centroids sorted by mean ascending after each `compress()`.
    pub centroids: Vec<Centroid>,
    /// Total weight of all samples added.
    pub total_weight: f64,
}

impl TDigest {
    /// Create a fresh, empty t-digest.
    pub fn new() -> Self {
        Self {
            centroids: Vec::new(),
            total_weight: 0.0,
        }
    }

    /// Maximum weight allowed for a centroid at quantile position `q`.
    ///
    /// Standard Dunning 2019 formula: `4·n·q·(1-q)/delta`.
    /// We use a minimum of 1.0 to avoid zero-cap at q=0 or q=1.
    #[inline]
    fn max_weight(q: f64, n: f64) -> f64 {
        let cap = 4.0 * n * q * (1.0 - q) / DELTA;
        cap.max(1.0)
    }

    /// Add a single `value` with weight 1.0.
    pub fn add(&mut self, value: f64) {
        self.centroids.push(Centroid {
            mean: value,
            weight: 1.0,
        });
        self.total_weight += 1.0;
        // Compress every 512 raw inserts to keep the buffer bounded.
        if self.centroids.len() > 512 {
            self.compress();
        }
    }

    /// Merge another t-digest into this one (union of all centroids).
    pub fn merge(&mut self, other: &TDigest) {
        self.centroids.extend_from_slice(&other.centroids);
        self.total_weight += other.total_weight;
        self.compress();
    }

    /// Sort centroids by mean and merge adjacent ones that fit within the
    /// weight cap for their quantile position.
    pub fn compress(&mut self) {
        if self.centroids.is_empty() {
            return;
        }

        self.centroids
            .sort_unstable_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap_or(std::cmp::Ordering::Equal));

        let n = self.total_weight;
        let mut merged: Vec<Centroid> = Vec::with_capacity(self.centroids.len());
        let mut cumulative_weight = 0.0_f64;

        for c in self.centroids.drain(..) {
            if merged.is_empty() {
                cumulative_weight = c.weight;
                merged.push(c);
                continue;
            }

            let last = merged.last_mut().unwrap();
            let q = cumulative_weight / n;
            let cap = Self::max_weight(q, n);

            if last.weight + c.weight <= cap {
                // Merge into existing centroid: weighted mean.
                let combined = last.weight + c.weight;
                last.mean = (last.mean * last.weight + c.mean * c.weight) / combined;
                last.weight = combined;
                cumulative_weight += c.weight;
            } else {
                cumulative_weight += c.weight;
                merged.push(c);
            }
        }

        self.centroids = merged;
    }

    /// Estimate the value at the `p`-th percentile (p ∈ [0.0, 1.0]).
    pub fn quantile(&mut self, p: f64) -> f64 {
        // Ensure centroids are sorted and compressed.
        self.compress();

        if self.centroids.is_empty() || !p.is_finite() {
            return f64::NAN;
        }

        let p = p.clamp(0.0, 1.0);

        // Handle edge cases.
        if p == 0.0 {
            return self.centroids.first().unwrap().mean;
        }
        if p == 1.0 {
            return self.centroids.last().unwrap().mean;
        }

        let target = p * self.total_weight;

        // Walk centroids tracking cumulative weight at the midpoint of each.
        let mut cumulative = 0.0_f64;
        for (i, c) in self.centroids.iter().enumerate() {
            // The centroid spans [cumulative, cumulative + weight).
            // The centroid's "representative" quantile is its midpoint.
            let midpoint = cumulative + c.weight / 2.0;

            if midpoint >= target {
                // Interpolate between the previous centroid midpoint and this one.
                if i == 0 {
                    return c.mean;
                }
                let prev = &self.centroids[i - 1];
                let prev_mid = cumulative - prev.weight / 2.0;
                // Linear interpolation between prev.mean and c.mean.
                let t = if (midpoint - prev_mid).abs() < 1e-12 {
                    0.5
                } else {
                    (target - prev_mid) / (midpoint - prev_mid)
                };
                return prev.mean + t * (c.mean - prev.mean);
            }

            cumulative += c.weight;
        }

        // Fell through — return the last centroid mean.
        self.centroids.last().unwrap().mean
    }

    // ── Serialization ─────────────────────────────────────────────────────

    /// Serialize to bytes: `u64le count` followed by `(f64le mean, f64le weight)` per centroid.
    pub fn to_bytes(&self) -> Vec<u8> {
        let n = self.centroids.len();
        let mut buf = Vec::with_capacity(8 + n * 16);
        buf.extend_from_slice(&(n as u64).to_le_bytes());
        for c in &self.centroids {
            buf.extend_from_slice(&c.mean.to_le_bytes());
            buf.extend_from_slice(&c.weight.to_le_bytes());
        }
        buf
    }

    /// Deserialize from bytes produced by [`TDigest::to_bytes`].
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 8 {
            return None;
        }
        let n = u64::from_le_bytes(bytes[0..8].try_into().ok()?) as usize;
        if bytes.len() != 8 + n * 16 {
            return None;
        }
        let mut centroids = Vec::with_capacity(n);
        let mut total_weight = 0.0_f64;
        for i in 0..n {
            let off = 8 + i * 16;
            let mean = f64::from_le_bytes(bytes[off..off + 8].try_into().ok()?);
            let weight = f64::from_le_bytes(bytes[off + 8..off + 16].try_into().ok()?);
            total_weight += weight;
            centroids.push(Centroid { mean, weight });
        }
        Some(TDigest {
            centroids,
            total_weight,
        })
    }
}

impl Default for TDigest {
    fn default() -> Self {
        Self::new()
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Feed `n` sequential integers (0..n) and verify that the quantile
    /// estimate is within `tol` (fraction) of the true value.
    fn check_quantile(n: usize, p: f64, tol: f64) {
        let mut digest = TDigest::new();
        for i in 0..n {
            digest.add(i as f64);
        }
        let estimated = digest.quantile(p);
        let true_val = p * (n as f64 - 1.0);
        let err = (estimated - true_val).abs() / (n as f64);
        assert!(
            err <= tol,
            "n={n} p={p}: estimated={estimated:.2} true={true_val:.2} rel_err={:.4} > tol={tol}",
            err
        );
    }

    #[test]
    fn tdigest_median_100k() {
        check_quantile(100_000, 0.5, 0.005);
    }

    #[test]
    fn tdigest_p95_100k() {
        check_quantile(100_000, 0.95, 0.01);
    }

    #[test]
    fn tdigest_p05_100k() {
        check_quantile(100_000, 0.05, 0.01);
    }

    #[test]
    fn tdigest_edge_min() {
        let mut d = TDigest::new();
        for i in 0..1000 {
            d.add(i as f64);
        }
        let min = d.quantile(0.0);
        assert!((min - 0.0).abs() < 1.0, "min estimate={min}");
    }

    #[test]
    fn tdigest_edge_max() {
        let mut d = TDigest::new();
        for i in 0..1000 {
            d.add(i as f64);
        }
        let max = d.quantile(1.0);
        assert!((max - 999.0).abs() < 1.0, "max estimate={max}");
    }

    #[test]
    fn tdigest_serialization_roundtrip() {
        let mut d = TDigest::new();
        for i in 0..5000_u64 {
            d.add(i as f64);
        }
        d.compress();
        let bytes = d.to_bytes();
        let restored = TDigest::from_bytes(&bytes).expect("deserialize failed");
        let orig_q = d.quantile(0.5);
        let rest_q = {
            let mut r = restored;
            r.quantile(0.5)
        };
        assert!(
            (orig_q - rest_q).abs() < 1.0,
            "roundtrip diverged: orig={orig_q} restored={rest_q}"
        );
    }

    #[test]
    fn tdigest_merge_two_halves() {
        let mut a = TDigest::new();
        let mut b = TDigest::new();
        for i in 0..5000 {
            a.add(i as f64);
        }
        for i in 5000..10000 {
            b.add(i as f64);
        }
        a.merge(&b);
        let median = a.quantile(0.5);
        let err = (median - 4999.5).abs() / 10000.0;
        assert!(err <= 0.01, "merged median={median:.2} rel_err={err:.4}");
    }

    #[test]
    fn tdigest_empty_returns_nan() {
        let mut d = TDigest::new();
        assert!(d.quantile(0.5).is_nan());
    }
}
