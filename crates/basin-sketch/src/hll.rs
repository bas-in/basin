//! HyperLogLog (HLL) sketch — hoisted from `basin-engine` Phase 5.14.B1.
//!
//! ## Algorithm
//! - 2^14 = 16 384 u8 registers (b=14 → relative std-error ≈ 0.8 %)
//! - MurmurHash3-like mixing for speed and uniformity
//! - LogLog-Beta bias correction from Deudon 2017 (replaces HLL++ correction)
//! - Serialization: raw 16 384-byte `Vec<u8>`
//!
//! Wire format is identical to the inline `Hll` that was in
//! `basin-engine/src/approx_count_distinct.rs`; existing UDF state bytes
//! deserialise without any migration.

/// Number of register bits. 2^14 = 16 384 registers → ~0.8 % std-error.
const HLL_B: u32 = 14;
/// Number of registers.
const HLL_M: usize = 1 << HLL_B; // 16 384
/// Mask for the register index (lower b bits of hash).
const HLL_MASK: u64 = (HLL_M as u64) - 1;

/// 16 384-byte HyperLogLog register set.
#[derive(Clone)]
pub struct Hll {
    regs: Vec<u8>,
}

impl std::fmt::Debug for Hll {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hll({}regs)", HLL_M)
    }
}

impl Hll {
    /// Create a fresh, zeroed HLL.
    pub fn new() -> Self {
        Self {
            regs: vec![0u8; HLL_M],
        }
    }

    /// Reconstruct from raw bytes produced by [`Hll::to_bytes`].
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != HLL_M {
            return None;
        }
        Some(Self {
            regs: bytes.to_vec(),
        })
    }

    /// Serialize to raw bytes (16 384 bytes).
    pub fn to_bytes(&self) -> Vec<u8> {
        self.regs.clone()
    }

    /// Hash bytes using a MurmurHash3-like mix.
    ///
    /// Processes 8-byte blocks for speed and quality; processes a final
    /// partial block if needed.  Applies the MurmurHash3 64-bit finalizer
    /// (fmix64) to every block so the bits are well-avalanched even for
    /// short keys such as 8-byte integers.
    #[inline]
    fn hash_bytes(data: &[u8]) -> u64 {
        /// MurmurHash3 64-bit finalizer (fmix64).
        #[inline(always)]
        fn fmix64(mut h: u64) -> u64 {
            h ^= h >> 33;
            h = h.wrapping_mul(0xff51afd7ed558ccd);
            h ^= h >> 33;
            h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
            h ^= h >> 33;
            h
        }

        const C1: u64 = 0x87c37b91114253d5;
        const C2: u64 = 0x4cf5ad432745937f;

        let len = data.len();
        let nblocks = len / 8;
        // Seed is fixed; determinism is required for merge correctness.
        let mut h1: u64 = 0x9368_5321_3456_789a;

        // Process 8-byte blocks.
        for i in 0..nblocks {
            let mut k1 = u64::from_le_bytes(data[i * 8..i * 8 + 8].try_into().unwrap());
            k1 = k1.wrapping_mul(C1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(C2);
            h1 ^= k1;
            h1 = h1.rotate_left(27);
            h1 = h1.wrapping_mul(5).wrapping_add(0x52dce729);
        }

        // Process the tail bytes (< 8).
        let tail = &data[nblocks * 8..];
        if !tail.is_empty() {
            let mut k1: u64 = 0;
            for (j, &b) in tail.iter().enumerate() {
                k1 ^= (b as u64) << (j * 8);
            }
            k1 = k1.wrapping_mul(C1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(C2);
            h1 ^= k1;
        }

        // Finalize.
        h1 ^= len as u64;
        fmix64(h1)
    }

    /// Insert a pre-computed 64-bit hash value.
    #[inline]
    fn insert_hash(&mut self, h: u64) {
        let idx = (h & HLL_MASK) as usize;
        // Upper (64 - b) bits: count leading zeros + 1
        let upper = h >> HLL_B;
        let rho = (upper.leading_zeros() - (HLL_B - 1)) as u8; // 1 … 50
        if rho > self.regs[idx] {
            self.regs[idx] = rho;
        }
    }

    /// Insert raw bytes (string or fixed-size primitive bytes).
    #[inline]
    pub fn insert(&mut self, data: &[u8]) {
        self.insert_hash(Self::hash_bytes(data));
    }

    /// Component-wise maximum merge (HLL union).
    pub fn merge(&mut self, other: &Hll) {
        for (a, b) in self.regs.iter_mut().zip(other.regs.iter()) {
            if *b > *a {
                *a = *b;
            }
        }
    }

    /// Estimate cardinality using LogLog-Beta bias correction.
    pub fn cardinality(&self) -> u64 {
        // Raw harmonic-mean estimator
        let alpha_m = 0.7213 / (1.0 + 1.079 / HLL_M as f64);
        let m = HLL_M as f64;

        let raw_sum: f64 = self.regs.iter().map(|&r| 2.0f64.powi(-(r as i32))).sum();
        let e = alpha_m * m * m / raw_sum;

        // Small-range correction (linear counting)
        let zeros = self.regs.iter().filter(|&&r| r == 0).count() as f64;
        let estimate = if zeros > 0.0 && e < 2.5 * m {
            m * (m / zeros).ln()
        } else {
            // LogLog-Beta correction (Deudon 2017)
            let ez = self.regs.iter().filter(|&&r| r == 0).count() as f64;
            let beta = -0.370393911 * ez
                + 0.070471823 * ez.ln_1p()
                + 0.17393686 * (ez * 2.0 + 1.0).ln()
                + 0.16339839 * (ez * 4.0 + 1.0).ln()
                + -0.09237745 * (ez * 8.0 + 1.0).ln()
                + 0.03738027 * (ez * 16.0 + 1.0).ln()
                + -0.005384159 * (ez * 32.0 + 1.0).ln()
                + 0.00042419 * (ez * 64.0 + 1.0).ln();
            alpha_m * m * (m - ez + beta) / raw_sum
        };

        estimate.round().max(0.0) as u64
    }
}

impl Default for Hll {
    fn default() -> Self {
        Self::new()
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Insert n distinct integers and verify the estimate is within ±5 %.
    fn check_estimate(n: usize, tolerance: f64) {
        let mut hll = Hll::new();
        for i in 0..n {
            hll.insert(&(i as u64).to_le_bytes());
        }
        let est = hll.cardinality() as f64;
        let err = (est - n as f64).abs() / n as f64;
        assert!(
            err <= tolerance,
            "n={n}: estimate={est}, error={:.2} > {:.2}",
            err * 100.0,
            tolerance * 100.0
        );
    }

    #[test]
    fn hll_small_cardinality_exact() {
        check_estimate(17, 0.05);
    }

    #[test]
    fn hll_medium_cardinality() {
        check_estimate(1_000, 0.05);
    }

    #[test]
    fn hll_large_cardinality() {
        check_estimate(100_000, 0.05);
    }

    #[test]
    fn hll_merge_is_union() {
        let mut a = Hll::new();
        let mut b = Hll::new();
        for i in 0u64..500 {
            a.insert(&i.to_le_bytes());
        }
        for i in 500u64..1000 {
            b.insert(&i.to_le_bytes());
        }
        a.merge(&b);
        let est = a.cardinality() as f64;
        let err = (est - 1000.0).abs() / 1000.0;
        assert!(
            err <= 0.05,
            "merged estimate={est}, error={:.2}",
            err * 100.0
        );
    }

    #[test]
    fn hll_serialization_roundtrip() {
        let mut hll = Hll::new();
        for i in 0u64..200 {
            hll.insert(&i.to_le_bytes());
        }
        let bytes = hll.to_bytes();
        assert_eq!(bytes.len(), HLL_M);
        let restored = Hll::from_bytes(&bytes).expect("deserialize failed");
        assert_eq!(hll.cardinality(), restored.cardinality());
    }

    #[test]
    fn hll_empty_returns_zero() {
        let hll = Hll::new();
        assert_eq!(hll.cardinality(), 0);
    }
}
