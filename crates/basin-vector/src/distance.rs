//! Distance functions used by both UDFs and HNSW.
//!
//! All three operate on `[f32]` slices of equal length. Length mismatch is a
//! programming error: distance comparisons across dimensionalities are
//! undefined and indicate a schema/type bug elsewhere. The functions panic
//! with a debug-mode `debug_assert_eq!`; release-mode UB is impossible
//! because the iteration is bounded by the shorter slice.

use serde::{Deserialize, Serialize};

/// Distance metric tag carried by the index format and the engine UDFs.
/// Stable wire form (snake_case) so the on-disk index file remains
/// readable across releases.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Distance {
    L2,
    Cosine,
    Dot,
}

impl Distance {
    /// Apply `self` to a pair of equal-length slices.
    pub fn apply(self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            Distance::L2 => l2_distance(a, b),
            Distance::Cosine => cosine_distance(a, b),
            Distance::Dot => -dot_product(a, b),
        }
    }
}

#[inline]
pub fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "vector dim mismatch");
    let n = a.len().min(b.len());
    let mut s = 0.0f32;
    for i in 0..n {
        let d = a[i] - b[i];
        s += d * d;
    }
    s.sqrt()
}

#[inline]
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "vector dim mismatch");
    let n = a.len().min(b.len());
    let mut s = 0.0f32;
    for i in 0..n {
        s += a[i] * b[i];
    }
    s
}

#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "vector dim mismatch");
    let n = a.len().min(b.len());
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for i in 0..n {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    let denom = (na.sqrt() * nb.sqrt()).max(f32::EPSILON);
    1.0 - dot / denom
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l2_known_values() {
        assert!((l2_distance(&[0.0, 0.0], &[3.0, 4.0]) - 5.0).abs() < 1e-6);
        assert_eq!(l2_distance(&[1.0; 4], &[1.0; 4]), 0.0);
    }

    #[test]
    fn dot_known_values() {
        assert_eq!(dot_product(&[1.0, 2.0, 3.0], &[1.0, 2.0, 3.0]), 14.0);
        assert_eq!(dot_product(&[0.0; 5], &[1.0; 5]), 0.0);
    }

    #[test]
    fn cosine_orthogonal_is_one() {
        let d = cosine_distance(&[1.0, 0.0], &[0.0, 1.0]);
        assert!((d - 1.0).abs() < 1e-6, "got {d}");
    }

    #[test]
    fn cosine_identical_is_zero() {
        let v = [0.5, -0.3, 0.7, 0.1];
        let d = cosine_distance(&v, &v);
        assert!(d.abs() < 1e-6, "got {d}");
    }
}
