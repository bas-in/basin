//! Case-insensitive B-tree index path for `CITEXT` UNIQUE constraints
//! (Phase 5.30.D).
//!
//! # Overview
//!
//! PostgreSQL's `citext` extension makes `UNIQUE (col citext)` reject both
//! `'Foo'` and `'foo'` as duplicates. Basin enforces this by case-folding the
//! dedup key before inserting it into the uniqueness hash-set — the same
//! strategy the main `enforce_one_unique` path in `basin-engine::constraints`
//! uses for in-memory dedup.
//!
//! This module provides storage-layer utilities for building and querying a
//! case-folded in-memory B-tree (`BTreeMap<String, Vec<RowLocation>>`) that
//! the engine can consult during UNIQUE enforcement without re-scanning data
//! files. At 100k rows the fold + BTreeMap lookup is O(log N) per inserted
//! row, meeting the performance requirement in Phase 5.30.D.
//!
//! # Relationship to the secondary-index path
//!
//! The general B-tree secondary index (`basin-engine::secondary_index`) stores
//! raw string values as keys. A citext column's logical uniqueness must be
//! enforced on *folded* keys; this module builds a thin wrapper around a
//! `BTreeMap<String, ()>` (a set) where every key is the lower-case fold of
//! the original value.
//!
//! # Usage
//!
//! `CitextUniqueSet` is created once per UNIQUE-constraint check. The
//! `insert` method returns `false` when a case-insensitive duplicate is
//! detected; the caller then raises `UniqueViolation`.

use std::collections::BTreeMap;

/// A case-insensitive uniqueness set for `CITEXT` UNIQUE constraints.
///
/// Internally stores the lower-case fold of each value. Two values that are
/// equal under PostgreSQL's citext case-folding (`lower()`) are considered
/// duplicates.
///
/// Designed to hold up to 100k entries efficiently; the underlying `BTreeMap`
/// guarantees O(log N) insert / lookup with no allocation per probe beyond the
/// fold allocation.
#[derive(Debug, Default)]
pub struct CitextUniqueSet {
    inner: BTreeMap<String, ()>,
}

impl CitextUniqueSet {
    /// Create an empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attempt to insert a value.
    ///
    /// Returns `true` if the value was inserted (i.e. no case-insensitive
    /// duplicate existed). Returns `false` if a duplicate was found (the
    /// caller should raise a `UniqueViolation` error).
    pub fn insert(&mut self, value: &str) -> bool {
        let key = value.to_lowercase();
        if self.inner.contains_key(&key) {
            false
        } else {
            self.inner.insert(key, ());
            true
        }
    }

    /// Returns `true` if the set contains a case-insensitive duplicate of
    /// `value`.
    pub fn contains(&self, value: &str) -> bool {
        self.inner.contains_key(&value.to_lowercase())
    }

    /// Number of unique (folded) values in the set.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// `true` when the set is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_contains_basic() {
        let mut set = CitextUniqueSet::new();
        assert!(set.insert("hello"));
        assert!(set.contains("hello"));
        assert!(set.contains("HELLO"));
        assert!(set.contains("Hello"));
    }

    #[test]
    fn duplicate_detection() {
        let mut set = CitextUniqueSet::new();
        assert!(set.insert("Foo"));
        assert!(!set.insert("foo")); // case-insensitive duplicate
        assert!(!set.insert("FOO")); // another case variant
    }

    #[test]
    fn distinct_values() {
        let mut set = CitextUniqueSet::new();
        assert!(set.insert("alpha"));
        assert!(set.insert("beta"));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn empty_string() {
        let mut set = CitextUniqueSet::new();
        assert!(set.insert(""));
        assert!(!set.insert(""));
        assert_eq!(set.len(), 1);
    }

    /// Smoke test at 100k rows to validate O(log N) performance gate.
    /// The insert loop should complete in < 1s on any CI host; we don't
    /// assert a time bound but the test will be obviously slow if the
    /// implementation regresses to O(N^2).
    #[test]
    fn hundred_thousand_rows_smoke() {
        let mut set = CitextUniqueSet::new();
        for i in 0u64..100_000 {
            let v = format!("user_{i}@example.com");
            assert!(set.insert(&v), "insert {v} should be unique");
        }
        assert_eq!(set.len(), 100_000);
        // Spot-check a few case variants.
        assert!(set.contains("User_0@example.com"));
        assert!(set.contains("USER_99999@EXAMPLE.COM"));
    }
}
