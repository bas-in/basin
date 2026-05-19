//! [`RowKey`] — big-endian encoded primary-key columns.
//!
//! Byte-encoding mirrors the cluster-key sort used by
//! `basin-storage::writer::sort_batch_by_cluster_cols`.  Lexicographic byte
//! order on the resulting `Vec<u8>` matches the natural PK sort order for
//! every supported column type:
//!
//! - **Integers** (`i8/i16/i32/i64/u8/u16/u32/u64`): big-endian, with signed
//!   types bias-flipped so they sort correctly (XOR `0x80…` on the high byte).
//! - **Strings** (`Utf8/LargeUtf8`): raw UTF-8 bytes followed by a `0x00`
//!   null-terminator so multi-column keys compose without length prefixes.
//! - **Nullable columns**: a leading sentinel byte — `0x00` for NULL (sorts
//!   before all non-NULL values), `0x01` for non-NULL (followed by the value
//!   encoding above).
//!
//! The encoding is append-only: add a column's bytes to the buffer in
//! declaration order, then wrap in `RowKey(buf)`.

use std::cmp::Ordering;

/// Owned, lexicographically-orderable primary-key encoding.
///
/// Wrap with `RowKey::builder()` or construct directly from pre-encoded bytes
/// when deserialising from the WAL.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RowKey(pub Vec<u8>);

impl RowKey {
    /// Wrap pre-encoded bytes (e.g. from WAL replay).
    #[inline]
    pub fn from_bytes(b: Vec<u8>) -> Self {
        Self(b)
    }

    /// Borrow the raw bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Start building a composite key from individual column values.
    pub fn builder() -> RowKeyBuilder {
        RowKeyBuilder::default()
    }
}

impl PartialOrd for RowKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RowKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

// ── Builder ──────────────────────────────────────────────────────────────────

/// Builds a `RowKey` by appending column values in declaration order.
#[derive(Default)]
pub struct RowKeyBuilder {
    buf: Vec<u8>,
}

impl RowKeyBuilder {
    /// Append an `i8` key column (bias-flipped big-endian).
    pub fn append_i8(mut self, v: i8) -> Self {
        let flipped = (v as u8) ^ 0x80;
        self.buf.push(flipped);
        self
    }

    /// Append an `i16` key column.
    pub fn append_i16(mut self, v: i16) -> Self {
        let b = (v as u16 ^ 0x8000u16).to_be_bytes();
        self.buf.extend_from_slice(&b);
        self
    }

    /// Append an `i32` key column.
    pub fn append_i32(mut self, v: i32) -> Self {
        let b = (v as u32 ^ 0x8000_0000u32).to_be_bytes();
        self.buf.extend_from_slice(&b);
        self
    }

    /// Append an `i64` key column.
    pub fn append_i64(mut self, v: i64) -> Self {
        let b = (v as u64 ^ 0x8000_0000_0000_0000u64).to_be_bytes();
        self.buf.extend_from_slice(&b);
        self
    }

    /// Append a `u8` key column (plain big-endian).
    pub fn append_u8(mut self, v: u8) -> Self {
        self.buf.push(v);
        self
    }

    /// Append a `u16` key column.
    pub fn append_u16(mut self, v: u16) -> Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    /// Append a `u32` key column.
    pub fn append_u32(mut self, v: u32) -> Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    /// Append a `u64` key column.
    pub fn append_u64(mut self, v: u64) -> Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    /// Append a `Utf8/LargeUtf8` key column (raw bytes + `0x00` terminator).
    ///
    /// Interior null bytes in the string are escaped so the null-terminator
    /// remains unambiguous, following the NUL-escape convention used by
    /// FoundationDB and many other ordered-key encodings.
    pub fn append_str(mut self, v: &str) -> Self {
        for &b in v.as_bytes() {
            match b {
                0x00 => {
                    self.buf.push(0x00);
                    self.buf.push(0xFF); // escape: 0x00 0xFF sorts between NUL and all non-NUL
                }
                _ => self.buf.push(b),
            }
        }
        self.buf.push(0x00); // null terminator
        self
    }

    /// Append a nullable column.  `None` → `0x00` sentinel (sorts first).
    /// `Some(f)` → `0x01` sentinel followed by the value encoding from `f`.
    pub fn append_nullable<F>(mut self, v: Option<F>, encode: impl FnOnce(Self, F) -> Self) -> Self {
        match v {
            None => {
                self.buf.push(0x00);
                self
            }
            Some(inner) => {
                self.buf.push(0x01);
                encode(self, inner)
            }
        }
    }

    /// Finalise the key.
    pub fn finish(self) -> RowKey {
        RowKey(self.buf)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── i64 ──────────────────────────────────────────────────────────────────

    #[test]
    fn i64_encodes_sort_order() {
        let neg = RowKey::builder().append_i64(i64::MIN).finish();
        let zero = RowKey::builder().append_i64(0).finish();
        let pos = RowKey::builder().append_i64(i64::MAX).finish();
        assert!(neg < zero, "i64::MIN must sort before 0");
        assert!(zero < pos, "0 must sort before i64::MAX");
    }

    #[test]
    fn i64_roundtrip_identity() {
        // Decoding: undo bias-flip then big-endian interpret.
        let v: i64 = -42_000_000;
        let key = RowKey::builder().append_i64(v).finish();
        assert_eq!(key.as_bytes().len(), 8);
        let raw = u64::from_be_bytes(key.as_bytes()[..8].try_into().unwrap());
        let decoded = (raw ^ 0x8000_0000_0000_0000u64) as i64;
        assert_eq!(decoded, v);
    }

    #[test]
    fn i64_negative_values_sort_correctly() {
        let cases: &[i64] = &[-1_000, -1, 0, 1, 1_000];
        let keys: Vec<_> = cases.iter().map(|&v| RowKey::builder().append_i64(v).finish()).collect();
        for w in keys.windows(2) {
            assert!(w[0] < w[1], "{:?} should be < {:?}", w[0], w[1]);
        }
    }

    // ── Utf8 ─────────────────────────────────────────────────────────────────

    #[test]
    fn utf8_encodes_sort_order() {
        let a = RowKey::builder().append_str("alpha").finish();
        let b = RowKey::builder().append_str("beta").finish();
        assert!(a < b, "alpha must sort before beta");
    }

    #[test]
    fn utf8_null_terminator_isolates_keys() {
        // "foo" must not be a prefix of "foobar" in key space.
        let short = RowKey::builder().append_str("foo").finish();
        let long = RowKey::builder().append_str("foobar").finish();
        assert_ne!(short, long);
        assert!(short < long, "shorter string with same prefix sorts first");
    }

    #[test]
    fn utf8_empty_string() {
        let empty = RowKey::builder().append_str("").finish();
        assert_eq!(empty.as_bytes(), &[0x00u8]);
    }

    // ── Nullable ─────────────────────────────────────────────────────────────

    #[test]
    fn nullable_null_sorts_before_non_null() {
        let null_key = RowKey::builder()
            .append_nullable(None::<i64>, |b, v| b.append_i64(v))
            .finish();
        let some_key = RowKey::builder()
            .append_nullable(Some(i64::MIN), |b, v| b.append_i64(v))
            .finish();
        assert!(null_key < some_key, "NULL must sort before any non-NULL value");
    }

    #[test]
    fn nullable_non_null_values_sort_correctly() {
        let k1 = RowKey::builder()
            .append_nullable(Some(-5i64), |b, v| b.append_i64(v))
            .finish();
        let k2 = RowKey::builder()
            .append_nullable(Some(5i64), |b, v| b.append_i64(v))
            .finish();
        assert!(k1 < k2);
    }

    #[test]
    fn nullable_utf8_null_sorts_first() {
        let null_key = RowKey::builder()
            .append_nullable(None::<&str>, |b, v| b.append_str(v))
            .finish();
        let some_key = RowKey::builder()
            .append_nullable(Some(""), |b, v| b.append_str(v))
            .finish();
        assert!(null_key < some_key);
    }

    // ── Composite ────────────────────────────────────────────────────────────

    #[test]
    fn composite_key_sort_order() {
        // (project_id: i64, seq: u64)
        let k1 = RowKey::builder().append_i64(1).append_u64(100).finish();
        let k2 = RowKey::builder().append_i64(1).append_u64(200).finish();
        let k3 = RowKey::builder().append_i64(2).append_u64(0).finish();
        assert!(k1 < k2);
        assert!(k2 < k3);
    }
}
