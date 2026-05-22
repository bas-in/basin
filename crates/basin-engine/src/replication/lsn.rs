//! Phase 5.21.D — WAL LSN management.
//!
//! Maps Basin's internal commit sequence (a monotonically-increasing `u64`
//! allocated by `Engine::next_tx_id`) to PostgreSQL-style LSN values.
//!
//! ## Design
//!
//! Basin doesn't have a byte-addressed write-ahead log in the PostgreSQL sense,
//! but for logical replication compatibility we synthesise LSN values:
//!
//!   `lsn = base_lsn + (commit_seq << 16)`
//!
//! where `base_lsn = 0x0000_0001_0000_0000` (so LSNs format as `1/XXXXXXXX`)
//! and `commit_seq` is the value from `Engine::next_tx_id()`.
//!
//! This gives monotonically-increasing, unique LSN values that encode the
//! commit sequence and are directly comparable.
//!
//! The frame-level LSN within one transaction is `tx_lsn + frame_offset`
//! where `frame_offset` is the 0-based index of the frame in (B, R, I, C)
//! order.

use basin_catalog::Lsn;

/// Base LSN: `1/00000000` in PG notation (0x0000_0001_0000_0000).
pub const BASE_LSN: Lsn = 0x0000_0001_0000_0000;

/// Synthesise the LSN for a given commit sequence number.
///
/// `commit_seq` is the value returned by `Engine::next_tx_id()`.
/// Each transaction occupies a 64-entry "window" in LSN space so individual
/// frames within the transaction get unique LSNs without a separate counter.
pub fn lsn_for_commit(commit_seq: u64) -> Lsn {
    BASE_LSN.saturating_add(commit_seq.saturating_mul(64))
}

/// LSN for a specific frame within a transaction.
///
/// Frame offsets: 0=BEGIN, 1=RELATION, 2=INSERT/UPDATE/DELETE, 3=COMMIT.
pub fn lsn_for_frame(commit_seq: u64, frame_offset: u64) -> Lsn {
    lsn_for_commit(commit_seq).saturating_add(frame_offset)
}

/// Current WAL LSN — the maximum LSN allocated so far.
///
/// `last_commit_seq` is the most recently allocated tx id (from
/// `Engine::next_tx_id().saturating_sub(1)` since next_tx_id post-increments).
pub fn current_wal_lsn(last_commit_seq: u64) -> Lsn {
    if last_commit_seq == 0 {
        BASE_LSN
    } else {
        lsn_for_commit(last_commit_seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_is_monotone() {
        let l1 = lsn_for_commit(1);
        let l2 = lsn_for_commit(2);
        let l3 = lsn_for_commit(100);
        assert!(l1 < l2);
        assert!(l2 < l3);
    }

    #[test]
    fn frame_lsns_are_monotone_within_tx() {
        let seq = 5;
        let b = lsn_for_frame(seq, 0);
        let r = lsn_for_frame(seq, 1);
        let i = lsn_for_frame(seq, 2);
        let c = lsn_for_frame(seq, 3);
        assert!(b < r);
        assert!(r < i);
        assert!(i < c);
    }

    #[test]
    fn no_overlap_between_transactions() {
        // tx N's COMMIT frame (offset 3) must be < tx N+1's BEGIN (offset 0)
        let commit_n = lsn_for_frame(1, 3);
        let begin_n1 = lsn_for_frame(2, 0);
        assert!(commit_n < begin_n1);
    }

    #[test]
    fn current_wal_lsn_is_at_least_base() {
        assert!(current_wal_lsn(0) >= BASE_LSN);
        assert!(current_wal_lsn(1) > BASE_LSN);
    }
}
