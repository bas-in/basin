//! `basin-hottier` — row-format LSM-style hot-tier memtable for recent writes.
//!
//! # Architecture
//!
//! Per ADR 0016 (HTAP hot-tier architecture), Basin pairs its columnar cold tier
//! (Vortex / Parquet object-store files) with an in-process row-format buffer.
//! Recent writes land here first; background flush drains to the cold tier on
//! size / age / scan-pressure triggers (Phase 5.14.C4).
//!
//! ## Key types
//!
//! | Type | Role |
//! |---|---|
//! | [`row_key::RowKey`] | Big-endian encoded PK bytes; lexicographic order matches PK sort order |
//! | [`memtable::MemTable`] | Per-`(project, table)` `BTreeMap` wrapped in `parking_lot::RwLock` |
//! | [`memtable::MemRowValue`] | `Row { bytes, schema_version }` or `Tombstone` |
//! | [`registry::MemTableRegistry`] | Process-wide `DashMap`; lazy allocation; per-project semaphore |
//! | [`budget::MemTableConfig`] | Per-process memory budget configuration (Phase 5.14.C5) |
//! | [`budget::GlobalPressureScheduler`] | Stateless largest-first flush scheduler when global pressure rises (Phase 5.14.C5) |
//! | [`flush::FlushTask`] | Background memtable → Vortex flush loop (Phase 5.14.C4) |
//!
//! ## Multi-project isolation
//!
//! Cost is O(bytes of actual data) + one `AtomicU64` counter + one
//! `Semaphore` per active project.  10 000 inactive projects cost sub-MB — the
//! registry is just 10 000 empty `DashMap` entries.
//!
//! ## Phase scope
//!
//! Phase 5.14.C1 ships the crate skeleton.  Phase 5.14.C5 adds the memory
//! budget enforcement module (`budget.rs`) and extends `MemTableRegistry` with
//! `try_reserve_bytes` / `release_bytes` / `largest_project`.  Integration with
//! the write path (C2), read-merge path (C3), and flush task (C4) lands in
//! subsequent sub-items.

pub mod budget;
pub mod flush;
pub mod memtable;
pub mod merge;
pub mod registry;
pub mod row_key;

pub use budget::{FlushPolicy, GlobalPressureScheduler, MemTableConfig};
pub use flush::{FlushBackend, FlushTask, FlushTrigger, ImmediateFlushRequest, RowBytes, WrittenFile};
pub use memtable::{MemRowValue, MemTable};
pub use merge::merge_scan;
pub use registry::{
    FlushRequest, MemTableEntry, MemTableRegistry, OverlayMemo, ProjectMemState,
    ReservationOutcome,
};
pub use row_key::{RowKey, RowKeyBuilder};
