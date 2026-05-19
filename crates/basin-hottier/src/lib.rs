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
//!
//! ## Multi-tenant isolation
//!
//! Cost is O(bytes of actual data) + one `AtomicU64` counter + one
//! `Semaphore` per active project.  10 000 inactive tenants cost sub-MB — the
//! registry is just 10 000 empty `DashMap` entries.
//!
//! ## Phase scope
//!
//! Phase 5.14.C1 ships this crate skeleton.  Integration with the write path
//! (C2), read-merge path (C3), flush task (C4), and budget enforcement (C5)
//! lands in subsequent sub-items.

pub mod memtable;
pub mod registry;
pub mod row_key;

pub use memtable::{MemRowValue, MemTable};
pub use registry::{MemTableEntry, MemTableRegistry, ProjectMemState};
pub use row_key::{RowKey, RowKeyBuilder};
