//! Phase 5.21 — Logical CDC / pgoutput replication infrastructure.
//!
//! ## Modules
//!
//! - `pgoutput` (5.21.C): encodes Basin's `ChangeEvent` stream into the
//!   PostgreSQL pgoutput logical replication binary message format
//!   (Begin/Relation/Insert/Update/Delete/Commit messages as text tags for
//!   the SQL-facing `pg_logical_slot_get_changes` function).
//! - `lsn` (5.21.D): WAL LSN management — maps commit sequences to LSN
//!   values, handles ack and replay position tracking.
//! - `slot_udf` (5.21.B/C): SQL function implementations for
//!   `pg_create_logical_replication_slot`, `pg_drop_replication_slot`,
//!   `pg_current_wal_lsn`, and `pg_logical_slot_get_changes`.

pub(crate) mod lsn;
pub(crate) mod pgoutput;
pub(crate) mod publication_ddl;
pub(crate) mod slot_udf;
