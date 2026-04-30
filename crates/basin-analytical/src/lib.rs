//! `basin-analytical` — analytical query path.
//!
//! Phase 5 work. Separate compute pool that reads Iceberg tables in S3
//! directly via DuckDB or DataFusion, bypassing shard owners. Same SQL
//! syntax as the OLTP path; very different perf characteristics.

#![forbid(unsafe_code)]
