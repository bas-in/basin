//! `basin-placement` — `(tenant_id, partition_key) → shard_owner_node` map.
//!
//! Phase 3 work. Stateless service backed by a strongly-consistent store
//! (etcd, FoundationDB, or Postgres advisory locks). Consistent hashing with
//! virtual nodes; per-tenant overrides for big customers; fast failover.

#![forbid(unsafe_code)]
