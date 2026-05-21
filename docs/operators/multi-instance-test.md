# Multi-Instance Lease / Handoff / Budget Smoke Harness

**Task:** #51  
**Test file:** `tests/integration/tests/multi_instance_smoke.rs`  
**ADR:** 0023 (lease-based ownership + partition-level routing + heartbeat budgets)

---

## What it proves

| Test fn | Assertion | ADR clause |
|---------|-----------|------------|
| `a_lease_assignment_distributes_across_replicas` | With N replicas and P > N partitions, no single replica owns all partitions. | ADR 0023 §lease-distribution |
| `b_voluntary_handoff_within_adr_0023_budget` | Voluntary `yield_partition` (flush + epoch transfer + WAL marker) completes under 500 ms p99 in the small-memtable case. | ADR 0023 §6.X.C |
| `c_per_project_cap_is_slice_not_n_times_per_replica` | Budget coordinator gives each partition `floor(project_cap / N)`, not `project_cap` — the aggregate across all partitions stays ≤ project cap. | ADR 0023 §heartbeat-budgets |
| `d_killed_replica_leases_reassigned_no_data_loss` | After a replica is dropped without releasing its lease (SIGKILL model), surviving replicas pick up the orphaned partition after TTL elapses. WAL replay surfaces the dead replica's committed writes on the new leaseholder (zero data loss). | ADR 0023 §replica-loss |
| `e_concurrent_multi_shard_cross_project_isolation` | Multiple shards sharing one Storage+Catalog+WAL have zero cross-project row leakage under concurrent write contention. | Multi-tenant wedge invariant |
| `f_budget_resliced_when_partition_count_grows` | When a new partition joins the coordinator, all existing partitions are re-sliced so the total stays ≤ project cap. | ADR 0023 §dynamic-reslicing |

---

## How to run

```bash
# Full harness, all assertions, with stdout output:
cargo test --test multi_instance_smoke -- --nocapture

# Single assertion:
cargo test --test multi_instance_smoke a_lease_assignment -- --nocapture
cargo test --test multi_instance_smoke b_voluntary_handoff -- --nocapture
cargo test --test multi_instance_smoke c_per_project_cap -- --nocapture
cargo test --test multi_instance_smoke d_killed_replica -- --nocapture
cargo test --test multi_instance_smoke e_concurrent_multi -- --nocapture
cargo test --test multi_instance_smoke f_budget_resliced -- --nocapture
```

No external services are required. All six assertions are fully self-contained (in-process).

---

## Implementation approach (in-process multi-Shard simulation)

The task brief notes that spawning N external `basin-server` processes is the most faithful simulation. However, it also offers a soft-cap fallback: use N in-process `Shard` instances sharing one `InMemoryCatalog` (as both `Catalog` and `LeaseRegistry`) and one shared `LocalWal`.

This harness takes the soft-cap path because:

1. **The load-bearing invariants live in the right layer.** The lease registry, WAL-fencing, handoff protocol, and budget coordinator are all fully exercised at the `basin-shard` / `basin-catalog` / `basin-wal` level — the same layer production multi-process deployments exercise. The only difference is the transport: `InMemoryCatalog` vs `PostgresCatalog` (same trait, same call sites).

2. **Speed and hermeticity.** Each test completes in < 2 s on a developer laptop and requires no network stack, no binary build, no port allocation beyond loopback.

3. **Established pattern.** `tests/integration/tests/lease_failure_paths.rs` (Phase 6.X.E) and `crates/basin-router/src/test_cluster.rs` both use this pattern. The harness is explicitly modelled on them.

### What a real multi-process variant would add

A future `multi_instance_external.rs` variant could:

1. Build `basin-server` with `cargo build -p basin-server --no-default-features` (minimal build, no auth/REST).
2. Spawn N processes via `std::process::Command` with:
   - `BASIN_CATALOG=<shared-postgres-url>`
   - `BASIN_SHARD_ENABLED=1`
   - `BASIN_WAL_DIR=<shared-tmpdir>`
   - `BASIN_BIND=127.0.0.1:0` (or fixed ephemeral ports)
   - `BASIN_DATA_DIR=<shared-tmpdir>`
3. Poll the shared `partition_leases` table to confirm all N processes have registered heartbeats.
4. Drive pgwire writes via `tokio-postgres` against each process's listen address.
5. Kill one process (`child.kill()`) and assert takeover via the catalog table.

Gate that test on `BASIN_AUTH_TEST_POSTGRES_URL` being set (use the existing `test_config::BasinTestConfig` loader) and print a skip message when absent.

---

## Key timings (ADR 0023 budgets)

| Budget | Value | Where asserted |
|--------|-------|----------------|
| Voluntary handoff p99 stall | < 500 ms | `b_voluntary_handoff_within_adr_0023_budget` |
| Unplanned failover window | `TTL + grace` = 200 ms + 250 ms = 450 ms | `d_killed_replica_leases_reassigned_no_data_loss` |

The SHORT_TTL used by the harness (200 ms) is intentionally faster than production defaults (15 s) to keep the test runtime under 2 s. Production operators should tune `BASIN_LEASE_TTL_SECS` and `BASIN_LEASE_RENEW_SECS` per ADR 0023's guidance.

---

## Environment variables (production defaults, not overridden by this test)

| Variable | Default | Description |
|----------|---------|-------------|
| `BASIN_LEASE_TTL_SECS` | 15 | Lease TTL; unplanned failover window = TTL + coordinator poll interval |
| `BASIN_LEASE_RENEW_SECS` | 5 | Heartbeat cadence; over-cap window ≤ this |
| `BASIN_SHARD_ENABLED` | 0 | Set to 1 to enable WAL-acked write path |
| `BASIN_CATALOG` | `memory` | Set to a `postgres://` URL for durable catalog + lease registry |

---

## Deviation from task brief

The task brief specifies spawning N `basin-server` child processes. This harness uses the in-process multi-Shard simulation (the task's own soft-cap fallback) for the reasons above. The external-process variant is documented in "What a real multi-process variant would add" and is a straightforward extension once a CI environment with a shared Postgres catalog is available.
