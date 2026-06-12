#!/usr/bin/env bash
# benchmark/run/_lib.sh
#
# Shared helpers + the config×category group table. Sourced by _group.sh
# and run_all.sh. No side effects on source.
#
# The group arrays + ALL_GROUPS are consumed by sourcing scripts via
# indirect (`eval`) expansion keyed off group_spec(), so shellcheck's
# "appears unused" (SC2034) is a false positive across this whole file.
# shellcheck disable=SC2034

# Resolve paths whether sourced from run_all.sh or _group.sh.
_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_RUN_DIR="${BENCH_RUN_DIR:-${_LIB_DIR}}"
BENCH_DIR="${BENCH_DIR:-$(cd "${_LIB_DIR}/.." && pwd)}"
BENCH_REPO_ROOT="${BENCH_REPO_ROOT:-$(cd "${BENCH_DIR}/.." && pwd)}"

# ---------------------------------------------------------------------------
# Group definitions.
#
# A group is "<config>_<category>". Each names the integration-test BINARIES
# (`--test <name>`) it owns. Binaries are disjoint across groups within a
# config so groups never write the same JSON.
#
# Parallel-safety notes (verified against the test sources):
#   * Almost every localfs test binds an EPHEMERAL port (127.0.0.1:0) or an
#     in-process engine, so the binaries are mutually parallel-safe.
#   * EXCEPTION: viability_per_project_provisioning binds the FIXED pgwire
#     port 127.0.0.1:5433 and also drives Postgres on :5432. It is pinned
#     into its own forced-serial group (localfs_serial) so it never races
#     another listener for :5433.
#   * compare_* are latency/RSS benchmarks against Postgres on :5432.
#     Running them under CPU contention skews the measured numbers, so the
#     localfs_compare group runs single-threaded and is the only localfs
#     group that touches Postgres heavily — kept off the hot path.
#   * s3_* tests namespace every write under <prefix>/<test>/<ulid>/ and
#     delete it on teardown, so seaweedfs groups use DISJOINT prefixes and
#     are safe to run concurrently against one shared SeaweedFS instance.
# ---------------------------------------------------------------------------

# localfs viability — parallel-safe binaries only (5433-binder excluded).
LOCALFS_VIABILITY=(
  viability_alter_add_column viability_alter_table viability_basin_cron
  viability_basin_net viability_bloom_filter_pruning
  viability_catalog_stats_pruning viability_cluster_by_sql
  viability_cold_start viability_compression_ratio
  viability_continuous_aggregate viability_disk_cache
  viability_idle_project_ram viability_isolation_under_load viability_jsonb
  viability_kms_envelope viability_large_dataset_pointquery
  viability_migration_manager viability_page_cache
  viability_partition_pruning viability_perf_stack viability_pg_trgm
  viability_postgis viability_predicate_pushdown viability_project_deletion
  viability_rls_basic viability_rls_isolation viability_row_group_sizing
  viability_tiered_storage viability_update_delete viability_uuid
)

LOCALFS_SCALING=(
  scaling_compute_shards scaling_concurrency scaling_data_size
  scaling_idle_projects scaling_noisy_neighbor scaling_project_count
  scaling_project_deletion scaling_project_deletion_realistic
)

LOCALFS_COMPARE=(
  compare_backup_cost compare_lifecycle_ops compare_postgres
  compare_server_lifecycle
)

# Forced-serial: fixed-port binder. One binary, runs alone, single-threaded.
LOCALFS_SERIAL=(
  viability_per_project_provisioning
)

# seaweedfs groups — all #[ignore]'d s3_* binaries, run with --ignored.
SEAWEEDFS_VIABILITY=(
  s3_viability_alter_add_column s3_viability_alter_table
  s3_viability_basin_cron s3_viability_bloom_filter_pruning
  s3_viability_cold_start s3_viability_disk_cache
  s3_viability_durable_catalog s3_viability_extended_protocol
  s3_viability_idle_project_ram s3_viability_orm_compat
  s3_viability_page_cache s3_viability_partition_pruning
  s3_viability_rls_basic s3_viability_rls_isolation
  s3_viability_row_group_sizing s3_viability_tiered_storage
  s3_viability_update_delete s3_compression_ratio
  s3_isolation_under_load s3_large_dataset_pointquery
  s3_predicate_pushdown s3_project_deletion s3_shard_insert_path
  s3_vector_search
)

SEAWEEDFS_SCALING=(
  s3_scaling_compute_shards s3_scaling_concurrency s3_scaling_data_size
  s3_scaling_idle_projects s3_scaling_noisy_neighbor s3_scaling_perf_stack
  s3_scaling_project_count s3_scaling_project_deletion_at_scale
  s3_scaling_project_deletion_realistic
)

SEAWEEDFS_COMPARE=(
  s3_compare_backup_cost s3_compare_lifecycle_ops s3_compare_postgres
  s3_compare_postgres_1m s3_compare_server_lifecycle
)

SEAWEEDFS_S3=(
  s3_credentials_smoke
)

# Map a group name to its binary array name + execution attributes.
#   echo "<arrayvar> <config> <threads> <ignored:0|1> <data_dir>"
#
# `threads` = libtest `--test-threads`. Every bench binary has exactly ONE
# `#[test]` fn, so intra-binary test parallelism is a no-op; _group.sh
# already runs binaries one-at-a-time (so a hung binary can be killed
# without taking out its siblings). The REAL parallelism is cross-group,
# managed by run_all.sh's bounded pool. We therefore pin threads=2 (the
# libtest harness itself spawns no extra heavy threads) everywhere —
# cores-based fan-out here would only oversubscribe, since each test's
# tokio runtime is independently sized inside the test.
group_spec() {
  local g="$1"
  case "$g" in
    localfs_viability)    echo "LOCALFS_VIABILITY   localfs 2 0 data" ;;
    localfs_scaling)      echo "LOCALFS_SCALING     localfs 2 0 data" ;;
    localfs_compare)      echo "LOCALFS_COMPARE     localfs 1 0 data" ;;
    localfs_serial)       echo "LOCALFS_SERIAL      localfs 1 0 data" ;;
    seaweedfs_viability)  echo "SEAWEEDFS_VIABILITY seaweedfs 2 1 data_seaweedfs" ;;
    seaweedfs_scaling)    echo "SEAWEEDFS_SCALING   seaweedfs 2 1 data_seaweedfs" ;;
    seaweedfs_compare)    echo "SEAWEEDFS_COMPARE   seaweedfs 1 1 data_seaweedfs" ;;
    seaweedfs_s3)         echo "SEAWEEDFS_S3        seaweedfs 1 1 data_seaweedfs" ;;
    *) echo "" ; return 1 ;;
  esac
}

ALL_GROUPS=(
  localfs_viability localfs_scaling localfs_compare localfs_serial
  seaweedfs_viability seaweedfs_scaling seaweedfs_compare seaweedfs_s3
)
