//! `basin-catalog` — typed Iceberg-style catalog client used by the rest of
//! Basin.
//!
//! We do **not** implement the catalog server here. In production we plan to
//! deploy [Lakekeeper](https://github.com/lakekeeper/lakekeeper) (or any other
//! Iceberg REST catalog implementation) and talk to it over HTTP. This crate
//! is the Rust-side abstraction the storage, shard, router, and analytical
//! layers depend on, plus an in-memory implementation good enough for unit
//! tests and the Phase 1 integration test.
//!
//! Phase 1 ships three implementations behind a single [`Catalog`] trait:
//!
//! 1. [`InMemoryCatalog`] — async-safe, volatile. Default for unit tests and
//!    the original Phase 1 integration test.
//! 2. [`PostgresCatalog`] — durable, backed by a Postgres schema. The
//!    production path: every operation persists, restart preserves all
//!    table metadata and snapshot history.
//! 3. [`RestCatalog`] — stub locking in the API shape; implemented in a later
//!    phase against a real Iceberg REST catalog (Lakekeeper et al).
//!
//! Project scoping is part of the API: every method takes a [`ProjectId`].
//! There is intentionally no method that lists or enumerates across projects;
//! a future cross-project admin path will be a separate, capability-gated
//! interface.

#![forbid(unsafe_code)]

/// Phase 6.X.D — heartbeat-reconciled per-project budgets (ADR 0023).
pub mod budgets;
mod domains;
mod enums;
mod functions;
mod in_memory;
pub mod info_schema;
mod inbound_webhooks;
/// Phase 6.X.A — partition-lease primitive (ADR 0023 foundation).
pub mod leases;
mod metadata;
mod postgres;
mod procedures;
mod project_storage_config;
mod reactors;
/// Phase 5.18.A — reserved schema enum + back-compat resolution helpers.
pub mod reserved_schema;
mod rest;
mod sequences;
mod snapshot;
/// T-053 — per-project usage counters, hourly snapshot task, and cloud metering
/// HTTP POST to `/internal/v1/metering/counters`.
pub mod usage;
mod views;
/// Phase 5.21.B — logical replication slot registry.
pub mod replication_slots;
/// Phase 5.21.E — publication registry (CREATE/ALTER/DROP PUBLICATION).
pub mod publications;

use std::sync::Arc;

use async_trait::async_trait;
use basin_common::{ChangeOp, ProjectId, QualifiedTableName, Result, SchemaName, TableName};

pub use domains::{DomainDef, DomainError, BASIN_DOMAIN_KEY};
pub use enums::{EnumError, EnumTypeDef, BASIN_ENUM_TYPE_KEY};
pub use reserved_schema::{resolve_qualified, resolve_schema, ReservedSchema};
pub use inbound_webhooks::{InboundWebhookDef, InboundWebhookError};
pub use functions::{
    SqlArgType, SqlFunctionArg, SqlFunctionDef, SqlFunctionLanguage, SqlReturnType,
};
pub use in_memory::InMemoryCatalog;
pub use replication_slots::{lsn_to_pgtext, Lsn, PendingFrame, SlotRegistry, SlotSnapshot};
pub use publications::{Publication, PublicationRegistry, PublicationTable};
pub use budgets::{
    BudgetCoordinator, CapKind, InMemoryBudgetCoordinator, ProjectBudget, SliceBudget,
    SliceBudgetView, SliceGate, UsageDelta, UsageSnapshot, SLICE_UNSET,
};
pub use leases::{
    Lease, LeaseRegistry, DEFAULT_LEASE_RENEW_SECS, DEFAULT_LEASE_TTL_SECS,
};
pub use metadata::{
    CheckConstraint, ColumnStats, CvDef, DataFileRef, ForeignKeyDef, PartitionSpec, Policy,
    PolicyCommand, PromotedJsonbPath, RefAction, S3Config, SecondaryIndex, TableFileFormat,
    TableMetadata, ProjectMetadata, UniqueConstraint,
};
pub use postgres::PostgresCatalog;
pub use procedures::{ProcedureError, SqlProcedureDef};
pub use project_storage_config::ProjectStorageConfig;
pub use reactors::{ReactorDef, ReactorError, ReactorOps};
pub use rest::RestCatalog;
pub use sequences::SequenceDef;
pub use snapshot::{Snapshot, SnapshotId, SnapshotOperation, SnapshotSummary};

/// Result of a [`Catalog::gc_orphaned_files`] call.
///
/// `orphaned_paths` lists every data-file path that had refcount 0 after the
/// GC sweep — i.e. paths that appear in the catalog's recorded history but are
/// not referenced by any live snapshot of any table in the project. These are
/// safe to physically delete from object storage.
///
/// `live_paths` is the complementary set: paths still referenced by at least
/// one live snapshot (or by a forked table that shares the file).  Callers
/// must NOT delete these.
#[derive(Clone, Debug, Default)]
pub struct GcReport {
    /// Paths with refcount 0 — physically deletable.
    pub orphaned_paths: Vec<String>,
    /// Paths still referenced by ≥ 1 live snapshot — must be kept.
    pub live_paths: Vec<String>,
}
pub use views::ViewDef;

/// One row in the project-wide snapshot timeline returned by
/// [`Catalog::list_snapshots_project_wide`]. Carries enough context (table,
/// snapshot id, parent, commit time, summary) to render a unified
/// migration-history view across every table a project owns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectSnapshotEntry {
    pub table: TableName,
    pub snapshot_id: SnapshotId,
    pub parent_id: Option<SnapshotId>,
    pub committed_at: chrono::DateTime<chrono::Utc>,
    pub operation: SnapshotOperation,
    pub summary: SnapshotSummary,
}

/// Structured delta for [`Catalog::diff_snapshots`]. `per_table` groups every
/// snapshot committed in the requested window by its table; `created_in_window`
/// flags tables whose Genesis snapshot itself falls inside the window so the UI
/// can highlight "new tables" separately from "new commits to existing tables".
#[derive(Clone, Debug)]
pub struct ProjectSnapshotDiff {
    /// New snapshots in [from, to] grouped by table.
    pub per_table: std::collections::BTreeMap<TableName, Vec<ProjectSnapshotEntry>>,
    /// Tables that gained their first snapshot in the window
    /// (i.e. `created_at` falls in [from, to]).
    pub created_in_window: Vec<TableName>,
}

/// Iceberg-style catalog client. Every operation is project-scoped.
///
/// Implementations must be `Send + Sync` so a single instance can be cloned
/// (or wrapped in [`std::sync::Arc`]) and shared across the shard owner,
/// the router, and the analytical pool.
#[async_trait]
pub trait Catalog: Send + Sync {
    /// Returns a monotonically increasing counter that is bumped on every
    /// catalog mutation. Session caches use this to validate cached metadata
    /// without round-tripping to the catalog backend: if the epoch observed
    /// at cache-fill time equals the current epoch, the entry is still fresh.
    /// The default implementation returns 0 (always-stale semantics, which
    /// degrades gracefully to a cache miss on every read). Implementations
    /// that want cache-hit performance override this with an `AtomicU64`.
    fn epoch(&self) -> u64 {
        0
    }

    /// Idempotently create the namespace for `project`. Catalog implementations
    /// model a project as an Iceberg namespace (e.g. `projects.<ulid>`).
    async fn create_namespace(&self, project: &ProjectId) -> Result<()>;

    /// Create a new empty table. Returns the freshly minted metadata, including
    /// the genesis snapshot (id 0, no data files).
    async fn create_table(
        &self,
        project: &ProjectId,
        table: &TableName,
        schema: &arrow_schema::Schema,
    ) -> Result<TableMetadata>;

    /// Load the current metadata for `(project, table)`. Returns
    /// [`basin_common::BasinError::NotFound`] if the table does not exist.
    async fn load_table(&self, project: &ProjectId, table: &TableName) -> Result<TableMetadata>;

    /// Load metadata for `(project, table)` as it appeared at a historical
    /// `snapshot_id`, for transaction snapshot-stable reads (REPEATABLE READ).
    ///
    /// The returned [`TableMetadata`] is the current metadata with
    /// `current_snapshot` rewound to `snapshot_id`, so that
    /// [`TableMetadata::live_data_files`] (which the engine's read path calls)
    /// reconstructs exactly the file set that was live at `snapshot_id`. The
    /// snapshot *chain* itself is left intact (it always contains a superset),
    /// because `live_data_files` already caps replay at `current_snapshot`.
    ///
    /// ## Default implementation
    ///
    /// The default loads current metadata and, if `snapshot_id` is present in
    /// the retained chain, rewinds `current_snapshot` to it. Schema is taken
    /// from the *current* metadata: Basin's snapshot model versions only the
    /// data-file set, not the schema, so an in-flight `ALTER TABLE` is not
    /// time-travelled (acceptable — DDL inside a concurrent writer's tx is out
    /// of scope for read snapshot stability, and matches the fact that the
    /// snapshot chain carries no per-snapshot schema).
    ///
    /// This works correctly for every backend that returns the full snapshot
    /// chain from [`load_table`](Catalog::load_table) — which is both
    /// `InMemoryCatalog` and `PostgresCatalog`. If `snapshot_id` is **not** in
    /// the retained chain (e.g. it was pruned by a `rollback_to_snapshot`, or
    /// the backend — like the `RestCatalog` stub — does not retain history),
    /// the call returns [`basin_common::BasinError::FeatureNotSupported`] so
    /// the caller can fall back to a current read rather than silently serving
    /// the wrong point-in-time.
    async fn load_table_at_snapshot(
        &self,
        project: &ProjectId,
        table: &TableName,
        snapshot_id: SnapshotId,
    ) -> Result<TableMetadata> {
        let mut meta = self.load_table(project, table).await?;
        if snapshot_id == meta.current_snapshot {
            return Ok(meta);
        }
        if meta.snapshots.iter().any(|s| s.id == snapshot_id) {
            meta.current_snapshot = snapshot_id;
            Ok(meta)
        } else {
            Err(basin_common::BasinError::FeatureNotSupported(format!(
                "load_table_at_snapshot: snapshot {snapshot_id} for table \
                 {table} is not retained in the catalog snapshot chain"
            )))
        }
    }

    /// Drop the table. Does not delete the underlying object-store data; that
    /// is the storage layer's responsibility (and may be deferred for
    /// time-travel / point-in-time-restore in Phase 6).
    async fn drop_table(&self, project: &ProjectId, table: &TableName) -> Result<()>;

    /// Rename `(project, old)` to `(project, new)`. Used by
    /// `ALTER TABLE … RENAME TO`. The table's identity (snapshot history,
    /// constraints, partition spec, etc.) is preserved — only the key
    /// changes. Returns [`basin_common::BasinError::NotFound`] if `old`
    /// does not exist for `project`, [`basin_common::BasinError::Catalog`]
    /// if `new` is already taken. Default impl returns
    /// `Internal("not implemented")` so the stub `RestCatalog` stays
    /// buildable; in-memory and Postgres backends override.
    async fn rename_table(
        &self,
        project: &ProjectId,
        old: &TableName,
        new: &TableName,
    ) -> Result<()> {
        let _ = (project, old, new);
        Err(basin_common::BasinError::Internal(
            "rename_table not implemented for this catalog backend".into(),
        ))
    }

    /// Tables visible to `project`. By construction this is *only* `project`'s
    /// tables — there is no cross-project variant in this trait.
    async fn list_tables(&self, project: &ProjectId) -> Result<Vec<TableName>>;

    /// Drop every catalog row owned by `project`: tables, snapshots, and the
    /// namespace itself. Does **not** delete the underlying object-store
    /// bytes; that is the storage layer's job. The default implementation
    /// iterates `list_tables` and calls `drop_table` on each; backends that
    /// can issue a single statement (Postgres `DELETE … WHERE project_id = …`)
    /// override for one round-trip instead of N.
    ///
    /// Idempotent: calling on a project with no tables / no namespace row is
    /// a no-op (returns Ok). The engine's project-deletion path calls this
    /// last, after the storage bytes are gone.
    async fn drop_namespace(&self, project: &ProjectId) -> Result<()> {
        let tables = self.list_tables(project).await?;
        for table in tables {
            // Drop best-effort: a NotFound from a racing dropper is fine.
            match self.drop_table(project, &table).await {
                Ok(()) => {}
                Err(basin_common::BasinError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Enumerate every data file currently catalogued for `project`. The
    /// returned set is the union of `DataFileRef`s recorded across every
    /// snapshot of every table the project owns; in the snapshot-delta model
    /// this is a *superset* of the live file set (paths swapped out by
    /// `replace_data_files` may still appear). For deletion that's fine —
    /// `DeleteObjects` is idempotent on already-deleted keys. Higher-level
    /// readers should keep using `list_data_files`, which goes through the
    /// LIST RPC and sees only physically-present files.
    ///
    /// Used by `Storage::delete_project` to skip the LIST RPC for the bulk
    /// of the bytes; a follow-up LIST mops up files the catalog doesn't
    /// know about (HNSW segments, orphans).
    ///
    /// Default impl: iterate `list_tables` then `load_table` per table.
    /// Postgres can do this in a single SELECT joining the snapshots table.
    async fn list_project_data_files(&self, project: &ProjectId) -> Result<Vec<DataFileRef>> {
        let tables = self.list_tables(project).await?;
        let mut out: Vec<DataFileRef> = Vec::new();
        for table in tables {
            match self.load_table(project, &table).await {
                Ok(meta) => {
                    for snap in &meta.snapshots {
                        out.extend(snap.data_files.iter().cloned());
                    }
                }
                Err(basin_common::BasinError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    /// Persist per-project metadata (BYO-bucket config, etc.) for `project`.
    /// Idempotent: a second call with the same project replaces the prior
    /// value. `None` on `ProjectMetadata::byo_bucket` restores the default
    /// shared-bucket behaviour. Default impl returns
    /// `Internal("not implemented")` so non-default backends opt in
    /// explicitly.
    async fn set_project_metadata(
        &self,
        project: &ProjectId,
        meta: ProjectMetadata,
    ) -> Result<()> {
        let _ = (project, meta);
        Err(basin_common::BasinError::Internal(
            "set_project_metadata not implemented for this catalog backend".into(),
        ))
    }

    /// Look up the persisted [`ProjectMetadata`] for `project`. Returns
    /// `ProjectMetadata::default()` (i.e. `byo_bucket = None`) when no
    /// metadata has been set — meaning "use Basin's shared bucket". Default
    /// impl: returns `Ok(ProjectMetadata::default())`.
    async fn get_project_metadata(&self, project: &ProjectId) -> Result<ProjectMetadata> {
        let _ = project;
        Ok(ProjectMetadata::default())
    }

    /// Persist the per-`(project, partition)` compaction watermark: the highest
    /// WAL LSN whose tail batch has been compacted into a catalog-committed
    /// Parquet/Vortex file. The shard compactor advances this right after the
    /// file commit lands and *before* it truncates the WAL, so a crash in the
    /// commit→truncate window leaves the watermark recording exactly what was
    /// already made durable.
    ///
    /// ## Durability bug this fixes
    ///
    /// `compact_one` commits the cold file, then truncates the WAL. A crash
    /// *between* those two steps leaves the WAL entries intact, so cold-start
    /// replay re-appends them to the tail and the next compaction commits a
    /// **second** Parquet file for the same rows — duplicating every row in the
    /// cold tier and over-counting on read. Persisting the watermark and
    /// replaying only entries strictly above it makes replay-after-crash skip
    /// the already-committed batch.
    ///
    /// Monotonicity: implementations must never lower a stored watermark (use a
    /// `GREATEST(existing, new)` upsert), so an out-of-order or retried call
    /// cannot rewind the replay floor.
    ///
    /// ## Default implementation
    ///
    /// Returns `Ok(())` (a durable no-op). Backends that do not persist a
    /// watermark fall back to the legacy "replay from `Lsn::ZERO`" behavior,
    /// which is correct for in-process / ephemeral catalogs where a process
    /// restart also loses the WAL tail (no durable WAL ⇒ no replay ⇒ no
    /// duplication). Production durable backends (Postgres) override this.
    async fn set_compaction_watermark(
        &self,
        project: &ProjectId,
        partition_id: &str,
        watermark_lsn: u64,
    ) -> Result<()> {
        let _ = (project, partition_id, watermark_lsn);
        Ok(())
    }

    /// Read the persisted compaction watermark for `(project, partition)`.
    /// Returns `None` when no watermark has ever been recorded — which a
    /// **legacy catalog** (one written before this column/table existed) is
    /// indistinguishable from. The caller MUST treat `None` as "replay from
    /// `Lsn::ZERO`" so legacy catalogs keep working unchanged.
    ///
    /// ## Default implementation
    ///
    /// Returns `Ok(None)` so non-overriding backends always take the legacy
    /// replay-from-zero path.
    async fn get_compaction_watermark(
        &self,
        project: &ProjectId,
        partition_id: &str,
    ) -> Result<Option<u64>> {
        let _ = (project, partition_id);
        Ok(None)
    }

    /// Atomic commit: append `files` to the table, producing a new snapshot.
    ///
    /// Optimistic concurrency: the caller passes the snapshot id it last saw.
    /// If the table has advanced since then, the call returns
    /// [`basin_common::BasinError::CommitConflict`] and the caller must
    /// reload and retry.
    async fn append_data_files(
        &self,
        project: &ProjectId,
        table: &TableName,
        expected_snapshot: SnapshotId,
        files: Vec<DataFileRef>,
    ) -> Result<TableMetadata>;

    /// Atomically replace the data file set: the resulting snapshot's data
    /// files = (parent.data_files − removed_paths) ∪ added_files.
    ///
    /// Used by copy-on-write UPDATE / DELETE: the engine reads the affected
    /// data files, writes their replacements, and commits the swap in one
    /// catalog round trip. Optimistic concurrency on `expected_snapshot` —
    /// same contract as [`append_data_files`](Catalog::append_data_files).
    /// Returns [`basin_common::BasinError::CommitConflict`] if the snapshot
    /// has advanced. If `removed_paths` references a path that isn't in the
    /// parent snapshot's data file set, the call returns
    /// [`basin_common::BasinError::Catalog`] (caller bug — the path list
    /// must come from a prior `load_table`).
    async fn replace_data_files(
        &self,
        project: &ProjectId,
        table: &TableName,
        expected_snapshot: SnapshotId,
        removed_paths: Vec<String>,
        added_files: Vec<DataFileRef>,
    ) -> Result<TableMetadata>;

    /// Snapshots in commit order (oldest first). Used by point-in-time-restore
    /// and the analytical reader.
    async fn list_snapshots(&self, project: &ProjectId, table: &TableName)
        -> Result<Vec<Snapshot>>;

    /// Copy-on-write table fork. Creates `dst_table` for `project` as a
    /// clone of `src_table`'s current state: same schema, same snapshot
    /// history (same `DataFileRef` paths), same partition / RLS / tier /
    /// bloom / row-group / CV settings. From this point forward the two
    /// tables have independent lineages — commits to `src_table` don't
    /// affect `dst_table` and vice versa.
    ///
    /// **Data files are shared, not copied.** Both tables' snapshot
    /// histories reference the same Parquet files on object storage.
    /// Reads from either side go to the same bytes; this is the whole
    /// point of "copy-on-write". Subsequent writes (and copy-on-write
    /// `replace_data_files`) produce new files referenced by only the
    /// writing side.
    ///
    /// Project deletion is the safety boundary: `Storage::delete_project`
    /// scans the catalog's full file list, so deleting a project
    /// correctly removes only files referenced by that project. The
    /// shared-file design assumes both fork and source live under the
    /// same project; cross-project forking is a v0.2 extension that needs
    /// a refcount-aware GC story.
    ///
    /// Errors:
    /// - [`basin_common::BasinError::NotFound`] when `src_table`
    ///   doesn't exist.
    /// - [`basin_common::BasinError::Catalog`] when `dst_table` already
    ///   exists (no implicit overwrite).
    /// - Default impl returns [`basin_common::BasinError::Internal`]
    ///   (`"fork_table not implemented"`) so backends opt in
    ///   explicitly. The in-memory and Postgres backends override.
    async fn fork_table(
        &self,
        project: &ProjectId,
        src_table: &TableName,
        dst_table: &TableName,
    ) -> Result<TableMetadata> {
        let _ = (project, src_table, dst_table);
        Err(basin_common::BasinError::Internal(
            "fork_table not implemented for this catalog backend".into(),
        ))
    }

    /// Point-in-time restore: rewind `(project, table)` to `snapshot_id`,
    /// discarding any snapshots with `id > snapshot_id`. The current
    /// snapshot pointer becomes `snapshot_id`; subsequent commits chain
    /// off it. Returns the updated [`TableMetadata`].
    ///
    /// **Data files written between `snapshot_id` and the previous head
    /// are not deleted by this method** — they become catalog-orphans.
    /// The compactor's orphan sweep (or `Storage::delete_project` on
    /// project teardown) is the GC path. Reads after rollback see only
    /// files referenced by snapshots `≤ snapshot_id`, so the orphans are
    /// invisible to queries.
    ///
    /// v0.1 semantics: this is a destructive *truncate*, not a
    /// branch-from-snapshot. v0.2 will add a non-destructive variant
    /// that keeps post-rollback snapshots in a sibling lineage so a
    /// subsequent rollback can pick either side.
    ///
    /// Errors:
    /// - [`basin_common::BasinError::NotFound`] when `snapshot_id` isn't
    ///   in the table's history.
    /// - Default impl returns [`basin_common::BasinError::Internal`]
    ///   (`"rollback_to_snapshot not implemented"`) so backends opt in
    ///   explicitly. The in-memory and Postgres backends override.
    async fn rollback_to_snapshot(
        &self,
        project: &ProjectId,
        table: &TableName,
        snapshot_id: SnapshotId,
    ) -> Result<TableMetadata> {
        let _ = (project, table, snapshot_id);
        Err(basin_common::BasinError::Internal(
            "rollback_to_snapshot not implemented for this catalog backend".into(),
        ))
    }

    /// Project-wide snapshot listing: every (table, snapshot_id, committed_at,
    /// summary) row across all tables this project owns, sorted by
    /// `committed_at` ascending. Used by the migration UI / `basinctl
    /// migrations list` to show one timeline for the whole project.
    ///
    /// Default impl iterates `list_tables` then `list_snapshots` per table —
    /// preserves existing semantics and keeps `RestCatalog` (stub) buildable.
    /// Postgres backend overrides to a single `SELECT … ORDER BY committed_at`.
    async fn list_snapshots_project_wide(
        &self,
        project: &ProjectId,
    ) -> Result<Vec<ProjectSnapshotEntry>> {
        let tables = self.list_tables(project).await?;
        let mut out: Vec<ProjectSnapshotEntry> = Vec::new();
        for table in tables {
            // Skip tables that vanished mid-iteration; project-scoped racing
            // drops shouldn't poison the listing.
            match self.list_snapshots(project, &table).await {
                Ok(snaps) => {
                    for s in snaps {
                        out.push(ProjectSnapshotEntry {
                            table: table.clone(),
                            snapshot_id: s.id,
                            parent_id: s.parent,
                            committed_at: s.committed_at,
                            operation: s.summary.operation,
                            summary: s.summary,
                        });
                    }
                }
                Err(basin_common::BasinError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        // Tie-breakers keep the order stable so UI diffs are deterministic.
        out.sort_by(|a, b| {
            a.committed_at
                .cmp(&b.committed_at)
                .then_with(|| a.table.cmp(&b.table))
                .then_with(|| a.snapshot_id.cmp(&b.snapshot_id))
        });
        Ok(out)
    }

    /// Diff two project-wide checkpoints by `committed_at`. Returns the
    /// per-table delta: which tables gained snapshots, which were created /
    /// dropped, in commit order. Used by the migration UI's "what changed
    /// between yesterday and now" view.
    ///
    /// Default impl: collect every snapshot in [from, to] via
    /// `list_snapshots_project_wide`, group by table, return a structured
    /// diff.
    async fn diff_snapshots(
        &self,
        project: &ProjectId,
        from: chrono::DateTime<chrono::Utc>,
        to: chrono::DateTime<chrono::Utc>,
    ) -> Result<ProjectSnapshotDiff> {
        let all = self.list_snapshots_project_wide(project).await?;
        let mut per_table: std::collections::BTreeMap<TableName, Vec<ProjectSnapshotEntry>> =
            std::collections::BTreeMap::new();
        let mut created_in_window: Vec<TableName> = Vec::new();
        let mut seen_genesis_in_window: std::collections::BTreeSet<TableName> =
            std::collections::BTreeSet::new();
        for entry in all.into_iter() {
            if entry.committed_at < from || entry.committed_at > to {
                continue;
            }
            // Genesis-in-window flags a brand-new table; SnapshotId::GENESIS
            // is the only snapshot with no parent, by construction.
            if entry.parent_id.is_none()
                && entry.snapshot_id == SnapshotId::GENESIS
                && !seen_genesis_in_window.contains(&entry.table)
            {
                created_in_window.push(entry.table.clone());
                seen_genesis_in_window.insert(entry.table.clone());
            }
            per_table
                .entry(entry.table.clone())
                .or_default()
                .push(entry);
        }
        Ok(ProjectSnapshotDiff {
            per_table,
            created_in_window,
        })
    }

    /// Project-wide PITR: rewind every table to the snapshot whose
    /// `committed_at <= as_of`. Tables created after `as_of` are left alone
    /// (they have no pre-`as_of` state to rewind to; an explicit drop is the
    /// caller's responsibility). Returns the list of (table, new_head)
    /// pairs.
    ///
    /// Default impl: list every table; for each, find the latest snapshot
    /// with `committed_at <= as_of` via `list_snapshots`; call the existing
    /// `rollback_to_snapshot`. Best-effort — if one table fails, the others
    /// keep their rolled-back state and the caller gets the per-table
    /// error list.
    async fn rollback_to_snapshot_project_wide(
        &self,
        project: &ProjectId,
        as_of: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<(TableName, SnapshotId)>> {
        let tables = self.list_tables(project).await?;
        let mut out: Vec<(TableName, SnapshotId)> = Vec::new();
        for table in tables {
            let snaps = match self.list_snapshots(project, &table).await {
                Ok(s) => s,
                Err(basin_common::BasinError::NotFound(_)) => continue,
                Err(e) => return Err(e),
            };
            // Latest snapshot at-or-before as_of; tables with no qualifying
            // snapshot were created after the cutoff and are skipped.
            let target = snaps
                .iter()
                .filter(|s| s.committed_at <= as_of)
                .max_by_key(|s| s.committed_at);
            let Some(target) = target else { continue };
            let target_id = target.id;
            let meta = self
                .rollback_to_snapshot(project, &table, target_id)
                .await?;
            out.push((table, meta.current_snapshot));
        }
        Ok(out)
    }

    /// Set the partitioning declared for `(project, table)`. The engine calls
    /// this after a `CREATE TABLE … PARTITION BY …` so subsequent INSERTs
    /// see the spec via [`load_table`](Catalog::load_table).
    ///
    /// Idempotent: calling repeatedly with the same spec is a no-op. Calling
    /// with a different spec on an existing table replaces the spec; the
    /// engine itself rejects mid-life partition changes — this method is the
    /// catalog's "store whatever you're told" trapdoor.
    async fn set_partition_spec(
        &self,
        project: &ProjectId,
        table: &TableName,
        spec: PartitionSpec,
    ) -> Result<()>;

    /// Replace the full RLS state for `(project, table)`. The engine collapses
    /// `ALTER TABLE ... ENABLE/DISABLE ROW LEVEL SECURITY` and every
    /// `CREATE/ALTER/DROP POLICY` into one of these calls so the catalog
    /// has a single, atomic write surface for the policy set. Default impl
    /// is a no-op so existing catalog implementations stay buildable; the
    /// in-memory and Postgres backends override this.
    async fn set_rls_state(
        &self,
        project: &ProjectId,
        table: &TableName,
        rls_enabled: bool,
        policies: Vec<Policy>,
    ) -> Result<()> {
        let _ = (project, table, rls_enabled, policies);
        Ok(())
    }

    /// Set the tiered-storage age policy for `(project, table)`. The compactor
    /// reads this on its periodic sweep. Passing `cold_after_seconds = None`
    /// disables the policy (the default). Default impl is a no-op so the
    /// stub `RestCatalog` and any future backends stay buildable; the
    /// in-memory and Postgres implementations override this.
    async fn set_tier_policy(
        &self,
        project: &ProjectId,
        table: &TableName,
        cold_after_seconds: Option<u64>,
        cold_age_column: Option<String>,
    ) -> Result<()> {
        let _ = (project, table, cold_after_seconds, cold_age_column);
        Ok(())
    }

    /// Configure the per-table bloom-filter column set. The writer consults
    /// this on each `write_batch` call to decide which columns get a native
    /// Parquet bloom filter section in the file footer; the reader uses the
    /// same metadata implicitly (no extra catalog round-trip — the bloom
    /// filter is in the Parquet file). Passing an empty `columns` vector
    /// disables bloom filters for the table (the default for back-compat).
    /// Default impl is a no-op so the stub `RestCatalog` and any future
    /// backends stay buildable; the in-memory and Postgres implementations
    /// override this.
    async fn set_bloom_filter_columns(
        &self,
        project: &ProjectId,
        table: &TableName,
        columns: Vec<String>,
    ) -> Result<()> {
        let _ = (project, table, columns);
        Ok(())
    }

    /// Override the writer's `max_row_group_size` for `(project, table)`.
    /// `Some(n)` makes the next write group rows in chunks of `n`;
    /// `None` (the default) restores the writer-global default. The
    /// engine consults this on every INSERT and passes the value via
    /// [`basin_storage::WriteOptions::max_row_group_size`]. Default impl
    /// is a no-op so the stub `RestCatalog` and any future backends stay
    /// buildable; the in-memory and Postgres implementations override
    /// this. See `tests/integration/tests/viability_row_group_sizing.rs`.
    async fn set_row_group_rows(
        &self,
        project: &ProjectId,
        table: &TableName,
        rows: Option<usize>,
    ) -> Result<()> {
        let _ = (project, table, rows);
        Ok(())
    }

    /// Replace the table's Arrow [`Schema`]. Used by `ALTER TABLE … ADD
    /// COLUMN` (and any future evolutionary DDL — DROP COLUMN, type
    /// widening, etc.). The catalog records the new schema; existing data
    /// files keep their original Parquet schemas and the reader handles the
    /// resulting null-padded columns transparently via Arrow's schema
    /// projection.
    ///
    /// The catalog is intentionally permissive — it stores whatever the
    /// engine hands it and trusts the engine to validate that the new
    /// schema is a *legal* evolution (additive, types compatible). Default
    /// impl is a no-op so the stub `RestCatalog` and any future backends
    /// stay buildable.
    async fn set_schema(
        &self,
        project: &ProjectId,
        table: &TableName,
        schema: arrow_schema::Schema,
    ) -> Result<()> {
        let _ = (project, table, schema);
        Ok(())
    }

    /// Replace the continuous-aggregate definition (or clear it with `None`)
    /// for `(project, table)`. The `basin-cv` refresher writes back a fresh
    /// `CvDef` each tick (with updated `last_refreshed_at_unix_ms` /
    /// `last_bucket_max_unix_ms`). Default impl is a no-op so the stub
    /// `RestCatalog` and any future backend stay buildable; the in-memory
    /// and Postgres backends override this. See the `basin-cv` crate docs
    /// for the refresh / read-path semantics.
    async fn set_continuous_aggregate(
        &self,
        project: &ProjectId,
        table: &TableName,
        def: Option<metadata::CvDef>,
    ) -> Result<()> {
        let _ = (project, table, def);
        Ok(())
    }

    /// Phase 5.7 B2: replace the cluster-column list for `(project, table)`.
    /// Empty `columns` clears the cluster spec. The writer reads this on
    /// every put and physically sorts the batch by these columns before
    /// flushing to Parquet, so combined with A3 bloom filters and A4
    /// catalog stats, point queries on cluster columns prune to one file
    /// in the common case. Default impl is a no-op so the stub
    /// `RestCatalog` and any future backend stay buildable; the in-memory
    /// and Postgres backends override this.
    async fn set_cluster_columns(
        &self,
        project: &ProjectId,
        table: &TableName,
        columns: Vec<String>,
    ) -> Result<()> {
        let _ = (project, table, columns);
        Ok(())
    }

    /// #161: set the on-disk data-file format for `(project, table)`.
    /// Selected at CREATE TABLE via `WITH (basin.file_format = 'vortex')`.
    /// The writer reads `TableMetadata::file_format` to choose the encoder
    /// (Parquet vs Vortex) and the read path picks the DataFusion
    /// `FileFormat` per table. Default impl is a no-op so the stub
    /// `RestCatalog` and any future backend stay buildable; the in-memory
    /// and Postgres backends override this.
    async fn set_file_format(
        &self,
        project: &ProjectId,
        table: &TableName,
        format: crate::metadata::TableFileFormat,
    ) -> Result<()> {
        let _ = (project, table, format);
        Ok(())
    }

    /// W3-R3: declare the user-asserted global sort order for `(project,
    /// table)`. Set at CREATE TABLE time via `WITH (basin.sort_by = '...')`.
    /// When `columns` is non-empty, every data file is sorted ascending on
    /// those columns AND cross-file ranges are non-overlapping — the user
    /// asserts monotone-append. The session reads this back and wires
    /// `ListingOptions::with_file_sort_order` so DataFusion can skip
    /// `SortExec` insertion before window aggregations.
    ///
    /// Default impl is a no-op so the stub `RestCatalog` and any future
    /// backend stay buildable; the in-memory backend overrides this.
    async fn set_global_sort_order(
        &self,
        project: &ProjectId,
        table: &TableName,
        columns: Vec<String>,
    ) -> Result<()> {
        let _ = (project, table, columns);
        Ok(())
    }

    /// Per-table Vortex chunk / Parquet row-group size in rows.
    ///
    /// Declares the `basin.row_block_size` option set at `CREATE TABLE … WITH
    /// (basin.row_block_size = N)`. The value is validated at DDL time (power
    /// of two, in [256, 65536]); callers of this method receive an
    /// already-validated value. `None` restores the writer's built-in default
    /// for each format. Default impl is a no-op so the stub `RestCatalog` and
    /// any future backends stay buildable; the in-memory and Postgres
    /// implementations override this.
    async fn set_row_block_size(
        &self,
        project: &ProjectId,
        table: &TableName,
        size: Option<u32>,
    ) -> Result<()> {
        let _ = (project, table, size);
        Ok(())
    }

    /// Phase 5.14.D2: set the `adaptive_sort_override` flag for `(project, table)`.
    async fn set_adaptive_sort_override(
        &self,
        project: &ProjectId,
        table: &TableName,
        value: Option<bool>,
    ) -> Result<()> {
        let _ = (project, table, value);
        Ok(())
    }

    /// Phase 6 multi-region scaffolding (ADR 0009). Pin `(project, table)`
    /// to a home region (or clear the pin with `None`). v0.1 only
    /// *records* the value; the cross-region routing / replication that
    /// will eventually consume it is future work per ADR 0009. Default
    /// impl is a no-op so the stub `RestCatalog` and any future backend
    /// stay buildable; the in-memory and Postgres backends override.
    async fn set_home_region(
        &self,
        project: &ProjectId,
        table: &TableName,
        region: Option<String>,
    ) -> Result<()> {
        let _ = (project, table, region);
        Ok(())
    }

    /// Phase 5.7 B1: declare a per-project secondary index on `(table, columns)`.
    /// The catalog records the declaration; the storage reader materialises
    /// the physical (value → file/row_group/row) map lazily on first query.
    /// Returns [`basin_common::BasinError::InvalidSchema`] if any column is not
    /// in the table's schema, or [`basin_common::BasinError::Catalog`] if
    /// `name` collides with an existing index on the same table (unless
    /// `if_not_exists` is set, in which case the call is a no-op).
    /// Default impl returns `Internal("not implemented")` so the stub
    /// `RestCatalog` stays buildable; in-memory and Postgres backends
    /// override.
    async fn create_index(
        &self,
        project: &ProjectId,
        table: &TableName,
        name: &str,
        columns: &[String],
        if_not_exists: bool,
    ) -> Result<()> {
        let _ = (project, table, name, columns, if_not_exists);
        Err(basin_common::BasinError::Internal(
            "create_index not implemented for this catalog backend".into(),
        ))
    }

    /// Phase 5.19.B: declare a GIN index with an explicit access method and
    /// optional operator class. The default implementation delegates to
    /// [`create_index`](Catalog::create_index) so existing catalog backends
    /// (RestCatalog, PostgresCatalog) stay buildable. InMemoryCatalog overrides
    /// this to persist `access_method` and `opclass` onto `SecondaryIndex`.
    ///
    /// `access_method` is the lowercase access method string (e.g. `"gin"`).
    /// `opclass` is the operator class declared on the single indexed column,
    /// e.g. `"jsonb_ops"` or `"jsonb_path_ops"`. `None` means the default for
    /// the access method applies.
    async fn create_index_with_method(
        &self,
        project: &ProjectId,
        table: &TableName,
        name: &str,
        columns: &[String],
        if_not_exists: bool,
        access_method: &str,
        opclass: Option<&str>,
    ) -> Result<()> {
        // Default: delegate to the existing create_index (btree-only backends).
        let _ = (access_method, opclass);
        self.create_index(project, table, name, columns, if_not_exists)
            .await
    }

    /// Phase 5.7 B1: drop a previously-declared secondary index by name.
    /// Returns [`basin_common::BasinError::NotFound`] if no index with that
    /// name exists on the table. Default impl returns `Internal("not
    /// implemented")` so the stub `RestCatalog` stays buildable.
    async fn drop_index(&self, project: &ProjectId, table: &TableName, name: &str) -> Result<()> {
        let _ = (project, table, name);
        Err(basin_common::BasinError::Internal(
            "drop_index not implemented for this catalog backend".into(),
        ))
    }

    /// ADR 0027 Phase 4: declare a promoted JSONB top-level path for
    /// `(project, table)`.
    ///
    /// If the `(source_col, json_key)` pair is already promoted this is a
    /// no-op.  The catalog records the declaration; the engine's write path
    /// then materialises a shadow column `__promoted$<source_col>$<json_key>`
    /// at INSERT time, and the planner substitutes a direct column read for
    /// matching `payload->>'key'` expressions.
    ///
    /// Default impl is a no-op (returns `Ok(())`) so the stub `RestCatalog`
    /// and future backends stay buildable without changes.  In-memory backend
    /// overrides to persist the declaration.
    async fn promote_jsonb_path(
        &self,
        project: &ProjectId,
        table: &TableName,
        source_col: &str,
        json_key: &str,
    ) -> Result<()> {
        let _ = (project, table, source_col, json_key);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Phase A.2 (#116 / #118): schema-qualified API
    //
    // BRIDGING STRATEGY (chosen: "default-impl wrappers on new methods"):
    //   - Old `*_table` / `*_data_files` / etc. methods are KEPT as required
    //     methods so PostgresCatalog (which implements them today) continues to
    //     compile without changes.
    //   - New `*_qualified` methods have DEFAULT implementations that delegate
    //     to the old method for the `public` schema, and return
    //     `FeatureNotSupported` for any other schema. This means:
    //       * PostgresCatalog inherits the defaults — works for public schema,
    //         honest error for non-public. #119 will override them properly.
    //       * InMemoryCatalog OVERRIDES every `*_qualified` method with a real
    //         schema-aware implementation (its internal storage is keyed by
    //         `QualifiedTableName`).
    //       * Downstream callers (basin-engine, basin-storage) that currently
    //         use the old `&TableName` API continue to compile unchanged; they
    //         can migrate to `*_qualified` in future phases (#120-#122).
    // -----------------------------------------------------------------------

    /// Schema-qualified variant of [`Catalog::create_table`].
    ///
    /// Creates a new empty table inside the given schema. The default
    /// implementation forwards to the old `create_table` for the `public`
    /// schema and returns [`basin_common::BasinError::FeatureNotSupported`] for
    /// any other schema (honest error until #119 lands the Postgres impl).
    async fn create_table_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        schema: Arc<arrow_schema::Schema>,
    ) -> Result<TableMetadata> {
        if qtable.schema != SchemaName::public() {
            return Err(basin_common::BasinError::FeatureNotSupported(format!(
                "non-public schema not yet supported in this catalog impl: {}",
                qtable.schema
            )));
        }
        self.create_table(project, &qtable.name, schema.as_ref())
            .await
    }

    /// Schema-qualified variant of [`Catalog::load_table`].
    async fn load_table_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
    ) -> Result<TableMetadata> {
        if qtable.schema != SchemaName::public() {
            return Err(basin_common::BasinError::FeatureNotSupported(format!(
                "non-public schema not yet supported in this catalog impl: {}",
                qtable.schema
            )));
        }
        self.load_table(project, &qtable.name).await
    }

    /// Schema-qualified variant of [`Catalog::drop_table`].
    async fn drop_table_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
    ) -> Result<()> {
        if qtable.schema != SchemaName::public() {
            return Err(basin_common::BasinError::FeatureNotSupported(format!(
                "non-public schema not yet supported in this catalog impl: {}",
                qtable.schema
            )));
        }
        self.drop_table(project, &qtable.name).await
    }

    /// Schema-qualified variant of [`Catalog::rename_table`].
    async fn rename_table_qualified(
        &self,
        project: &ProjectId,
        old: &QualifiedTableName,
        new: &QualifiedTableName,
    ) -> Result<()> {
        if old.schema != SchemaName::public() || new.schema != SchemaName::public() {
            return Err(basin_common::BasinError::FeatureNotSupported(format!(
                "non-public schema not yet supported in this catalog impl"
            )));
        }
        self.rename_table(project, &old.name, &new.name).await
    }

    /// List all tables across all schemas for `project`. Returns
    /// [`QualifiedTableName`] entries. The old [`Catalog::list_tables`] method
    /// continues to return only public-schema tables (for back-compat). Use
    /// this method when schema-aware enumeration is needed.
    async fn list_tables_qualified(&self, project: &ProjectId) -> Result<Vec<QualifiedTableName>> {
        // Default: wrap every TableName from list_tables as public-schema.
        let names = self.list_tables(project).await?;
        Ok(names
            .into_iter()
            .map(QualifiedTableName::in_public)
            .collect())
    }

    /// Schema-qualified variant of [`Catalog::append_data_files`].
    async fn append_data_files_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        expected_snapshot: SnapshotId,
        files: Vec<DataFileRef>,
    ) -> Result<TableMetadata> {
        if qtable.schema != SchemaName::public() {
            return Err(basin_common::BasinError::FeatureNotSupported(format!(
                "non-public schema not yet supported in this catalog impl: {}",
                qtable.schema
            )));
        }
        self.append_data_files(project, &qtable.name, expected_snapshot, files)
            .await
    }

    /// Schema-qualified variant of [`Catalog::replace_data_files`].
    async fn replace_data_files_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        expected_snapshot: SnapshotId,
        removed_paths: Vec<String>,
        added_files: Vec<DataFileRef>,
    ) -> Result<TableMetadata> {
        if qtable.schema != SchemaName::public() {
            return Err(basin_common::BasinError::FeatureNotSupported(format!(
                "non-public schema not yet supported in this catalog impl: {}",
                qtable.schema
            )));
        }
        self.replace_data_files(
            project,
            &qtable.name,
            expected_snapshot,
            removed_paths,
            added_files,
        )
        .await
    }

    /// Schema-qualified variant of [`Catalog::list_snapshots`].
    async fn list_snapshots_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
    ) -> Result<Vec<Snapshot>> {
        if qtable.schema != SchemaName::public() {
            return Err(basin_common::BasinError::FeatureNotSupported(format!(
                "non-public schema not yet supported in this catalog impl: {}",
                qtable.schema
            )));
        }
        self.list_snapshots(project, &qtable.name).await
    }

    /// Schema-qualified variant of [`Catalog::fork_table`].
    async fn fork_table_qualified(
        &self,
        project: &ProjectId,
        src: &QualifiedTableName,
        dst: &QualifiedTableName,
    ) -> Result<TableMetadata> {
        if src.schema != SchemaName::public() || dst.schema != SchemaName::public() {
            return Err(basin_common::BasinError::FeatureNotSupported(format!(
                "non-public schema not yet supported in this catalog impl"
            )));
        }
        self.fork_table(project, &src.name, &dst.name).await
    }

    /// Schema-qualified variant of [`Catalog::rollback_to_snapshot`].
    async fn rollback_to_snapshot_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        snapshot_id: SnapshotId,
    ) -> Result<TableMetadata> {
        if qtable.schema != SchemaName::public() {
            return Err(basin_common::BasinError::FeatureNotSupported(format!(
                "non-public schema not yet supported in this catalog impl: {}",
                qtable.schema
            )));
        }
        self.rollback_to_snapshot(project, &qtable.name, snapshot_id)
            .await
    }

    /// Schema-qualified variant of [`Catalog::set_partition_spec`].
    async fn set_partition_spec_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        spec: PartitionSpec,
    ) -> Result<()> {
        if qtable.schema != SchemaName::public() {
            return Err(basin_common::BasinError::FeatureNotSupported(format!(
                "non-public schema not yet supported in this catalog impl: {}",
                qtable.schema
            )));
        }
        self.set_partition_spec(project, &qtable.name, spec).await
    }

    /// Schema-qualified variant of [`Catalog::set_rls_state`].
    async fn set_rls_state_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        rls_enabled: bool,
        policies: Vec<Policy>,
    ) -> Result<()> {
        if qtable.schema != SchemaName::public() {
            return Err(basin_common::BasinError::FeatureNotSupported(format!(
                "non-public schema not yet supported in this catalog impl: {}",
                qtable.schema
            )));
        }
        self.set_rls_state(project, &qtable.name, rls_enabled, policies)
            .await
    }

    /// Schema-qualified variant of [`Catalog::set_tier_policy`].
    async fn set_tier_policy_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        cold_after_seconds: Option<u64>,
        cold_age_column: Option<String>,
    ) -> Result<()> {
        if qtable.schema != SchemaName::public() {
            return Err(basin_common::BasinError::FeatureNotSupported(format!(
                "non-public schema not yet supported in this catalog impl: {}",
                qtable.schema
            )));
        }
        self.set_tier_policy(project, &qtable.name, cold_after_seconds, cold_age_column)
            .await
    }

    /// Schema-qualified variant of [`Catalog::set_bloom_filter_columns`].
    async fn set_bloom_filter_columns_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        columns: Vec<String>,
    ) -> Result<()> {
        if qtable.schema != SchemaName::public() {
            return Err(basin_common::BasinError::FeatureNotSupported(format!(
                "non-public schema not yet supported in this catalog impl: {}",
                qtable.schema
            )));
        }
        self.set_bloom_filter_columns(project, &qtable.name, columns)
            .await
    }

    /// Schema-qualified variant of [`Catalog::set_row_group_rows`].
    async fn set_row_group_rows_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        rows: Option<usize>,
    ) -> Result<()> {
        if qtable.schema != SchemaName::public() {
            return Err(basin_common::BasinError::FeatureNotSupported(format!(
                "non-public schema not yet supported in this catalog impl: {}",
                qtable.schema
            )));
        }
        self.set_row_group_rows(project, &qtable.name, rows).await
    }

    /// Schema-qualified variant of [`Catalog::set_schema`].
    async fn set_schema_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        schema: arrow_schema::Schema,
    ) -> Result<()> {
        if qtable.schema != SchemaName::public() {
            return Err(basin_common::BasinError::FeatureNotSupported(format!(
                "non-public schema not yet supported in this catalog impl: {}",
                qtable.schema
            )));
        }
        self.set_schema(project, &qtable.name, schema).await
    }

    /// Schema-qualified variant of [`Catalog::set_continuous_aggregate`].
    async fn set_continuous_aggregate_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        def: Option<metadata::CvDef>,
    ) -> Result<()> {
        if qtable.schema != SchemaName::public() {
            return Err(basin_common::BasinError::FeatureNotSupported(format!(
                "non-public schema not yet supported in this catalog impl: {}",
                qtable.schema
            )));
        }
        self.set_continuous_aggregate(project, &qtable.name, def)
            .await
    }

    /// Schema-qualified variant of [`Catalog::set_cluster_columns`].
    async fn set_cluster_columns_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        columns: Vec<String>,
    ) -> Result<()> {
        if qtable.schema != SchemaName::public() {
            return Err(basin_common::BasinError::FeatureNotSupported(format!(
                "non-public schema not yet supported in this catalog impl: {}",
                qtable.schema
            )));
        }
        self.set_cluster_columns(project, &qtable.name, columns)
            .await
    }

    /// Schema-qualified variant of [`Catalog::set_home_region`].
    async fn set_home_region_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        region: Option<String>,
    ) -> Result<()> {
        if qtable.schema != SchemaName::public() {
            return Err(basin_common::BasinError::FeatureNotSupported(format!(
                "non-public schema not yet supported in this catalog impl: {}",
                qtable.schema
            )));
        }
        self.set_home_region(project, &qtable.name, region).await
    }

    /// Schema-qualified variant of [`Catalog::create_index`].
    async fn create_index_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        name: &str,
        columns: &[String],
        if_not_exists: bool,
    ) -> Result<()> {
        if qtable.schema != SchemaName::public() {
            return Err(basin_common::BasinError::FeatureNotSupported(format!(
                "non-public schema not yet supported in this catalog impl: {}",
                qtable.schema
            )));
        }
        self.create_index(project, &qtable.name, name, columns, if_not_exists)
            .await
    }

    /// Schema-qualified variant of [`Catalog::drop_index`].
    async fn drop_index_qualified(
        &self,
        project: &ProjectId,
        qtable: &QualifiedTableName,
        name: &str,
    ) -> Result<()> {
        if qtable.schema != SchemaName::public() {
            return Err(basin_common::BasinError::FeatureNotSupported(format!(
                "non-public schema not yet supported in this catalog impl: {}",
                qtable.schema
            )));
        }
        self.drop_index(project, &qtable.name, name).await
    }

    // -----------------------------------------------------------------------
    // Schema-level operations (new in #118)
    // -----------------------------------------------------------------------

    /// List all schemas that exist for `project`. The `public` schema always
    /// exists after the first `create_namespace` call. Default impl returns
    /// `[SchemaName::public()]` so existing backends that don't support
    /// multi-schema still satisfy callers that iterate schemas.
    async fn list_schemas(&self, project: &ProjectId) -> Result<Vec<SchemaName>> {
        let _ = project;
        Ok(vec![SchemaName::public()])
    }

    /// Create a new schema under `project`. Idempotent — creating `public`
    /// again is a no-op. Default impl returns
    /// [`basin_common::BasinError::FeatureNotSupported`] for any schema other
    /// than `public`; backends that support multi-schema override this.
    async fn create_schema(&self, project: &ProjectId, schema: &SchemaName) -> Result<()> {
        if schema == &SchemaName::public() {
            return Ok(());
        }
        let _ = (project, schema);
        Err(basin_common::BasinError::FeatureNotSupported(
            "non-public schemas not yet supported in this catalog impl".into(),
        ))
    }

    /// Drop a schema under `project`.
    ///
    /// `cascade = false` (RESTRICT): returns
    /// [`basin_common::BasinError::Catalog`] if any tables remain in the schema.
    /// `cascade = true`: drops all tables in the schema first.
    ///
    /// Dropping the `public` schema always returns
    /// [`basin_common::BasinError::Catalog`] — the public schema is permanent.
    ///
    /// Default impl returns [`basin_common::BasinError::FeatureNotSupported`];
    /// backends that support multi-schema override this.
    async fn drop_schema(
        &self,
        project: &ProjectId,
        schema: &SchemaName,
        cascade: bool,
    ) -> Result<()> {
        let _ = (project, schema, cascade);
        Err(basin_common::BasinError::FeatureNotSupported(
            "non-public schemas not yet supported in this catalog impl".into(),
        ))
    }

    /// Register a `LANGUAGE sql` user-defined scalar function. The catalog
    /// stores the definition verbatim; the engine's inliner reparses the
    /// body on each call site. Idempotent in the sense that re-registering
    /// the same `(project, name)` replaces the prior definition (matches
    /// `CREATE OR REPLACE FUNCTION` semantics).
    ///
    /// Default impl returns `Internal("not implemented")`. The in-memory
    /// and Postgres backends override.
    async fn register_sql_function(&self, def: SqlFunctionDef) -> Result<()> {
        let _ = def;
        Err(basin_common::BasinError::Internal(
            "register_sql_function not implemented for this catalog backend".into(),
        ))
    }

    /// Drop a previously-registered SQL function. Returns
    /// [`basin_common::BasinError::NotFound`] if no function with the
    /// given `(project, name)` exists.
    async fn drop_sql_function(&self, project: &ProjectId, name: &str) -> Result<()> {
        let _ = (project, name);
        Err(basin_common::BasinError::Internal(
            "drop_sql_function not implemented for this catalog backend".into(),
        ))
    }

    /// Look up a registered SQL function by `(project, name)`. Returns
    /// `None` for both "project has no functions" and "name is unknown" —
    /// callers can't distinguish, mirroring the lookup semantics of
    /// `load_table` for tables. Default impl returns `None`.
    async fn lookup_sql_function(&self, project: &ProjectId, name: &str) -> Option<SqlFunctionDef> {
        let _ = (project, name);
        None
    }

    /// List every SQL function registered for `project`. Order is
    /// unspecified; callers that need a stable order should sort by
    /// `name`. Default impl returns an empty list.
    async fn list_sql_functions(&self, project: &ProjectId) -> Vec<SqlFunctionDef> {
        let _ = project;
        Vec::new()
    }

    /// Register a sequence under `(def.project, def.name)`. Returns
    /// [`basin_common::BasinError::Catalog`] if a sequence with the same
    /// `(project, name)` already exists; PG semantics treat
    /// `CREATE SEQUENCE` as non-idempotent (use `CREATE SEQUENCE IF
    /// NOT EXISTS` at the SQL surface to opt into idempotency once
    /// that path lands). Returns [`basin_common::BasinError::InvalidSchema`]
    /// when `def.increment == 0`. Default impl returns
    /// `Internal("not implemented")` so the stub `RestCatalog` stays
    /// buildable.
    async fn create_sequence(&self, def: SequenceDef) -> Result<()> {
        let _ = def;
        Err(basin_common::BasinError::Internal(
            "create_sequence not implemented for this catalog backend".into(),
        ))
    }

    /// Drop a previously-created sequence. Returns
    /// [`basin_common::BasinError::NotFound`] if no sequence with that
    /// `(project, name)` exists. Default impl: not-implemented.
    async fn drop_sequence(&self, project: &ProjectId, name: &str) -> Result<()> {
        let _ = (project, name);
        Err(basin_common::BasinError::Internal(
            "drop_sequence not implemented for this catalog backend".into(),
        ))
    }

    /// Look up a registered sequence by `(project, name)`. Returns
    /// `None` when the sequence does not exist (mirrors the lookup
    /// shape of [`Catalog::lookup_sql_function`] and [`Catalog::load_table`]
    /// for the no-entry case). Default impl: `None`.
    async fn lookup_sequence(&self, project: &ProjectId, name: &str) -> Option<SequenceDef> {
        let _ = (project, name);
        None
    }

    /// Allocate and return the next value of `(project, name)`. The first
    /// call after `create_sequence` returns `def.start`; subsequent calls
    /// step by `def.increment`. Concurrent callers always see distinct
    /// values — the per-sequence mutex serialises the increment.
    /// Returns [`basin_common::BasinError::NotFound`] if no sequence is
    /// registered under that name; [`basin_common::BasinError::Catalog`]
    /// when the sequence is exhausted (no-cycle and would cross
    /// `max_value` / `min_value`). Default impl: not-implemented.
    async fn nextval(&self, project: &ProjectId, name: &str) -> Result<i64> {
        let _ = (project, name);
        Err(basin_common::BasinError::Internal(
            "nextval not implemented for this catalog backend".into(),
        ))
    }

    /// Return the most recent value handed out by
    /// [`Catalog::nextval`] across all sessions. PG models `currval` as
    /// per-session: the engine layer overlays a session-bound cache so
    /// the SQL-visible `currval` reflects only the calling session's
    /// last `nextval`. This catalog-level method is the underlying
    /// "value most recently advanced to" — useful for diagnostics and
    /// the `setval(advance=false)` path. Returns
    /// [`basin_common::BasinError::NotFound`] when the sequence does
    /// not exist or has never been advanced. Default impl:
    /// not-implemented.
    async fn currval(&self, project: &ProjectId, name: &str) -> Result<i64> {
        let _ = (project, name);
        Err(basin_common::BasinError::Internal(
            "currval not implemented for this catalog backend".into(),
        ))
    }

    /// Reset the sequence's value. With `advance == true` (PG's default
    /// for `setval(seq, n)`), the next `nextval` returns `n + increment`;
    /// with `advance == false` (PG's `setval(seq, n, false)`), the next
    /// `nextval` returns exactly `n`. Returns the value of `n` (matches
    /// PG's return-the-just-set-value behaviour). Default impl:
    /// not-implemented.
    async fn setval(
        &self,
        project: &ProjectId,
        name: &str,
        value: i64,
        advance: bool,
    ) -> Result<i64> {
        let _ = (project, name, value, advance);
        Err(basin_common::BasinError::Internal(
            "setval not implemented for this catalog backend".into(),
        ))
    }

    /// Register a SQL-bodied reactor. The catalog stores the definition
    /// verbatim; the engine's `ReactorSink` reparses the body / predicate
    /// on each fire. Re-registering the same `(project, table, name)` is
    /// rejected — the SQL surface routes that case through `DROP REACTOR`
    /// + a fresh `register_reactor`.
    ///
    /// Validates: body parses as a single SQL statement, optional `WHEN`
    /// predicate parses as a SQL expression, `ops` is non-empty, name is
    /// unique per `(project, table)`. Default impl returns
    /// `Internal("not implemented")`. The in-memory backend overrides;
    /// the Postgres backend will override when the durable reactor row
    /// lands (the SQL surface is held back per the 5.11.C scope split).
    async fn register_reactor(&self, def: ReactorDef) -> Result<()> {
        let _ = def;
        Err(basin_common::BasinError::Internal(
            "register_reactor not implemented for this catalog backend".into(),
        ))
    }

    /// Drop a previously-registered reactor. Returns
    /// [`basin_common::BasinError::NotFound`] if no reactor with that
    /// `(project, table, name)` exists.
    async fn drop_reactor(&self, project: &ProjectId, table: &TableName, name: &str) -> Result<()> {
        let _ = (project, table, name);
        Err(basin_common::BasinError::Internal(
            "drop_reactor not implemented for this catalog backend".into(),
        ))
    }

    /// Look up every reactor that should fire for `(project, table, op)`.
    /// Order is registration order — `ReactorSink` invokes them in this
    /// sequence and short-circuits on the first error. Default impl
    /// returns an empty vec, so backends that haven't opted in see no
    /// reactor activity.
    async fn lookup_reactors_for(
        &self,
        project: &ProjectId,
        table: &TableName,
        op: ChangeOp,
    ) -> Vec<ReactorDef> {
        let _ = (project, table, op);
        Vec::new()
    }

    /// List every reactor registered for `project` across all tables.
    /// Order is unspecified; callers that need a stable order should
    /// sort by `(table, name)`. Default impl returns an empty list.
    async fn list_reactors(&self, project: &ProjectId) -> Vec<ReactorDef> {
        let _ = project;
        Vec::new()
    }

    /// Register a `CREATE TYPE … AS ENUM` declaration. Returns
    /// [`basin_common::BasinError::Catalog`] when the name collides
    /// with an existing enum / domain for the same project, and
    /// [`basin_common::BasinError::InvalidSchema`] when the label
    /// list fails [`enums::validate_new`]. Default impl returns
    /// `Internal("not implemented")` so non-default backends opt in
    /// explicitly.
    async fn register_enum_type(&self, def: EnumTypeDef) -> Result<()> {
        let _ = def;
        Err(basin_common::BasinError::Internal(
            "register_enum_type not implemented for this catalog backend".into(),
        ))
    }

    /// Look up a registered enum by `(project, name)`. Returns `None`
    /// when the name does not resolve. Default impl: `None`.
    async fn lookup_enum_type(&self, project: &ProjectId, name: &str) -> Option<EnumTypeDef> {
        let _ = (project, name);
        None
    }

    /// Append `value` to an existing enum's label list. PG's
    /// `ALTER TYPE … ADD VALUE` is append-only; insert-before /
    /// remove are explicitly out of scope. Returns
    /// [`basin_common::BasinError::NotFound`] when no enum is
    /// registered under that name, and
    /// [`basin_common::BasinError::Catalog`] when `value` already
    /// exists in the label list. Default impl: not-implemented.
    async fn add_enum_value(&self, project: &ProjectId, name: &str, value: &str) -> Result<()> {
        let _ = (project, name, value);
        Err(basin_common::BasinError::Internal(
            "add_enum_value not implemented for this catalog backend".into(),
        ))
    }

    /// Drop a previously-registered enum. Returns
    /// [`basin_common::BasinError::NotFound`] if no enum with that
    /// name exists, and [`basin_common::BasinError::Catalog`] when at
    /// least one table column still references the type — v0.1 has no
    /// CASCADE; the caller must drop the column(s) first. Default
    /// impl: not-implemented.
    async fn drop_enum_type(&self, project: &ProjectId, name: &str) -> Result<()> {
        let _ = (project, name);
        Err(basin_common::BasinError::Internal(
            "drop_enum_type not implemented for this catalog backend".into(),
        ))
    }

    /// List every enum registered for `project`. Order is unspecified;
    /// callers that need a stable order should sort by `name`. Default
    /// impl returns an empty list.
    async fn list_enum_types(&self, project: &ProjectId) -> Vec<EnumTypeDef> {
        let _ = project;
        Vec::new()
    }

    /// Register a `CREATE DOMAIN` declaration. Returns
    /// [`basin_common::BasinError::Catalog`] when the name collides
    /// with an existing enum / domain for the same project, and
    /// [`basin_common::BasinError::InvalidSchema`] when the predicate
    /// fails to parse. Default impl: not-implemented.
    async fn register_domain(&self, def: DomainDef) -> Result<()> {
        let _ = def;
        Err(basin_common::BasinError::Internal(
            "register_domain not implemented for this catalog backend".into(),
        ))
    }

    /// Look up a registered domain by `(project, name)`. Default impl:
    /// `None`.
    async fn lookup_domain(&self, project: &ProjectId, name: &str) -> Option<DomainDef> {
        let _ = (project, name);
        None
    }

    /// Drop a previously-registered domain. Returns
    /// [`basin_common::BasinError::NotFound`] when missing,
    /// [`basin_common::BasinError::Catalog`] when at least one table
    /// column still references the domain.
    async fn drop_domain(&self, project: &ProjectId, name: &str) -> Result<()> {
        let _ = (project, name);
        Err(basin_common::BasinError::Internal(
            "drop_domain not implemented for this catalog backend".into(),
        ))
    }

    /// List every domain registered for `project`. Default impl: empty.
    async fn list_domains(&self, project: &ProjectId) -> Vec<DomainDef> {
        let _ = project;
        Vec::new()
    }

    /// List every sequence registered for `project`. Default impl: empty.
    async fn list_sequences(&self, project: &ProjectId) -> Vec<SequenceDef> {
        let _ = project;
        Vec::new()
    }

    /// Register a `LANGUAGE sql` user-defined procedure. The catalog
    /// stores the definition verbatim; the engine reparses the body
    /// (and re-substitutes call-site arguments into each statement) on
    /// every `CALL`. Re-registering the same `(project, name)` is
    /// rejected — the SQL surface routes that case through `DROP
    /// PROCEDURE` + a fresh `register_procedure`.
    ///
    /// Default impl returns `Internal("not implemented")` so non-default
    /// backends opt in explicitly.
    async fn register_procedure(&self, def: SqlProcedureDef) -> Result<()> {
        let _ = def;
        Err(basin_common::BasinError::Internal(
            "register_procedure not implemented for this catalog backend".into(),
        ))
    }

    /// Drop a previously-registered procedure. Returns
    /// [`basin_common::BasinError::NotFound`] if no procedure with that
    /// `(project, name)` exists. Default impl: not-implemented.
    async fn drop_procedure(&self, project: &ProjectId, name: &str) -> Result<()> {
        let _ = (project, name);
        Err(basin_common::BasinError::Internal(
            "drop_procedure not implemented for this catalog backend".into(),
        ))
    }

    /// Look up a registered procedure by `(project, name)`. Returns
    /// `None` for both "project has no procedures" and "name is
    /// unknown" — same lookup semantics as
    /// [`Catalog::lookup_sql_function`]. Default impl returns `None`.
    async fn lookup_procedure(&self, project: &ProjectId, name: &str) -> Option<SqlProcedureDef> {
        let _ = (project, name);
        None
    }

    /// List every procedure registered for `project`. Order is
    /// unspecified; callers that need a stable order should sort by
    /// `name`. Default impl returns an empty list.
    async fn list_procedures(&self, project: &ProjectId) -> Vec<SqlProcedureDef> {
        let _ = project;
        Vec::new()
    }

    /// Persist `config` for `project`. Idempotent: a second call with the
    /// same project replaces the prior config (mirrors the
    /// `INSERT … ON CONFLICT DO UPDATE` shape of the durable backends).
    /// Read by the storage layer's encryption call path via
    /// [`Catalog::get_project_storage_config`]; threaded into the
    /// `EncryptionProvider` so external KMS adapters can route per-project
    /// CMK lookups without owning a registry of their own. Default impl
    /// returns `Internal("not implemented")` so non-default backends opt
    /// in explicitly.
    async fn set_project_storage_config(
        &self,
        project: &ProjectId,
        config: ProjectStorageConfig,
    ) -> Result<()> {
        let _ = (project, config);
        Err(basin_common::BasinError::Internal(
            "set_project_storage_config not implemented for this catalog backend".into(),
        ))
    }

    /// Look up the persisted [`ProjectStorageConfig`] for `project`.
    /// Returns `None` when no config has been set — callers (the
    /// encryption call path) interpret that as "no per-project routing,
    /// fall back to the provider's default behaviour". Default impl:
    /// `None`.
    async fn get_project_storage_config(
        &self,
        project: &ProjectId,
    ) -> Result<Option<ProjectStorageConfig>> {
        let _ = project;
        Ok(None)
    }

    /// Persist the PRIMARY KEY column list, CHECK constraints, and
    /// FOREIGN KEY definitions for `(project, table)`. Called by
    /// `CREATE TABLE` after the table itself has been created. Empty
    /// vectors clear the corresponding constraint set. Default impl
    /// is a no-op so the stub `RestCatalog` and any future backend
    /// stay buildable; the in-memory and Postgres backends override.
    async fn set_table_constraints(
        &self,
        project: &ProjectId,
        table: &TableName,
        pk_columns: Vec<String>,
        check_constraints: Vec<metadata::CheckConstraint>,
        foreign_keys: Vec<metadata::ForeignKeyDef>,
    ) -> Result<()> {
        let _ = (project, table, pk_columns, check_constraints, foreign_keys);
        Ok(())
    }

    /// Persist the UNIQUE-constraint list for `(project, table)`. Called
    /// by `CREATE TABLE` after the table has been created (only when at
    /// least one UNIQUE constraint was declared). An empty vector clears
    /// the set. Stored as a separate call from `set_table_constraints`
    /// to keep the latter's existing signature stable. Default impl is a
    /// no-op so the stub `RestCatalog` stays buildable; the in-memory
    /// backend overrides.
    async fn set_unique_constraints(
        &self,
        project: &ProjectId,
        table: &TableName,
        unique_constraints: Vec<metadata::UniqueConstraint>,
    ) -> Result<()> {
        let _ = (project, table, unique_constraints);
        Ok(())
    }

    /// Register or replace a plain-view definition. Default impl returns
    /// `Internal("not implemented")` so the stub `RestCatalog` stays buildable;
    /// the in-memory backend overrides.
    async fn register_view(&self, def: ViewDef, or_replace: bool) -> Result<()> {
        let _ = (def, or_replace);
        Err(basin_common::BasinError::Internal(
            "register_view not implemented for this catalog backend".into(),
        ))
    }

    /// Drop a plain-view definition. Default impl returns
    /// `Internal("not implemented")`.
    async fn drop_view(&self, project: &ProjectId, name: &str, if_exists: bool) -> Result<()> {
        let _ = (project, name, if_exists);
        Err(basin_common::BasinError::Internal(
            "drop_view not implemented for this catalog backend".into(),
        ))
    }

    /// Look up a single view by (project, name). Default impl returns `None`.
    async fn lookup_view(&self, project: &ProjectId, name: &str) -> Option<ViewDef> {
        let _ = (project, name);
        None
    }

    /// List all views for `project`. Default impl returns an empty vec.
    async fn list_views(&self, project: &ProjectId) -> Vec<ViewDef> {
        let _ = project;
        vec![]
    }

    // -----------------------------------------------------------------------
    // Phase 5.11.N — inbound webhook receivers (ADR 0019)
    // -----------------------------------------------------------------------

    /// Register a `CREATE INBOUND WEBHOOK` declaration. Returns
    /// [`basin_common::BasinError::Catalog`] when `name` collides with an
    /// existing inbound webhook for the same project, and
    /// [`basin_common::BasinError::InvalidSchema`] when the body fails to
    /// parse. Default impl: not implemented.
    async fn register_inbound_webhook(&self, def: InboundWebhookDef) -> Result<()> {
        let _ = def;
        Err(basin_common::BasinError::Internal(
            "register_inbound_webhook not implemented for this catalog backend".into(),
        ))
    }

    /// Drop a previously-registered inbound webhook. Returns
    /// [`basin_common::BasinError::NotFound`] if no webhook with that
    /// `(project, name)` exists. Default impl: not implemented.
    async fn drop_inbound_webhook(&self, project: &ProjectId, name: &str) -> Result<()> {
        let _ = (project, name);
        Err(basin_common::BasinError::Internal(
            "drop_inbound_webhook not implemented for this catalog backend".into(),
        ))
    }

    /// Look up a registered inbound webhook by `(project, name)`. Returns
    /// `None` when the name does not resolve. Default impl: `None`.
    async fn lookup_inbound_webhook(
        &self,
        project: &ProjectId,
        name: &str,
    ) -> Option<InboundWebhookDef> {
        let _ = (project, name);
        None
    }

    /// List every inbound webhook registered for `project`. Default impl:
    /// empty.
    async fn list_inbound_webhooks(&self, project: &ProjectId) -> Vec<InboundWebhookDef> {
        let _ = project;
        Vec::new()
    }

    // -----------------------------------------------------------------------
    // Phase 6 — Refcount-aware data-file GC
    //
    // A data-file path is "live" when it appears in the live file set of at
    // least one table snapshot accessible from any table currently registered
    // under `project`.  The live set is computed by [`TableMetadata::live_data_files`]
    // for every table, then union-ed across all tables.  Because forked
    // tables share file paths, a path shared between a source and its fork
    // counts as refcount ≥ 2, and dropping the source does not make the path
    // orphaned while the fork still references it.
    //
    // `gc_orphaned_files` scans the union of every path ever recorded in any
    // snapshot of any table (the "known universe"), subtracts the live set,
    // and returns the difference as `GcReport::orphaned_paths`.  Callers
    // (the compactor, the PITR post-rollback hook) then physically delete
    // those paths from object storage.
    // -----------------------------------------------------------------------

    /// Compute the reference count for `path` across all live snapshots of
    /// all tables owned by `project`.  A path is referenced once per live
    /// snapshot that contains it; a path shared between a source table and
    /// a fork counts as referenced by each independently.  Returns 0 when the
    /// path is not in any live file set — i.e. it is safe to physically
    /// delete.
    ///
    /// Default impl iterates `list_tables` then `load_table` for each,
    /// calling `TableMetadata::live_data_files`. O(tables × files) — fine
    /// for the GC sweep cadence.
    async fn file_refcount(&self, project: &ProjectId, path: &str) -> Result<usize> {
        let tables = self.list_tables(project).await?;
        let mut count: usize = 0;
        for table in &tables {
            match self.load_table(project, table).await {
                Ok(meta) => {
                    // Count how many live files in this table reference the path.
                    // A single table can reference it at most once in its live set
                    // (live_data_files deduplicates by path), but a shared path
                    // between source and fork each appear once per table.
                    let live = meta.live_data_files();
                    if live.iter().any(|f| f.path == path) {
                        count += 1;
                    }
                }
                Err(basin_common::BasinError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(count)
    }

    /// Garbage-collect data files for `project` whose refcount has dropped to
    /// zero.  Returns a [`GcReport`] listing orphaned paths (deletable) and
    /// live paths (must be kept).  The caller is responsible for the actual
    /// physical deletion — this method only inspects catalog state.
    ///
    /// The algorithm:
    /// 1. Build the "known universe": every path ever recorded in any snapshot
    ///    of any table (including `removed_paths` from Replace snapshots, which
    ///    may have been physically deleted already — callers should use
    ///    idempotent `DeleteObject` on S3/R2 so re-deleting is harmless).
    /// 2. Build the "live set": union of `live_data_files()` across all tables.
    ///    A path in a fork and its source counts once per table, but for GC
    ///    purposes what matters is whether the path is in *any* live set —
    ///    if yes, it must be kept.
    /// 3. `orphaned = universe \ live` — these are catalog-orphans with refcount 0.
    ///
    /// Typical callers:
    /// * Post-`rollback_to_snapshot`: call this to identify files written
    ///   between the rollback target and the old head that are now invisible.
    /// * Compactor periodic sweep: finds files from Replace commits that are
    ///   no longer in any live snapshot.
    ///
    /// Default impl: single pass over `list_tables` + `load_table`.
    async fn gc_orphaned_files(
        &self,
        project: &ProjectId,
        table: &TableName,
    ) -> Result<GcReport> {
        let meta = self.load_table(project, table).await?;

        // Universe: all paths ever recorded in this table's snapshot history
        // (both added and removed), deduplicated.  Also include paths saved
        // by prior `rollback_to_snapshot` calls — those snapshot records were
        // pruned but the paths may still need physical deletion.
        let mut universe: std::collections::HashSet<String> = std::collections::HashSet::new();
        for snap in &meta.snapshots {
            for f in &snap.data_files {
                universe.insert(f.path.clone());
            }
            for p in &snap.removed_paths {
                universe.insert(p.clone());
            }
        }
        for p in &meta.gc_orphan_paths {
            universe.insert(p.clone());
        }

        // Live set across ALL tables in the project so cross-table fork
        // references are respected: a path shared with a fork must not be
        // treated as orphaned just because it was removed from this table.
        let all_tables = self.list_tables(project).await?;
        let mut live: std::collections::HashSet<String> = std::collections::HashSet::new();
        for t in &all_tables {
            match self.load_table(project, t).await {
                Ok(m) => {
                    for f in m.live_data_files() {
                        live.insert(f.path);
                    }
                }
                Err(basin_common::BasinError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }

        let mut orphaned_paths: Vec<String> = universe
            .iter()
            .filter(|p| !live.contains(*p))
            .cloned()
            .collect();
        orphaned_paths.sort();

        let mut live_paths: Vec<String> = universe
            .iter()
            .filter(|p| live.contains(*p))
            .cloned()
            .collect();
        live_paths.sort();

        Ok(GcReport {
            orphaned_paths,
            live_paths,
        })
    }

}

