//! Phase 5.19.E — GIN posting-list maintenance for UPDATE / DELETE.
//!
//! When a copy-on-write UPDATE or DELETE commits, the old Parquet file is
//! replaced by a new one (or dropped).  The in-RAM GIN posting list must be
//! kept consistent: stale entries for the removed file must be purged, and
//! new entries for the replacement file must be inserted.
//!
//! # Design
//!
//! The `GinPostingListMaintainer` struct holds references to the engine's
//! `GinIndexRegistry` and the catalog's table metadata.  It is constructed
//! once per UPDATE/DELETE commit path and provides two operations:
//!
//! * `on_file_removed(file_path)` — purge all posting entries for that file.
//! * `on_file_replaced(old_path, new_path, batches)` — purge the old file and
//!   rebuild entries for the new file from the replacement batches.
//!
//! The maintainer iterates the table's GIN indexes (from `meta.indexes`) to
//! find which columns are indexed and what opclass they use.  Multiple GIN
//! indexes on different columns of the same table are all maintained.
//!
//! # Correctness contract
//!
//! The maintainer is called *after* `commit_replace` (catalog is updated) and
//! *before* the physical file delete.  If the process crashes between commit
//! and maintenance the posting list will be stale (may include entries for the
//! old file or miss entries for the new file).  On restart the registry starts
//! empty and rebuilds lazily — so stale entries are never persisted across
//! restarts.  Within a running process the worst outcome is a false positive
//! (probe returns FileCandidates for a file that no longer exists), which the
//! DataFusion scan layer handles correctly by returning zero matching rows.

use basin_catalog::TableMetadata;
use basin_common::{ProjectId, TableName};

/// GIN posting-list maintainer for a single UPDATE/DELETE operation.
///
/// Hold a reference to the shared `GinIndexRegistry` and the table metadata.
/// Call `on_file_removed` / `on_file_replaced` for each file touched by the
/// mutation; call `rebuild_from_batches` for the newly written replacement file.
pub struct GinPostingListMaintainer<'a, R> {
    registry: &'a R,
    project: &'a ProjectId,
    table: &'a TableName,
    /// GIN index declarations for this table: `(col_name, opclass)` pairs.
    gin_columns: Vec<(String, String)>,
}

/// Minimal interface the maintainer requires from the GIN registry.
/// Implemented by `basin_engine::index_probe::GinIndexRegistry`.
pub trait GinRegistry {
    fn remove_file(&self, project: &ProjectId, table: &TableName, col: &str, file_path: &str);
    fn rebuild_file_entries(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        opclass: &str,
        batches: &[arrow_array::RecordBatch],
        new_file_path: &str,
    );
}

impl<'a, R: GinRegistry> GinPostingListMaintainer<'a, R> {
    /// Create a maintainer for `(project, table)` using `meta` to discover
    /// which columns have GIN indexes.
    ///
    /// Returns `None` when `meta.indexes` has no GIN entries — the caller can
    /// skip maintenance entirely in that case.
    pub fn new(
        registry: &'a R,
        project: &'a ProjectId,
        table: &'a TableName,
        meta: &TableMetadata,
    ) -> Option<Self> {
        let gin_columns: Vec<(String, String)> = meta
            .indexes
            .iter()
            .filter(|idx| idx.access_method == "gin" && idx.columns.len() == 1)
            .map(|idx| {
                let col = idx.columns[0].clone();
                let opclass = idx
                    .opclass
                    .clone()
                    .unwrap_or_else(|| "jsonb_ops".to_string());
                (col, opclass)
            })
            .collect();

        if gin_columns.is_empty() {
            return None;
        }
        Some(Self {
            registry,
            project,
            table,
            gin_columns,
        })
    }

    /// Remove all posting entries for `file_path`.
    ///
    /// Call this for every file that is fully dropped (DELETE AllMatch or
    /// AllMatch UPDATE where the file is replaced).
    pub fn on_file_removed(&self, file_path: &str) {
        for (col, _opclass) in &self.gin_columns {
            self.registry
                .remove_file(self.project, self.table, col, file_path);
        }
    }

    /// Purge `old_file_path` entries and rebuild from `new_batches` into
    /// `new_file_path`.
    ///
    /// Call this for every copy-on-write replacement (a file is rewritten with
    /// some rows modified or removed).  `new_batches` should be the full set of
    /// batches written to `new_file_path`; `old_file_path` must be the replaced
    /// file's path.
    pub fn on_file_replaced(
        &self,
        old_file_path: &str,
        new_file_path: &str,
        new_batches: &[arrow_array::RecordBatch],
    ) {
        for (col, opclass) in &self.gin_columns {
            // Purge old entries first so there's no window where both old and
            // new entries co-exist in the posting list.
            self.registry
                .remove_file(self.project, self.table, col, old_file_path);
            // Rebuild from the replacement batches.
            self.registry.rebuild_file_entries(
                self.project,
                self.table,
                col,
                opclass,
                new_batches,
                new_file_path,
            );
        }
    }
}
