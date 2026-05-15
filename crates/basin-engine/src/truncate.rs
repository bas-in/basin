//! TRUNCATE TABLE — delete all rows in one or more tables.
//!
//! # Implementation
//!
//! TRUNCATE is a real operation (unlike VACUUM/ANALYZE which are noops in
//! Basin). For each named table we:
//!
//! 1. Load the current [`TableMetadata`] to get the live snapshot id.
//! 2. Call `replace_data_files` with all existing paths in `removed_paths`
//!    and an empty `added_files` list. This produces a new empty snapshot
//!    in the catalog atomically.
//! 3. Physically delete the replaced Parquet files (and any HNSW sidecars)
//!    from object storage — same best-effort path as DELETE's `delete_objects`.
//! 4. Refresh the DataFusion session listing so the next SELECT in this
//!    session sees an empty table.
//!
//! `RESTART IDENTITY` restarts every `SERIAL`/`BIGSERIAL`/`SMALLSERIAL`-backed
//! sequence by calling `setval(seq, start - increment, advance=true)` so the
//! next `nextval` returns the original `start`. We discover sequence names
//! from the table's Arrow field metadata (`BASIN_COLUMN_DEFAULT` = `nextval('<seq>')`).
//!
//! `CASCADE` and `RESTRICT` on TRUNCATE are silently accepted — Basin's
//! referential-integrity enforcement in v0.1 is row-level; TRUNCATE does not
//! currently cascade to child tables (out of scope for v0.1; add when FK
//! enforcement matures).
//!
//! Multi-table TRUNCATE (PG allows `TRUNCATE t1, t2`) is supported: tables
//! are processed sequentially, best-effort. A failure on any table is returned
//! immediately (earlier tables already truncated remain truncated — no
//! cross-table rollback in v0.1 since we're auto-commit).

use arrow_schema::DataType;
use basin_common::{BasinError, Result, TableName};
use object_store::path::Path as ObjectStorePath;
use pg_query::protobuf::node::Node as NodeEnum;
use std::str::FromStr;

use crate::session::refresh_table;
use crate::types::BASIN_COLUMN_DEFAULT;
use crate::{ExecResult, ProjectSession};

/// Execute `TRUNCATE [TABLE] t1, t2 [CASCADE|RESTRICT] [RESTART IDENTITY|CONTINUE IDENTITY]`.
pub(crate) async fn exec_truncate(
    sess: &ProjectSession,
    tree: &crate::pg_ast::ParseTree,
) -> Result<ExecResult> {
    // Extract the TruncateStmt from the parse tree.
    let node = tree
        .stmts()
        .next()
        .ok_or_else(|| BasinError::internal("TRUNCATE: empty parse tree"))?;

    let trunc = match node.node.as_ref() {
        Some(NodeEnum::TruncateStmt(s)) => s,
        _ => {
            return Err(BasinError::internal(
                "exec_truncate called on non-TRUNCATE statement",
            ));
        }
    };

    let restart_seqs = trunc.restart_seqs;

    // Collect table names from the relations list.
    let mut tables: Vec<TableName> = Vec::new();
    for rel_node in &trunc.relations {
        let rv = match rel_node.node.as_ref() {
            Some(NodeEnum::RangeVar(rv)) => rv,
            _ => {
                return Err(BasinError::internal(
                    "TRUNCATE: unexpected node type in relations list",
                ));
            }
        };
        let name = TableName::from_str(rv.relname.as_str()).map_err(|e| {
            BasinError::internal(format!("TRUNCATE: invalid table name {:?}: {e}", rv.relname))
        })?;
        tables.push(name);
    }

    if tables.is_empty() {
        return Err(BasinError::internal(
            "TRUNCATE: no table names found in statement",
        ));
    }

    // If a shard WAL is attached, flush it to Parquet so we see all
    // in-flight inserts before we capture the file list.
    if let Some(shard) = sess.engine.config().shard.as_ref() {
        shard.flush_to_parquet().await?;
    }

    let catalog = sess.engine.config().catalog.clone();
    let storage = sess.engine.config().storage.clone();

    for table in &tables {
        // Load current metadata (snapshot id + schema).
        let meta = catalog.load_table(&sess.project, table).await?;
        let schema = meta.schema.clone();

        // Collect all current data file paths — these become `removed_paths`.
        let data_files = storage
            .list_data_files_with_stats(&sess.project, table)
            .await?;

        let removed_paths: Vec<String> = data_files.iter().map(|f| f.path.as_ref().to_string()).collect();

        if removed_paths.is_empty() {
            // Nothing to do for this table — it's already empty.
            // Still refresh the session listing to be safe.
            refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, table).await?;
            continue;
        }

        // Commit an empty snapshot: remove all files, add none.
        // Retry once on CommitConflict (another writer may have raced us;
        // for TRUNCATE we don't care about the content — we want empty).
        let current_snap = meta.current_snapshot;
        match catalog
            .replace_data_files(&sess.project, table, current_snap, removed_paths.clone(), vec![])
            .await
        {
            Ok(_) => {}
            Err(BasinError::CommitConflict(_)) => {
                // Reload and retry with the fresh snapshot id.
                let fresh = catalog.load_table(&sess.project, table).await?;
                catalog
                    .replace_data_files(
                        &sess.project,
                        table,
                        fresh.current_snapshot,
                        removed_paths.clone(),
                        vec![],
                    )
                    .await?;
            }
            Err(e) => return Err(e),
        }

        // Best-effort physical deletion (mirrors DELETE's delete_objects).
        let store = storage.project_object_store(&sess.project);
        let root = storage.root_prefix_handle();
        let vector_columns: Vec<String> = schema
            .fields()
            .iter()
            .filter_map(|f| match f.data_type() {
                DataType::FixedSizeList(child, _)
                    if *child.data_type() == DataType::Float32 =>
                {
                    Some(f.name().clone())
                }
                _ => None,
            })
            .collect();

        for path_str in &removed_paths {
            let obj = ObjectStorePath::from(path_str.as_str());
            if let Err(e) = store.delete(&obj).await {
                tracing::warn!(path = %path_str, error = %e, "TRUNCATE: object delete failed");
            }
            // HNSW sidecar sweep.
            for col in &vector_columns {
                if let Some(sidecar_path) =
                    basin_storage::vector_index_segment_key_for_data_file(
                        root.as_ref(),
                        &sess.project,
                        table,
                        col,
                        path_str,
                    )
                {
                    if let Err(e) = store.delete(&sidecar_path).await {
                        tracing::debug!(path = %sidecar_path, error = %e, "TRUNCATE: sidecar delete failed (ok if missing)");
                    }
                }
            }
        }

        // RESTART IDENTITY: reset any SERIAL-backed sequences.
        if restart_seqs {
            restart_sequences_for_table(sess, table, &schema).await;
        }

        // Refresh the DataFusion session listing.
        refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, table).await?;
    }

    Ok(ExecResult::Empty {
        tag: "TRUNCATE".into(),
    })
}

/// Best-effort sequence restart for a table.
///
/// We detect SERIAL-backed columns by the `BASIN_COLUMN_DEFAULT = nextval('<seq>')`
/// field metadata and call `setval(seq, start - increment, advance=true)` so
/// the next `nextval` returns the original `start` value.
///
/// Errors from `setval` are swallowed (the table was already truncated; a
/// failed sequence restart is a nuisance but not a correctness bug).
async fn restart_sequences_for_table(
    sess: &ProjectSession,
    table: &TableName,
    schema: &arrow_schema::Schema,
) {
    let catalog = sess.engine.config().catalog.clone();

    for field in schema.fields() {
        let default_expr = match field.metadata().get(BASIN_COLUMN_DEFAULT) {
            Some(v) => v.clone(),
            None => continue,
        };

        // Pattern: `nextval('<seq_name>')`
        let seq_name = match extract_nextval_seq_name(&default_expr) {
            Some(n) => n,
            None => continue,
        };

        // Look up the sequence definition to find the start value.
        let def = match catalog.lookup_sequence(&sess.project, &seq_name).await {
            Some(d) => d,
            None => continue,
        };

        // PG's RESTART WITH start: setval(seq, start - increment, advance=true)
        // means the NEXT nextval() returns start.
        let reset_to = def.start.saturating_sub(def.increment);
        if let Err(e) = catalog
            .setval(&sess.project, &seq_name, reset_to, true)
            .await
        {
            tracing::warn!(
                table = %table,
                seq = %seq_name,
                error = %e,
                "TRUNCATE RESTART IDENTITY: setval failed"
            );
        }
    }
}

/// Extract the sequence name from a `nextval('<name>')` expression string.
fn extract_nextval_seq_name(expr: &str) -> Option<String> {
    // Accept: nextval('seq_name') or nextval("seq_name") with optional spaces.
    let lower = expr.trim().to_ascii_lowercase();
    if !lower.starts_with("nextval(") {
        return None;
    }
    let inner = lower["nextval(".len()..].trim();
    let inner = inner.strip_suffix(")")?;
    let inner = inner.trim();
    // Strip surrounding quotes (single or double).
    let name = inner
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .or_else(|| {
            inner
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
        })
        .unwrap_or(inner);
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}
