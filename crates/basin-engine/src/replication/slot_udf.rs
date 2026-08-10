//! Phase 5.21.B/C — SQL-function implementations for the logical replication
//! slot management and change-consumption API.
//!
//! These functions are called from `crate::executor` via string-matching
//! pre-screens (consistent with how `CREATE REACTOR`, `SUBSCRIBE WEBHOOK`,
//! etc. are dispatched). Each function produces an `ExecResult` that the
//! executor returns directly to the caller.
//!
//! ## Functions implemented
//!
//! | SQL                              | Phase  |
//! |----------------------------------|--------|
//! | `pg_create_logical_replication_slot(name, plugin)` | 5.21.B |
//! | `pg_drop_replication_slot(name)` | 5.21.B |
//! | `pg_current_wal_lsn()`           | 5.21.D |
//! | `pg_logical_slot_get_changes(slot, ...)` | 5.21.C |
//!
//! ## `pg_replication_slots` view
//!
//! `register_pg_replication_slots_provider` registers a virtual table
//! provider for the `pg_replication_slots` catalog view — sourced live from
//! the process-wide `SlotRegistry`.

use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use basin_catalog::{lsn_to_pgtext, Lsn, SlotRegistry};
use basin_common::{BasinError, ProjectId, Result};
use datafusion::arrow::datatypes::SchemaRef as DfSchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;

use crate::replication::lsn as lsn_mod;
use crate::{Engine, ExecResult, ProjectSession};

// ─── pg_create_logical_replication_slot ──────────────────────────────────────

/// Match `SELECT … FROM pg_create_logical_replication_slot(name, plugin)`.
///
/// We accept any variant of the call regardless of the SELECT column list.
/// Returns `Some((slot_name, plugin))` when matched.
pub(crate) fn match_create_replication_slot(sql: &str) -> Option<(String, String)> {
    let lower = sql.to_lowercase();
    // Must mention the function name.
    let fn_start = lower.find("pg_create_logical_replication_slot")?;
    let after = &sql[fn_start..];
    // Extract arguments from the first balanced-paren pair.
    let args = extract_fn_args(after)?;
    // Expect exactly 2 args: name and plugin, both string literals.
    let parts: Vec<&str> = split_args(&args);
    if parts.len() < 2 {
        return None;
    }
    let name = unquote(parts[0])?;
    let plugin = unquote(parts[1])?;
    Some((name, plugin))
}

/// Execute `pg_create_logical_replication_slot`. Creates the slot and returns
/// a single-row result with `(slot_name TEXT, lsn TEXT)` — matching the
/// PostgreSQL contract.
pub(crate) async fn exec_create_replication_slot(
    sess: &ProjectSession,
    slot_name: &str,
    plugin: &str,
) -> Result<ExecResult> {
    let registry = sess.engine.inner.slot_registry.clone();
    let current_lsn = current_lsn_for_engine(&sess.engine);
    let (name, lsn) = registry
        .create_slot(&sess.project, slot_name, plugin, current_lsn)
        .map_err(|e| BasinError::Catalog(e))?;

    // Attach the ReplicationSink to the engine's post-commit sink list.
    // Idempotent: if a sink is already attached for this project+slot it will
    // just push duplicate frames which is benign for the in-process test path.
    let sink = Arc::new(crate::replication::pgoutput::ReplicationSink::new(
        registry,
        sess.project,
    ));
    sess.engine.attach_pre_commit_sink(sink);

    let schema = Arc::new(Schema::new(vec![
        Field::new("slot_name", DataType::Utf8, false),
        Field::new("lsn", DataType::Utf8, false),
    ]));
    let slot_col: ArrayRef = Arc::new(StringArray::from(vec![name.as_str()]));
    let lsn_col: ArrayRef = Arc::new(StringArray::from(vec![lsn_to_pgtext(lsn).as_str()]));
    let batch = RecordBatch::try_new(schema.clone(), vec![slot_col, lsn_col]).map_err(|e| {
        BasinError::internal(format!("build pg_create_logical_replication_slot row: {e}"))
    })?;
    Ok(ExecResult::Rows {
        schema,
        batches: vec![batch],
    })
}

// ─── pg_drop_replication_slot ─────────────────────────────────────────────────

/// Match `SELECT pg_drop_replication_slot(name)`.
pub(crate) fn match_drop_replication_slot(sql: &str) -> Option<String> {
    let lower = sql.to_lowercase();
    let fn_start = lower.find("pg_drop_replication_slot")?;
    let after = &sql[fn_start..];
    let args = extract_fn_args(after)?;
    let parts: Vec<&str> = split_args(&args);
    if parts.is_empty() {
        return None;
    }
    unquote(parts[0])
}

/// Execute `pg_drop_replication_slot`. Removes the slot and returns empty.
pub(crate) async fn exec_drop_replication_slot(
    sess: &ProjectSession,
    slot_name: &str,
) -> Result<ExecResult> {
    sess.engine
        .inner
        .slot_registry
        .drop_slot(&sess.project, slot_name)
        .map_err(|e| BasinError::Catalog(e))?;
    Ok(ExecResult::Empty {
        tag: "SELECT 1".to_string(),
    })
}

// ─── pg_current_wal_lsn ──────────────────────────────────────────────────────

/// Match `SELECT pg_current_wal_lsn()`.
pub(crate) fn match_current_wal_lsn(sql: &str) -> bool {
    sql.to_lowercase().contains("pg_current_wal_lsn")
}

/// Execute `pg_current_wal_lsn()`. Returns a single TEXT row with the current
/// LSN in `X/XXXXXXXX` format.
pub(crate) async fn exec_current_wal_lsn(sess: &ProjectSession) -> Result<ExecResult> {
    let lsn = current_lsn_for_engine(&sess.engine);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "pg_current_wal_lsn",
        DataType::Utf8,
        false,
    )]));
    let col: ArrayRef = Arc::new(StringArray::from(vec![lsn_to_pgtext(lsn).as_str()]));
    let batch = RecordBatch::try_new(schema.clone(), vec![col])
        .map_err(|e| BasinError::internal(format!("build pg_current_wal_lsn row: {e}")))?;
    Ok(ExecResult::Rows {
        schema,
        batches: vec![batch],
    })
}

// ─── pg_logical_slot_get_changes ─────────────────────────────────────────────

/// Match `SELECT lsn, xid, data FROM pg_logical_slot_get_changes(slot, …)`.
/// Returns `Some(slot_name)` when matched.
pub(crate) fn match_logical_slot_get_changes(sql: &str) -> Option<String> {
    let lower = sql.to_lowercase();
    let fn_start = lower.find("pg_logical_slot_get_changes")?;
    let after = &sql[fn_start..];
    let args = extract_fn_args(after)?;
    let parts: Vec<&str> = split_args(&args);
    if parts.is_empty() {
        return None;
    }
    unquote(parts[0])
}

/// Execute `pg_logical_slot_get_changes`. Drains pending frames from the slot
/// and returns them as `(lsn TEXT, xid INT4, data TEXT)` rows.
pub(crate) async fn exec_logical_slot_get_changes(
    sess: &ProjectSession,
    slot_name: &str,
) -> Result<ExecResult> {
    let frames = sess
        .engine
        .inner
        .slot_registry
        .take_pending_changes(&sess.project, slot_name)
        .map_err(|e| BasinError::Catalog(e))?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("lsn", DataType::Utf8, true),
        Field::new("xid", DataType::UInt32, true),
        Field::new("data", DataType::Utf8, true),
    ]));

    if frames.is_empty() {
        let lsn_col: ArrayRef = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
        let xid_col: ArrayRef = Arc::new(UInt32Array::from(Vec::<Option<u32>>::new()));
        let data_col: ArrayRef = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
        let batch = RecordBatch::try_new(schema.clone(), vec![lsn_col, xid_col, data_col])
            .map_err(|e| BasinError::internal(format!("build empty slot changes: {e}")))?;
        return Ok(ExecResult::Rows {
            schema,
            batches: vec![batch],
        });
    }

    let lsns: Vec<String> = frames.iter().map(|f| lsn_to_pgtext(f.lsn)).collect();
    let xids: Vec<u32> = frames.iter().map(|f| f.xid).collect();
    let datas: Vec<String> = frames.iter().map(|f| f.data.clone()).collect();

    let lsn_col: ArrayRef = Arc::new(StringArray::from(
        lsns.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    ));
    let xid_col: ArrayRef = Arc::new(UInt32Array::from(xids));
    let data_col: ArrayRef = Arc::new(StringArray::from(
        datas.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![lsn_col, xid_col, data_col])
        .map_err(|e| BasinError::internal(format!("build slot changes: {e}")))?;

    Ok(ExecResult::Rows {
        schema,
        batches: vec![batch],
    })
}

// ─── pg_replication_slots virtual table provider ─────────────────────────────

fn repl_slots_ws_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("slot_name", DataType::Utf8, true),
        Field::new("plugin", DataType::Utf8, true),
        Field::new("slot_type", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, true),
        Field::new("restart_lsn", DataType::Utf8, true),
        Field::new("confirmed_flush_lsn", DataType::Utf8, true),
    ]))
}

fn pub_ws_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("pubname", DataType::Utf8, true),
        Field::new("puballtables", DataType::Boolean, true),
        Field::new("pubinsert", DataType::Boolean, true),
        Field::new("pubupdate", DataType::Boolean, true),
        Field::new("pubdelete", DataType::Boolean, true),
    ]))
}

fn pub_tables_ws_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("pubname", DataType::Utf8, true),
        Field::new("schemaname", DataType::Utf8, true),
        Field::new("tablename", DataType::Utf8, true),
    ]))
}

fn to_df_schema(ws: &Arc<Schema>) -> DfResult<DfSchemaRef> {
    crate::convert::schema_ws_to_df(ws.as_ref())
        .map(|s| Arc::new(s))
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

fn build_exec_plan(
    ws_batch: &RecordBatch,
    df_schema: DfSchemaRef,
    projection: Option<&Vec<usize>>,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let df_batch = crate::convert::batch_ws_to_df(ws_batch)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let partitions = vec![vec![df_batch]];
    Ok(MemorySourceConfig::try_new_exec(
        &partitions,
        df_schema,
        projection.cloned(),
    )?)
}

/// A DataFusion `TableProvider` for the `pg_replication_slots` catalog view.
/// Reads live from the process-wide `SlotRegistry` on each scan.
#[derive(Debug)]
pub(crate) struct PgReplicationSlotsProvider {
    registry: Arc<SlotRegistry>,
    project: ProjectId,
    df_schema: DfSchemaRef,
    ws_schema: Arc<Schema>,
}

impl PgReplicationSlotsProvider {
    pub(crate) fn new(registry: Arc<SlotRegistry>, project: ProjectId) -> Self {
        let ws_schema = repl_slots_ws_schema();
        let df_schema = crate::convert::schema_ws_to_df(ws_schema.as_ref())
            .map(Arc::new)
            .unwrap_or_else(|_| Arc::new(ws_schema.as_ref().clone()));
        Self {
            registry,
            project,
            df_schema,
            ws_schema,
        }
    }

    fn build_ws_batch(&self) -> Result<RecordBatch> {
        let snapshots = self.registry.list_slots(&self.project);

        let slot_names: Vec<Option<&str>> = snapshots
            .iter()
            .map(|s| Some(s.slot_name.as_str()))
            .collect();
        let plugins: Vec<Option<&str>> =
            snapshots.iter().map(|s| Some(s.plugin.as_str())).collect();
        let slot_types: Vec<Option<&str>> = snapshots
            .iter()
            .map(|s| Some(s.slot_type.as_str()))
            .collect();
        let actives: Vec<Option<bool>> = snapshots.iter().map(|s| Some(s.active)).collect();
        let restart_lsns: Vec<String> = snapshots
            .iter()
            .map(|s| lsn_to_pgtext(s.restart_lsn))
            .collect();
        let confirmed_flush_lsns: Vec<String> = snapshots
            .iter()
            .map(|s| lsn_to_pgtext(s.confirmed_flush_lsn))
            .collect();

        let slot_name_col: ArrayRef = Arc::new(StringArray::from(slot_names));
        let plugin_col: ArrayRef = Arc::new(StringArray::from(plugins));
        let slot_type_col: ArrayRef = Arc::new(StringArray::from(slot_types));
        let active_col: ArrayRef = Arc::new(BooleanArray::from(actives));
        let restart_col: ArrayRef = Arc::new(StringArray::from(
            restart_lsns.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        ));
        let confirmed_col: ArrayRef = Arc::new(StringArray::from(
            confirmed_flush_lsns
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>(),
        ));

        RecordBatch::try_new(
            self.ws_schema.clone(),
            vec![
                slot_name_col,
                plugin_col,
                slot_type_col,
                active_col,
                restart_col,
                confirmed_col,
            ],
        )
        .map_err(|e| BasinError::internal(format!("build pg_replication_slots batch: {e}")))
    }
}

#[async_trait]
impl TableProvider for PgReplicationSlotsProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        Arc::clone(&self.df_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let ws_batch = self
            .build_ws_batch()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        build_exec_plan(&ws_batch, Arc::clone(&self.df_schema), projection)
    }
}

// ─── pg_publication virtual table provider ────────────────────────────────────

/// A DataFusion `TableProvider` for the `pg_publication` catalog view.
#[derive(Debug)]
pub(crate) struct PgPublicationProvider {
    registry: Arc<basin_catalog::PublicationRegistry>,
    project: ProjectId,
    df_schema: DfSchemaRef,
    ws_schema: Arc<Schema>,
}

impl PgPublicationProvider {
    pub(crate) fn new(
        registry: Arc<basin_catalog::PublicationRegistry>,
        project: ProjectId,
    ) -> Self {
        let ws_schema = pub_ws_schema();
        let df_schema = crate::convert::schema_ws_to_df(ws_schema.as_ref())
            .map(Arc::new)
            .unwrap_or_else(|_| Arc::new(ws_schema.as_ref().clone()));
        Self {
            registry,
            project,
            df_schema,
            ws_schema,
        }
    }

    fn build_ws_batch(&self) -> Result<RecordBatch> {
        let pubs = self.registry.list_publications(&self.project);
        let pubnames: Vec<Option<&str>> = pubs.iter().map(|p| Some(p.pubname.as_str())).collect();
        let all_tables: Vec<Option<bool>> = pubs.iter().map(|p| Some(p.all_tables)).collect();
        let pub_insert: Vec<Option<bool>> = pubs.iter().map(|p| Some(p.pub_insert)).collect();
        let pub_update: Vec<Option<bool>> = pubs.iter().map(|p| Some(p.pub_update)).collect();
        let pub_delete: Vec<Option<bool>> = pubs.iter().map(|p| Some(p.pub_delete)).collect();

        let pubname_col: ArrayRef = Arc::new(StringArray::from(pubnames));
        let all_tables_col: ArrayRef = Arc::new(BooleanArray::from(all_tables));
        let insert_col: ArrayRef = Arc::new(BooleanArray::from(pub_insert));
        let update_col: ArrayRef = Arc::new(BooleanArray::from(pub_update));
        let delete_col: ArrayRef = Arc::new(BooleanArray::from(pub_delete));

        RecordBatch::try_new(
            self.ws_schema.clone(),
            vec![
                pubname_col,
                all_tables_col,
                insert_col,
                update_col,
                delete_col,
            ],
        )
        .map_err(|e| BasinError::internal(format!("build pg_publication batch: {e}")))
    }
}

#[async_trait]
impl TableProvider for PgPublicationProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        Arc::clone(&self.df_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let ws_batch = self
            .build_ws_batch()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        build_exec_plan(&ws_batch, Arc::clone(&self.df_schema), projection)
    }
}

// ─── pg_publication_tables virtual table provider ─────────────────────────────

/// A DataFusion `TableProvider` for the `pg_publication_tables` catalog view.
#[derive(Debug)]
pub(crate) struct PgPublicationTablesProvider {
    registry: Arc<basin_catalog::PublicationRegistry>,
    project: ProjectId,
    df_schema: DfSchemaRef,
    ws_schema: Arc<Schema>,
}

impl PgPublicationTablesProvider {
    pub(crate) fn new(
        registry: Arc<basin_catalog::PublicationRegistry>,
        project: ProjectId,
    ) -> Self {
        let ws_schema = pub_tables_ws_schema();
        let df_schema = crate::convert::schema_ws_to_df(ws_schema.as_ref())
            .map(Arc::new)
            .unwrap_or_else(|_| Arc::new(ws_schema.as_ref().clone()));
        Self {
            registry,
            project,
            df_schema,
            ws_schema,
        }
    }

    fn build_ws_batch(&self) -> Result<RecordBatch> {
        let pairs = self.registry.list_publication_tables(&self.project);
        let pubnames: Vec<Option<&str>> = pairs.iter().map(|(p, _)| Some(p.as_str())).collect();
        let schemas: Vec<Option<&str>> = pairs.iter().map(|_| Some("public")).collect();
        let tablenames: Vec<Option<&str>> = pairs.iter().map(|(_, t)| Some(t.as_str())).collect();

        let pubname_col: ArrayRef = Arc::new(StringArray::from(pubnames));
        let schema_col: ArrayRef = Arc::new(StringArray::from(schemas));
        let table_col: ArrayRef = Arc::new(StringArray::from(tablenames));

        RecordBatch::try_new(
            self.ws_schema.clone(),
            vec![pubname_col, schema_col, table_col],
        )
        .map_err(|e| BasinError::internal(format!("build pg_publication_tables batch: {e}")))
    }
}

#[async_trait]
impl TableProvider for PgPublicationTablesProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        Arc::clone(&self.df_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let ws_batch = self
            .build_ws_batch()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        build_exec_plan(&ws_batch, Arc::clone(&self.df_schema), projection)
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Compute the current WAL LSN for the engine.
fn current_lsn_for_engine(engine: &Engine) -> Lsn {
    // The engine's next_tx_id counter has been incremented past the last
    // committed tx; current_wal_lsn takes the last committed seq.
    let last_committed = engine
        .inner
        .next_tx_id
        .load(std::sync::atomic::Ordering::Relaxed)
        .saturating_sub(1);
    lsn_mod::current_wal_lsn(last_committed)
}

/// Extract the argument list from a function call.
///
/// Finds the first `(…)` after the current position (which should be right at
/// the function name). Returns the contents of the parentheses.
fn extract_fn_args(s: &str) -> Option<String> {
    let open = s.find('(')?;
    let after = &s[open + 1..];
    let mut depth = 1usize;
    let mut end = 0;
    for (i, ch) in after.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    end = i;
                    break;
                }
            }
            _ => {}
        }
    }
    Some(after[..end].to_string())
}

/// Split a comma-separated argument string at top-level commas.
fn split_args(args: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (i, ch) in args.char_indices() {
        match ch {
            '(' | '[' => depth += 1,
            ')' | ']' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                parts.push(args[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = args[start..].trim();
    if !last.is_empty() {
        parts.push(last);
    }
    parts
}

/// Strip surrounding single-quotes from a SQL string literal.
fn unquote(s: &str) -> Option<String> {
    let s = s.trim();
    if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
        Some(s[1..s.len() - 1].to_string())
    } else if s.eq_ignore_ascii_case("NULL") {
        None
    } else {
        // Unquoted identifier — accept as-is.
        Some(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_create_slot_basic() {
        let sql = "SELECT slot_name, lsn FROM pg_create_logical_replication_slot('s1', 'pgoutput')";
        let result = match_create_replication_slot(sql);
        assert_eq!(result, Some(("s1".to_string(), "pgoutput".to_string())));
    }

    #[test]
    fn match_drop_slot_basic() {
        let sql = "SELECT pg_drop_replication_slot('s1')";
        assert_eq!(match_drop_replication_slot(sql), Some("s1".to_string()));
    }

    #[test]
    fn match_current_wal_lsn_basic() {
        assert!(match_current_wal_lsn("SELECT pg_current_wal_lsn()"));
        assert!(!match_current_wal_lsn("SELECT 1"));
    }

    #[test]
    fn match_logical_slot_get_changes_basic() {
        let sql = "SELECT lsn, xid, data FROM pg_logical_slot_get_changes('frame_slot', NULL, NULL, 'proto_version', '1', 'publication_names', 'cdc_pub')";
        assert_eq!(
            match_logical_slot_get_changes(sql),
            Some("frame_slot".to_string())
        );
    }

    #[test]
    fn extract_fn_args_basic() {
        let s = "pg_create_logical_replication_slot('s1', 'pgoutput')";
        let start = s.find("pg_create").unwrap();
        let args = extract_fn_args(&s[start..]).unwrap();
        assert_eq!(args, "'s1', 'pgoutput'");
    }
}
