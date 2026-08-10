//! Glue point for `basin-cron` — `cron.schedule` / `cron.unschedule` scalar UDFs.
//!
//! `basin-cron` depends on `basin-engine`. These UDFs replicate cron table ops
//! (INSERT/DELETE on `cron_job`) without introducing a circular dep, using
//! independent engine sessions per call. The SQL surface is rewritten via
//! [`rewrite_cron_schema_functions`] before DataFusion sees the SQL.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

use basin_common::ProjectId;

use crate::Engine;

/// Hook called once from [`crate::Engine::new`]. No-op; registration happens
/// per-session via [`register_cron_udfs`].
#[inline]
pub(crate) fn install() {}

/// Register `cron_schedule` and `cron_unschedule` UDFs on `ctx`. Both capture
/// `engine` and `project` so table ops run in an independent session.
pub(crate) fn register_cron_udfs(ctx: &SessionContext, engine: Engine, project: ProjectId) {
    let eng = Arc::new(engine);
    ctx.register_udf(ScalarUDF::from(CronScheduleUdf {
        engine: eng.clone(),
        project,
        signature: Signature::one_of(
            vec![TypeSignature::Exact(vec![
                DataType::Utf8,
                DataType::Utf8,
                DataType::Utf8,
            ])],
            Volatility::Volatile,
        ),
    }));
    ctx.register_udf(ScalarUDF::from(CronUnscheduleUdf {
        engine: eng,
        project,
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Volatile),
    }));
}

/// Rewrite `cron.schedule(...)` / `cron.unschedule(...)` to underscore forms.
/// Case-insensitive, identifier-boundary checked (mirrors auth schema rewrite).
pub(crate) fn rewrite_cron_schema_functions(sql: &str) -> String {
    const TARGETS: &[(&str, &str)] = &[
        ("cron.schedule", "cron_schedule"),
        ("cron.unschedule", "cron_unschedule"),
    ];
    rewrite_schema_dot(sql, TARGETS)
}

/// Generic schema-dot rewriter (shared by cron and net modules).
pub(crate) fn rewrite_schema_dot(sql: &str, targets: &[(&str, &str)]) -> String {
    let mut out = sql.to_string();
    for (schema_dot_name, replacement) in targets {
        let mut scan_pos = 0usize;
        loop {
            let lower = out.to_ascii_lowercase();
            let Some(rel_found) = lower[scan_pos..].find(schema_dot_name) else {
                break;
            };
            let abs_start = scan_pos + rel_found;
            let abs_end = abs_start + schema_dot_name.len();
            let pre_ok = abs_start == 0 || {
                let prev = out.as_bytes()[abs_start - 1];
                !(prev.is_ascii_alphanumeric() || prev == b'_')
            };
            let post_ok = abs_end >= out.len() || {
                let next = out.as_bytes()[abs_end];
                !(next.is_ascii_alphanumeric() || next == b'_')
            };
            if pre_ok && post_ok {
                out.replace_range(abs_start..abs_end, replacement);
                scan_pos = abs_start + replacement.len();
            } else {
                scan_pos = abs_start + 1;
            }
        }
    }
    out
}

// cron_schedule(name TEXT, schedule TEXT, command TEXT) -> BIGINT
struct CronScheduleUdf {
    engine: Arc<Engine>,
    project: ProjectId,
    signature: Signature,
}
impl std::fmt::Debug for CronScheduleUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CronScheduleUdf({})", self.project)
    }
}
impl PartialEq for CronScheduleUdf {
    fn eq(&self, o: &Self) -> bool {
        self.project == o.project && Arc::ptr_eq(&self.engine, &o.engine)
    }
}
impl Eq for CronScheduleUdf {}
impl std::hash::Hash for CronScheduleUdf {
    fn hash<H: std::hash::Hasher>(&self, s: &mut H) {
        self.project.hash(s);
    }
}
impl ScalarUDFImpl for CronScheduleUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "cron_schedule" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> { Ok(DataType::Int64) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, number_rows, .. } = args;
        if args.len() != 3 {
            return Err(DataFusionError::Execution(format!(
                "cron_schedule expects 3 args, got {}", args.len())));
        }
        let name = cron_scalar_utf8(&args[0], "name")?;
        let schedule = cron_scalar_utf8(&args[1], "schedule")?;
        let command = cron_scalar_utf8(&args[2], "command")?;
        let engine = self.engine.clone();
        let project = self.project;
        let jobid = tokio::runtime::Handle::current()
            .block_on(async move {
                cron_schedule_impl(&engine, &project, &name, &schedule, &command).await
            })
            .map_err(DataFusionError::Execution)?;
        Ok(ColumnarValue::Array(Arc::new(Int64Array::from(vec![jobid; number_rows]))))
    }
}

// cron_unschedule(name TEXT) -> BIGINT
struct CronUnscheduleUdf {
    engine: Arc<Engine>,
    project: ProjectId,
    signature: Signature,
}
impl std::fmt::Debug for CronUnscheduleUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CronUnscheduleUdf({})", self.project)
    }
}
impl PartialEq for CronUnscheduleUdf {
    fn eq(&self, o: &Self) -> bool {
        self.project == o.project && Arc::ptr_eq(&self.engine, &o.engine)
    }
}
impl Eq for CronUnscheduleUdf {}
impl std::hash::Hash for CronUnscheduleUdf {
    fn hash<H: std::hash::Hasher>(&self, s: &mut H) {
        self.project.hash(s);
    }
}
impl ScalarUDFImpl for CronUnscheduleUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "cron_unschedule" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> { Ok(DataType::Int64) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, number_rows, .. } = args;
        if args.len() != 1 {
            return Err(DataFusionError::Execution(format!(
                "cron_unschedule expects 1 arg, got {}", args.len())));
        }
        let name = cron_scalar_utf8(&args[0], "name")?;
        let engine = self.engine.clone();
        let project = self.project;
        let jobid = tokio::runtime::Handle::current()
            .block_on(async move { cron_unschedule_impl(&engine, &project, &name).await })
            .map_err(DataFusionError::Execution)?;
        Ok(ColumnarValue::Array(Arc::new(Int64Array::from(vec![jobid; number_rows]))))
    }
}

/// Canonical job table name (Phase 5.18.C: `cron.job` → bare `job` in the
/// flat catalog). Falls back to [`LEGACY_CRON_JOB_TABLE`] when the legacy
/// table exists but the canonical one does not.
const CRON_JOB_TABLE: &str = "job";

/// Legacy flat table name used before Phase 5.18.C.
const LEGACY_CRON_JOB_TABLE: &str = "cron_job";

fn validate_cron_expr(expr: &str) -> Result<(), String> {
    let trimmed = expr.trim();
    let count = trimmed.split_whitespace().count();
    let normalized = if count == 5 { format!("0 {trimmed}") } else { trimmed.to_string() };
    use std::str::FromStr;
    cron::Schedule::from_str(&normalized)
        .map(|_| ())
        .map_err(|e| format!("invalid cron expression {expr:?}: {e}"))
}

/// Returns the effective job table name to use for this project.
///
/// Phase 5.18.C: tries the canonical name (`job`) first; falls back to the
/// legacy name (`cron_job`) when only the legacy table is present.
async fn effective_cron_job_table(engine: &Engine, project: &ProjectId) -> Result<&'static str, String> {
    use crate::ExecResult;
    let sess = engine.open_session(*project).await.map_err(|e| e.to_string())?;
    let res = sess.execute("SHOW TABLES").await.map_err(|e| e.to_string())?;
    let names: Vec<String> = match res {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                if let Some(arr) = b.column_by_name("table_name")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                {
                    for i in 0..arr.len() {
                        out.push(arr.value(i).to_string());
                    }
                }
            }
            out
        }
        ExecResult::Empty { .. } => Vec::new(),
    };
    if names.iter().any(|n| n == CRON_JOB_TABLE) {
        Ok(CRON_JOB_TABLE)
    } else {
        Ok(LEGACY_CRON_JOB_TABLE)
    }
}

async fn ensure_cron_table(engine: &Engine, project: &ProjectId) -> Result<(), String> {
    use crate::ExecResult;
    let sess = engine.open_session(*project).await.map_err(|e| e.to_string())?;
    let res = sess.execute("SHOW TABLES").await.map_err(|e| e.to_string())?;
    let names: Vec<String> = match res {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                if let Some(arr) = b.column_by_name("table_name")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                {
                    for i in 0..arr.len() {
                        out.push(arr.value(i).to_string());
                    }
                }
            }
            out
        }
        ExecResult::Empty { .. } => Vec::new(),
    };
    // Neither the canonical nor the legacy table exists — create the canonical.
    let has_canonical = names.iter().any(|n| n == CRON_JOB_TABLE);
    let has_legacy = names.iter().any(|n| n == LEGACY_CRON_JOB_TABLE);
    if !has_canonical && !has_legacy {
        sess.execute(&format!(
            "CREATE TABLE {CRON_JOB_TABLE} (
                jobid BIGINT NOT NULL, jobname TEXT NOT NULL, schedule TEXT NOT NULL,
                command TEXT NOT NULL, active BIGINT NOT NULL, username TEXT NOT NULL,
                last_run_unix_ms BIGINT, last_status TEXT)"
        )).await.map_err(|e| e.to_string())?;
    }
    Ok(())
}

async fn cron_schedule_impl(
    engine: &Engine, project: &ProjectId, name: &str, schedule: &str, command: &str,
) -> Result<i64, String> {
    validate_cron_expr(schedule)?;
    ensure_cron_table(engine, project).await?;
    let job_tbl = effective_cron_job_table(engine, project).await?;
    use crate::ExecResult;
    use datafusion::arrow::array::Int64Array as ArrowI64;
    let sess = engine.open_session(*project).await.map_err(|e| e.to_string())?;
    let res = sess
        .execute(&format!("SELECT jobid, jobname FROM {job_tbl} ORDER BY jobid"))
        .await.map_err(|e| e.to_string())?;
    let mut max_jobid = 0i64;
    let mut name_exists = false;
    if let ExecResult::Rows { batches, .. } = res {
        for b in &batches {
            let ids = b.column_by_name("jobid").and_then(|c| c.as_any().downcast_ref::<ArrowI64>());
            let names = b.column_by_name("jobname").and_then(|c| c.as_any().downcast_ref::<StringArray>());
            if let (Some(ids), Some(names)) = (ids, names) {
                for i in 0..b.num_rows() {
                    if ids.value(i) > max_jobid { max_jobid = ids.value(i); }
                    if names.value(i) == name { name_exists = true; }
                }
            }
        }
    }
    if name_exists { return Err(format!("job {name:?} already exists")); }
    let jobid = max_jobid + 1;
    sess.execute(&format!(
        "INSERT INTO {job_tbl} VALUES ({jobid}, {}, {}, {}, 1, 'current_user', NULL, NULL)",
        sql_text(name), sql_text(schedule), sql_text(command),
    )).await.map_err(|e| e.to_string())?;
    Ok(jobid)
}

async fn cron_unschedule_impl(engine: &Engine, project: &ProjectId, name: &str) -> Result<i64, String> {
    ensure_cron_table(engine, project).await?;
    let job_tbl = effective_cron_job_table(engine, project).await?;
    use crate::ExecResult;
    use datafusion::arrow::array::Int64Array as ArrowI64;
    let sess = engine.open_session(*project).await.map_err(|e| e.to_string())?;
    let res = sess
        .execute(&format!("SELECT jobid FROM {job_tbl} WHERE jobname = {}", sql_text(name)))
        .await.map_err(|e| e.to_string())?;
    let jobid = match res {
        ExecResult::Rows { batches, .. } => {
            let mut found: Option<i64> = None;
            'outer: for b in &batches {
                if let Some(arr) = b.column_by_name("jobid").and_then(|c| c.as_any().downcast_ref::<ArrowI64>()) {
                    // First row of the first batch that carries the column wins;
                    // jobname is unique so there is at most one match anyway. An
                    // empty batch falls through to the next one rather than
                    // ending the search.
                    if !arr.is_empty() { found = Some(arr.value(0)); break 'outer; }
                }
            }
            found.ok_or_else(|| format!("job {name:?} not found"))?
        }
        ExecResult::Empty { .. } => return Err(format!("job {name:?} not found")),
    };
    sess.execute(&format!("DELETE FROM {job_tbl} WHERE jobid = {jobid}"))
        .await.map_err(|e| e.to_string())?;
    Ok(jobid)
}

fn sql_text(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn cron_scalar_utf8(col: &ColumnarValue, name: &str) -> DFResult<String> {
    match col {
        ColumnarValue::Scalar(sv) => match sv {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => Ok(s.clone()),
            ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Utf8View(None) =>
                Err(DataFusionError::Execution(format!("cron arg {name} must not be NULL"))),
            other => Err(DataFusionError::Execution(format!("cron arg {name} expected TEXT, got {other:?}"))),
        },
        ColumnarValue::Array(arr) => {
            use datafusion::arrow::array::Array;
            let arr = arr.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Execution(format!("cron arg {name} expected TEXT array")))?;
            if arr.is_null(0) {
                return Err(DataFusionError::Execution(format!("cron arg {name} must not be NULL")));
            }
            Ok(arr.value(0).to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrite_cron_schedule() {
        let r = rewrite_cron_schema_functions("SELECT cron.schedule('j', '* * * * *', 'SELECT 1')");
        assert_eq!(r, "SELECT cron_schedule('j', '* * * * *', 'SELECT 1')");
    }
    #[test]
    fn rewrite_cron_unschedule() {
        assert_eq!(rewrite_cron_schema_functions("SELECT cron.unschedule('j')"), "SELECT cron_unschedule('j')");
    }
    #[test]
    fn rewrite_cron_case_insensitive() {
        assert_eq!(rewrite_cron_schema_functions("SELECT CRON.SCHEDULE('j', '* * * * *', 'S')"), "SELECT cron_schedule('j', '* * * * *', 'S')");
    }
    #[test]
    fn rewrite_cron_leaves_non_matching() {
        assert_eq!(rewrite_cron_schema_functions("SELECT my_cron.schedule('j')"), "SELECT my_cron.schedule('j')");
    }

    // ── Phase 5.18.C back-compat table name constants ─────────────────────

    #[test]
    fn canonical_job_table_is_bare_name() {
        // The canonical name should be bare `job` (not `cron_job`) to reflect
        // the `cron` schema. The flat catalog stores it without the schema prefix.
        assert_eq!(CRON_JOB_TABLE, "job");
    }

    #[test]
    fn legacy_job_table_has_schema_prefix() {
        // The legacy name must include the `cron_` prefix so back-compat
        // resolution can distinguish it from the canonical name.
        assert_eq!(LEGACY_CRON_JOB_TABLE, "cron_job");
        assert!(LEGACY_CRON_JOB_TABLE.starts_with("cron_"));
    }

    #[test]
    fn canonical_and_legacy_cron_tables_differ() {
        assert_ne!(CRON_JOB_TABLE, LEGACY_CRON_JOB_TABLE);
    }
}
