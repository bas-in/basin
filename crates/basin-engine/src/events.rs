//! Engine-side wiring for the [`ChangeEventSink`] trait.
//!
//! The trait + struct live in `basin-common::events`. This module owns:
//!
//! - The dispatch helpers the executor / dml_mutate paths call right
//!   before and after their respective catalog commits.
//! - [`TracingSink`], a trivial sink that logs each event via
//!   `tracing::info!`. Auto-attached when `BASIN_TRACE_CHANGE_EVENTS=1`.
//! - [`build_row_json`], an Arrow row -> `serde_json::Value` helper for
//!   constructing `before` / `after` payloads.
//!
//! The pre/post split mirrors PG's BEFORE / AFTER trigger semantics:
//! pre-commit sinks can abort the mutation; post-commit sinks are
//! fire-and-forget.

use std::sync::Arc;

use arrow_array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::DataType;
use async_trait::async_trait;
use basin_common::{
    BasinError, ChangeEvent, ChangeEventSink, ChangeOp, EventSinkRegistry, Result, TableName,
    TenantId,
};
use chrono::Utc;
use serde_json::{Map, Value};

use crate::Engine;

/// Run every pre-commit sink in registration order. The first sink to
/// return `Err` aborts the mutation; subsequent sinks are not called.
pub(crate) async fn dispatch_pre_commit(engine: &Engine, events: &[ChangeEvent]) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    let sinks: Vec<Arc<dyn ChangeEventSink>> = {
        let guard = engine
            .event_sinks()
            .read()
            .expect("event_sinks lock poisoned");
        if guard.pre_commit_is_empty() {
            return Ok(());
        }
        guard.pre_commit().to_vec()
    };
    for ev in events {
        for sink in &sinks {
            sink.publish(ev).await?;
        }
    }
    Ok(())
}

/// Fan post-commit sinks out via `tokio::spawn` per (sink, event). Errors
/// are warn-logged and dropped — by contract, post-commit failure does
/// not roll back the mutation.
pub(crate) fn dispatch_post_commit(engine: &Engine, events: Vec<ChangeEvent>) {
    if events.is_empty() {
        return;
    }
    let sinks: Vec<Arc<dyn ChangeEventSink>> = {
        let guard = engine
            .event_sinks()
            .read()
            .expect("event_sinks lock poisoned");
        if guard.post_commit_is_empty() {
            return;
        }
        guard.post_commit().to_vec()
    };
    for ev in events {
        let ev = Arc::new(ev);
        for sink in &sinks {
            let sink = sink.clone();
            let ev = ev.clone();
            tokio::spawn(async move {
                if let Err(e) = sink.publish(&ev).await {
                    tracing::warn!(
                        tenant = %ev.tenant,
                        table = %ev.table,
                        seq = ev.seq,
                        error = %e,
                        "post-commit change-event sink failed",
                    );
                }
            });
        }
    }
}

/// True iff at least one sink (either side) is currently attached. The
/// hot-path zero-overhead check.
pub(crate) fn registry_has_any(reg: &EventSinkRegistry) -> bool {
    !reg.pre_commit_is_empty() || !reg.post_commit_is_empty()
}

/// Materialize the events for a freshly-allocated `(seq, op, before, after)`
/// shape. Caller has already taken `next_event_seq` for each row.
pub(crate) fn make_event(
    tenant: &TenantId,
    table: &TableName,
    op: ChangeOp,
    before: Option<Value>,
    after: Option<Value>,
    seq: u64,
    causation_user: Option<String>,
) -> ChangeEvent {
    ChangeEvent {
        tenant: *tenant,
        table: table.clone(),
        op,
        before,
        after,
        committed_at: Utc::now(),
        seq,
        causation_user,
    }
}

/// Convert one row of a `RecordBatch` to a `serde_json::Object`. Covers
/// the scalar Arrow types the engine writes today; unknown types render
/// as `null` (correctness over fidelity — the trait's contract is "JSON
/// view," not "round-trippable Arrow").
pub(crate) fn build_row_json(batch: &RecordBatch, row: usize) -> Result<Value> {
    let schema = batch.schema();
    let mut map = Map::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        let field = schema.field(col_idx);
        let arr = batch.column(col_idx);
        let v = if arr.is_null(row) {
            Value::Null
        } else {
            scalar_at(arr.as_ref(), field.data_type(), row)?
        };
        map.insert(field.name().clone(), v);
    }
    Ok(Value::Object(map))
}

fn scalar_at(arr: &dyn Array, dt: &DataType, row: usize) -> Result<Value> {
    match dt {
        DataType::Int64 => Ok(Value::from(
            arr.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| BasinError::internal("event row: expected Int64Array"))?
                .value(row),
        )),
        DataType::Int32 => Ok(Value::from(
            arr.as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| BasinError::internal("event row: expected Int32Array"))?
                .value(row),
        )),
        DataType::Float64 => {
            let v = arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| BasinError::internal("event row: expected Float64Array"))?
                .value(row);
            Ok(serde_json::Number::from_f64(v)
                .map(Value::Number)
                .unwrap_or(Value::Null))
        }
        DataType::Boolean => Ok(Value::from(
            arr.as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| BasinError::internal("event row: expected BooleanArray"))?
                .value(row),
        )),
        DataType::Utf8 => Ok(Value::from(
            arr.as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| BasinError::internal("event row: expected StringArray"))?
                .value(row),
        )),
        // Anything we haven't taught explicitly renders as null. Sinks
        // that need fidelity for these types can extend this match
        // when they ship.
        _ => Ok(Value::Null),
    }
}

/// Trivial sink that logs each event at info level. Opt-in via
/// `BASIN_TRACE_CHANGE_EVENTS=1`.
pub struct TracingSink;

#[async_trait]
impl ChangeEventSink for TracingSink {
    async fn publish(&self, event: &ChangeEvent) -> Result<()> {
        tracing::info!(
            tenant = %event.tenant,
            table = %event.table,
            op = ?event.op,
            seq = event.seq,
            committed_at = %event.committed_at,
            user = ?event.causation_user,
            "change event",
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use basin_catalog::InMemoryCatalog;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use super::*;
    use crate::{Engine, EngineConfig, ExecResult, TenantSession};

    fn engine_in(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        })
    }

    /// Sink that captures every published event into a shared vector.
    struct CapturingSink {
        events: Arc<Mutex<Vec<ChangeEvent>>>,
    }

    #[async_trait]
    impl ChangeEventSink for CapturingSink {
        async fn publish(&self, event: &ChangeEvent) -> Result<()> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    /// Sink that always returns `Err`. Used to verify pre-commit
    /// rollback semantics.
    struct FailingSink;

    #[async_trait]
    impl ChangeEventSink for FailingSink {
        async fn publish(&self, _event: &ChangeEvent) -> Result<()> {
            Err(BasinError::internal("sink failed"))
        }
    }

    async fn seed_table(sess: &TenantSession) {
        sess.execute("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
            .await
            .unwrap();
    }

    fn select_count(batches: &[arrow_array::RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    #[tokio::test]
    async fn pre_commit_err_rolls_back_insert() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        eng.attach_pre_commit_sink(Arc::new(FailingSink));
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        seed_table(&sess).await;

        let err = sess
            .execute("INSERT INTO t VALUES (1, 'a')")
            .await
            .unwrap_err();
        assert!(matches!(err, BasinError::Internal(_)), "got {err:?}");

        let res = sess.execute("SELECT id FROM t").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(select_count(&batches), 0, "row should not be visible");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn post_commit_err_does_not_roll_back_insert() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        eng.attach_post_commit_sink(Arc::new(FailingSink));
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        seed_table(&sess).await;

        sess.execute("INSERT INTO t VALUES (1, 'a')")
            .await
            .expect("insert should succeed despite post-commit error");

        // Yield so the spawned post-commit task definitely runs.
        tokio::task::yield_now().await;

        let res = sess.execute("SELECT id FROM t").await.unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(select_count(&batches), 1, "row should be visible");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn captures_insert_update_delete_events() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let captured = Arc::new(Mutex::new(Vec::<ChangeEvent>::new()));
        eng.attach_pre_commit_sink(Arc::new(CapturingSink {
            events: captured.clone(),
        }));
        let tenant = TenantId::new();
        let sess = eng.open_session(tenant).await.unwrap();
        seed_table(&sess).await;

        sess.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
            .await
            .unwrap();
        sess.execute("UPDATE t SET name = 'B' WHERE id = 2")
            .await
            .unwrap();
        sess.execute("DELETE FROM t WHERE id = 1").await.unwrap();

        let events = captured.lock().unwrap().clone();
        assert_eq!(events.len(), 4, "expected 2 inserts + 1 update + 1 delete");

        // Two inserts.
        assert!(matches!(events[0].op, ChangeOp::Insert));
        assert!(events[0].before.is_none());
        assert_eq!(events[0].after.as_ref().unwrap()["id"].as_i64(), Some(1),);
        assert!(matches!(events[1].op, ChangeOp::Insert));
        assert_eq!(events[1].after.as_ref().unwrap()["id"].as_i64(), Some(2),);

        // One update.
        assert!(matches!(events[2].op, ChangeOp::Update));
        assert_eq!(
            events[2].before.as_ref().unwrap()["name"].as_str(),
            Some("b")
        );
        assert_eq!(
            events[2].after.as_ref().unwrap()["name"].as_str(),
            Some("B")
        );

        // One delete.
        assert!(matches!(events[3].op, ChangeOp::Delete));
        assert_eq!(events[3].before.as_ref().unwrap()["id"].as_i64(), Some(1),);
        assert!(events[3].after.is_none());

        // Seq monotonic per-tenant.
        for w in events.windows(2) {
            assert!(w[0].seq < w[1].seq, "seq should increase: {events:?}");
        }
        // All carry the right tenant.
        for ev in &events {
            assert_eq!(ev.tenant, tenant);
        }
    }

    #[tokio::test]
    async fn multi_tenant_seq_counters_independent() {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let captured = Arc::new(Mutex::new(Vec::<ChangeEvent>::new()));
        eng.attach_pre_commit_sink(Arc::new(CapturingSink {
            events: captured.clone(),
        }));

        let a = TenantId::new();
        let b = TenantId::new();
        let sa = eng.open_session(a).await.unwrap();
        let sb = eng.open_session(b).await.unwrap();
        for s in [&sa, &sb] {
            s.execute("CREATE TABLE t (id BIGINT NOT NULL, name TEXT NOT NULL)")
                .await
                .unwrap();
        }
        for i in 1..=5 {
            sa.execute(&format!("INSERT INTO t VALUES ({i}, 'a')"))
                .await
                .unwrap();
            sb.execute(&format!("INSERT INTO t VALUES ({i}, 'b')"))
                .await
                .unwrap();
        }

        let events = captured.lock().unwrap().clone();
        let a_seqs: Vec<u64> = events
            .iter()
            .filter(|e| e.tenant == a)
            .map(|e| e.seq)
            .collect();
        let b_seqs: Vec<u64> = events
            .iter()
            .filter(|e| e.tenant == b)
            .map(|e| e.seq)
            .collect();
        assert_eq!(a_seqs, vec![1, 2, 3, 4, 5]);
        assert_eq!(b_seqs, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn zero_sink_path_emits_nothing() {
        // Smoke check that a normal INSERT/UPDATE/DELETE roundtrip
        // succeeds with no sinks attached. The byte-identical guarantee
        // is best argued from the code (the registry-empty branch in
        // `build_*_events` returns before any allocation), but a pass
        // here proves the no-op path doesn't accidentally regress.
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = eng.open_session(TenantId::new()).await.unwrap();
        seed_table(&sess).await;
        sess.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
            .await
            .unwrap();
        sess.execute("UPDATE t SET name = 'B' WHERE id = 2")
            .await
            .unwrap();
        sess.execute("DELETE FROM t WHERE id = 1").await.unwrap();
        let res = sess
            .execute("SELECT id, name FROM t ORDER BY id")
            .await
            .unwrap();
        match res {
            ExecResult::Rows { batches, .. } => {
                assert_eq!(select_count(&batches), 1);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}
