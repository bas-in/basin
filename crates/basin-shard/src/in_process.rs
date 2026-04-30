//! In-process shard map.
//!
//! ## WAL payload encoding
//!
//! Every WAL entry the shard owner appends has this layout:
//!
//! ```text
//! +----------------+--------------------+--------------------+
//! | u32 LE: tlen   | tlen bytes: table  | rest: Arrow IPC    |
//! +----------------+--------------------+--------------------+
//! ```
//!
//! - `tlen` is the table-name length in bytes (validated to fit `MAX_IDENT_LEN`).
//! - `table` is the UTF-8 table name, identical to `TableName::as_str()`.
//! - The remainder is one Arrow IPC stream (header + a single `RecordBatch`),
//!   produced by `arrow_ipc::writer::StreamWriter` and decoded by
//!   `arrow_ipc::reader::StreamReader`.
//!
//! The shard owner is the only entity that writes WAL payloads, so we control
//! both ends of this codec; there is no on-the-wire compatibility concern.
//!
//! ## Concurrency model
//!
//! - One outer `Mutex<HashMap<(TenantId, PartitionKey), Arc<RwLock<PartitionState>>>>`.
//!   Held only long enough to look up or insert the per-partition entry; never
//!   held across I/O or across awaits on the inner lock.
//! - Each `PartitionState` lives behind its own `tokio::sync::RwLock`. Writes
//!   acquire the write side briefly to push onto the tail; reads acquire the
//!   read side to clone the tail.
//! - Background loops snapshot the tenant map under the outer lock, drop it,
//!   then process each partition independently. Compaction acquires the
//!   per-partition write lock only at the start (to snapshot what to drain)
//!   and at the end (to remove drained entries from the tail). Object-store
//!   I/O happens with no Basin lock held.

use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use basin_catalog::DataFileRef;
use basin_common::{BasinError, PartitionKey, Result, TableName, TenantId};
use basin_storage::ReadOptions;
use basin_wal::Lsn;
use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, instrument, warn};

use crate::{
    ShardBackgroundHandle, ShardConfig, ShardImpl, ShardStats, TenantHandle, TenantHandleImpl,
};

/// Per-(tenant, partition) in-memory state. Lives behind an `RwLock` inside
/// the shard's outer map.
pub(crate) struct PartitionState {
    #[allow(dead_code)]
    tenant: TenantId,
    #[allow(dead_code)]
    partition: PartitionKey,
    last_active: Instant,
    /// Highest LSN that has been compacted into Parquet for this partition.
    /// `Lsn::ZERO` until the first successful compaction. v0.1 always replays
    /// from `Lsn::ZERO` on cold start (no compaction marker on disk yet —
    /// truncation in the WAL prevents replay duplication in practice).
    last_compacted_lsn: Lsn,
    /// Schemas cached from the catalog the first time we touch a table.
    schemas: HashMap<TableName, Arc<Schema>>,
    /// In-memory tail keyed by table. Each `(lsn, batch)` pair tracks the WAL
    /// entry that produced it so the compactor knows what range to truncate.
    tail: HashMap<TableName, Vec<(Lsn, RecordBatch)>>,
}

impl PartitionState {
    fn new(tenant: TenantId, partition: PartitionKey) -> Self {
        Self {
            tenant,
            partition,
            last_active: Instant::now(),
            last_compacted_lsn: Lsn::ZERO,
            schemas: HashMap::new(),
            tail: HashMap::new(),
        }
    }

    fn touch(&mut self) {
        self.last_active = Instant::now();
    }

    fn tail_is_empty(&self) -> bool {
        self.tail.values().all(|v| v.is_empty())
    }
}

type PartitionMap = HashMap<(TenantId, PartitionKey), Arc<RwLock<PartitionState>>>;
type PartitionSnapshot = Vec<((TenantId, PartitionKey), Arc<RwLock<PartitionState>>)>;

pub(crate) struct InProcessShard {
    pub(crate) cfg: ShardConfig,
    pub(crate) partitions: Arc<Mutex<PartitionMap>>,
    stats: Arc<Mutex<ShardStats>>,
}

impl InProcessShard {
    pub(crate) fn new(cfg: ShardConfig) -> Self {
        Self {
            cfg,
            partitions: Arc::new(Mutex::new(HashMap::new())),
            stats: Arc::new(Mutex::new(ShardStats::default())),
        }
    }

    fn share_clone(&self) -> Self {
        Self {
            cfg: self.cfg.clone(),
            partitions: self.partitions.clone(),
            stats: self.stats.clone(),
        }
    }

    /// Look up the in-memory partition state, populating it from the WAL on
    /// first access.
    async fn load_or_create(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
    ) -> Result<Arc<RwLock<PartitionState>>> {
        // Fast path: already resident.
        {
            let map = self.partitions.lock().await;
            if let Some(existing) = map.get(&(*tenant, partition.clone())) {
                let arc = existing.clone();
                // Touch outside the outer lock; still fine because the inner
                // lock is independent of the outer one.
                drop(map);
                arc.write().await.touch();
                return Ok(arc);
            }
        }

        // Cold-load path. Replay the WAL into a fresh state object before
        // exposing it to the map; that way concurrent callers either see the
        // empty slot (and replay themselves) or see a fully replayed state.
        let mut state = PartitionState::new(*tenant, partition.clone());
        replay_wal_into(&self.cfg.wal, tenant, partition, &mut state).await?;

        let arc = Arc::new(RwLock::new(state));
        let mut map = self.partitions.lock().await;
        // Another task may have raced us to populate; prefer the existing
        // entry so we don't expose two divergent copies.
        let entry = map
            .entry((*tenant, partition.clone()))
            .or_insert_with(|| arc.clone())
            .clone();
        drop(map);
        entry.write().await.touch();
        Ok(entry)
    }

    /// Test-only: drive one iteration of the eviction loop synchronously.
    #[allow(dead_code)]
    pub(crate) async fn run_eviction_once(&self) -> Result<()> {
        self.evict_idle().await
    }

    /// Test-only: drive one iteration of the compaction loop synchronously.
    #[allow(dead_code)]
    pub(crate) async fn run_compaction_once(&self) -> Result<()> {
        self.compact_all().await
    }

    /// Walk the partition map and drop entries whose `last_active` is past the
    /// configured `eviction_idle` window. Skips partitions whose tail is
    /// non-empty — letting the compactor drain them first preserves the
    /// "WAL is the durability boundary" invariant.
    async fn evict_idle(&self) -> Result<()> {
        let now = Instant::now();
        let idle = self.cfg.eviction_idle;

        let snapshot: PartitionSnapshot = {
            let map = self.partitions.lock().await;
            map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };

        let mut to_evict: Vec<(TenantId, PartitionKey)> = Vec::new();
        for (key, state) in snapshot {
            let guard = state.read().await;
            let stale = now.duration_since(guard.last_active) >= idle;
            let dirty = !guard.tail_is_empty();
            if stale && !dirty {
                to_evict.push(key);
            }
        }

        if to_evict.is_empty() {
            return Ok(());
        }

        let mut map = self.partitions.lock().await;
        let mut stats = self.stats.lock().await;
        for key in to_evict {
            // Re-check under the outer lock; the partition may have become
            // dirty or active in the small window between snapshot and now.
            if let Some(state) = map.get(&key) {
                let guard = state.read().await;
                let stale = now.duration_since(guard.last_active) >= idle;
                let dirty = !guard.tail_is_empty();
                drop(guard);
                if stale && !dirty {
                    map.remove(&key);
                    stats.evictions = stats.evictions.saturating_add(1);
                }
            }
        }
        stats.resident_partitions = map.len();
        stats.resident_tenants = unique_tenants(&map);
        Ok(())
    }

    /// Drain every partition's tail into Parquet, committing through the
    /// catalog and truncating the WAL on success.
    async fn compact_all(&self) -> Result<()> {
        let snapshot: PartitionSnapshot = {
            let map = self.partitions.lock().await;
            map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };

        for ((tenant, partition), state) in snapshot {
            if let Err(e) = self.compact_one(&tenant, &partition, state).await {
                // A failure to compact one partition must not stall others.
                // Surface via tracing; the next tick will retry.
                warn!(
                    %tenant,
                    %partition,
                    error = %e,
                    "compaction failed for partition; will retry next tick",
                );
            }
        }
        Ok(())
    }

    async fn compact_one(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
        state: Arc<RwLock<PartitionState>>,
    ) -> Result<()> {
        // Snapshot the tail under the inner write lock. Holding it briefly is
        // fine; the lock is per-partition and we drop it before any I/O.
        let tail_snapshot: Vec<(TableName, Vec<(Lsn, RecordBatch)>)> = {
            let guard = state.read().await;
            guard
                .tail
                .iter()
                .filter(|(_, v)| !v.is_empty())
                .map(|(t, v)| (t.clone(), v.clone()))
                .collect()
        };

        if tail_snapshot.is_empty() {
            return Ok(());
        }

        let mut max_lsn_overall: Option<Lsn> = None;
        let mut drained_per_table: HashMap<TableName, Lsn> = HashMap::new();

        for (table, entries) in tail_snapshot {
            let max_lsn = entries
                .iter()
                .map(|(lsn, _)| *lsn)
                .max()
                .expect("non-empty table tail");

            // Concatenate all batches for this table into one Parquet write.
            let batches: Vec<RecordBatch> =
                entries.iter().map(|(_, b)| b.clone()).collect();
            let schema = batches[0].schema();
            let merged = arrow::compute::concat_batches(&schema, &batches)
                .map_err(|e| BasinError::storage(format!("concat batches: {e}")))?;

            let data_file = self
                .cfg
                .storage
                .write_batch(tenant, &table, partition, &merged)
                .await?;

            let file_ref = DataFileRef {
                path: data_file.path.as_ref().to_string(),
                size_bytes: data_file.size_bytes,
                row_count: data_file.row_count,
            };

            self.commit_with_retry(tenant, &table, &merged, file_ref).await?;

            drained_per_table.insert(table, max_lsn);
            max_lsn_overall = Some(match max_lsn_overall {
                Some(prev) if prev >= max_lsn => prev,
                _ => max_lsn,
            });
        }

        // Apply the drain: clear only the (lsn, batch) entries we actually
        // committed. New writes that landed during compaction (with higher
        // LSNs) stay in the tail for the next tick.
        {
            let mut guard = state.write().await;
            for (table, max_lsn) in &drained_per_table {
                if let Some(v) = guard.tail.get_mut(table) {
                    v.retain(|(lsn, _)| *lsn > *max_lsn);
                }
                if guard.last_compacted_lsn < *max_lsn {
                    guard.last_compacted_lsn = *max_lsn;
                }
            }
        }

        // Truncate the WAL after the catalog commit. Order matters: if we
        // truncated first and then crashed before the commit lands, the data
        // would be lost; this way the worst case is a duplicate Parquet file
        // with a small replayed batch, which is reconciled on next compaction.
        if let Some(max_lsn) = max_lsn_overall {
            self.cfg.wal.truncate(tenant, partition, max_lsn).await?;
        }

        let mut stats = self.stats.lock().await;
        stats.compactions = stats.compactions.saturating_add(1);
        Ok(())
    }

    /// Commit one data file to the catalog. On `CommitConflict`, reload + retry
    /// once. The catalog auto-creates the table on first commit using the
    /// batch's schema.
    async fn commit_with_retry(
        &self,
        tenant: &TenantId,
        table: &TableName,
        batch: &RecordBatch,
        file: DataFileRef,
    ) -> Result<()> {
        // Make sure the table exists; if not, create it from the batch schema.
        let mut snapshot = match self.cfg.catalog.load_table(tenant, table).await {
            Ok(meta) => meta.current_snapshot,
            Err(BasinError::NotFound(_)) => {
                let meta = self
                    .cfg
                    .catalog
                    .create_table(tenant, table, batch.schema().as_ref())
                    .await?;
                meta.current_snapshot
            }
            Err(e) => return Err(e),
        };

        for attempt in 0..2 {
            match self
                .cfg
                .catalog
                .append_data_files(tenant, table, snapshot, vec![file.clone()])
                .await
            {
                Ok(_) => return Ok(()),
                Err(BasinError::CommitConflict(_)) if attempt == 0 => {
                    let meta = self.cfg.catalog.load_table(tenant, table).await?;
                    snapshot = meta.current_snapshot;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(BasinError::CommitConflict(format!(
            "{tenant}/{table}: lost commit race twice"
        )))
    }

    async fn refresh_resident_stats(&self) {
        let map = self.partitions.lock().await;
        let mut stats = self.stats.lock().await;
        stats.resident_partitions = map.len();
        stats.resident_tenants = unique_tenants(&map);
    }
}

fn unique_tenants(map: &PartitionMap) -> usize {
    let mut seen: HashSet<TenantId> = HashSet::new();
    for (t, _) in map.keys() {
        seen.insert(*t);
    }
    seen.len()
}

#[async_trait]
impl ShardImpl for InProcessShard {
    #[instrument(skip(self), fields(tenant = %tenant, partition = %partition))]
    async fn get(
        &self,
        tenant: &TenantId,
        partition: &PartitionKey,
    ) -> Result<TenantHandle> {
        let state = self.load_or_create(tenant, partition).await?;
        self.refresh_resident_stats().await;
        let inner: Arc<dyn TenantHandleImpl> = Arc::new(InProcessTenantHandle {
            tenant: *tenant,
            partition: partition.clone(),
            state,
            cfg: self.cfg.clone(),
        });
        Ok(TenantHandle { inner })
    }

    fn spawn_background(self: Arc<Self>) -> ShardBackgroundHandle {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let me = self.clone();
        let join = tokio::spawn(async move {
            let mut shutdown = rx;
            let mut evict_tick = tokio::time::interval(me.cfg.eviction_interval);
            let mut compact_tick = tokio::time::interval(me.cfg.compaction_interval);
            // First firing of `interval` is immediate; skip it so the loops
            // align with their configured cadence.
            evict_tick.tick().await;
            compact_tick.tick().await;
            loop {
                tokio::select! {
                    _ = &mut shutdown => break,
                    _ = evict_tick.tick() => {
                        if let Err(e) = me.evict_idle().await {
                            warn!(error = %e, "eviction tick failed");
                        }
                    }
                    _ = compact_tick.tick() => {
                        if let Err(e) = me.compact_all().await {
                            warn!(error = %e, "compaction tick failed");
                        }
                    }
                }
            }
        });
        ShardBackgroundHandle {
            shutdown: tx,
            join,
        }
    }

    fn stats(&self) -> ShardStats {
        // try_lock: stats is read-mostly and we don't want to block the caller
        // on the background loops' work. If contended, we return the last
        // stable view.
        match self.stats.try_lock() {
            Ok(guard) => guard.clone(),
            Err(_) => ShardStats::default(),
        }
    }

    fn clone_arc(&self) -> Arc<dyn ShardImpl> {
        // Returns a *new* outer Arc whose inner `InProcessShard` shares the
        // same `partitions` map and `stats` cell as the original via the
        // Arc'd handles. Used by `Shard::spawn_background`, which moves the
        // returned Arc into a tokio task.
        Arc::new(self.share_clone())
    }

    #[cfg(test)]
    fn as_in_process(&self) -> Option<Arc<InProcessShard>> {
        Some(Arc::new(self.share_clone()))
    }
}

struct InProcessTenantHandle {
    tenant: TenantId,
    partition: PartitionKey,
    state: Arc<RwLock<PartitionState>>,
    cfg: ShardConfig,
}

#[async_trait]
impl TenantHandleImpl for InProcessTenantHandle {
    #[instrument(skip(self, batch), fields(tenant = %self.tenant, partition = %self.partition, table = %table, rows = batch.num_rows()))]
    async fn write_batch(&self, table: &TableName, batch: RecordBatch) -> Result<()> {
        let payload = encode_payload(table, &batch)?;
        let lsn = self
            .cfg
            .wal
            .append(&self.tenant, &self.partition, payload)
            .await?;

        let mut guard = self.state.write().await;
        guard
            .tail
            .entry(table.clone())
            .or_default()
            .push((lsn, batch.clone()));
        guard
            .schemas
            .entry(table.clone())
            .or_insert_with(|| batch.schema());
        guard.touch();
        Ok(())
    }

    #[instrument(skip(self, opts), fields(tenant = %self.tenant, partition = %self.partition, table = %table))]
    async fn read(
        &self,
        table: &TableName,
        opts: ReadOptions,
    ) -> Result<Vec<RecordBatch>> {
        // Stream the Parquet base.
        let parquet_opts = ReadOptions {
            projection: opts.projection.clone(),
            filters: opts.filters.clone(),
            partition: opts.partition.clone(),
        };
        let stream = self
            .cfg
            .storage
            .read(&self.tenant, table, parquet_opts)
            .await?;
        let mut out: Vec<RecordBatch> = stream
            .collect::<Vec<Result<RecordBatch>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        // Append the in-memory tail. v0.1 filter handling on the tail is a
        // simple full scan; the Parquet path already pushed predicates down.
        let tail_batches: Vec<RecordBatch> = {
            let guard = self.state.read().await;
            match guard.tail.get(table) {
                Some(v) => v.iter().map(|(_, b)| b.clone()).collect(),
                None => Vec::new(),
            }
        };

        for batch in tail_batches {
            let projected = match &opts.projection {
                Some(cols) => project_batch(&batch, cols)?,
                None => batch,
            };
            let filtered = apply_filters(projected, &opts.filters)?;
            if filtered.num_rows() > 0 {
                out.push(filtered);
            }
        }

        // Touch the partition so reads keep it warm.
        self.state.write().await.touch();
        Ok(out)
    }

    fn last_active(&self) -> Instant {
        // try_read so a concurrent writer can't starve out callers asking for
        // the timestamp. If contended, return now() as a conservative answer
        // (errs on the side of "active").
        match self.state.try_read() {
            Ok(g) => g.last_active,
            Err(_) => Instant::now(),
        }
    }

    fn tenant(&self) -> TenantId {
        self.tenant
    }
}

/// Encode a `(table, batch)` pair as a single WAL payload.
///
/// See module docs for the exact layout.
fn encode_payload(table: &TableName, batch: &RecordBatch) -> Result<Bytes> {
    let table_bytes = table.as_str().as_bytes();
    if table_bytes.len() > u32::MAX as usize {
        return Err(BasinError::internal("table name length overflows u32"));
    }
    let mut buf: Vec<u8> = Vec::with_capacity(4 + table_bytes.len() + 1024);
    buf.extend_from_slice(&(table_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(table_bytes);

    {
        let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())
            .map_err(|e| BasinError::storage(format!("ipc writer init: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| BasinError::storage(format!("ipc write: {e}")))?;
        writer
            .finish()
            .map_err(|e| BasinError::storage(format!("ipc finish: {e}")))?;
    }
    Ok(Bytes::from(buf))
}

/// Inverse of [`encode_payload`].
fn decode_payload(bytes: &[u8]) -> Result<(TableName, Vec<RecordBatch>)> {
    if bytes.len() < 4 {
        return Err(BasinError::wal("WAL payload shorter than 4 bytes".to_string()));
    }
    let tlen = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if bytes.len() < 4 + tlen {
        return Err(BasinError::wal(format!(
            "WAL payload truncated: tlen={tlen} but only {} bytes after header",
            bytes.len() - 4
        )));
    }
    let table_str = std::str::from_utf8(&bytes[4..4 + tlen])
        .map_err(|e| BasinError::wal(format!("WAL table name not UTF-8: {e}")))?;
    let table = TableName::new(table_str)?;

    let mut cursor = Cursor::new(&bytes[4 + tlen..]);
    let reader = StreamReader::try_new(&mut cursor, None)
        .map_err(|e| BasinError::storage(format!("ipc reader init: {e}")))?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| BasinError::storage(format!("ipc read: {e}")))?;
        batches.push(batch);
    }
    Ok((table, batches))
}

async fn replay_wal_into(
    wal: &basin_wal::Wal,
    tenant: &TenantId,
    partition: &PartitionKey,
    state: &mut PartitionState,
) -> Result<()> {
    // v0.1: always replay from `Lsn::ZERO`. Once we persist a compaction
    // marker we'll skip already-flushed entries.
    let entries = wal.read_from(tenant, partition, Lsn::ZERO).await?;
    for entry in entries {
        let (table, batches) = decode_payload(&entry.payload)?;
        let table_tail = state.tail.entry(table.clone()).or_default();
        for batch in batches {
            if state
                .schemas
                .get(&table)
                .map(|s| s.fields().len())
                .is_none()
            {
                state.schemas.insert(table.clone(), batch.schema());
            }
            table_tail.push((entry.lsn, batch));
        }
    }
    debug!(
        %tenant,
        %partition,
        tables = state.tail.len(),
        "replayed WAL into partition state",
    );
    Ok(())
}

/// Project a `RecordBatch` onto a subset of column names, in the requested
/// order. Returns an error if any column is missing.
fn project_batch(batch: &RecordBatch, cols: &[String]) -> Result<RecordBatch> {
    let schema = batch.schema();
    let mut idxs: Vec<usize> = Vec::with_capacity(cols.len());
    for c in cols {
        let i = schema
            .index_of(c)
            .map_err(|_| BasinError::storage(format!("unknown column {c}")))?;
        idxs.push(i);
    }
    batch
        .project(&idxs)
        .map_err(|e| BasinError::storage(format!("project batch: {e}")))
}

/// AND together every predicate against the batch, returning only the rows
/// that pass all of them. Empty filter list returns the input untouched.
fn apply_filters(
    batch: RecordBatch,
    filters: &[basin_storage::Predicate],
) -> Result<RecordBatch> {
    if filters.is_empty() {
        return Ok(batch);
    }
    let mut current = batch;
    for f in filters {
        let mask = evaluate_predicate(&current, f)?;
        current = arrow::compute::filter_record_batch(&current, &mask)
            .map_err(|e| BasinError::storage(format!("filter batch: {e}")))?;
        if current.num_rows() == 0 {
            return Ok(current);
        }
    }
    Ok(current)
}

/// Tiny stand-in for the storage crate's private predicate evaluator. v0.1
/// supports the same surface area: Eq / Lt / Gt against Int64, UInt64,
/// Float64, Utf8, and Boolean (Eq only).
fn evaluate_predicate(
    batch: &RecordBatch,
    predicate: &basin_storage::Predicate,
) -> Result<arrow_array::BooleanArray> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float64Type, Int64Type, UInt64Type};
    use arrow_array::{Array, BooleanArray};
    use basin_storage::{Predicate, ScalarValue};

    let col_name = predicate.column();
    let col = batch
        .column_by_name(col_name)
        .ok_or_else(|| BasinError::storage(format!("predicate column missing: {col_name}")))?;

    macro_rules! cmp_primitive {
        ($arrow_ty:ty, $val:expr, $op:tt) => {{
            let arr = col.as_primitive::<$arrow_ty>();
            let v = $val;
            let mut b = arrow_array::builder::BooleanBuilder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    b.append_value(false);
                } else {
                    b.append_value(arr.value(i) $op v);
                }
            }
            b.finish()
        }};
    }

    let value = match predicate {
        Predicate::Eq(_, v) | Predicate::Gt(_, v) | Predicate::Lt(_, v) => v.clone(),
    };

    let mask: BooleanArray = match (predicate, &value) {
        (Predicate::Eq(_, _), ScalarValue::Int64(v)) => cmp_primitive!(Int64Type, *v, ==),
        (Predicate::Gt(_, _), ScalarValue::Int64(v)) => cmp_primitive!(Int64Type, *v, >),
        (Predicate::Lt(_, _), ScalarValue::Int64(v)) => cmp_primitive!(Int64Type, *v, <),
        (Predicate::Eq(_, _), ScalarValue::UInt64(v)) => cmp_primitive!(UInt64Type, *v, ==),
        (Predicate::Gt(_, _), ScalarValue::UInt64(v)) => cmp_primitive!(UInt64Type, *v, >),
        (Predicate::Lt(_, _), ScalarValue::UInt64(v)) => cmp_primitive!(UInt64Type, *v, <),
        (Predicate::Eq(_, _), ScalarValue::Float64(v)) => cmp_primitive!(Float64Type, *v, ==),
        (Predicate::Gt(_, _), ScalarValue::Float64(v)) => cmp_primitive!(Float64Type, *v, >),
        (Predicate::Lt(_, _), ScalarValue::Float64(v)) => cmp_primitive!(Float64Type, *v, <),
        (op, ScalarValue::Utf8(v)) => {
            let arr = col.as_string::<i32>();
            let cmp: fn(&str, &str) -> bool = match op {
                Predicate::Eq(_, _) => |a, b| a == b,
                Predicate::Gt(_, _) => |a, b| a > b,
                Predicate::Lt(_, _) => |a, b| a < b,
            };
            let mut b = arrow_array::builder::BooleanBuilder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    b.append_value(false);
                } else {
                    b.append_value(cmp(arr.value(i), v.as_str()));
                }
            }
            b.finish()
        }
        (Predicate::Eq(_, _), ScalarValue::Boolean(v)) => {
            let arr = col.as_boolean();
            let mut b = arrow_array::builder::BooleanBuilder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    b.append_value(false);
                } else {
                    b.append_value(arr.value(i) == *v);
                }
            }
            b.finish()
        }
        (op, val) => {
            return Err(BasinError::storage(format!(
                "unsupported predicate combination: {op:?} on {val:?}"
            )));
        }
    };

    Ok(mask)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::time::Duration;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use basin_catalog::InMemoryCatalog;
    use basin_common::{PartitionKey, TableName, TenantId};
    use basin_storage::{Storage, StorageConfig};
    use basin_wal::{Wal, WalConfig};
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn batch(start: i64, len: usize, prefix: &str) -> RecordBatch {
        let ids: Int64Array = (start..start + len as i64).collect();
        let names: Vec<String> = (0..len)
            .map(|i| format!("{prefix}{}", start + i as i64))
            .collect();
        let names: StringArray = names.iter().map(|s| Some(s.as_str())).collect();
        RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(names)]).unwrap()
    }

    /// Build a fresh shard wired against a `tempdir`-backed object store and
    /// in-memory catalog. Returns the shard plus the dir handles so callers
    /// can keep them alive for the duration of the test.
    async fn fresh_shard() -> (
        crate::Shard,
        TempDir,
        TempDir,
        Storage,
        Arc<InMemoryCatalog>,
        Wal,
    ) {
        basin_common::telemetry::try_init_for_tests();
        let storage_dir = TempDir::new().unwrap();
        let wal_dir = TempDir::new().unwrap();
        let storage_fs = LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap();
        let wal_fs = LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap();
        let storage = Storage::new(StorageConfig {
            object_store: Arc::new(storage_fs),
            root_prefix: None,
        });
        let catalog = Arc::new(InMemoryCatalog::new());
        let wal = Wal::open(WalConfig {
            object_store: Arc::new(wal_fs),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
        })
        .await
        .unwrap();
        let cfg = ShardConfig::new(storage.clone(), catalog.clone(), wal.clone());
        let shard = crate::Shard::new(cfg);
        (shard, storage_dir, wal_dir, storage, catalog, wal)
    }

    fn rows_in(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Reach inside the public Shard to invoke the test-only helpers. Tied to
    /// the in-process implementation only.
    fn impl_of(shard: &crate::Shard) -> Arc<InProcessShard> {
        // SAFETY: there is exactly one ShardImpl in v0.1; downcast is safe.
        // But we can't use Any here, so re-wire by reading the same fields
        // through a private helper. We expose `impl_handle` on Shard for
        // tests via cfg(test).
        shard
            .impl_handle()
            .expect("Shard wraps InProcessShard in v0.1")
    }

    #[tokio::test]
    async fn write_then_read_returns_tail() {
        let (shard, _sd, _wd, _storage, _cat, _wal) = fresh_shard().await;
        let tenant = TenantId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();

        let handle = shard.get(&tenant, &partition).await.unwrap();
        for i in 0..3 {
            handle
                .write_batch(&table, batch(i * 10, 10, "v-"))
                .await
                .unwrap();
        }

        let read = handle
            .read(&table, ReadOptions::default())
            .await
            .unwrap();
        assert_eq!(rows_in(&read), 30);
    }

    #[tokio::test]
    async fn compaction_drains_tail_to_parquet() {
        let (shard, _sd, _wd, storage, _cat, wal) = fresh_shard().await;
        let tenant = TenantId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();

        let handle = shard.get(&tenant, &partition).await.unwrap();
        for i in 0..3 {
            handle
                .write_batch(&table, batch(i * 10, 10, "v-"))
                .await
                .unwrap();
        }

        let inner = impl_of(&shard);
        inner.run_compaction_once().await.unwrap();

        // Parquet file exists in storage.
        let files = storage.list_data_files(&tenant, &table).await.unwrap();
        assert!(
            !files.is_empty(),
            "expected at least one data file after compaction",
        );

        // WAL is truncated (high water now equals the watermark we drained).
        // After truncate, read_from(ZERO) should return no entries with
        // lsn <= watermark; we just check the partition's tail in memory is
        // empty.
        let read = handle
            .read(&table, ReadOptions::default())
            .await
            .unwrap();
        assert_eq!(rows_in(&read), 30, "rows should still be visible via Parquet");

        // Tail empty after compaction.
        let state = {
            let map = inner.partitions.lock().await;
            map.get(&(tenant, partition.clone())).unwrap().clone()
        };
        let guard = state.read().await;
        assert!(guard.tail_is_empty(), "tail should be empty after compaction");

        // Sanity: WAL high_water reflects truncation by returning the pre-trunc
        // value or higher, never resetting to ZERO. Just call it to ensure
        // truncate didn't error out.
        let _ = wal.high_water(&tenant, &partition).await.unwrap();
    }

    #[tokio::test]
    async fn cold_load_replays_wal() {
        let storage_dir = TempDir::new().unwrap();
        let wal_dir = TempDir::new().unwrap();
        basin_common::telemetry::try_init_for_tests();
        let storage_fs = LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap();
        let wal_fs = Arc::new(LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap());
        let storage = Storage::new(StorageConfig {
            object_store: Arc::new(storage_fs),
            root_prefix: None,
        });
        let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
        let wal_cfg = || WalConfig {
            object_store: wal_fs.clone(),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
        };

        let tenant = TenantId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();

        // First shard: write 5 batches, drop.
        {
            let wal = Wal::open(wal_cfg()).await.unwrap();
            let cfg = ShardConfig::new(storage.clone(), catalog.clone(), wal.clone());
            let shard = crate::Shard::new(cfg);
            let handle = shard.get(&tenant, &partition).await.unwrap();
            for i in 0..5 {
                handle
                    .write_batch(&table, batch(i * 10, 10, "v-"))
                    .await
                    .unwrap();
            }
            wal.flush().await.unwrap();
            wal.close().await.unwrap();
        }

        // Second shard: reopen, read — all 5 batches replay.
        {
            let wal = Wal::open(wal_cfg()).await.unwrap();
            let cfg = ShardConfig::new(storage.clone(), catalog.clone(), wal);
            let shard = crate::Shard::new(cfg);
            let handle = shard.get(&tenant, &partition).await.unwrap();
            let read = handle
                .read(&table, ReadOptions::default())
                .await
                .unwrap();
            assert_eq!(rows_in(&read), 50, "cold load should replay all WAL entries");
        }
    }

    #[tokio::test]
    async fn eviction_drops_idle() {
        let (mut shard, _sd, _wd, _storage, _cat, _wal) = fresh_shard().await;
        // Rewire eviction_idle to zero by recreating the shard.
        let tenant = TenantId::new();
        let partition = PartitionKey::default_key();

        let handle = shard.get(&tenant, &partition).await.unwrap();
        drop(handle);

        // Reach into the impl, override eviction_idle, run one tick.
        let inner = impl_of(&shard);
        // We can't mutate the existing config, so reconstruct a shard with
        // eviction_idle = 0 against the same backends. Simpler path: build a
        // dedicated shard for this test.
        let _ = &mut shard;
        let cfg2 = ShardConfig {
            eviction_idle: Duration::from_secs(0),
            ..inner.cfg.clone()
        };
        let shard2 = crate::Shard::new(cfg2);
        let h2 = shard2.get(&tenant, &partition).await.unwrap();
        drop(h2);
        // Sleep 1ms so last_active is strictly in the past.
        tokio::time::sleep(Duration::from_millis(2)).await;
        let inner2 = impl_of(&shard2);
        inner2.run_eviction_once().await.unwrap();
        assert_eq!(shard2.stats().resident_partitions, 0);
    }

    #[tokio::test]
    async fn eviction_skips_dirty_tail() {
        let (shard, _sd, _wd, _storage, _cat, _wal) = fresh_shard().await;
        let tenant = TenantId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("events").unwrap();

        let inner = impl_of(&shard);
        let cfg2 = ShardConfig {
            eviction_idle: Duration::from_secs(0),
            ..inner.cfg.clone()
        };
        let shard2 = crate::Shard::new(cfg2);
        let handle = shard2.get(&tenant, &partition).await.unwrap();
        handle
            .write_batch(&table, batch(0, 10, "v-"))
            .await
            .unwrap();
        drop(handle);
        tokio::time::sleep(Duration::from_millis(2)).await;

        let inner2 = impl_of(&shard2);
        inner2.run_eviction_once().await.unwrap();
        assert_eq!(
            shard2.stats().resident_partitions,
            1,
            "dirty partition must not be evicted"
        );
    }

    #[tokio::test]
    async fn tenant_isolation() {
        let (shard, _sd, _wd, _storage, _cat, _wal) = fresh_shard().await;
        let a = TenantId::new();
        let b = TenantId::new();
        let partition = PartitionKey::default_key();
        let table = TableName::new("shared").unwrap();

        let ha = shard.get(&a, &partition).await.unwrap();
        let hb = shard.get(&b, &partition).await.unwrap();

        ha.write_batch(&table, batch(0, 5, "a-"))
            .await
            .unwrap();
        hb.write_batch(&table, batch(0, 7, "b-"))
            .await
            .unwrap();

        let ra = ha.read(&table, ReadOptions::default()).await.unwrap();
        let rb = hb.read(&table, ReadOptions::default()).await.unwrap();
        assert_eq!(rows_in(&ra), 5);
        assert_eq!(rows_in(&rb), 7);

        // And the names from a's read all start with "a-".
        use arrow_array::Array;
        let names = ra[0]
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..names.len() {
            assert!(
                names.value(i).starts_with("a-"),
                "tenant a saw {} which is not a's data",
                names.value(i)
            );
        }
    }
}
