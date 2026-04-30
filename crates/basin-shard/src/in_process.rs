//! In-process shard map. Filled in by the basin-shard implementation step.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use basin_common::{BasinError, PartitionKey, Result, TableName, TenantId};

use crate::{
    ShardBackgroundHandle, ShardConfig, ShardImpl, ShardStats, TenantHandle, TenantHandleImpl,
};

pub(crate) struct InProcessShard {
    _cfg: ShardConfig,
}

impl InProcessShard {
    pub(crate) fn new(cfg: ShardConfig) -> Self {
        Self { _cfg: cfg }
    }
}

#[async_trait]
impl ShardImpl for InProcessShard {
    async fn get(&self, _tenant: &TenantId, _partition: &PartitionKey) -> Result<TenantHandle> {
        Err(BasinError::internal(
            "InProcessShard::get: not yet implemented",
        ))
    }

    fn spawn_background(self: Arc<Self>) -> ShardBackgroundHandle {
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let join = tokio::spawn(async move {});
        ShardBackgroundHandle {
            shutdown: tx,
            join,
        }
    }

    fn stats(&self) -> ShardStats {
        ShardStats::default()
    }

    fn clone_arc(&self) -> Arc<dyn ShardImpl> {
        Arc::new(InProcessShard {
            _cfg: self._cfg.clone(),
        })
    }
}

#[allow(dead_code)]
struct _StubTenantHandle;

#[async_trait]
impl TenantHandleImpl for _StubTenantHandle {
    async fn write_batch(&self, _table: &TableName, _batch: RecordBatch) -> Result<()> {
        Err(BasinError::internal("stub TenantHandle"))
    }
    async fn read(
        &self,
        _table: &TableName,
        _opts: basin_storage::ReadOptions,
    ) -> Result<Vec<RecordBatch>> {
        Err(BasinError::internal("stub TenantHandle"))
    }
    fn last_active(&self) -> Instant {
        Instant::now()
    }
    fn tenant(&self) -> TenantId {
        TenantId::new()
    }
}
