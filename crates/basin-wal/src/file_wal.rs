//! File-backed (single-node) WAL implementation. Filled in by the
//! basin-wal implementation step.

use async_trait::async_trait;
use basin_common::{BasinError, PartitionKey, Result, TenantId};
use bytes::Bytes;

use crate::{Lsn, WalConfig, WalEntry, WalImpl};

pub(crate) struct FileWal {
    _cfg: WalConfig,
}

impl FileWal {
    pub(crate) async fn open(cfg: WalConfig) -> Result<Self> {
        Ok(Self { _cfg: cfg })
    }
}

#[async_trait]
impl WalImpl for FileWal {
    async fn append(
        &self,
        _tenant: &TenantId,
        _partition: &PartitionKey,
        _payload: Bytes,
    ) -> Result<Lsn> {
        Err(BasinError::wal("FileWal::append: not yet implemented"))
    }

    async fn flush(&self) -> Result<()> {
        Err(BasinError::wal("FileWal::flush: not yet implemented"))
    }

    async fn read_from(
        &self,
        _tenant: &TenantId,
        _partition: &PartitionKey,
        _since_lsn: Lsn,
    ) -> Result<Vec<WalEntry>> {
        Err(BasinError::wal("FileWal::read_from: not yet implemented"))
    }

    async fn high_water(
        &self,
        _tenant: &TenantId,
        _partition: &PartitionKey,
    ) -> Result<Lsn> {
        Err(BasinError::wal("FileWal::high_water: not yet implemented"))
    }

    async fn truncate(
        &self,
        _tenant: &TenantId,
        _partition: &PartitionKey,
        _up_to: Lsn,
    ) -> Result<()> {
        Err(BasinError::wal("FileWal::truncate: not yet implemented"))
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}
