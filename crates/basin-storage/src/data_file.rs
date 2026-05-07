//! `DataFile` descriptor: what we hand back from a write and what listing
//! returns. Mirrors the subset of Iceberg's `DataFile` we need now; we'll
//! grow it (sequence numbers, partition spec id, equality-delete pointers)
//! when basin-catalog learns to commit them.

use std::collections::BTreeMap;

use object_store::path::Path as ObjectPath;
use serde::{Deserialize, Serialize};

use crate::tier::Tier;

/// Per-column file-level Parquet statistics. Re-exported from
/// `basin-catalog` so the storage and catalog layers share a single
/// stats shape — Phase 5.7 A4 lifts these up into the catalog so the
/// reader can prune at file granularity without a footer fetch.
pub use basin_catalog::ColumnStats;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataFile {
    /// Object key relative to whatever bucket the configured `ObjectStore`
    /// points at.
    #[serde(with = "object_path_str")]
    pub path: ObjectPath,
    pub size_bytes: u64,
    pub row_count: u64,
    pub column_stats: BTreeMap<String, ColumnStats>,
    /// Storage tier this file lives in. `#[serde(default)]` so manifests
    /// written before the field existed deserialise to [`Tier::Hot`].
    /// Listing helpers populate this from the path layout, so a file that
    /// lives under `…/tables/<t>/cold/…` always reports `Cold` even if a
    /// stale snapshot recorded otherwise.
    #[serde(default)]
    pub tier: Tier,
}

mod object_path_str {
    use object_store::path::Path as ObjectPath;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(p: &ObjectPath, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(p.as_ref())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<ObjectPath, D::Error> {
        let s = String::deserialize(d)?;
        Ok(ObjectPath::from(s))
    }
}
