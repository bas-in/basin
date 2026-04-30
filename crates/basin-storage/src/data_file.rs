//! `DataFile` descriptor: what we hand back from a write and what listing
//! returns. Mirrors the subset of Iceberg's `DataFile` we need now; we'll
//! grow it (sequence numbers, partition spec id, equality-delete pointers)
//! when basin-catalog learns to commit them.

use std::collections::BTreeMap;

use object_store::path::Path as ObjectPath;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataFile {
    /// Object key relative to whatever bucket the configured `ObjectStore`
    /// points at.
    #[serde(with = "object_path_str")]
    pub path: ObjectPath,
    pub size_bytes: u64,
    pub row_count: u64,
    pub column_stats: BTreeMap<String, ColumnStats>,
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

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ColumnStats {
    pub null_count: Option<u64>,
    /// Best-effort min/max as a bytes-or-string blob, encoded by Parquet's
    /// `Statistics::min_bytes` / `max_bytes`. Higher layers can decode
    /// according to the Arrow schema.
    pub min_bytes: Option<Vec<u8>>,
    pub max_bytes: Option<Vec<u8>>,
}
