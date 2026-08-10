//! Phase PG-Wave-α — R-tree spatial index segment format.
//!
//! Mirrors [`gin_rowgroup`] in structure but stores per-row-group spatial
//! envelopes instead of GIN bloom filters. The sidecar layout follows the
//! HNSW vector index: one `.rtree` file per `(table, column, data file)`
//! at `projects/{p}/tables/{t}/index/{col}.rtree/{ulid}.rtree`. The
//! contents are a bincode-serialised `rstar::RTree<SpatialEntry>` plus a
//! tiny magic header for forward-compat (`b"BR1\n"` = BasinRtree v1).
//!
//! # Spatial entry
//!
//! Each row in the indexed RecordBatch contributes one [`SpatialEntry`].
//! Entries carry the row's `(row_group_id, row_id_in_group)` (per the
//! design doc §7-R2: this eliminates a Parquet-footer round-trip at probe
//! time) plus the row's WGS84 bounding box. For a POINT the bbox
//! degenerates to a zero-area envelope (`min == max`).
//!
//! # Engine wiring
//!
//! This is the STORAGE-side capability only. The engine's spatial
//! pushdown — `RTreePrunedTable` + `apply_rtree_pruning_for_query` —
//! lands in PG-Wave-β (#139). Until then the compactor populates the
//! sidecar but no read path consumes it. Same shape as how
//! [`gin_rowgroup`] shipped on the storage side ahead of the engine
//! probe wiring.

use std::sync::Arc;

use arrow_array::{Array, FixedSizeBinaryArray, RecordBatch};
use basin_common::{ProjectId, TableName};
use object_store::path::Path as ObjectPath;
use rstar::{primitives::Rectangle, PointDistance, RTree, RTreeObject, AABB};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Re-export of `rstar`'s axis-aligned bounding box type so engine callers
/// can construct probe envelopes without taking a direct dependency on
/// the `rstar` crate. Keeps the workspace dep graph compact.
pub use rstar::AABB as SpatialAabb;

/// Identifier for a single row-group within a file. Matches the
/// `RowGroupId` shape used by [`super::gin_rowgroup`].
pub type RowGroupId = u32;

/// On-disk sidecar magic header. Lets the deserialiser reject foreign
/// blobs (or a future v2 format) instead of bincode-misparsing silently.
const RTREE_MAGIC: &[u8; 4] = b"BR1\n";

/// One indexed row's contribution to the R-tree.
///
/// Carries the row's data-file coordinates (`row_group_id`,
/// `row_id_in_group`) plus its WGS84 envelope (`[min_x, min_y]` /
/// `[max_x, max_y]`). For a POINT the envelope is zero-area (`min ==
/// max`); for a BOX2D it is the full bounding rectangle.
///
/// `RTreeObject` is implemented manually so the entry stays plain-data
/// (no `Box` / `Arc`) — entries are `Copy` and `Serialize`-friendly,
/// which keeps the bincode round-trip cheap.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct SpatialEntry {
    /// Parquet row-group index this row lives in.
    pub row_group_id: RowGroupId,
    /// Row offset within the row-group. Engine combines with
    /// `row_group_id` to dereference into the row's full record.
    pub row_id_in_group: u32,
    /// Minimum corner of the row's envelope `[x, y]`. For a POINT this
    /// equals `max_corner`.
    pub min_corner: [f64; 2],
    /// Maximum corner of the row's envelope `[x, y]`.
    pub max_corner: [f64; 2],
}

impl RTreeObject for SpatialEntry {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_corners(self.min_corner, self.max_corner)
    }
}

/// Squared L2 distance (in degree-space) from `query` to the entry's
/// envelope, so `nearest_neighbor_iter` can order candidates. For a POINT
/// (zero-area envelope) this is the squared distance to the point itself; for
/// a BOX2D it is the squared distance to the nearest point on the rectangle
/// (zero when inside). `rstar` re-ranks by this value during the NN walk; the
/// engine then re-ranks the L2-degree candidates by exact Haversine meters.
impl PointDistance for SpatialEntry {
    fn distance_2(&self, query: &[f64; 2]) -> f64 {
        // Clamp the query to the entry's envelope per axis; the squared
        // distance to that clamped point is the envelope-to-point distance.
        let cx = query[0].clamp(self.min_corner[0], self.max_corner[0]);
        let cy = query[1].clamp(self.min_corner[1], self.max_corner[1]);
        let dx = query[0] - cx;
        let dy = query[1] - cy;
        dx * dx + dy * dy
    }
}

impl SpatialEntry {
    /// Build a zero-area entry for a POINT row.
    pub fn point(row_group_id: RowGroupId, row_id_in_group: u32, x: f64, y: f64) -> Self {
        Self {
            row_group_id,
            row_id_in_group,
            min_corner: [x, y],
            max_corner: [x, y],
        }
    }

    /// Build an entry for a BOX2D row.
    pub fn bbox(
        row_group_id: RowGroupId,
        row_id_in_group: u32,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
    ) -> Self {
        Self {
            row_group_id,
            row_id_in_group,
            min_corner: [min_x.min(max_x), min_y.min(max_y)],
            max_corner: [min_x.max(max_x), min_y.max(max_y)],
        }
    }
}

// ── Build helper ──────────────────────────────────────────────────────────────

/// Decode a 21-byte WKB POINT into `(x, y)`. Mirrors
/// `basin_geo::decode_point` but skips the SRID-bearing `Point` wrapper
/// — we only need the coordinates for the R-tree envelope.
fn decode_wkb_point_xy(bytes: &[u8]) -> Option<(f64, f64)> {
    if bytes.len() != basin_geo::POINT_WKB_LEN {
        return None;
    }
    let endian = bytes[0];
    let type_bytes: [u8; 4] = bytes[1..5].try_into().ok()?;
    let x_bytes: [u8; 8] = bytes[5..13].try_into().ok()?;
    let y_bytes: [u8; 8] = bytes[13..21].try_into().ok()?;
    let (type_code, x, y) = match endian {
        0x01 => (
            u32::from_le_bytes(type_bytes),
            f64::from_le_bytes(x_bytes),
            f64::from_le_bytes(y_bytes),
        ),
        0x00 => (
            u32::from_be_bytes(type_bytes),
            f64::from_be_bytes(x_bytes),
            f64::from_be_bytes(y_bytes),
        ),
        _ => return None,
    };
    if type_code != 1 {
        return None;
    }
    Some((x, y))
}

/// Build an R-tree from a RecordBatch's geometry column.
///
/// `geom_col_idx` must point at a `FixedSizeBinary(21)` column whose
/// values are WKB-encoded POINTs (the basin-geo on-the-wire format).
/// NULL rows are skipped silently. `rg_size` is the effective row-group
/// row count used by the Parquet writer for this compaction; rows at
/// position `i` are assigned to row-group `i / rg_size`.
///
/// Returns an `RTree<SpatialEntry>` whose `locate_in_envelope_intersecting`
/// can be used by the engine pushdown layer.
pub fn build_rtree_for_batch(
    batch: &RecordBatch,
    geom_col_idx: usize,
    rg_size: u32,
) -> RTree<SpatialEntry> {
    let col = batch.column(geom_col_idx);
    let rg = rg_size.max(1);
    let mut entries: Vec<SpatialEntry> = Vec::with_capacity(col.len());
    if let Some(arr) = col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        if arr.value_length() == basin_geo::POINT_WKB_LEN as i32 {
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    continue;
                }
                let bytes = arr.value(row);
                let Some((x, y)) = decode_wkb_point_xy(bytes) else {
                    continue;
                };
                let row_group_id = (row as u32) / rg;
                let row_id_in_group = (row as u32) % rg;
                entries.push(SpatialEntry::point(row_group_id, row_id_in_group, x, y));
            }
        }
    }
    RTree::bulk_load(entries)
}

// ── Serialisation ────────────────────────────────────────────────────────────

/// Serialise an R-tree to a self-describing byte blob (magic + bincode).
///
/// The leading 4-byte magic (`b"BR1\n"`) lets [`deserialize_rtree`]
/// reject blobs it didn't write — a forward-compat hook if a v2 layout
/// ever ships.
pub fn serialize_rtree(rtree: &RTree<SpatialEntry>) -> Result<Vec<u8>, bincode::Error> {
    let mut out = Vec::with_capacity(64 + 32 * rtree.size());
    out.extend_from_slice(RTREE_MAGIC);
    let body = bincode::serialize(rtree)?;
    out.extend_from_slice(&body);
    Ok(out)
}

/// Inverse of [`serialize_rtree`]. Returns `None` if the magic header
/// is missing or wrong (e.g. an HNSW segment was mis-routed here).
pub fn deserialize_rtree(bytes: &[u8]) -> Option<RTree<SpatialEntry>> {
    if bytes.len() < RTREE_MAGIC.len() || &bytes[..RTREE_MAGIC.len()] != RTREE_MAGIC {
        return None;
    }
    bincode::deserialize(&bytes[RTREE_MAGIC.len()..]).ok()
}

// ── Sidecar path helpers ─────────────────────────────────────────────────────

/// Build the prefix at which all `.rtree` sidecars for a given
/// `(project, table, column)` live. Used when listing segments for the
/// warm-up scan (PG-Wave-β).
pub fn column_index_prefix(
    root: Option<&ObjectPath>,
    project: &ProjectId,
    table: &TableName,
    column: &str,
) -> ObjectPath {
    let mut p = root.cloned().unwrap_or_else(|| ObjectPath::from(""));
    p = p.child(crate::paths::PROJECTS_SEGMENT);
    p = p.child(project.as_prefix());
    p = p.child("tables");
    p = p.child(table.as_str());
    p = p.child("index");
    p.child(format!("{column}.rtree"))
}

fn index_segment_key(
    root: Option<&ObjectPath>,
    project: &ProjectId,
    table: &TableName,
    column: &str,
    file_id: Ulid,
) -> ObjectPath {
    column_index_prefix(root, project, table, column).child(format!("{file_id}.rtree"))
}

/// Given a data file path under the canonical
/// `projects/{p}/tables/{t}/.../{ulid}.parquet` layout, return the
/// `.rtree` sidecar path that *would* exist for `column` if that column
/// had spatial entries in this file. Mirrors
/// [`crate::vector_index_segment_key_for_data_file`] for the HNSW path.
///
/// Returns `None` if the data path can't be parsed (e.g. a non-project
/// path). Callers either write to this path during compaction or list
/// the prefix at warm-up time.
pub fn rtree_segment_key_for_data_file(
    root: Option<&ObjectPath>,
    project: &ProjectId,
    table: &TableName,
    column: &str,
    data_file_path: &str,
) -> Option<ObjectPath> {
    let last = data_file_path.rsplit('/').next()?;
    let stem = last.strip_suffix(".parquet")?;
    let ulid: Ulid = stem.parse().ok()?;
    Some(index_segment_key(root, project, table, column, ulid))
}

// ── Registry (lazy warm-up cache) ────────────────────────────────────────────

/// Process-wide registry of parsed `.rtree` segments, keyed by
/// `(project, table, col, file_path)`. Mirrors the lifecycle of
/// [`super::gin_rowgroup::GinRowGroupRegistry`]: compactor writes a
/// segment, planner probes for candidates, eviction on file removal.
///
/// The registry stores the parsed `RTree<SpatialEntry>` so probe-time
/// access avoids the bincode decode hot-path cost. Engine wave β will
/// populate this lazily from object-store reads.
pub struct RTreeRegistry {
    inner: std::sync::Mutex<std::collections::HashMap<RegKey, Arc<std::sync::Mutex<FileRTrees>>>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RegKey {
    project: ProjectId,
    table: TableName,
    col: String,
}

#[derive(Default)]
struct FileRTrees {
    by_file: std::collections::HashMap<String, Arc<RTree<SpatialEntry>>>,
}

impl RTreeRegistry {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    fn get_or_create(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
    ) -> Arc<std::sync::Mutex<FileRTrees>> {
        let key = RegKey {
            project: *project,
            table: table.clone(),
            col: col.to_string(),
        };
        let mut map = self
            .inner
            .lock()
            .expect("RTreeRegistry outer lock poisoned");
        map.entry(key)
            .or_insert_with(|| Arc::new(std::sync::Mutex::new(FileRTrees::default())))
            .clone()
    }

    /// Insert a parsed R-tree segment into the registry. Called from
    /// the warm-up path after a sidecar has been fetched + deserialised.
    pub fn insert_segment(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
        rtree: RTree<SpatialEntry>,
    ) {
        let arc = self.get_or_create(project, table, col);
        let mut files = arc.lock().expect("RTreeRegistry file table lock poisoned");
        files.by_file.insert(file_path.to_string(), Arc::new(rtree));
    }

    /// Probe a file's R-tree for entries whose envelope intersects
    /// `query_bbox`. Returns the surviving row-groups (ascending,
    /// dedup'd) or `None` if the file has no segment in the registry.
    pub fn candidate_row_groups(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
        query_bbox: AABB<[f64; 2]>,
    ) -> Option<Vec<RowGroupId>> {
        let arc = {
            let map = self
                .inner
                .lock()
                .expect("RTreeRegistry outer lock poisoned");
            map.get(&RegKey {
                project: *project,
                table: table.clone(),
                col: col.to_string(),
            })
            .cloned()
        }?;
        let files = arc.lock().expect("RTreeRegistry file table lock poisoned");
        let rtree = files.by_file.get(file_path)?.clone();
        let query: Rectangle<[f64; 2]> = query_bbox.into();
        let mut rgs: Vec<RowGroupId> = rtree
            .locate_in_envelope_intersecting(&query.envelope())
            .map(|e| e.row_group_id)
            .collect();
        rgs.sort_unstable();
        rgs.dedup();
        Some(rgs)
    }

    /// Probe a file's R-tree for the EXACT set of rows whose envelope
    /// intersects `query_bbox`, returned as `(row_group_id,
    /// row_id_in_group)` pairs (ascending, dedup'd) — or `None` if the
    /// file has no segment in the registry.
    ///
    /// For a POINT column this is an *exact* membership test: a point's
    /// zero-area envelope intersects the AABB query iff the point lies in
    /// the (closed) rectangle, so `locate_in_envelope_intersecting`
    /// answers `geom && ST_MakeEnvelope(...)` (PostGIS bbox-overlap,
    /// boundary-inclusive) with no residual geometry decode required.
    ///
    /// This is the row-granular companion to [`candidate_row_groups`]:
    /// the engine bbox-`&&` pushdown consumes these pairs to build an
    /// exact row selection, so the FilterExec residual above the scan
    /// does NOT have to re-decode every WKB POINT in a surviving
    /// row-group (the dominant cost of the planar `ST_X/ST_Y BETWEEN`
    /// rewrite). Mirrors how [`nearest_entries`] exposes per-row
    /// candidates for the KNN path.
    ///
    /// [`candidate_row_groups`]: Self::candidate_row_groups
    /// [`nearest_entries`]: Self::nearest_entries
    pub fn candidate_rows(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
        query_bbox: AABB<[f64; 2]>,
    ) -> Option<Vec<(RowGroupId, u32)>> {
        let arc = {
            let map = self
                .inner
                .lock()
                .expect("RTreeRegistry outer lock poisoned");
            map.get(&RegKey {
                project: *project,
                table: table.clone(),
                col: col.to_string(),
            })
            .cloned()
        }?;
        let files = arc.lock().expect("RTreeRegistry file table lock poisoned");
        let rtree = files.by_file.get(file_path)?.clone();
        let query: Rectangle<[f64; 2]> = query_bbox.into();
        let mut rows: Vec<(RowGroupId, u32)> = rtree
            .locate_in_envelope_intersecting(&query.envelope())
            .map(|e| (e.row_group_id, e.row_id_in_group))
            .collect();
        rows.sort_unstable();
        rows.dedup();
        Some(rows)
    }

    /// Nearest-neighbour probe for KNN spatial queries.
    ///
    /// Returns up to `take` entries from `file_path`'s R-tree in ascending
    /// L2-degree distance from `query` (`[x, y]`), or `None` if the file has
    /// no segment loaded.  The caller over-fetches (more than the user's
    /// `LIMIT k`) and re-ranks the union of per-file candidates by the exact
    /// Haversine metric — L2-degree order is only an approximation of
    /// great-circle order, so the over-fetch + re-rank is what makes the
    /// final top-k correct (see `rtree_knn_scan`).
    ///
    /// Entries are returned as `(row_group_id, row_id_in_group, x, y)`.
    pub fn nearest_entries(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
        query: [f64; 2],
        take: usize,
    ) -> Option<Vec<(RowGroupId, u32, f64, f64)>> {
        let arc = {
            let map = self
                .inner
                .lock()
                .expect("RTreeRegistry outer lock poisoned");
            map.get(&RegKey {
                project: *project,
                table: table.clone(),
                col: col.to_string(),
            })
            .cloned()
        }?;
        let files = arc.lock().expect("RTreeRegistry file table lock poisoned");
        let rtree = files.by_file.get(file_path)?.clone();
        let out: Vec<(RowGroupId, u32, f64, f64)> = rtree
            .nearest_neighbor_iter(&query)
            .take(take)
            .map(|e| {
                // For a POINT the envelope is zero-area, so min == max == coord.
                (
                    e.row_group_id,
                    e.row_id_in_group,
                    e.min_corner[0],
                    e.min_corner[1],
                )
            })
            .collect();
        Some(out)
    }

    /// Drop the segment for `file_path` (called on compaction so
    /// superseded files don't keep their stale R-tree resident).
    pub fn remove_file(&self, project: &ProjectId, table: &TableName, col: &str, file_path: &str) {
        let key = RegKey {
            project: *project,
            table: table.clone(),
            col: col.to_string(),
        };
        let arc = {
            let map = self
                .inner
                .lock()
                .expect("RTreeRegistry outer lock poisoned");
            map.get(&key).cloned()
        };
        if let Some(arc) = arc {
            let mut files = arc.lock().expect("RTreeRegistry file table lock poisoned");
            files.by_file.remove(file_path);
        }
    }

    /// Whether `file_path` has a segment loaded for this column.
    pub fn is_file_indexed(
        &self,
        project: &ProjectId,
        table: &TableName,
        col: &str,
        file_path: &str,
    ) -> bool {
        let key = RegKey {
            project: *project,
            table: table.clone(),
            col: col.to_string(),
        };
        let arc = {
            let map = self
                .inner
                .lock()
                .expect("RTreeRegistry outer lock poisoned");
            map.get(&key).cloned()
        };
        match arc {
            None => false,
            Some(arc) => {
                let files = arc.lock().expect("RTreeRegistry file table lock poisoned");
                files.by_file.contains_key(file_path)
            }
        }
    }
}

impl Default for RTreeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::FixedSizeBinaryBuilder;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;

    fn make_point_batch(points: &[(i64, f64, f64)]) -> RecordBatch {
        let ids: Int64Array = points.iter().map(|(id, _, _)| *id).collect();
        let mut wkb_b =
            FixedSizeBinaryBuilder::with_capacity(points.len(), basin_geo::POINT_WKB_LEN as i32);
        for (_, x, y) in points {
            let bytes = basin_geo::encode_point(&basin_geo::Point::new(*x, *y));
            wkb_b.append_value(bytes).unwrap();
        }
        let mut meta = HashMap::new();
        meta.insert("BASIN_TYPE".to_string(), "POINT".to_string());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "p",
                DataType::FixedSizeBinary(basin_geo::POINT_WKB_LEN as i32),
                true,
            )
            .with_metadata(meta),
        ]));
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(wkb_b.finish())]).unwrap()
    }

    #[test]
    fn rtree_build_for_simple_batch() {
        // Ten points scattered in a 100x100 box; query bbox covers half
        // the plane and we expect ~half the entries to survive.
        let pts: Vec<(i64, f64, f64)> = (0..10)
            .map(|i| (i, i as f64 * 10.0, i as f64 * 10.0))
            .collect();
        let batch = make_point_batch(&pts);
        let rtree = build_rtree_for_batch(&batch, 1, 4);
        assert_eq!(rtree.size(), 10, "all ten points indexed");

        // All entries lie on the diagonal y = x; the query bbox covers x ∈
        // [0, 45] which selects rows 0..=4 (5 rows).
        let q = AABB::from_corners([0.0, 0.0], [45.0, 45.0]);
        let hits: Vec<&SpatialEntry> = rtree.locate_in_envelope_intersecting(&q).collect();
        assert_eq!(hits.len(), 5, "expected 5 entries in [0..45]^2 quadrant");
    }

    #[test]
    fn rtree_bincode_round_trip() {
        // Build, serialise, deserialise; same query must return the same
        // entries (bit-for-bit equal).
        let pts: Vec<(i64, f64, f64)> = (0..16)
            .map(|i| (i, i as f64 * 5.0, (i % 4) as f64 * 5.0))
            .collect();
        let batch = make_point_batch(&pts);
        let rtree = build_rtree_for_batch(&batch, 1, 4);

        let bytes = serialize_rtree(&rtree).expect("serialize");
        assert!(bytes.starts_with(b"BR1\n"), "magic header present");

        let restored = deserialize_rtree(&bytes).expect("deserialize");
        assert_eq!(restored.size(), rtree.size());

        let q = AABB::from_corners([0.0, 0.0], [40.0, 100.0]);
        let before: Vec<SpatialEntry> =
            rtree.locate_in_envelope_intersecting(&q).copied().collect();
        let after: Vec<SpatialEntry> = restored
            .locate_in_envelope_intersecting(&q)
            .copied()
            .collect();
        let mut bs = before.clone();
        let mut as_ = after.clone();
        bs.sort_by_key(|e| (e.row_group_id, e.row_id_in_group));
        as_.sort_by_key(|e| (e.row_group_id, e.row_id_in_group));
        assert_eq!(
            bs, as_,
            "round-trip must preserve exact entry set under the same query"
        );

        // Forward-compat: a blob without the magic header must fail.
        let mut tampered = bytes.clone();
        tampered[0] = b'X';
        assert!(
            deserialize_rtree(&tampered).is_none(),
            "wrong magic must yield None, not garbage"
        );
    }

    #[test]
    fn rtree_row_group_assignment_uses_rg_size() {
        // With rg_size = 3 and 10 rows, row 0..=2 are rg 0, 3..=5 rg 1,
        // 6..=8 rg 2, 9 rg 3. Probing the bbox containing the last row
        // must yield row_group_id == 3.
        let pts: Vec<(i64, f64, f64)> = (0..10).map(|i| (i, i as f64, 0.0)).collect();
        let batch = make_point_batch(&pts);
        let rtree = build_rtree_for_batch(&batch, 1, 3);
        let q = AABB::from_corners([8.5, -1.0], [9.5, 1.0]);
        let hits: Vec<&SpatialEntry> = rtree.locate_in_envelope_intersecting(&q).collect();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].row_group_id, 3);
        assert_eq!(hits[0].row_id_in_group, 0);
    }

    #[test]
    fn registry_probe_returns_candidate_row_groups() {
        let reg = RTreeRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("locs").unwrap();
        let col = "p";
        let file = "f1.parquet";

        // Two row-groups: rg 0 holds points near origin, rg 1 holds points far away.
        let pts: Vec<(i64, f64, f64)> = vec![
            (0, 0.1, 0.1),
            (1, 0.2, 0.2),
            (2, 0.3, 0.3),
            (3, 100.0, 100.0),
            (4, 101.0, 101.0),
            (5, 102.0, 102.0),
        ];
        let batch = make_point_batch(&pts);
        let rtree = build_rtree_for_batch(&batch, 1, 3);
        reg.insert_segment(&project, &table, col, file, rtree);

        // Query near origin → only rg 0.
        let near = reg
            .candidate_row_groups(
                &project,
                &table,
                col,
                file,
                AABB::from_corners([-1.0, -1.0], [1.0, 1.0]),
            )
            .expect("file is indexed");
        assert_eq!(near, vec![0]);

        // Query far → only rg 1.
        let far = reg
            .candidate_row_groups(
                &project,
                &table,
                col,
                file,
                AABB::from_corners([99.0, 99.0], [103.0, 103.0]),
            )
            .expect("file is indexed");
        assert_eq!(far, vec![1]);

        // Eviction must drop the segment.
        reg.remove_file(&project, &table, col, file);
        assert!(!reg.is_file_indexed(&project, &table, col, file));
        assert!(reg
            .candidate_row_groups(
                &project,
                &table,
                col,
                file,
                AABB::from_corners([0.0, 0.0], [200.0, 200.0])
            )
            .is_none());
    }

    #[test]
    fn sidecar_path_mirrors_hnsw_layout() {
        let project = ProjectId::new();
        let table = TableName::new("locs").unwrap();
        let prefix = format!(
            "projects/{}/tables/locs/data/01J7Z0FZ0K0K0K0K0K0K0K0K0K.parquet",
            project.as_prefix()
        );
        let key = rtree_segment_key_for_data_file(None, &project, &table, "p", &prefix)
            .expect("valid data path");
        let s = key.as_ref();
        assert!(
            s.contains("/index/p.rtree/01J7Z0FZ0K0K0K0K0K0K0K0K0K.rtree"),
            "unexpected sidecar path: {s}"
        );
    }

    #[test]
    fn envelope_query_boundary_disjoint_and_containing() {
        // Three points: on the box boundary, strictly inside, far outside.
        // PostGIS `&&` (and `locate_in_envelope_intersecting`) is
        // boundary-INCLUSIVE, so a point exactly on the edge/corner counts.
        let pts: Vec<(i64, f64, f64)> = vec![
            (0, 10.0, 10.0), // on the upper-right corner of [0,10]^2
            (1, 5.0, 5.0),   // strictly inside
            (2, 50.0, 50.0), // disjoint
            (3, 0.0, 5.0),   // on the left edge
        ];
        let batch = make_point_batch(&pts);
        // rg_size large so every row is row-group 0 → exercise pure
        // envelope geometry, not row-group bucketing.
        let rtree = build_rtree_for_batch(&batch, 1, 1000);

        // Box exactly [0,10]^2: boundary-touching corner (0) + left-edge (3)
        // + interior (1) all qualify; the disjoint point (2) does not.
        let q = AABB::from_corners([0.0, 0.0], [10.0, 10.0]);
        let mut ids: Vec<u32> = rtree
            .locate_in_envelope_intersecting(&q)
            .map(|e| e.row_id_in_group)
            .collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![0, 1, 3],
            "boundary-inclusive: corner+edge+interior, not the disjoint point"
        );

        // Fully-containing query: a huge box swallows everything.
        let q_all = AABB::from_corners([-1000.0, -1000.0], [1000.0, 1000.0]);
        assert_eq!(
            rtree.locate_in_envelope_intersecting(&q_all).count(),
            4,
            "a box containing the whole dataset returns every entry"
        );

        // Fully-disjoint query: a box touching nothing returns nothing.
        let q_none = AABB::from_corners([200.0, 200.0], [300.0, 300.0]);
        assert_eq!(
            rtree.locate_in_envelope_intersecting(&q_none).count(),
            0,
            "a disjoint box returns no entries"
        );
    }

    #[test]
    fn candidate_rows_is_exact_for_points() {
        // candidate_rows must return the EXACT (rg, row) set a bbox-`&&`
        // would match — no row-group over-approximation, no residual decode.
        let reg = RTreeRegistry::new();
        let project = ProjectId::new();
        let table = TableName::new("locs").unwrap();
        let col = "p";
        let file = "f1.parquet";

        // rg_size = 3 → rows 0..=2 in rg 0, rows 3..=5 in rg 1.  rg 0 has a
        // mix of inside/outside points so a row-group prune would over-select
        // it, but candidate_rows must pinpoint only the matching rows.
        let pts: Vec<(i64, f64, f64)> = vec![
            (0, 1.0, 1.0),   // rg0 row0 — inside [0,5]^2
            (1, 4.0, 4.0),   // rg0 row1 — inside
            (2, 99.0, 99.0), // rg0 row2 — OUTSIDE (but shares rg0)
            (3, 2.0, 2.0),   // rg1 row0 — inside
            (4, 80.0, 80.0), // rg1 row1 — outside
            (5, 5.0, 0.0),   // rg1 row2 — on the boundary (inside, inclusive)
        ];
        let batch = make_point_batch(&pts);
        let rtree = build_rtree_for_batch(&batch, 1, 3);
        reg.insert_segment(&project, &table, col, file, rtree);

        let q = AABB::from_corners([0.0, 0.0], [5.0, 5.0]);
        let rows = reg
            .candidate_rows(&project, &table, col, file, q)
            .expect("file is indexed");
        // Expect exactly rg0/row0, rg0/row1, rg1/row0, rg1/row2 — NOT the
        // two outside points even though they share row-groups with hits.
        assert_eq!(rows, vec![(0, 0), (0, 1), (1, 0), (1, 2)]);

        // Row-group prune is a strict superset of the exact rows: both rg0
        // and rg1 survive because each holds at least one hit.
        let rgs = reg
            .candidate_row_groups(&project, &table, col, file, q)
            .expect("file is indexed");
        assert_eq!(rgs, vec![0, 1]);

        // Unknown file → None (caller falls back to full scan).
        assert!(reg
            .candidate_rows(&project, &table, col, "missing.parquet", q)
            .is_none());
    }

    #[test]
    fn non_point_column_yields_empty_tree() {
        // A column that isn't FixedSizeBinary(21) — e.g. Int64 — must not
        // crash and must produce an empty tree.
        let ids: Int64Array = (0..5).collect();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ids)]).unwrap();
        let rtree = build_rtree_for_batch(&batch, 0, 4);
        assert_eq!(rtree.size(), 0);
    }
}
