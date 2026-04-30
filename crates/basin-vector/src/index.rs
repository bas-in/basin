//! HNSW index segment, read/written via the storage layer.
//!
//! Wraps `instant-distance::Hnsw`. The on-disk format is bincode-of-bincode-
//! style: a tiny header (`Distance`, dim, count) followed by the serialised
//! HNSW. Future versions can grow the header without breaking readers.
//!
//! The id type is `u64`. Storage layer maps Parquet row positions to these
//! ids; the engine uses ids to fetch the corresponding payload columns
//! after the index returns its top-k.

use std::io::{Read, Write};

use basin_common::{BasinError, Result};
use instant_distance::{Builder, HnswMap, Point, Search};
use serde::{Deserialize, Serialize};

use crate::distance::Distance;

const MAGIC: [u8; 4] = *b"BVEC";
const VERSION: u16 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Header {
    magic: [u8; 4],
    version: u16,
    distance: Distance,
    dim: u32,
    count: u32,
}

/// One vector inserted into the index. The `instant_distance` crate requires
/// `Point + Clone` for the value being indexed; this wrapper supplies the
/// distance metric chosen at build time. Serde derives are required because
/// `HnswMap` is itself serde-derived (with-serde feature) and propagates the
/// bound to its Point parameter.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct DistancedPoint {
    data: Vec<f32>,
    distance: Distance,
}

impl Point for DistancedPoint {
    fn distance(&self, other: &Self) -> f32 {
        debug_assert_eq!(
            self.distance, other.distance,
            "mixed distances in one index"
        );
        self.distance.apply(&self.data, &other.data)
    }
}

/// Builder accumulates `(id, vector)` pairs, then `.build()` constructs the
/// index. `instant_distance` builds the graph at construction time, so this
/// is a one-shot per segment.
pub struct HnswIndexBuilder {
    distance: Distance,
    dim: usize,
    points: Vec<DistancedPoint>,
    ids: Vec<u64>,
}

impl HnswIndexBuilder {
    pub fn new(distance: Distance, dim: usize) -> Self {
        Self {
            distance,
            dim,
            points: Vec::new(),
            ids: Vec::new(),
        }
    }

    pub fn insert(&mut self, id: u64, vector: Vec<f32>) -> Result<()> {
        if vector.len() != self.dim {
            return Err(BasinError::Internal(format!(
                "vector dim {} != index dim {}",
                vector.len(),
                self.dim
            )));
        }
        self.points.push(DistancedPoint {
            data: vector,
            distance: self.distance,
        });
        self.ids.push(id);
        Ok(())
    }

    pub fn build(self) -> HnswIndex {
        let map = Builder::default().build(self.points.clone(), self.ids.clone());
        HnswIndex {
            distance: self.distance,
            dim: self.dim,
            count: self.points.len(),
            inner: map,
        }
    }
}

/// Built HNSW index ready for queries.
pub struct HnswIndex {
    distance: Distance,
    dim: usize,
    count: usize,
    inner: HnswMap<DistancedPoint, u64>,
}

#[derive(Clone, Debug)]
pub struct HnswSearchResult {
    pub id: u64,
    pub distance: f32,
}

impl HnswIndex {
    pub fn distance(&self) -> Distance {
        self.distance
    }
    pub fn dim(&self) -> usize {
        self.dim
    }
    pub fn count(&self) -> usize {
        self.count
    }

    /// Top-k approximate nearest neighbours of `query`.
    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<HnswSearchResult>> {
        if query.len() != self.dim {
            return Err(BasinError::Internal(format!(
                "query dim {} != index dim {}",
                query.len(),
                self.dim
            )));
        }
        let q = DistancedPoint {
            data: query.to_vec(),
            distance: self.distance,
        };
        let mut search = Search::default();
        let out: Vec<HnswSearchResult> = self
            .inner
            .search(&q, &mut search)
            .take(k)
            .map(|item| HnswSearchResult {
                id: *item.value,
                distance: item.distance,
            })
            .collect();
        Ok(out)
    }

    /// Serialise to the on-disk format. Bincode for the body so the parser
    /// runs at byte-copy speed; the header stays JSON for human-debuggable
    /// version/dim/count tags. Format: u32 little-endian header length,
    /// then the JSON header bytes, then the bincode-encoded HnswMap.
    pub fn write_to<W: Write>(&self, mut w: W) -> Result<()> {
        let header = Header {
            magic: MAGIC,
            version: VERSION,
            distance: self.distance,
            dim: self.dim as u32,
            count: self.count as u32,
        };
        let header_bytes = serde_json::to_vec(&header)
            .map_err(|e| BasinError::Internal(format!("header serialise: {e}")))?;
        w.write_all(&(header_bytes.len() as u32).to_le_bytes())
            .map_err(BasinError::from)?;
        w.write_all(&header_bytes).map_err(BasinError::from)?;
        let body = bincode::serialize(&self.inner)
            .map_err(|e| BasinError::Internal(format!("hnsw serialise: {e}")))?;
        w.write_all(&body).map_err(BasinError::from)?;
        Ok(())
    }

    pub fn read_from<R: Read>(mut r: R) -> Result<Self> {
        let mut len_buf = [0u8; 4];
        r.read_exact(&mut len_buf).map_err(BasinError::from)?;
        let header_len = u32::from_le_bytes(len_buf) as usize;
        let mut header_bytes = vec![0u8; header_len];
        r.read_exact(&mut header_bytes).map_err(BasinError::from)?;
        let header: Header = serde_json::from_slice(&header_bytes)
            .map_err(|e| BasinError::Internal(format!("header parse: {e}")))?;
        if header.magic != MAGIC {
            return Err(BasinError::Internal("bad magic in hnsw index".into()));
        }
        if header.version != VERSION {
            return Err(BasinError::Internal(format!(
                "unsupported hnsw index version {}",
                header.version
            )));
        }
        let mut body = Vec::new();
        r.read_to_end(&mut body).map_err(BasinError::from)?;
        let inner: HnswMap<DistancedPoint, u64> = bincode::deserialize(&body)
            .map_err(|e| BasinError::Internal(format!("hnsw parse: {e}")))?;
        Ok(Self {
            distance: header.distance,
            dim: header.dim as usize,
            count: header.count as usize,
            inner,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rand_vec(seed: u64, dim: usize) -> Vec<f32> {
        let mut s = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (0..dim)
            .map(|_| {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                ((s >> 33) as f32 / u32::MAX as f32) - 0.5
            })
            .collect()
    }

    #[test]
    fn roundtrip_and_search() {
        let dim = 16;
        let mut b = HnswIndexBuilder::new(Distance::L2, dim);
        for i in 0..200 {
            b.insert(i, rand_vec(i, dim)).unwrap();
        }
        let idx = b.build();
        assert_eq!(idx.count(), 200);
        assert_eq!(idx.dim(), dim);

        // Self-query: a vector identical to one we inserted should appear in the top-k.
        let target_id = 42u64;
        let query = rand_vec(target_id, dim);
        let res = idx.search(&query, 10).unwrap();
        assert!(res.iter().any(|r| r.id == target_id), "top-k missed self");

        // Roundtrip via the on-disk format.
        let mut buf = Vec::new();
        idx.write_to(&mut buf).unwrap();
        let restored = HnswIndex::read_from(buf.as_slice()).unwrap();
        assert_eq!(restored.count(), 200);
        assert_eq!(restored.dim(), dim);
        let res2 = restored.search(&query, 10).unwrap();
        assert!(res2.iter().any(|r| r.id == target_id));
    }

    #[test]
    fn dim_mismatch_errors() {
        let mut b = HnswIndexBuilder::new(Distance::Cosine, 8);
        b.insert(0, vec![0.1; 8]).unwrap();
        let err = b.insert(1, vec![0.1; 9]).unwrap_err();
        assert!(matches!(err, BasinError::Internal(_)));
    }
}
