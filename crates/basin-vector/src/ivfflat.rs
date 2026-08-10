//! IVFFlat (inverted-file flat) approximate nearest-neighbour index.
//!
//! pgvector's IVFFlat partitions the vector space into `lists` cells via a
//! coarse quantiser (cluster centroids). A query probes the `probes` cells
//! whose centroids are nearest to the query vector and brute-forces the
//! vectors inside only those cells. This is *approximate*: a true nearest
//! neighbour can sit just across a cell boundary in a cell we didn't probe.
//! Recall rises with `probes` (probe more cells) and is the documented ANN
//! tradeoff — identical in spirit to HNSW's `ef_search`.
//!
//! Basin's IVFFlat is a self-contained in-memory structure built from a set
//! of `(id, vector)` pairs:
//!
//!   1. **Build** runs a small, deterministic Lloyd's-style k-means
//!      (`lists` centroids, fixed iteration budget) over the vectors, then
//!      assigns every vector to its nearest centroid → the inverted lists.
//!   2. **Search** ranks the centroids by distance to the query, takes the
//!      `probes` nearest, gathers every vector in those lists as *candidates*,
//!      computes exact distances, and returns the top-k. Because the final
//!      sort is over exact distances, the *returned rows are exactly ordered*
//!      among the candidates that were retrieved — recall < 100% only means a
//!      true neighbour was never made a candidate, never that the order is
//!      wrong.
//!
//! Determinism: k-means seeding picks the first `lists` distinct vectors as
//! initial centroids (stable for a given input order). No RNG, so builds are
//! reproducible — important for differential tests.
//!
//! The metric is carried the same way [`crate::HnswIndex`] carries it: the
//! `Distance` chosen at build time is applied both for centroid assignment
//! and for the final candidate ranking.

use crate::distance::Distance;
use basin_common::{BasinError, Result};

/// Default number of cells to probe at query time. Matches pgvector's
/// `ivfflat.probes` default of 1.
pub const DEFAULT_PROBES: usize = 1;

/// Fixed k-means iteration budget. pgvector caps Lloyd iterations similarly;
/// a small budget keeps build O(iters * rows * lists * dim) bounded and is
/// plenty for the coarse quantiser (we don't need tight clusters, just a
/// reasonable partition). Build is one-shot per segment.
const KMEANS_ITERS: usize = 10;

/// One match from an IVFFlat search: the inserted `id` and its exact distance
/// under the index's metric.
#[derive(Clone, Debug, PartialEq)]
pub struct IvfFlatSearchResult {
    pub id: u64,
    pub distance: f32,
}

/// Accumulates `(id, vector)` pairs, then `.build(lists)` constructs the
/// partitioned index in one shot (mirrors [`crate::HnswIndexBuilder`]).
pub struct IvfFlatIndexBuilder {
    distance: Distance,
    dim: usize,
    points: Vec<Vec<f32>>,
    ids: Vec<u64>,
}

impl IvfFlatIndexBuilder {
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
        self.points.push(vector);
        self.ids.push(id);
        Ok(())
    }

    /// Build the index with `lists` cells. `lists` is clamped to `[1, n]`
    /// where `n` is the number of inserted vectors — pgvector likewise can't
    /// produce more non-empty lists than it has rows, and `lists = 0` is
    /// meaningless. An empty input yields an empty (but queryable) index.
    pub fn build(self, lists: usize) -> IvfFlatIndex {
        let n = self.points.len();
        if n == 0 {
            return IvfFlatIndex {
                distance: self.distance,
                dim: self.dim,
                count: 0,
                centroids: Vec::new(),
                lists: Vec::new(),
            };
        }
        let lists = lists.clamp(1, n);
        let centroids = kmeans(&self.points, lists, self.distance, self.dim);
        // Assign every point to its nearest centroid → inverted lists.
        let mut inverted: Vec<Vec<(u64, Vec<f32>)>> = vec![Vec::new(); centroids.len()];
        for (vec, id) in self.points.into_iter().zip(self.ids.into_iter()) {
            let c = nearest_centroid(&centroids, &vec, self.distance);
            inverted[c].push((id, vec));
        }
        IvfFlatIndex {
            distance: self.distance,
            dim: self.dim,
            count: n,
            centroids,
            lists: inverted,
        }
    }
}

/// Built IVFFlat index ready for queries. Held in memory; the storage layer
/// can serialise it via the same sidecar discipline as HNSW if/when an
/// on-disk IVFFlat segment format is wired (today the index is built on demand
/// for tests and the programmatic API).
pub struct IvfFlatIndex {
    distance: Distance,
    dim: usize,
    count: usize,
    /// `lists.len()` centroids (the coarse quantiser).
    centroids: Vec<Vec<f32>>,
    /// `lists[c]` holds every `(id, vector)` assigned to centroid `c`.
    lists: Vec<Vec<(u64, Vec<f32>)>>,
}

impl IvfFlatIndex {
    pub fn distance(&self) -> Distance {
        self.distance
    }
    pub fn dim(&self) -> usize {
        self.dim
    }
    pub fn count(&self) -> usize {
        self.count
    }
    /// Number of cells (non-empty + empty) in the partition.
    pub fn num_lists(&self) -> usize {
        self.centroids.len()
    }

    /// Approximate top-k nearest neighbours of `query`, probing the `probes`
    /// nearest cells. `probes` is clamped to `[1, num_lists]`. Candidates from
    /// the probed cells are ranked by *exact* distance, so the returned rows
    /// are exactly ordered among the retrieved candidates.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        probes: usize,
    ) -> Result<Vec<IvfFlatSearchResult>> {
        if query.len() != self.dim {
            return Err(BasinError::Internal(format!(
                "query dim {} != index dim {}",
                query.len(),
                self.dim
            )));
        }
        if self.centroids.is_empty() || k == 0 {
            return Ok(Vec::new());
        }
        let probes = probes.clamp(1, self.centroids.len());

        // Rank centroids by distance to the query, take the nearest `probes`.
        let mut cell_dists: Vec<(usize, f32)> = self
            .centroids
            .iter()
            .enumerate()
            .map(|(i, c)| (i, self.distance.apply(query, c)))
            .collect();
        cell_dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        cell_dists.truncate(probes);

        // Gather candidates from the probed cells and compute exact distances.
        let mut hits: Vec<IvfFlatSearchResult> = Vec::new();
        for (cell, _) in cell_dists {
            for (id, vec) in &self.lists[cell] {
                hits.push(IvfFlatSearchResult {
                    id: *id,
                    distance: self.distance.apply(query, vec),
                });
            }
        }
        hits.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        hits.truncate(k);
        Ok(hits)
    }
}

/// Deterministic Lloyd's k-means over `points`. Seeds with the first `lists`
/// distinct points (stable for a given input order — no RNG so builds are
/// reproducible). Returns the final centroids. Empty clusters keep their seed
/// centroid (standard "no reassignment" handling) so the centroid count is
/// always exactly `min(lists, distinct_seeds)`.
fn kmeans(points: &[Vec<f32>], lists: usize, distance: Distance, dim: usize) -> Vec<Vec<f32>> {
    // Seed: first `lists` distinct points. If there are fewer distinct points
    // than `lists`, we end up with fewer centroids — every point still lands
    // in some list, which is what matters.
    let mut centroids: Vec<Vec<f32>> = Vec::with_capacity(lists);
    for p in points {
        if centroids.len() >= lists {
            break;
        }
        if !centroids.iter().any(|c| c == p) {
            centroids.push(p.clone());
        }
    }
    if centroids.is_empty() {
        centroids.push(points[0].clone());
    }

    for _ in 0..KMEANS_ITERS {
        // Accumulate per-centroid sums for the mean update.
        let mut sums: Vec<Vec<f64>> = vec![vec![0.0; dim]; centroids.len()];
        let mut counts: Vec<usize> = vec![0; centroids.len()];
        for p in points {
            let c = nearest_centroid(&centroids, p, distance);
            counts[c] += 1;
            for (s, &v) in sums[c].iter_mut().zip(p.iter()) {
                *s += v as f64;
            }
        }
        let mut moved = false;
        for c in 0..centroids.len() {
            if counts[c] == 0 {
                // Empty cluster: keep its seed centroid (no reassignment).
                continue;
            }
            for d in 0..dim {
                let mean = (sums[c][d] / counts[c] as f64) as f32;
                if (centroids[c][d] - mean).abs() > f32::EPSILON {
                    moved = true;
                }
                centroids[c][d] = mean;
            }
        }
        if !moved {
            break;
        }
    }
    centroids
}

/// Index of the centroid nearest to `p` under `distance`.
fn nearest_centroid(centroids: &[Vec<f32>], p: &[f32], distance: Distance) -> usize {
    let mut best = 0usize;
    let mut best_d = f32::INFINITY;
    for (i, c) in centroids.iter().enumerate() {
        let d = distance.apply(p, c);
        if d < best_d {
            best_d = d;
            best = i;
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rand_vec(seed: u64, dim: usize) -> Vec<f32> {
        let mut s = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (0..dim)
            .map(|_| {
                s = s
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                ((s >> 33) as f32 / u32::MAX as f32) - 0.5
            })
            .collect()
    }

    /// A clustered vector: pick one of `num_clusters` cluster centres (derived
    /// from `seed`) and jitter it. Real embeddings are clustered, not uniform
    /// random — single-probe IVFFlat recall is only meaningful on clustered
    /// data, where a query's true neighbours mostly share its cell.
    fn clustered_vec(seed: u64, dim: usize, num_clusters: u64) -> Vec<f32> {
        let cluster = seed % num_clusters;
        let centre = rand_vec(cluster.wrapping_mul(7919).wrapping_add(13), dim);
        let jitter = rand_vec(seed.wrapping_add(1_000_000), dim);
        centre
            .iter()
            .zip(jitter.iter())
            // Scale the cluster centre well above the jitter so clusters are
            // separated (jitter * 0.05 keeps points tight around their centre).
            .map(|(c, j)| c * 4.0 + j * 0.05)
            .collect()
    }

    /// Brute-force exact top-k (ground truth for recall).
    fn brute_force(points: &[(u64, Vec<f32>)], query: &[f32], k: usize, d: Distance) -> Vec<u64> {
        let mut all: Vec<(u64, f32)> = points
            .iter()
            .map(|(id, v)| (*id, d.apply(query, v)))
            .collect();
        all.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        all.into_iter().take(k).map(|(id, _)| id).collect()
    }

    fn recall(approx: &[u64], truth: &[u64]) -> f64 {
        if truth.is_empty() {
            return 1.0;
        }
        let hit = approx.iter().filter(|id| truth.contains(id)).count();
        hit as f64 / truth.len() as f64
    }

    #[test]
    fn build_partitions_all_points() {
        let dim = 16;
        let mut b = IvfFlatIndexBuilder::new(Distance::L2, dim);
        for i in 0..500 {
            b.insert(i, rand_vec(i, dim)).unwrap();
        }
        let idx = b.build(16);
        assert_eq!(idx.count(), 500);
        assert_eq!(idx.num_lists(), 16);
        // Every point must be in some list — total assignments == count.
        let total: usize = idx.lists.iter().map(|l| l.len()).sum();
        assert_eq!(
            total, 500,
            "every point must be assigned to exactly one cell"
        );
    }

    #[test]
    fn self_query_is_top1() {
        // A vector identical to one inserted must come back as the nearest,
        // PROVIDED we probe its own cell — with probes covering all cells it
        // is guaranteed. Exact ordering among candidates is the contract.
        let dim = 16;
        let mut b = IvfFlatIndexBuilder::new(Distance::L2, dim);
        for i in 0..200 {
            b.insert(i, rand_vec(i, dim)).unwrap();
        }
        let idx = b.build(8);
        let target = 42u64;
        let q = rand_vec(target, dim);
        // Probe every cell → exact result.
        let res = idx.search(&q, 1, idx.num_lists()).unwrap();
        assert_eq!(res[0].id, target, "all-cells probe must find self as top-1");
        assert!(res[0].distance.abs() < 1e-6);
    }

    #[test]
    fn more_probes_never_lowers_recall() {
        let dim = 32;
        let n = 1000u64;
        let lists = 32;
        let num_clusters = 24u64;
        let mut b = IvfFlatIndexBuilder::new(Distance::L2, dim);
        let mut pts = Vec::new();
        for i in 0..n {
            let v = clustered_vec(i, dim, num_clusters);
            pts.push((i, v.clone()));
            b.insert(i, v).unwrap();
        }
        let idx = b.build(lists);

        // Queries drawn from the same clustered distribution (the realistic
        // case: a query embedding lands near some cluster of stored vectors).
        let queries: Vec<Vec<f32>> = (0..50u64)
            .map(|q| clustered_vec(q.wrapping_mul(37) + 5, dim, num_clusters))
            .collect();
        let k = 10;

        let recall_at = |probes: usize| -> f64 {
            let mut acc = 0.0;
            for q in &queries {
                let truth = brute_force(&pts, q, k, Distance::L2);
                let got: Vec<u64> = idx
                    .search(q, k, probes)
                    .unwrap()
                    .into_iter()
                    .map(|r| r.id)
                    .collect();
                acc += recall(&got, &truth);
            }
            acc / queries.len() as f64
        };

        let r1 = recall_at(1);
        let r4 = recall_at(4);
        let r_all = recall_at(lists);
        println!("[ivfflat] recall@10 probes=1:{r1:.3} probes=4:{r4:.3} probes=all:{r_all:.3}");
        assert!(
            r4 >= r1 - 1e-9,
            "more probes must not lower recall: r1={r1} r4={r4}"
        );
        assert!(r_all >= r4 - 1e-9, "all-cells probe must not lower recall");
        // Probing every cell is exhaustive → recall must be 1.0.
        assert!(
            (r_all - 1.0).abs() < 1e-9,
            "all-cells probe must be exact, got {r_all}"
        );
        // On clustered data the single-probe default already recovers most of
        // the true top-k (a few probes reach the ≥0.7 the conformance suite
        // gates on); pin a meaningful floor here.
        assert!(
            r1 >= 0.5,
            "single-probe recall on clustered data too low: {r1}"
        );
        assert!(
            r4 >= 0.7,
            "4-probe recall on clustered data should clear 0.7: {r4}"
        );
    }

    #[test]
    fn candidates_are_exactly_ordered() {
        // The returned rows must be in exact ascending-distance order among
        // the rows that were retrieved (the ANN ordering contract).
        let dim = 24;
        let mut b = IvfFlatIndexBuilder::new(Distance::L2, dim);
        let mut pts = Vec::new();
        for i in 0..400 {
            let v = rand_vec(i, dim);
            pts.push((i, v.clone()));
            b.insert(i, v).unwrap();
        }
        let idx = b.build(16);
        let q = rand_vec(999_999, dim);
        let res = idx.search(&q, 10, 4).unwrap();
        for w in res.windows(2) {
            assert!(
                w[0].distance <= w[1].distance + 1e-6,
                "results not exactly ordered"
            );
        }
        // And each returned distance must equal the true distance to that id.
        for r in &res {
            let v = &pts.iter().find(|(id, _)| *id == r.id).unwrap().1;
            let exact = Distance::L2.apply(&q, v);
            assert!(
                (exact - r.distance).abs() < 1e-5,
                "reported distance not exact"
            );
        }
    }

    #[test]
    fn cosine_metric_works() {
        let dim = 12;
        let mut b = IvfFlatIndexBuilder::new(Distance::Cosine, dim);
        for i in 0..100 {
            b.insert(i, rand_vec(i, dim)).unwrap();
        }
        let idx = b.build(8);
        assert_eq!(idx.distance(), Distance::Cosine);
        let q = rand_vec(7, dim);
        let res = idx.search(&q, 1, idx.num_lists()).unwrap();
        assert_eq!(res[0].id, 7, "cosine self-query top-1");
    }

    #[test]
    fn lists_clamped_and_empty_input() {
        // lists > n is clamped to n.
        let dim = 4;
        let mut b = IvfFlatIndexBuilder::new(Distance::L2, dim);
        for i in 0..3 {
            b.insert(i, rand_vec(i, dim)).unwrap();
        }
        let idx = b.build(100);
        assert!(
            idx.num_lists() <= 3,
            "lists must clamp to n; got {}",
            idx.num_lists()
        );

        // Empty index is queryable and returns nothing.
        let empty = IvfFlatIndexBuilder::new(Distance::L2, dim).build(4);
        assert_eq!(empty.count(), 0);
        assert!(empty.search(&[0.0; 4], 5, 1).unwrap().is_empty());
    }

    #[test]
    fn dim_mismatch_errors() {
        let mut b = IvfFlatIndexBuilder::new(Distance::L2, 8);
        assert!(b.insert(0, vec![0.0; 7]).is_err());
        b.insert(1, vec![0.0; 8]).unwrap();
        let idx = b.build(2);
        assert!(idx.search(&[0.0; 7], 1, 1).is_err());
    }
}
