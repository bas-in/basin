//! `basin-vector` — native vector search primitives for Basin.
//!
//! Implements distance functions and an HNSW-backed approximate nearest
//! neighbour index, with a serialisable on-disk format the storage layer
//! writes alongside Parquet. Not pg_vector compatibility — see
//! [ADR 0003](../../../docs/decisions/0003-native-vector-search.md).
//!
//! Two consumers:
//!
//! 1. `basin-engine` calls the distance functions directly as DataFusion
//!    UDFs (`l2_distance`, `cosine_distance`, `dot_product`).
//! 2. `basin-storage` writes / reads HNSW segment files via [`HnswIndex`].
//!    Each Parquet data file may have a sibling `.hnsw` file under
//!    `tenants/{tenant}/tables/{table}/index/{column}.hnsw`.
//!
//! The crate stays small on purpose. New ANN algorithms (IVF-flat, ScaNN)
//! land here only when a wedge customer pays for them.

#![forbid(unsafe_code)]

mod distance;
mod index;

pub use distance::{cosine_distance, dot_product, l2_distance, Distance};
pub use index::{HnswIndex, HnswIndexBuilder, HnswSearchResult};
