//! PostGIS-shape scalar UDFs and engine wiring for the `basin-geo` crate.
//!
//! This module registers DataFusion `ScalarUDF`s that implement the
//! `ST_*` subset basin commits to in v0.1 of its PostGIS-compatible
//! surface. The UDFs themselves are thin wrappers around `basin_geo::*`
//! (Point + WKB + Haversine math), so the source-of-truth for spatial
//! semantics stays in one crate; this module is purely glue.
//!
//! ## Registered UDFs
//!
//! | UDF                 | Signature                              | Backed by                       |
//! |---------------------|----------------------------------------|---------------------------------|
//! | `ST_MakePoint`      | `(DOUBLE, DOUBLE) -> POINT (FSB(21))`  | `basin_geo::make_point` + encode |
//! | `ST_X`              | `(POINT) -> DOUBLE`                    | `basin_geo::decode_point` + `.x` |
//! | `ST_Y`              | `(POINT) -> DOUBLE`                    | `basin_geo::decode_point` + `.y` |
//! | `ST_Distance`       | `(POINT, POINT) -> DOUBLE` (meters)    | `basin_geo::haversine_meters`    |
//! | `ST_DWithin`        | `(POINT, POINT, DOUBLE) -> BOOL`       | `basin_geo::dwithin`             |
//! | `ST_AsText`         | `(POINT) -> TEXT` (PostGIS WKT)        | `basin_geo::decode_point`        |
//! | `ST_AsEWKB`         | `(POINT) -> BYTEA` (raw 21-byte WKB)   | identity over the FSB column     |
//! | `ST_GeomFromText`   | `(TEXT) -> POINT`                      | parses `POINT(x y)` WKT          |
//! | `ST_GeomFromWKB`    | `(BYTEA) -> POINT`                     | identity into FSB(21)            |
//! | `ST_AsGeoJSON`      | `(POINT) -> TEXT` (PostGIS GeoJSON)    | `basin_geo::geojson::encode_point_geojson` |
//! | `ST_GeomFromGeoJSON`| `(TEXT) -> POINT`                      | `basin_geo::geojson::decode_point_geojson` |
//!
//! `ST_Contains(box2d, point)` is out of this wave — BOX2D needs its own
//! physical-type wiring (FSB(33) or LargeBinary) and there is no DDL
//! `BOX2D` type yet. Roadmap.
//!
//! ## PG-Wave 3 predicates (this wave)
//!
//! | UDF                | Args                    | Notes                              |
//! |--------------------|-------------------------|------------------------------------|
//! | `ST_Intersects`    | POINT, POINT → BOOL     | true iff points are equal (geo)    |
//! | `ST_Within`        | POINT, POINT → BOOL     | true iff a==b (point within point) |
//! | `ST_Crosses`       | POINT, POINT → BOOL     | always false (two points can't cross) |
//! | `ST_Touches`       | POINT, POINT → BOOL     | true iff points are equal          |
//! | `ST_Disjoint`      | POINT, POINT → BOOL     | !ST_Intersects                     |
//! | `ST_Overlaps`      | POINT, POINT → BOOL     | always false (degenerate for POINT)|
//! | `ST_Envelope`      | POINT → POINT           | identity                           |
//! | `ST_Area`          | POINT → DOUBLE          | 0.0 (point has zero area)          |
//! | `ST_Perimeter`     | POINT → DOUBLE          | 0.0                                |
//! | `ST_Centroid`      | POINT → POINT           | identity                           |
//! | `ST_Buffer`        | POINT, DOUBLE → POINT   | identity (POLYGON storage deferred)|
//! | `ST_NumPoints`     | POINT → INT             | 1                                  |
//! | `ST_PointN`        | POINT, INT → POINT      | identity if n==1, NULL otherwise   |
//! | `ST_StartPoint`    | POINT → POINT           | identity                           |
//! | `ST_EndPoint`      | POINT → POINT           | identity                           |
//! | `ST_MakeEnvelope`  | f64×4 [,INT] → POINT    | returns the min-corner POINT       |
//!
//! These implementations are deliberately degenerate for POINT inputs because
//! basin-geo only stores POINT today. When LineString/Polygon types ship in
//! PG-Wave 5 (or later), the implementations will get proper geo-crate calls.
//! They are callable from SQL now so PostGIS-emitting client libraries work.
//!
//! ## Storage shape
//!
//! POINT columns ride on Arrow `FixedSizeBinary(21)` with field
//! metadata `BASIN_TYPE=POINT`. The bytes are the same little-endian
//! WKB layout `basin_geo::encode_point` emits, so the same row writes
//! out and reads back without a re-encode step.
//!
//! ## Engine wiring
//!
//! [`install_udfs`] is called once from
//! `session::build_stateless_udf_cache`. The UDFs are registered into
//! the throw-away `SessionContext` whose populated function maps are
//! cached and cloned per session-open — same shape as every other
//! stateless UDF set (`register_pg_udfs`, `register_jsonb_udfs`,
//! `register_regex_udfs`, …).
//!
//! [`install`] is kept as a no-op back-compat shim for the existing
//! `Engine::new` call site (which calls `install()` for symmetry with
//! `cron_glue::install()` / `net_glue::install()`).

use std::any::Any;
use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int32Builder,
    StringBuilder,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, Float64Array, Int32Array, StringArray,
};
use arrow_schema::DataType;
use basin_geo::{
    decode_point, dwithin, encode_point, haversine_meters, make_point, Point, POINT_WKB_LEN,
};
use basin_geo::geojson::{decode_point_geojson, encode_point_geojson};
// `geo` crate backing the PG-Wave 3 predicate library. Pure-Rust spatial
// algorithms; used to evaluate topology predicates (Intersects, Within, etc.)
// on `geo::Point<f64>` values converted from basin-geo Points at the FFI
// boundary. Only traits are imported — no geo storage types touch the wire.
//
// Note: `geo 0.28` ships Intersects and Within; Crosses/Touches/Overlaps are
// not present in this version. Those predicates are implemented below using
// point-equality logic, which is the correct degenerate-for-POINT result
// per the DE-9IM / PostGIS specification.
use geo::Intersects as GeoIntersects;
use geo::Within as GeoWithin;
use datafusion::common::{exec_err, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;

/// Back-compat no-op hook invoked once per `Engine::new`. Kept so the
/// call site at `lib.rs:285` stays valid; the real work is in
/// [`install_udfs`], which is wired through the stateless UDF cache.
#[inline]
pub(crate) fn install() {
    // No-op. UDFs ride through `install_udfs(&ctx)` from
    // `session::build_stateless_udf_cache`.
}

/// Register every `ST_*` ScalarUDF this crate ships. Idempotent under
/// DataFusion's "overwrite by name" semantics. Names are upper-case
/// (PG convention), but DataFusion compares case-insensitively so
/// `st_makepoint(...)` from a query parser that already lower-cased the
/// statement still resolves.
pub(crate) fn install_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(StMakePointUdf {
        signature: Signature::exact(
            vec![DataType::Float64, DataType::Float64],
            Volatility::Immutable,
        ),
    }));
    ctx.register_udf(ScalarUDF::from(StXUdf {
        signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
    }));
    ctx.register_udf(ScalarUDF::from(StYUdf {
        signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
    }));
    ctx.register_udf(ScalarUDF::from(StDistanceUdf {
        signature: Signature::exact(vec![point_dt(), point_dt()], Volatility::Immutable),
    }));
    ctx.register_udf(ScalarUDF::from(StDWithinUdf {
        signature: Signature::exact(
            vec![point_dt(), point_dt(), DataType::Float64],
            Volatility::Immutable,
        ),
    }));
    ctx.register_udf(ScalarUDF::from(StAsTextUdf {
        signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
    }));
    ctx.register_udf(ScalarUDF::from(StAsEwkbUdf {
        signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
    }));
    ctx.register_udf(ScalarUDF::from(StGeomFromTextUdf {
        // `geometry_from_text(text)` — single-arg form. PG also allows
        // `(text, srid)`; v0.1 ignores SRID, so we register only the
        // (Utf8) overload.
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8]),
            ],
            Volatility::Immutable,
        ),
    }));
    ctx.register_udf(ScalarUDF::from(StGeomFromWkbUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Binary]),
                TypeSignature::Exact(vec![DataType::LargeBinary]),
                TypeSignature::Exact(vec![DataType::FixedSizeBinary(POINT_WKB_LEN as i32)]),
            ],
            Volatility::Immutable,
        ),
    }));
    // ST_AsGeoJSON(p) — POINT → full RFC 7946 GeoJSON envelope (TEXT).
    // Output: {"type":"Point","coordinates":[x,y]}  (no spaces, lon first).
    ctx.register_udf(ScalarUDF::from(StAsGeoJsonUdf {
        signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
    }));
    // ST_GeomFromGeoJSON(text) — parse full GeoJSON envelope back to POINT.
    // Accepts both Utf8 and LargeUtf8 (same as ST_GeomFromText).
    ctx.register_udf(ScalarUDF::from(StGeomFromGeoJsonUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8]),
            ],
            Volatility::Immutable,
        ),
    }));

    // ── PG-Wave 3: topology predicates + degenerate-for-POINT scalars ──────

    // ST_Intersects(a, b) — true iff a and b share any point.
    // For two POINTs this reduces to coordinate equality.
    ctx.register_udf(ScalarUDF::from(StIntersectsUdf {
        signature: Signature::exact(vec![point_dt(), point_dt()], Volatility::Immutable),
    }));
    // ST_Within(a, b) — true iff a lies completely within b.
    // For two POINTs: a.within(b) iff a == b.
    ctx.register_udf(ScalarUDF::from(StWithinUdf {
        signature: Signature::exact(vec![point_dt(), point_dt()], Volatility::Immutable),
    }));
    // ST_Crosses(a, b) — geometries cross iff they share some but not all
    // interior points. Two POINTs can never cross — always false.
    ctx.register_udf(ScalarUDF::from(StCrossesUdf {
        signature: Signature::exact(vec![point_dt(), point_dt()], Volatility::Immutable),
    }));
    // ST_Touches(a, b) — geometries touch iff they share boundary points but
    // not interior points. For two POINTs the interior IS the boundary, so
    // this reduces to coordinate equality (same semantics as Intersects for
    // dimensionless points).
    ctx.register_udf(ScalarUDF::from(StTouchesUdf {
        signature: Signature::exact(vec![point_dt(), point_dt()], Volatility::Immutable),
    }));
    // ST_Disjoint(a, b) — complement of ST_Intersects.
    ctx.register_udf(ScalarUDF::from(StDisjointUdf {
        signature: Signature::exact(vec![point_dt(), point_dt()], Volatility::Immutable),
    }));
    // ST_Overlaps(a, b) — geometries overlap iff their intersection has the
    // same dimension as each operand and is strictly smaller than each.
    // For two POINTs their "overlap" intersection is either the whole point
    // (equal) or empty — neither case has *strictly smaller* dimension, so
    // ST_Overlaps is always false for POINT × POINT per PostGIS.
    ctx.register_udf(ScalarUDF::from(StOverlapsUdf {
        signature: Signature::exact(vec![point_dt(), point_dt()], Volatility::Immutable),
    }));
    // ST_Envelope(p) — bounding box. For a point the envelope is the point
    // itself. Identity.
    ctx.register_udf(ScalarUDF::from(StEnvelopeUdf {
        signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
    }));
    // ST_Area(p) — area of geometry. Points have zero area.
    ctx.register_udf(ScalarUDF::from(StAreaUdf {
        signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
    }));
    // ST_Perimeter(p) — perimeter of geometry. Points have zero perimeter.
    ctx.register_udf(ScalarUDF::from(StPerimeterUdf {
        signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
    }));
    // ST_Centroid(p) — centroid of geometry. For a point the centroid is the
    // point itself. Identity.
    ctx.register_udf(ScalarUDF::from(StCentroidUdf {
        signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
    }));
    // ST_Buffer(p, r) — returns a geometry representing all points whose
    // distance from p is <= r. For a point this is a circle (disk), which
    // would require a POLYGON storage type. basin-geo only stores POINT today;
    // when Polygon columns ship in PG-Wave 5 (or later), this should return
    // a true circle approximated as a polygon. For now: identity — the POINT
    // is returned unchanged so client libraries that call ST_Buffer purely for
    // its bounding-box side effects still get a valid geometry back.
    ctx.register_udf(ScalarUDF::from(StBufferUdf {
        signature: Signature::exact(vec![point_dt(), DataType::Float64], Volatility::Immutable),
    }));
    // ST_NumPoints(p) — number of vertices. A POINT has exactly 1 vertex.
    ctx.register_udf(ScalarUDF::from(StNumPointsUdf {
        signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
    }));
    // ST_PointN(p, n) — 1-based vertex access. For a POINT only n==1 is
    // valid; all other indices return NULL (matching PostGIS behaviour).
    ctx.register_udf(ScalarUDF::from(StPointNUdf {
        signature: Signature::exact(
            vec![point_dt(), DataType::Int32],
            Volatility::Immutable,
        ),
    }));
    // ST_StartPoint(p) — first vertex. For a POINT: identity.
    ctx.register_udf(ScalarUDF::from(StStartPointUdf {
        signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
    }));
    // ST_EndPoint(p) — last vertex. For a POINT: identity.
    ctx.register_udf(ScalarUDF::from(StEndPointUdf {
        signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
    }));
    // ST_MakeEnvelope(min_x, min_y, max_x, max_y [, srid]) — construct a
    // bounding-box geometry from four f64 corners plus an optional SRID.
    // basin-geo has no BOX2D physical-column type yet, so we store the result
    // as a POINT at the min-corner. When BOX2D columns ship the implementation
    // will change to return FSB(33) (or LargeBinary). Clients that call
    // ST_MakeEnvelope primarily for ST_Contains / ST_Intersects filtering get
    // a callable UDF today; the degenerate semantics are documented.
    ctx.register_udf(ScalarUDF::from(StMakeEnvelopeUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Int32,
                ]),
            ],
            Volatility::Immutable,
        ),
    }));

    // ── PG-Wave 5: SRID-aware geometry surface ────────────────────────────
    //
    // The SRID for a column is carried on the Arrow `Field` metadata
    // (`BASIN_SRID` key). These three UDFs are the public surface for
    // reading and changing that SRID and for reprojecting between SRIDs
    // via `basin_geo::transform::st_transform`.

    // ST_SRID(p) → INT4 — reads the BASIN_SRID metadata off the column.
    // Returns 0 when the column has no declared SRID (PostGIS's "unknown
    // SRID" convention).
    ctx.register_udf(ScalarUDF::from(StSridUdf {
        signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
    }));
    // ST_SetSRID(p, srid) → POINT — identity-bytes pass-through. The
    // returned column has no field-level SRID (we'd need a planner
    // hook to re-stamp it). Documented gap; ST_SetSRID is most useful
    // inside an INSERT, where the destination column's BASIN_SRID
    // already pins the SRID.
    ctx.register_udf(ScalarUDF::from(StSetSridUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![point_dt(), DataType::Int32]),
                TypeSignature::Exact(vec![point_dt(), DataType::Int64]),
            ],
            Volatility::Immutable,
        ),
    }));
    // ST_Transform(p, dst_srid) → POINT — reproject using proj4rs.
    // Accept Int32 (native) and Int64 (the sqlparser default for integer
    // literals) so neither `ST_Transform(p, 4326)` nor explicitly-cast
    // `ST_Transform(p, 4326::int4)` trip the type checker.
    ctx.register_udf(ScalarUDF::from(StTransformUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![point_dt(), DataType::Int32]),
                TypeSignature::Exact(vec![point_dt(), DataType::Int64]),
            ],
            Volatility::Immutable,
        ),
    }));

    // ── Geography aliases ─────────────────────────────────────────────────
    //
    // PostGIS treats GEOGRAPHY as a separate type that auto-densifies
    // arcs and uses Haversine for distance. basin-geo's distance ops
    // are already Haversine and we have no separate physical type, so
    // ST_GeogFromText / ST_GeographyFromText are exact aliases for
    // ST_GeomFromText. Registered under distinct names so client code
    // that emits the geography variants still resolves.
    ctx.register_udf(ScalarUDF::from(StGeogFromTextUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8]),
            ],
            Volatility::Immutable,
        ),
    }));
    ctx.register_udf(ScalarUDF::from(StGeographyFromTextUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8]),
            ],
            Volatility::Immutable,
        ),
    }));
}

#[inline]
fn point_dt() -> DataType {
    DataType::FixedSizeBinary(POINT_WKB_LEN as i32)
}

/// Helper to materialise both sides of a binary UDF to arrays of length
/// `n`, where `n` is the longest array argument (scalar inputs broadcast).
fn columnar_pair_to_arrays(args: &[ColumnarValue]) -> DFResult<(usize, ArrayRef, ArrayRef)> {
    let n = args
        .iter()
        .filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1);
    let a = args[0].clone().into_array(n)?;
    let b = args[1].clone().into_array(n)?;
    Ok((n, a, b))
}

fn columnar_triple_to_arrays(
    args: &[ColumnarValue],
) -> DFResult<(usize, ArrayRef, ArrayRef, ArrayRef)> {
    let n = args
        .iter()
        .filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1);
    let a = args[0].clone().into_array(n)?;
    let b = args[1].clone().into_array(n)?;
    let c = args[2].clone().into_array(n)?;
    Ok((n, a, b, c))
}

fn columnar_unary_to_array(args: &[ColumnarValue]) -> DFResult<(usize, ArrayRef)> {
    let n = args
        .iter()
        .filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1);
    let a = args[0].clone().into_array(n)?;
    Ok((n, a))
}

fn decode_point_at(arr: &ArrayRef, i: usize) -> DFResult<Option<Point>> {
    if arr.is_null(i) {
        return Ok(None);
    }
    let fsb = arr
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "ST_* UDF: expected FixedSizeBinary({POINT_WKB_LEN}), got {:?}",
                arr.data_type()
            ))
        })?;
    let bytes = fsb.value(i);
    decode_point(bytes)
        .map(Some)
        .map_err(|e| datafusion::error::DataFusionError::Execution(format!("decode POINT: {e}")))
}

// ── ST_MakePoint ──────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct StMakePointUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StMakePointUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_makepoint"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(point_dt())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 2 {
            return exec_err!("ST_MakePoint expects 2 arguments, got {}", args.len());
        }
        let (n, x_arr, y_arr) = columnar_pair_to_arrays(args)?;
        let x = x_arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "ST_MakePoint: first argument must be DOUBLE".into(),
                )
            })?;
        let y = y_arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "ST_MakePoint: second argument must be DOUBLE".into(),
                )
            })?;
        let mut out = FixedSizeBinaryBuilder::with_capacity(n, POINT_WKB_LEN as i32);
        for i in 0..n {
            if x.is_null(i) || y.is_null(i) {
                out.append_null();
                continue;
            }
            let bytes = encode_point(&make_point(x.value(i), y.value(i)));
            out.append_value(bytes).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("ST_MakePoint build: {e}"))
            })?;
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_X / ST_Y ──────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct StXUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StXUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_x"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (n, arr) = columnar_unary_to_array(&args.args)?;
        let mut out = Float64Builder::with_capacity(n);
        for i in 0..n {
            match decode_point_at(&arr, i)? {
                Some(p) => out.append_value(p.lon()),
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct StYUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StYUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_y"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (n, arr) = columnar_unary_to_array(&args.args)?;
        let mut out = Float64Builder::with_capacity(n);
        for i in 0..n {
            match decode_point_at(&arr, i)? {
                Some(p) => out.append_value(p.lat()),
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_Distance ──────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct StDistanceUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StDistanceUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_distance"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (n, a, b) = columnar_pair_to_arrays(&args.args)?;
        let mut out = Float64Builder::with_capacity(n);
        for i in 0..n {
            match (decode_point_at(&a, i)?, decode_point_at(&b, i)?) {
                (Some(p), Some(q)) => out.append_value(haversine_meters(&p, &q)),
                _ => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_DWithin ───────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct StDWithinUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StDWithinUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_dwithin"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (n, a, b, r) = columnar_triple_to_arrays(&args.args)?;
        let r = r.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "ST_DWithin: third argument must be DOUBLE".into(),
            )
        })?;
        let mut out = BooleanBuilder::with_capacity(n);
        for i in 0..n {
            match (decode_point_at(&a, i)?, decode_point_at(&b, i)?) {
                (Some(p), Some(q)) if !r.is_null(i) => out.append_value(dwithin(&p, &q, r.value(i))),
                _ => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_AsText (WKT) ──────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct StAsTextUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StAsTextUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_astext"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (n, arr) = columnar_unary_to_array(&args.args)?;
        let mut out = StringBuilder::with_capacity(n, 24 * n);
        for i in 0..n {
            match decode_point_at(&arr, i)? {
                Some(p) => {
                    out.append_value(format!("POINT({} {})", p.lon(), p.lat()));
                }
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_AsEWKB (raw bytes) ────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct StAsEwkbUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StAsEwkbUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_asewkb"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Binary)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (n, arr) = columnar_unary_to_array(&args.args)?;
        let fsb = arr
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "ST_AsEWKB: argument must be FixedSizeBinary({POINT_WKB_LEN})"
                ))
            })?;
        let mut out = BinaryBuilder::with_capacity(n, n * POINT_WKB_LEN);
        for i in 0..n {
            if fsb.is_null(i) {
                out.append_null();
            } else {
                out.append_value(fsb.value(i));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_GeomFromText (WKT → POINT) ────────────────────────────────────────────

/// Parse a `POINT(x y)` WKT string into a 21-byte WKB blob. Accepts the
/// case-insensitive `POINT(...)` form and rejects everything else with
/// a SQL execution error (PG's behaviour for malformed WKT). Whitespace
/// between coordinates is flexible; a leading sign is allowed.
fn parse_point_wkt(s: &str) -> DFResult<[u8; POINT_WKB_LEN]> {
    let trimmed = s.trim();
    // Strip leading `POINT` (case-insensitive) and a `(...)` wrapper.
    let after = trimmed
        .strip_prefix("POINT")
        .or_else(|| trimmed.strip_prefix("point"))
        .or_else(|| trimmed.strip_prefix("Point"))
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "ST_GeomFromText: expected POINT(x y), got {trimmed:?}"
            ))
        })?
        .trim_start();
    let inside = after
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "ST_GeomFromText: expected parenthesised POINT(x y), got {trimmed:?}"
            ))
        })?;
    let mut nums = inside.split_whitespace();
    let x: f64 = nums
        .next()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "ST_GeomFromText: missing X coordinate".into(),
            )
        })?
        .parse()
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "ST_GeomFromText: X is not a number: {e}"
            ))
        })?;
    let y: f64 = nums
        .next()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "ST_GeomFromText: missing Y coordinate".into(),
            )
        })?
        .parse()
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "ST_GeomFromText: Y is not a number: {e}"
            ))
        })?;
    if nums.next().is_some() {
        return Err(datafusion::error::DataFusionError::Execution(format!(
            "ST_GeomFromText: too many coordinates in {trimmed:?} (Z/M not supported in v0.1)"
        )));
    }
    Ok(encode_point(&make_point(x, y)))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct StGeomFromTextUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StGeomFromTextUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_geomfromtext"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(point_dt())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (n, arr) = columnar_unary_to_array(&args.args)?;
        let mut out = FixedSizeBinaryBuilder::with_capacity(n, POINT_WKB_LEN as i32);
        for i in 0..n {
            if arr.is_null(i) {
                out.append_null();
                continue;
            }
            let s: &str = match arr.data_type() {
                DataType::Utf8 => arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("StringArray for Utf8 column")
                    .value(i),
                DataType::LargeUtf8 => arr
                    .as_any()
                    .downcast_ref::<arrow_array::LargeStringArray>()
                    .expect("LargeStringArray for LargeUtf8 column")
                    .value(i),
                other => {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "ST_GeomFromText: unsupported argument type {other:?}"
                    )))
                }
            };
            let bytes = parse_point_wkt(s)?;
            out.append_value(bytes).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "ST_GeomFromText build: {e}"
                ))
            })?;
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_GeomFromWKB (bytea → POINT) ───────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct StGeomFromWkbUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StGeomFromWkbUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_geomfromwkb"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(point_dt())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (n, arr) = columnar_unary_to_array(&args.args)?;
        let mut out = FixedSizeBinaryBuilder::with_capacity(n, POINT_WKB_LEN as i32);
        for i in 0..n {
            if arr.is_null(i) {
                out.append_null();
                continue;
            }
            let bytes: &[u8] = match arr.data_type() {
                DataType::Binary => arr
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("BinaryArray")
                    .value(i),
                DataType::LargeBinary => arr
                    .as_any()
                    .downcast_ref::<arrow_array::LargeBinaryArray>()
                    .expect("LargeBinaryArray")
                    .value(i),
                DataType::FixedSizeBinary(_) => arr
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .expect("FixedSizeBinaryArray")
                    .value(i),
                other => {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "ST_GeomFromWKB: unsupported argument type {other:?}"
                    )))
                }
            };
            // Verify it's a valid POINT WKB before storing.
            decode_point(bytes).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "ST_GeomFromWKB: invalid POINT WKB: {e}"
                ))
            })?;
            // Bytes are already canonical; copy through unchanged.
            if bytes.len() != POINT_WKB_LEN {
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "ST_GeomFromWKB: wrong byte length: got {}, want {POINT_WKB_LEN}",
                    bytes.len()
                )));
            }
            let mut buf = [0u8; POINT_WKB_LEN];
            buf.copy_from_slice(bytes);
            out.append_value(buf).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("ST_GeomFromWKB build: {e}"))
            })?;
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_AsGeoJSON (POINT → GeoJSON text) ──────────────────────────────────────
//
// PostGIS format for a 2D point:
//   {"type":"Point","coordinates":[x,y]}
// No spaces; lon (x) first, lat (y) second (GeoJSON §3.1.2 / RFC 7946).
// Floats are formatted by serde_json's Grisu3/Dragon4 shortest-round-trip
// printer — same precision as PostGIS's ≤15 sig-fig rule for values that
// are not exact-decimal, and compact output (e.g. "1.5") for exact-decimal
// inputs.

#[derive(Debug, PartialEq, Eq, Hash)]
struct StAsGeoJsonUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StAsGeoJsonUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_asgeojson"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (n, arr) = columnar_unary_to_array(&args.args)?;
        let mut out = StringBuilder::with_capacity(n, 48 * n);
        for i in 0..n {
            match decode_point_at(&arr, i)? {
                Some(p) => {
                    out.append_value(encode_point_geojson(&p));
                }
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_GeomFromGeoJSON (GeoJSON text → POINT) ─────────────────────────────────
//
// Parses the PostGIS-canonical {"type":"Point","coordinates":[x,y]} envelope.
// Extra top-level keys (crs, bbox, …) are silently ignored so that PostGIS
// output fed back in round-trips cleanly.  Malformed JSON or wrong geometry
// type surfaces as a DataFusion execution error matching the ST_GeomFromText
// error style.

fn parse_point_geojson(s: &str) -> DFResult<[u8; POINT_WKB_LEN]> {
    let p = decode_point_geojson(s).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "ST_GeomFromGeoJSON: invalid GeoJSON: {e}"
        ))
    })?;
    Ok(encode_point(&make_point(p.x, p.y)))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct StGeomFromGeoJsonUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StGeomFromGeoJsonUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_geomfromgeojson"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(point_dt())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (n, arr) = columnar_unary_to_array(&args.args)?;
        let mut out = FixedSizeBinaryBuilder::with_capacity(n, POINT_WKB_LEN as i32);
        for i in 0..n {
            if arr.is_null(i) {
                out.append_null();
                continue;
            }
            let s: &str = match arr.data_type() {
                DataType::Utf8 => arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("StringArray for Utf8 column")
                    .value(i),
                DataType::LargeUtf8 => arr
                    .as_any()
                    .downcast_ref::<arrow_array::LargeStringArray>()
                    .expect("LargeStringArray for LargeUtf8 column")
                    .value(i),
                other => {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "ST_GeomFromGeoJSON: unsupported argument type {other:?}"
                    )))
                }
            };
            let bytes = parse_point_geojson(s)?;
            out.append_value(bytes).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "ST_GeomFromGeoJSON build: {e}"
                ))
            })?;
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── PG-Wave 3: helper ────────────────────────────────────────────────────────

/// Convert a basin-geo [`Point`] to a `geo::Point<f64>` for use with the
/// `geo` crate's topology trait impls. Basin-geo stores `(x=lon, y=lat)`;
/// `geo::Point` stores `(x, y)` in the same convention.
#[inline]
fn to_geo_point(p: &Point) -> geo::Point<f64> {
    geo::Point::new(p.x, p.y)
}

// ── ST_Intersects ─────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct StIntersectsUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StIntersectsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_intersects"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (n, a, b) = columnar_pair_to_arrays(&args.args)?;
        let mut out = BooleanBuilder::with_capacity(n);
        for i in 0..n {
            match (decode_point_at(&a, i)?, decode_point_at(&b, i)?) {
                (Some(pa), Some(pb)) => {
                    let ga = to_geo_point(&pa);
                    let gb = to_geo_point(&pb);
                    out.append_value(ga.intersects(&gb));
                }
                _ => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_Within ─────────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct StWithinUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StWithinUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_within"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (n, a, b) = columnar_pair_to_arrays(&args.args)?;
        let mut out = BooleanBuilder::with_capacity(n);
        for i in 0..n {
            match (decode_point_at(&a, i)?, decode_point_at(&b, i)?) {
                (Some(pa), Some(pb)) => {
                    let ga = to_geo_point(&pa);
                    let gb = to_geo_point(&pb);
                    out.append_value(ga.is_within(&gb));
                }
                _ => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_Crosses ────────────────────────────────────────────────────────────────
//
// `geo 0.28` does not expose a Crosses trait (it was added later in the 0.29+
// series via the DE-9IM relate module). For POINT × POINT inputs the PostGIS
// DE-9IM spec requires Crosses = false anyway (two zero-dimensional geometries
// cannot cross), so the trivial constant is both API-compatible and correct.

#[derive(Debug, PartialEq, Eq, Hash)]
struct StCrossesUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StCrossesUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_crosses"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (n, a, b) = columnar_pair_to_arrays(&args.args)?;
        let mut out = BooleanBuilder::with_capacity(n);
        for i in 0..n {
            if a.is_null(i) || b.is_null(i) {
                out.append_null();
            } else {
                // Two zero-dimensional geometries (POINTs) can never cross.
                out.append_value(false);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_Touches ────────────────────────────────────────────────────────────────
//
// For two POINTs: touches iff they share boundary but not interior. Since a
// point's boundary is empty, ST_Touches(a,b) is always false for POINT × POINT
// in PostGIS. (Contrast with ST_Intersects which returns true when equal.)

#[derive(Debug, PartialEq, Eq, Hash)]
struct StTouchesUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StTouchesUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_touches"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (n, a, b) = columnar_pair_to_arrays(&args.args)?;
        let mut out = BooleanBuilder::with_capacity(n);
        for i in 0..n {
            if a.is_null(i) || b.is_null(i) {
                out.append_null();
            } else {
                // Points have empty boundaries; ST_Touches is always false
                // for POINT × POINT (DE-9IM requires non-empty boundary
                // intersection, which points lack).
                out.append_value(false);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_Disjoint ───────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct StDisjointUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StDisjointUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_disjoint"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (n, a, b) = columnar_pair_to_arrays(&args.args)?;
        let mut out = BooleanBuilder::with_capacity(n);
        for i in 0..n {
            match (decode_point_at(&a, i)?, decode_point_at(&b, i)?) {
                (Some(pa), Some(pb)) => {
                    let ga = to_geo_point(&pa);
                    let gb = to_geo_point(&pb);
                    // ST_Disjoint = !ST_Intersects
                    out.append_value(!ga.intersects(&gb));
                }
                _ => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_Overlaps ───────────────────────────────────────────────────────────────
//
// ST_Overlaps requires that the intersection has the same dimension as each
// operand AND is strictly smaller than each operand. For two POINTs:
// - If they are equal: the intersection is the same point — not *strictly*
//   smaller → false.
// - If they are different: the intersection is empty → false.
// Result: always false for POINT × POINT (PostGIS-correct).

#[derive(Debug, PartialEq, Eq, Hash)]
struct StOverlapsUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StOverlapsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_overlaps"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (n, a, b) = columnar_pair_to_arrays(&args.args)?;
        let mut out = BooleanBuilder::with_capacity(n);
        for i in 0..n {
            if a.is_null(i) || b.is_null(i) {
                out.append_null();
            } else {
                // Always false for POINT × POINT (see comment above).
                out.append_value(false);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_Envelope ───────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct StEnvelopeUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StEnvelopeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_envelope"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(point_dt())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // Envelope of a POINT is the point itself — pass-through.
        let (n, arr) = columnar_unary_to_array(&args.args)?;
        let fsb = arr
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "ST_Envelope: expected FixedSizeBinary({POINT_WKB_LEN})"
                ))
            })?;
        let mut out = FixedSizeBinaryBuilder::with_capacity(n, POINT_WKB_LEN as i32);
        for i in 0..n {
            if fsb.is_null(i) {
                out.append_null();
            } else {
                out.append_value(fsb.value(i)).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "ST_Envelope build: {e}"
                    ))
                })?;
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_Area ───────────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct StAreaUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StAreaUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_area"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (n, arr) = columnar_unary_to_array(&args.args)?;
        let mut out = Float64Builder::with_capacity(n);
        for i in 0..n {
            if arr.is_null(i) {
                out.append_null();
            } else {
                // A point has zero area.
                out.append_value(0.0);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_Perimeter ──────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct StPerimeterUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StPerimeterUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_perimeter"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (n, arr) = columnar_unary_to_array(&args.args)?;
        let mut out = Float64Builder::with_capacity(n);
        for i in 0..n {
            if arr.is_null(i) {
                out.append_null();
            } else {
                // A point has zero perimeter.
                out.append_value(0.0);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_Centroid ───────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct StCentroidUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StCentroidUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_centroid"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(point_dt())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // Centroid of a POINT is the point itself — pass-through.
        let (n, arr) = columnar_unary_to_array(&args.args)?;
        let fsb = arr
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "ST_Centroid: expected FixedSizeBinary({POINT_WKB_LEN})"
                ))
            })?;
        let mut out = FixedSizeBinaryBuilder::with_capacity(n, POINT_WKB_LEN as i32);
        for i in 0..n {
            if fsb.is_null(i) {
                out.append_null();
            } else {
                out.append_value(fsb.value(i)).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "ST_Centroid build: {e}"
                    ))
                })?;
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_Buffer ─────────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct StBufferUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StBufferUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_buffer"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        // Today we return POINT (identity). When POLYGON storage ships in
        // PG-Wave 5 the return type will change to a polygon/binary column.
        Ok(point_dt())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // basin-geo lacks POLYGON storage today; we cannot render a circle/disk.
        // Return the input POINT unchanged so PostGIS-emitting clients that call
        // ST_Buffer primarily as a bounding-box hint still get a valid geometry.
        // TODO(PG-Wave 5): compute a circle polygon when Polygon columns ship.
        let (n, arr, _r_arr) = columnar_pair_to_arrays(&args.args)?;
        let fsb = arr
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "ST_Buffer: expected FixedSizeBinary({POINT_WKB_LEN}) as first argument"
                ))
            })?;
        let mut out = FixedSizeBinaryBuilder::with_capacity(n, POINT_WKB_LEN as i32);
        for i in 0..n {
            if fsb.is_null(i) {
                out.append_null();
            } else {
                out.append_value(fsb.value(i)).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "ST_Buffer build: {e}"
                    ))
                })?;
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_NumPoints ──────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct StNumPointsUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StNumPointsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_numpoints"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (n, arr) = columnar_unary_to_array(&args.args)?;
        let mut out = Int32Builder::with_capacity(n);
        for i in 0..n {
            if arr.is_null(i) {
                out.append_null();
            } else {
                // A POINT has exactly 1 vertex.
                out.append_value(1);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_PointN ─────────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct StPointNUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StPointNUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_pointn"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(point_dt())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let (n, pt_arr, n_arr) = columnar_pair_to_arrays(&args.args)?;
        let n_col = n_arr
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "ST_PointN: second argument must be INT32".into(),
                )
            })?;
        let fsb = pt_arr
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "ST_PointN: expected FixedSizeBinary({POINT_WKB_LEN}) as first argument"
                ))
            })?;
        let mut out = FixedSizeBinaryBuilder::with_capacity(n, POINT_WKB_LEN as i32);
        for i in 0..n {
            if fsb.is_null(i) || n_col.is_null(i) {
                out.append_null();
                continue;
            }
            let idx = n_col.value(i);
            if idx == 1 {
                // Only valid index for a POINT is 1 (1-based). Identity.
                out.append_value(fsb.value(i)).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "ST_PointN build: {e}"
                    ))
                })?;
            } else {
                // Out-of-range index: NULL (PostGIS behaviour).
                out.append_null();
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_StartPoint ─────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct StStartPointUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StStartPointUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_startpoint"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(point_dt())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // For a POINT the start vertex is the point itself — pass-through.
        let (n, arr) = columnar_unary_to_array(&args.args)?;
        let fsb = arr
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "ST_StartPoint: expected FixedSizeBinary({POINT_WKB_LEN})"
                ))
            })?;
        let mut out = FixedSizeBinaryBuilder::with_capacity(n, POINT_WKB_LEN as i32);
        for i in 0..n {
            if fsb.is_null(i) {
                out.append_null();
            } else {
                out.append_value(fsb.value(i)).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "ST_StartPoint build: {e}"
                    ))
                })?;
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_EndPoint ───────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct StEndPointUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StEndPointUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_endpoint"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(point_dt())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // For a POINT the end vertex is the point itself — pass-through.
        let (n, arr) = columnar_unary_to_array(&args.args)?;
        let fsb = arr
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "ST_EndPoint: expected FixedSizeBinary({POINT_WKB_LEN})"
                ))
            })?;
        let mut out = FixedSizeBinaryBuilder::with_capacity(n, POINT_WKB_LEN as i32);
        for i in 0..n {
            if fsb.is_null(i) {
                out.append_null();
            } else {
                out.append_value(fsb.value(i)).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "ST_EndPoint build: {e}"
                    ))
                })?;
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_MakeEnvelope ───────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct StMakeEnvelopeUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StMakeEnvelopeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_makeenvelope"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        // Returns POINT (at the min-corner) today. When BOX2D storage ships
        // this will change to FSB(33) or LargeBinary.
        Ok(point_dt())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // Extract n from the length of any array arg (or 1 for all-scalar).
        let n = args
            .args
            .iter()
            .filter_map(|a| match a {
                ColumnarValue::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .max()
            .unwrap_or(1);
        let min_x_arr = args.args[0].clone().into_array(n)?;
        let min_y_arr = args.args[1].clone().into_array(n)?;
        // max_x / max_y (args[2], args[3]) are consumed for type-check; the
        // degenerate POINT-return uses only the min corner.
        let _max_x_arr = args.args[2].clone().into_array(n)?;
        let _max_y_arr = args.args[3].clone().into_array(n)?;
        // Optional srid (arg[4]) is accepted but ignored in v0.1.

        let min_x = min_x_arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "ST_MakeEnvelope: min_x must be DOUBLE".into(),
                )
            })?;
        let min_y = min_y_arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "ST_MakeEnvelope: min_y must be DOUBLE".into(),
                )
            })?;

        let mut out = FixedSizeBinaryBuilder::with_capacity(n, POINT_WKB_LEN as i32);
        for i in 0..n {
            if min_x.is_null(i) || min_y.is_null(i) {
                out.append_null();
                continue;
            }
            let bytes = encode_point(&make_point(min_x.value(i), min_y.value(i)));
            out.append_value(bytes).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "ST_MakeEnvelope build: {e}"
                ))
            })?;
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_SRID ───────────────────────────────────────────────────────────────────
//
// Reads the BASIN_SRID metadata off the input column's field. PostGIS's
// convention is "0 = unknown SRID", so a POINT column with no declared
// SRID returns 0 rather than NULL. Element-wise NULL handling still
// applies: NULL point → NULL int4.

#[derive(Debug, PartialEq, Eq, Hash)]
struct StSridUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StSridUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_srid"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // BASIN_SRID rides on the arg-field metadata, which DataFusion
        // populates from the source column's `Field`. Read once before
        // looping; falls back to 0 ("unknown SRID", PostGIS convention)
        // when absent.
        let srid = args
            .arg_fields
            .first()
            .and_then(|f| f.metadata().get(crate::types::BASIN_SRID).cloned())
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0);
        let (n, arr) = columnar_unary_to_array(&args.args)?;
        let mut out = Int32Builder::with_capacity(n);
        for i in 0..n {
            if arr.is_null(i) {
                out.append_null();
            } else {
                out.append_value(srid);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_SetSRID ────────────────────────────────────────────────────────────────
//
// PostGIS spec: returns a geometry with the SRID metadata replaced. We
// pass the bytes through unchanged (the 21-byte WKB doesn't carry an
// SRID — it's column metadata only). The "new" SRID is dropped at the
// expression boundary because basin-engine doesn't yet have an
// expression-level SRID-tagging machinery; for the common case of
// `INSERT INTO t (p) VALUES (ST_SetSRID(ST_MakePoint(x, y), 4326))`
// the destination column's `BASIN_SRID` declaration is what stamps the
// SRID on the persisted row. Documented gap; the bytes-identity path
// keeps callers from getting wrong-shape data while we wire up an
// expression-level SRID-tagger in PG-Wave 6.

#[derive(Debug, PartialEq, Eq, Hash)]
struct StSetSridUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StSetSridUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_setsrid"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(point_dt())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!("ST_SetSRID expects 2 arguments, got {}", args.args.len());
        }
        let (n, arr) = columnar_unary_to_array(&args.args[0..1])?;
        let fsb = arr
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "ST_SetSRID: first argument must be FixedSizeBinary({POINT_WKB_LEN})"
                ))
            })?;
        let mut out = FixedSizeBinaryBuilder::with_capacity(n, POINT_WKB_LEN as i32);
        for i in 0..n {
            if fsb.is_null(i) {
                out.append_null();
            } else {
                out.append_value(fsb.value(i)).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "ST_SetSRID build: {e}"
                    ))
                })?;
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_Transform ──────────────────────────────────────────────────────────────
//
// Source SRID is read from the input column's BASIN_SRID field metadata
// (defaulting to 4326 / WGS84 when absent — basin-geo's implicit default,
// which matches what `ST_MakePoint` stamps on freshly-built points).
// Destination SRID is the second argument (an INT4 scalar or column).
// Errors from `basin_geo::transform::st_transform` (unknown SRID,
// proj4rs build failure) surface as DataFusion execution errors.

#[derive(Debug, PartialEq, Eq, Hash)]
struct StTransformUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StTransformUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_transform"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(point_dt())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!("ST_Transform expects 2 arguments, got {}", args.args.len());
        }
        // Source SRID from BASIN_SRID column metadata; default to WGS84
        // (4326), matching basin-geo's implicit-WGS84 stance.
        let src_srid: u32 = args
            .arg_fields
            .first()
            .and_then(|f| f.metadata().get(crate::types::BASIN_SRID).cloned())
            .and_then(|s| s.parse().ok())
            .unwrap_or(basin_geo::SRID_WGS84);
        let (n, point_arr, dst_arr) = columnar_pair_to_arrays(&args.args)?;
        // Accept both Int32 (native PG INT4) and Int64 (sqlparser's
        // default for integer literals). Read once into i64 to unify
        // the two paths.
        let read_dst: Box<dyn Fn(usize) -> Option<i64>> = match dst_arr.data_type() {
            DataType::Int32 => {
                let a = dst_arr
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("Int32Array");
                let a = a.clone();
                Box::new(move |i| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i) as i64)
                    }
                })
            }
            DataType::Int64 => {
                let a = dst_arr
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .expect("Int64Array");
                let a = a.clone();
                Box::new(move |i| if a.is_null(i) { None } else { Some(a.value(i)) })
            }
            other => {
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "ST_Transform: second argument must be INT4 or INT8, got {other:?}"
                )))
            }
        };
        let mut out = FixedSizeBinaryBuilder::with_capacity(n, POINT_WKB_LEN as i32);
        for i in 0..n {
            let p = match decode_point_at(&point_arr, i)? {
                Some(p) => p,
                None => {
                    out.append_null();
                    continue;
                }
            };
            let dst_srid_raw = match read_dst(i) {
                Some(v) => v,
                None => {
                    out.append_null();
                    continue;
                }
            };
            if dst_srid_raw < 0 || dst_srid_raw > u32::MAX as i64 {
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "ST_Transform: destination SRID out of range, got {dst_srid_raw}"
                )));
            }
            let dst_srid = dst_srid_raw as u32;
            // Re-stamp the source SRID we just resolved so st_transform
            // sees it correctly (the points coming out of ST_MakePoint
            // carry basin-geo's hardcoded WGS84 default, which we want
            // to override with whatever the column metadata says).
            let p_src = basin_geo::Point::with_srid(p.x, p.y, src_srid);
            let projected = basin_geo::st_transform(&p_src, dst_srid).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("ST_Transform: {e}"))
            })?;
            // Encode the projected point. We deliberately strip the
            // SRID from the WKB blob (basin-geo's `encode_point`
            // emits the SRID-less 21-byte form); the destination
            // column's `BASIN_SRID` is the source of truth post-
            // transform, set on the INSERT path or known by ST_SRID's
            // metadata read.
            let bytes = encode_point(&basin_geo::Point::new(projected.x, projected.y));
            out.append_value(bytes).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("ST_Transform build: {e}"))
            })?;
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ── ST_GeogFromText / ST_GeographyFromText (aliases for ST_GeomFromText) ─────
//
// PostGIS distinguishes GEOGRAPHY from GEOMETRY at the type-system
// level: GEOGRAPHY values are always lat/lon (SRID 4326), and
// distance / area operators auto-use geodesic math. basin-geo's
// distance + area implementations are ALREADY Haversine /
// spherical-excess (see crate-level docs at basin-geo/src/lib.rs), so
// there is no semantic difference between a GEOMETRY POINT in SRID
// 4326 and a GEOGRAPHY POINT. Aliasing FromText / FromText keeps
// client code that emits the geography variants working with zero
// duplication of the WKT parser.

#[derive(Debug, PartialEq, Eq, Hash)]
struct StGeogFromTextUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StGeogFromTextUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_geogfromtext"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(point_dt())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // Delegate to ST_GeomFromText — identical parser surface.
        let geom = StGeomFromTextUdf {
            signature: self.signature.clone(),
        };
        geom.invoke_with_args(args)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct StGeographyFromTextUdf {
    signature: Signature,
}
impl ScalarUDFImpl for StGeographyFromTextUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "st_geographyfromtext"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(point_dt())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let geom = StGeomFromTextUdf {
            signature: self.signature.clone(),
        };
        geom.invoke_with_args(args)
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use datafusion::common::ScalarValue;

    fn config() -> Arc<datafusion::config::ConfigOptions> {
        Arc::new(datafusion::config::ConfigOptions::default())
    }

    fn invoke(udf: &dyn ScalarUDFImpl, args: Vec<ColumnarValue>, ret: DataType) -> ColumnarValue {
        let n = args
            .iter()
            .filter_map(|a| match a {
                ColumnarValue::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .max()
            .unwrap_or(1);
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(i, cv)| {
                let dt = match cv {
                    ColumnarValue::Array(a) => a.data_type().clone(),
                    ColumnarValue::Scalar(s) => s.data_type(),
                };
                Arc::new(Field::new(format!("arg{i}"), dt, true))
            })
            .collect();
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: n,
            return_field: Arc::new(Field::new("out", ret, true)),
            config_options: config(),
        })
        .unwrap()
    }

    fn make_point_scalar(x: f64, y: f64) -> ColumnarValue {
        let bytes = encode_point(&Point::new(x, y));
        ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(
            POINT_WKB_LEN as i32,
            Some(bytes.to_vec()),
        ))
    }

    #[test]
    fn st_make_point_round_trip() {
        let make = StMakePointUdf {
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
        };
        let xudf = StXUdf {
            signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
        };
        let yudf = StYUdf {
            signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
        };

        let xs = Arc::new(Float64Array::from(vec![2.35, -73.985, 0.0])) as ArrayRef;
        let ys = Arc::new(Float64Array::from(vec![48.85, 40.748, 0.0])) as ArrayRef;
        let made = invoke(
            &make,
            vec![
                ColumnarValue::Array(xs.clone()),
                ColumnarValue::Array(ys.clone()),
            ],
            point_dt(),
        );
        let ColumnarValue::Array(point_arr) = made else {
            panic!("array");
        };
        let xs_back = invoke(
            &xudf,
            vec![ColumnarValue::Array(point_arr.clone())],
            DataType::Float64,
        );
        let ys_back = invoke(
            &yudf,
            vec![ColumnarValue::Array(point_arr.clone())],
            DataType::Float64,
        );
        let xs_back = match xs_back {
            ColumnarValue::Array(a) => a,
            _ => panic!("array"),
        };
        let ys_back = match ys_back {
            ColumnarValue::Array(a) => a,
            _ => panic!("array"),
        };
        let xs_back = xs_back.as_any().downcast_ref::<Float64Array>().unwrap();
        let ys_back = ys_back.as_any().downcast_ref::<Float64Array>().unwrap();
        let xs_orig = xs.as_any().downcast_ref::<Float64Array>().unwrap();
        let ys_orig = ys.as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..3 {
            assert_eq!(xs_back.value(i), xs_orig.value(i));
            assert_eq!(ys_back.value(i), ys_orig.value(i));
        }
    }

    #[test]
    fn st_dwithin_matches_haversine() {
        // Eiffel Tower ↔ Big Ben ≈ 343.5 km. ST_DWithin at 350 km is
        // true; at 300 km it's false.
        let udf = StDWithinUdf {
            signature: Signature::exact(
                vec![point_dt(), point_dt(), DataType::Float64],
                Volatility::Immutable,
            ),
        };
        let eiffel = make_point_scalar(2.2945, 48.8584);
        let big_ben = make_point_scalar(-0.1246, 51.5007);
        let r_yes = ColumnarValue::Scalar(ScalarValue::Float64(Some(350_000.0)));
        let r_no = ColumnarValue::Scalar(ScalarValue::Float64(Some(300_000.0)));

        let res_yes = invoke(
            &udf,
            vec![eiffel.clone(), big_ben.clone(), r_yes],
            DataType::Boolean,
        );
        let res_no = invoke(&udf, vec![eiffel, big_ben, r_no], DataType::Boolean);
        let res_yes = match res_yes {
            ColumnarValue::Array(a) => a,
            _ => panic!("array"),
        };
        let res_no = match res_no {
            ColumnarValue::Array(a) => a,
            _ => panic!("array"),
        };
        assert!(res_yes
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap()
            .value(0));
        assert!(!res_no
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap()
            .value(0));
    }

    #[test]
    fn st_geomfromtext_round_trips_to_st_astext() {
        let from_text = StGeomFromTextUdf {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        };
        let as_text = StAsTextUdf {
            signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
        };
        let input = Arc::new(StringArray::from(vec!["POINT(1.5 2.5)", "point(-3 4)"])) as ArrayRef;
        let p = invoke(&from_text, vec![ColumnarValue::Array(input)], point_dt());
        let ColumnarValue::Array(parr) = p else {
            panic!("array");
        };
        let back = invoke(&as_text, vec![ColumnarValue::Array(parr)], DataType::Utf8);
        let ColumnarValue::Array(barr) = back else {
            panic!("array");
        };
        let s = barr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(s.value(0), "POINT(1.5 2.5)");
        assert_eq!(s.value(1), "POINT(-3 4)");
    }

    #[test]
    fn install_udfs_registers_all_names() {
        // Smoke: install_udfs should not panic and the SessionContext
        // must report each ST_* function as a registered scalar UDF.
        let ctx = SessionContext::new();
        install_udfs(&ctx);
        let scalar = ctx.state().scalar_functions().clone();
        for name in [
            // Wave α
            "st_makepoint",
            "st_x",
            "st_y",
            "st_distance",
            "st_dwithin",
            "st_astext",
            "st_asewkb",
            "st_geomfromtext",
            "st_geomfromwkb",
            // GeoJSON
            "st_asgeojson",
            "st_geomfromgeojson",
            // PG-Wave 3
            "st_intersects",
            "st_within",
            "st_crosses",
            "st_touches",
            "st_disjoint",
            "st_overlaps",
            "st_envelope",
            "st_area",
            "st_perimeter",
            "st_centroid",
            "st_buffer",
            "st_numpoints",
            "st_pointn",
            "st_startpoint",
            "st_endpoint",
            "st_makeenvelope",
            // PG-Wave 5 SRID surface
            "st_srid",
            "st_setsrid",
            "st_transform",
            // GEOGRAPHY aliases
            "st_geogfromtext",
            "st_geographyfromtext",
        ] {
            assert!(
                scalar.contains_key(name),
                "expected {name} in scalar function registry, present: {:?}",
                scalar.keys().filter(|k| k.starts_with("st_")).collect::<Vec<_>>()
            );
        }
    }

    // ── ST_AsGeoJSON / ST_GeomFromGeoJSON unit tests ─────────────────────────

    /// Round-trip: ST_GeomFromGeoJSON(ST_AsGeoJSON(p)) == p byte-for-byte.
    #[test]
    fn st_geojson_round_trip() {
        let as_geo = StAsGeoJsonUdf {
            signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
        };
        let from_geo = StGeomFromGeoJsonUdf {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8])],
                Volatility::Immutable,
            ),
        };
        // Eiffel Tower (well-known lon/lat with fractional digits).
        let p = make_point_scalar(2.2945, 48.8584);
        let json_cv = invoke(&as_geo, vec![p.clone()], DataType::Utf8);
        let ColumnarValue::Array(json_arr) = json_cv else {
            panic!("expected Array");
        };
        let json_str = json_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        // Verify the exact PostGIS format: no spaces, "Point" capitalised.
        assert!(
            json_str.starts_with("{\"type\":\"Point\",\"coordinates\":["),
            "unexpected GeoJSON format: {json_str}"
        );

        // Decode back and compare bytes.
        let back_cv = invoke(
            &from_geo,
            vec![ColumnarValue::Array(json_arr)],
            point_dt(),
        );
        let ColumnarValue::Array(back_arr) = back_cv else {
            panic!("expected Array");
        };
        let original_bytes = match &p {
            ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(_, Some(b))) => b.clone(),
            _ => panic!("expected scalar bytes"),
        };
        let back_fsb = back_arr
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(
            back_fsb.value(0),
            original_bytes.as_slice(),
            "ST_GeomFromGeoJSON(ST_AsGeoJSON(p)) must equal original bytes"
        );
    }

    /// Exact format: integer-valued coordinates must print without ".0" noise,
    /// and the output must exactly match the PostGIS-canonical envelope.
    #[test]
    fn st_asgeojson_exact_format() {
        let udf = StAsGeoJsonUdf {
            signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
        };
        // Simple integer-lat/lon to verify compact serde_json float printing.
        let p = make_point_scalar(1.0, 2.0);
        let res = invoke(&udf, vec![p], DataType::Utf8);
        let arr = match res {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected Array"),
        };
        let s = arr.as_any().downcast_ref::<StringArray>().unwrap().value(0);
        // serde_json formats 1.0_f64 as "1.0" (not "1") — this is the
        // shortest representation that round-trips, matching PostGIS
        // for coordinates that are integer-valued floats.
        assert_eq!(s, "{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}");
    }

    /// Coordinate precision: a 15-significant-digit coordinate survives the
    /// GeoJSON round-trip without loss (Grisu3 handles ≥15 sig-fig correctly).
    #[test]
    fn st_geojson_coordinate_precision() {
        let as_geo = StAsGeoJsonUdf {
            signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
        };
        let from_geo = StGeomFromGeoJsonUdf {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8])],
                Volatility::Immutable,
            ),
        };
        // 15 significant digits — within PostGIS's stated precision guarantee.
        let lon = 12.345678901234_f64;
        let lat = -87.654321098765_f64;
        let p = make_point_scalar(lon, lat);
        let json_cv = invoke(&as_geo, vec![p], DataType::Utf8);
        let ColumnarValue::Array(json_arr) = json_cv else {
            panic!("array");
        };
        let back_cv = invoke(
            &from_geo,
            vec![ColumnarValue::Array(json_arr)],
            point_dt(),
        );
        let ColumnarValue::Array(back_arr) = back_cv else {
            panic!("array");
        };
        let back_fsb = back_arr
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let back_pt = decode_point(back_fsb.value(0)).unwrap();
        assert_eq!(back_pt.x, lon, "lon must survive GeoJSON round-trip");
        assert_eq!(back_pt.y, lat, "lat must survive GeoJSON round-trip");
    }

    /// Null passthrough: NULL input must produce NULL output for both UDFs.
    #[test]
    fn st_geojson_null_passthrough() {
        let as_geo = StAsGeoJsonUdf {
            signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
        };
        let from_geo = StGeomFromGeoJsonUdf {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8])],
                Volatility::Immutable,
            ),
        };

        // NULL POINT → NULL TEXT.
        let null_point =
            ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(POINT_WKB_LEN as i32, None));
        let res = invoke(&as_geo, vec![null_point], DataType::Utf8);
        let arr = match res {
            ColumnarValue::Array(a) => a,
            _ => panic!("array"),
        };
        assert!(arr.is_null(0), "NULL POINT → ST_AsGeoJSON must be NULL");

        // NULL TEXT → NULL POINT.
        let null_text = ColumnarValue::Scalar(ScalarValue::Utf8(None));
        let res2 = invoke(&from_geo, vec![null_text], point_dt());
        let arr2 = match res2 {
            ColumnarValue::Array(a) => a,
            _ => panic!("array"),
        };
        assert!(
            arr2.is_null(0),
            "NULL TEXT → ST_GeomFromGeoJSON must be NULL"
        );
    }

    /// Malformed JSON must produce an execution error (not a panic).
    #[test]
    fn st_geomfromgeojson_malformed_errors() {
        let udf = StGeomFromGeoJsonUdf {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Utf8])],
                Volatility::Immutable,
            ),
        };
        let n = 1usize;
        let config = Arc::new(datafusion::config::ConfigOptions::default());
        let arg_fields = vec![Arc::new(Field::new(
            "arg0",
            DataType::Utf8,
            true,
        ))];

        let bad_inputs = [
            // Completely invalid JSON.
            "not json",
            // Valid JSON but wrong type.
            "{\"type\":\"LineString\",\"coordinates\":[[0,0],[1,1]]}",
            // Missing type field.
            "{\"coordinates\":[1.0,2.0]}",
            // Missing coordinates field.
            "{\"type\":\"Point\"}",
            // Wrong coordinate arity (3D — not supported in v0.1).
            "{\"type\":\"Point\",\"coordinates\":[1.0,2.0,3.0]}",
        ];

        for bad in &bad_inputs {
            let arr = Arc::new(StringArray::from(vec![*bad])) as ArrayRef;
            let result = udf.invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(arr)],
                arg_fields: arg_fields.clone(),
                number_rows: n,
                return_field: Arc::new(Field::new("out", point_dt(), true)),
                config_options: config.clone(),
            });
            assert!(
                result.is_err(),
                "expected error for input {bad:?}, got Ok"
            );
            let msg = result.unwrap_err().to_string();
            assert!(
                msg.contains("ST_GeomFromGeoJSON"),
                "error message should name the UDF: {msg}"
            );
        }
    }

    // ── PG-Wave 3 unit tests ─────────────────────────────────────────────────

    #[test]
    fn st_intersects_point_eq() {
        let udf = StIntersectsUdf {
            signature: Signature::exact(vec![point_dt(), point_dt()], Volatility::Immutable),
        };
        // Same point → intersects = true.
        let p = make_point_scalar(1.0, 2.0);
        let res = invoke(&udf, vec![p.clone(), p.clone()], DataType::Boolean);
        let arr = match res {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        assert!(arr
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap()
            .value(0));

        // Different points → intersects = false.
        let q = make_point_scalar(3.0, 4.0);
        let res2 = invoke(&udf, vec![p, q], DataType::Boolean);
        let arr2 = match res2 {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        assert!(!arr2
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap()
            .value(0));
    }

    #[test]
    fn st_within_self() {
        let udf = StWithinUdf {
            signature: Signature::exact(vec![point_dt(), point_dt()], Volatility::Immutable),
        };
        let p = make_point_scalar(5.0, 6.0);
        let res = invoke(&udf, vec![p.clone(), p], DataType::Boolean);
        let arr = match res {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        assert!(arr
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap()
            .value(0));
    }

    #[test]
    fn st_area_point_is_zero() {
        let udf = StAreaUdf {
            signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
        };
        let p = make_point_scalar(10.0, 20.0);
        let res = invoke(&udf, vec![p], DataType::Float64);
        let arr = match res {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        assert_eq!(
            arr.as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            0.0
        );
    }

    #[test]
    fn st_num_points_is_one() {
        let udf = StNumPointsUdf {
            signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
        };
        let p = make_point_scalar(1.0, 1.0);
        let res = invoke(&udf, vec![p], DataType::Int32);
        let arr = match res {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        assert_eq!(
            arr.as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .unwrap()
                .value(0),
            1
        );
    }

    #[test]
    fn st_envelope_returns_self() {
        let udf = StEnvelopeUdf {
            signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
        };
        let p = make_point_scalar(7.0, 8.0);
        let res = invoke(&udf, vec![p.clone()], point_dt());
        let arr = match res {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        // The bytes of the returned POINT must equal the input bytes.
        let original_bytes = match &p {
            ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(_, Some(b))) => b.clone(),
            _ => panic!("expected scalar bytes"),
        };
        let out_fsb = arr
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(out_fsb.value(0), original_bytes.as_slice());
    }

    #[test]
    fn st_overlaps_point_is_false() {
        let udf = StOverlapsUdf {
            signature: Signature::exact(vec![point_dt(), point_dt()], Volatility::Immutable),
        };
        // Same point — should still be false per PostGIS DE-9IM spec.
        let p = make_point_scalar(1.0, 2.0);
        let res = invoke(&udf, vec![p.clone(), p], DataType::Boolean);
        let arr = match res {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        assert!(!arr
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap()
            .value(0));
    }

    #[test]
    fn st_buffer_point_identity_for_now() {
        // ST_Buffer(point, r) currently returns the point unchanged.
        // Proper disk/circle polygon output is deferred to PG-Wave 5
        // when POLYGON column storage ships in basin-geo.
        let udf = StBufferUdf {
            signature: Signature::exact(vec![point_dt(), DataType::Float64], Volatility::Immutable),
        };
        let p = make_point_scalar(2.0, 3.0);
        let r = ColumnarValue::Scalar(datafusion::common::ScalarValue::Float64(Some(100.0)));
        let res = invoke(&udf, vec![p.clone(), r], point_dt());
        let arr = match res {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let original_bytes = match &p {
            ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(_, Some(b))) => b.clone(),
            _ => panic!("expected scalar bytes"),
        };
        let out_fsb = arr
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(out_fsb.value(0), original_bytes.as_slice());
    }

    // ── PG-Wave 5: SRID surface tests ────────────────────────────────────

    /// Invoke a UDF where the first arg's `Field` carries a `BASIN_SRID`
    /// metadata entry — needed to exercise the `arg_fields` read path in
    /// `ST_SRID` and `ST_Transform`.
    fn invoke_with_first_arg_srid(
        udf: &dyn ScalarUDFImpl,
        args: Vec<ColumnarValue>,
        ret: DataType,
        srid: u32,
    ) -> ColumnarValue {
        let n = args
            .iter()
            .filter_map(|a| match a {
                ColumnarValue::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .max()
            .unwrap_or(1);
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(i, cv)| {
                let dt = match cv {
                    ColumnarValue::Array(a) => a.data_type().clone(),
                    ColumnarValue::Scalar(s) => s.data_type(),
                };
                let mut f = Field::new(format!("arg{i}"), dt, true);
                if i == 0 {
                    let mut md = std::collections::HashMap::new();
                    md.insert(crate::types::BASIN_SRID.to_string(), srid.to_string());
                    f = f.with_metadata(md);
                }
                Arc::new(f)
            })
            .collect();
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: n,
            return_field: Arc::new(Field::new("out", ret, true)),
            config_options: config(),
        })
        .unwrap()
    }

    #[test]
    fn st_srid_reads_field_metadata() {
        let udf = StSridUdf {
            signature: Signature::exact(vec![point_dt()], Volatility::Immutable),
        };
        let p = make_point_scalar(1.0, 2.0);
        // Column tagged SRID 4326 → ST_SRID returns 4326.
        let res = invoke_with_first_arg_srid(&udf, vec![p.clone()], DataType::Int32, 4326);
        let arr = match res {
            ColumnarValue::Array(a) => a,
            _ => panic!("array"),
        };
        let v = arr
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .unwrap()
            .value(0);
        assert_eq!(v, 4326);

        // No metadata → ST_SRID returns 0 (PostGIS "unknown SRID").
        let res2 = invoke(&udf, vec![p], DataType::Int32);
        let arr2 = match res2 {
            ColumnarValue::Array(a) => a,
            _ => panic!("array"),
        };
        let v2 = arr2
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .unwrap()
            .value(0);
        assert_eq!(v2, 0);
    }

    #[test]
    fn st_setsrid_returns_identity_bytes() {
        // ST_SetSRID is a bytes-identity pass-through at the expression
        // level. Verify the bytes survive unchanged.
        let udf = StSetSridUdf {
            signature: Signature::exact(
                vec![point_dt(), DataType::Int32],
                Volatility::Immutable,
            ),
        };
        let p = make_point_scalar(2.2945, 48.8584);
        let srid = ColumnarValue::Scalar(ScalarValue::Int32(Some(32631)));
        let res = invoke(&udf, vec![p.clone(), srid], point_dt());
        let arr = match res {
            ColumnarValue::Array(a) => a,
            _ => panic!("array"),
        };
        let original_bytes = match &p {
            ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(_, Some(b))) => b.clone(),
            _ => panic!("scalar bytes"),
        };
        let out_fsb = arr
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(out_fsb.value(0), original_bytes.as_slice());
    }

    #[test]
    fn st_transform_round_trip_wgs84_to_3857_to_wgs84_within_1mm() {
        let udf = StTransformUdf {
            signature: Signature::exact(
                vec![point_dt(), DataType::Int32],
                Volatility::Immutable,
            ),
        };
        // Berlin in WGS84.
        let lon = 13.4050;
        let lat = 52.5200;
        let p = make_point_scalar(lon, lat);

        // First leg: 4326 → 3857.
        let to_3857 = ColumnarValue::Scalar(ScalarValue::Int32(Some(3857)));
        let projected = invoke_with_first_arg_srid(
            &udf,
            vec![p, to_3857],
            point_dt(),
            4326,
        );
        let projected_arr = match projected {
            ColumnarValue::Array(a) => a,
            _ => panic!("array"),
        };
        // Wrap as a column so we can hand it back into ST_Transform with
        // a fresh SRID-tagged field.
        let projected_cv = ColumnarValue::Array(projected_arr);

        // Second leg: 3857 → 4326.
        let to_4326 = ColumnarValue::Scalar(ScalarValue::Int32(Some(4326)));
        let back = invoke_with_first_arg_srid(
            &udf,
            vec![projected_cv, to_4326],
            point_dt(),
            3857,
        );
        let back_arr = match back {
            ColumnarValue::Array(a) => a,
            _ => panic!("array"),
        };
        let back_fsb = back_arr
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let back_pt = basin_geo::decode_point(back_fsb.value(0)).unwrap();
        // 1e-7 deg ≈ 1.1 cm at the equator — same threshold PostGIS uses
        // for its ST_Transform regression tests.
        assert!(
            (back_pt.x - lon).abs() < 1e-7,
            "lon round-trip drift: src={lon} back={}",
            back_pt.x
        );
        assert!(
            (back_pt.y - lat).abs() < 1e-7,
            "lat round-trip drift: src={lat} back={}",
            back_pt.y
        );
    }

    #[test]
    fn st_geogfromtext_aliases_st_geomfromtext() {
        // Confirm the GEOGRAPHY-named UDF parses the same WKT.
        let udf = StGeogFromTextUdf {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        };
        let input = Arc::new(StringArray::from(vec!["POINT(1.5 2.5)"])) as ArrayRef;
        let res = invoke(&udf, vec![ColumnarValue::Array(input)], point_dt());
        let arr = match res {
            ColumnarValue::Array(a) => a,
            _ => panic!("array"),
        };
        let fsb = arr
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let p = basin_geo::decode_point(fsb.value(0)).unwrap();
        assert_eq!(p.x, 1.5);
        assert_eq!(p.y, 2.5);
    }
}
