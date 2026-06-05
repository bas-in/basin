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
//!
//! `ST_Contains(box2d, point)` is out of this wave — BOX2D needs its own
//! physical-type wiring (FSB(33) or LargeBinary) and there is no DDL
//! `BOX2D` type yet. Roadmap.
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
    BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, StringBuilder,
};
use arrow_array::{Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, Float64Array, StringArray};
use arrow_schema::DataType;
use basin_geo::{
    decode_point, dwithin, encode_point, haversine_meters, make_point, Point, POINT_WKB_LEN,
};
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
            "st_makepoint",
            "st_x",
            "st_y",
            "st_distance",
            "st_dwithin",
            "st_astext",
            "st_asewkb",
            "st_geomfromtext",
            "st_geomfromwkb",
        ] {
            assert!(
                scalar.contains_key(name),
                "expected {name} in scalar function registry, present: {:?}",
                scalar.keys().filter(|k| k.starts_with("st_")).collect::<Vec<_>>()
            );
        }
    }
}
