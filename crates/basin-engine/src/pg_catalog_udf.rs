//! Stub scalar UDFs for pg_catalog functions probed by psql `\dt` and
//! related `\d`-family meta-commands.
//!
//! psql issues queries against `pg_catalog` that reference functions such as
//! `pg_table_is_visible`, `pg_get_userbyid`, `format_type`, etc.  We register
//! stub implementations that always return a plausible constant so psql's
//! queries plan and execute without "Invalid function" errors.  Correctness is
//! deliberately not a goal — psql only needs these to not *error*.
//!
//! Every function is registered twice: once under its bare name
//! (`pg_table_is_visible`) and once under the schema-qualified name
//! (`pg_catalog.pg_table_is_visible`).  DataFusion stores functions by name,
//! so we create two `ScalarUDF` instances (one per name) from the same
//! underlying struct — both just have their `name` field set differently.
//!
//! Phase 5.11.N — closes the psql `\dt` / `\d`-family row in CAPABILITIES.md.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

// ---------------------------------------------------------------------------
// Registration entry point
// ---------------------------------------------------------------------------

/// Register all pg_catalog stub UDFs on `ctx`.  Idempotent — DataFusion
/// overwrites by name so calling this multiple times is safe.
///
/// Every function is registered twice: once under the bare name
/// (e.g. `pg_table_is_visible`) and once under the schema-qualified name
/// (`pg_catalog.pg_table_is_visible`).  DataFusion stores UDFs by name, so
/// two separate `ScalarUDF` instances are created with the `name` field set
/// accordingly.
pub(crate) fn register_pg_catalog_udfs(ctx: &SessionContext) {
    register_all(ctx);
}

fn register_all(ctx: &SessionContext) {
    // ----------- pg_table_is_visible -----------
    let sig_oid = sig_oid_variants();
    ctx.register_udf(ScalarUDF::from(SimpleOidBoolUdf {
        name: "pg_table_is_visible".into(),
        value: true,
        signature: sig_oid.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(SimpleOidBoolUdf {
        name: "pg_catalog.pg_table_is_visible".into(),
        value: true,
        signature: sig_oid,
    }));

    // ----------- pg_get_userbyid -----------
    let sig_oid = sig_oid_variants();
    ctx.register_udf(ScalarUDF::from(SimpleOidTextUdf {
        name: "pg_get_userbyid".into(),
        value: "basin".into(),
        signature: sig_oid.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(SimpleOidTextUdf {
        name: "pg_catalog.pg_get_userbyid".into(),
        value: "basin".into(),
        signature: sig_oid,
    }));

    // ----------- pg_get_function_arguments -----------
    let sig_oid = sig_oid_variants();
    ctx.register_udf(ScalarUDF::from(SimpleOidTextUdf {
        name: "pg_get_function_arguments".into(),
        value: "".into(),
        signature: sig_oid.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(SimpleOidTextUdf {
        name: "pg_catalog.pg_get_function_arguments".into(),
        value: "".into(),
        signature: sig_oid,
    }));

    // ----------- pg_get_function_result -----------
    let sig_oid = sig_oid_variants();
    ctx.register_udf(ScalarUDF::from(SimpleOidTextUdf {
        name: "pg_get_function_result".into(),
        value: "".into(),
        signature: sig_oid.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(SimpleOidTextUdf {
        name: "pg_catalog.pg_get_function_result".into(),
        value: "".into(),
        signature: sig_oid,
    }));

    // ----------- pg_get_function_identity_arguments -----------
    let sig_oid = sig_oid_variants();
    ctx.register_udf(ScalarUDF::from(SimpleOidTextUdf {
        name: "pg_get_function_identity_arguments".into(),
        value: "".into(),
        signature: sig_oid.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(SimpleOidTextUdf {
        name: "pg_catalog.pg_get_function_identity_arguments".into(),
        value: "".into(),
        signature: sig_oid,
    }));

    // ----------- pg_get_expr -----------
    let sig_expr = sig_pg_get_expr();
    ctx.register_udf(ScalarUDF::from(EmptyTextMultiArgUdf {
        name: "pg_get_expr".into(),
        signature: sig_expr.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(EmptyTextMultiArgUdf {
        name: "pg_catalog.pg_get_expr".into(),
        signature: sig_expr,
    }));

    // ----------- pg_get_indexdef -----------
    let sig_indexdef = sig_pg_get_indexdef();
    ctx.register_udf(ScalarUDF::from(EmptyTextMultiArgUdf {
        name: "pg_get_indexdef".into(),
        signature: sig_indexdef.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(EmptyTextMultiArgUdf {
        name: "pg_catalog.pg_get_indexdef".into(),
        signature: sig_indexdef,
    }));

    // ----------- format_type -----------
    let sig_ft = sig_format_type();
    ctx.register_udf(ScalarUDF::from(FormatTypeUdf {
        name: "format_type".into(),
        signature: sig_ft.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(FormatTypeUdf {
        name: "pg_catalog.format_type".into(),
        signature: sig_ft,
    }));

    // ----------- pg_get_constraintdef -----------
    let sig_cd = sig_pg_get_constraintdef();
    ctx.register_udf(ScalarUDF::from(EmptyTextMultiArgUdf {
        name: "pg_get_constraintdef".into(),
        signature: sig_cd.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(EmptyTextMultiArgUdf {
        name: "pg_catalog.pg_get_constraintdef".into(),
        signature: sig_cd,
    }));

    // ----------- relation size stubs -----------
    let sig_rel = sig_relation_size();
    ctx.register_udf(ScalarUDF::from(RelationSizeUdf {
        name: "pg_total_relation_size".into(),
        signature: sig_rel.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(RelationSizeUdf {
        name: "pg_catalog.pg_total_relation_size".into(),
        signature: sig_rel.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(RelationSizeUdf {
        name: "pg_table_size".into(),
        signature: sig_rel.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(RelationSizeUdf {
        name: "pg_catalog.pg_table_size".into(),
        signature: sig_rel.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(RelationSizeUdf {
        name: "pg_relation_size".into(),
        signature: sig_rel.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(RelationSizeUdf {
        name: "pg_catalog.pg_relation_size".into(),
        signature: sig_rel,
    }));

    // ----------- obj_description -----------
    let sig_od = sig_obj_description();
    ctx.register_udf(ScalarUDF::from(NullTextMultiArgUdf {
        name: "obj_description".into(),
        signature: sig_od.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(NullTextMultiArgUdf {
        name: "pg_catalog.obj_description".into(),
        signature: sig_od,
    }));

    // ----------- col_description -----------
    let sig_coldes = sig_col_description();
    ctx.register_udf(ScalarUDF::from(NullTextMultiArgUdf {
        name: "col_description".into(),
        signature: sig_coldes.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(NullTextMultiArgUdf {
        name: "pg_catalog.col_description".into(),
        signature: sig_coldes,
    }));

    // ----------- pg_get_partkeydef -----------
    let sig_oid = sig_oid_variants();
    ctx.register_udf(ScalarUDF::from(NullTextMultiArgUdf {
        name: "pg_get_partkeydef".into(),
        signature: sig_oid.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(NullTextMultiArgUdf {
        name: "pg_catalog.pg_get_partkeydef".into(),
        signature: sig_oid,
    }));

    // ----------- pg_encoding_to_char -----------
    let sig_enc = Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Int32]),
        ],
        Volatility::Immutable,
    );
    ctx.register_udf(ScalarUDF::from(PgEncodingToCharUdf {
        name: "pg_encoding_to_char".into(),
        signature: sig_enc.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(PgEncodingToCharUdf {
        name: "pg_catalog.pg_encoding_to_char".into(),
        signature: sig_enc,
    }));

    // ----------- current_schema -----------
    let sig_null = Signature::nullary(Volatility::Stable);
    ctx.register_udf(ScalarUDF::from(CurrentSchemaUdf {
        name: "current_schema".into(),
        signature: sig_null.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(CurrentSchemaUdf {
        name: "pg_catalog.current_schema".into(),
        signature: sig_null,
    }));

    // ----------- current_schemas -----------
    let sig_bool = Signature::exact(vec![DataType::Boolean], Volatility::Stable);
    ctx.register_udf(ScalarUDF::from(CurrentSchemasUdf {
        name: "current_schemas".into(),
        signature: sig_bool.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(CurrentSchemasUdf {
        name: "pg_catalog.current_schemas".into(),
        signature: sig_bool,
    }));

    // ----------- has_table_privilege -----------
    let sig_priv = sig_has_privilege();
    ctx.register_udf(ScalarUDF::from(HasPrivilegeUdf {
        name: "has_table_privilege".into(),
        signature: sig_priv.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(HasPrivilegeUdf {
        name: "pg_catalog.has_table_privilege".into(),
        signature: sig_priv.clone(),
    }));

    // ----------- has_schema_privilege -----------
    ctx.register_udf(ScalarUDF::from(HasPrivilegeUdf {
        name: "has_schema_privilege".into(),
        signature: sig_priv.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(HasPrivilegeUdf {
        name: "pg_catalog.has_schema_privilege".into(),
        signature: sig_priv,
    }));
}

// ---------------------------------------------------------------------------
// Shared signature builders
// ---------------------------------------------------------------------------

/// Signature that accepts a single OID-ish value: Int64, Int32, UInt32, or Utf8.
fn sig_oid_variants() -> Signature {
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Int32]),
            TypeSignature::Exact(vec![DataType::UInt32]),
            TypeSignature::Exact(vec![DataType::Utf8]),
        ],
        Volatility::Stable,
    )
}

fn sig_pg_get_expr() -> Signature {
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Int32]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::UInt32]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64, DataType::Boolean]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Int32, DataType::Boolean]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::UInt32, DataType::Boolean]),
            TypeSignature::Exact(vec![DataType::Null, DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Null, DataType::Int32]),
            TypeSignature::Exact(vec![DataType::Null, DataType::UInt32]),
            TypeSignature::Exact(vec![DataType::Null, DataType::Int64, DataType::Boolean]),
        ],
        Volatility::Stable,
    )
}

fn sig_pg_get_indexdef() -> Signature {
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Int32]),
            TypeSignature::Exact(vec![DataType::UInt32]),
            TypeSignature::Exact(vec![DataType::Int64, DataType::Int64, DataType::Boolean]),
            TypeSignature::Exact(vec![DataType::Int64, DataType::Int32, DataType::Boolean]),
            TypeSignature::Exact(vec![DataType::Int32, DataType::Int32, DataType::Boolean]),
            TypeSignature::Exact(vec![DataType::UInt32, DataType::Int32, DataType::Boolean]),
            TypeSignature::Exact(vec![DataType::UInt32, DataType::Int64, DataType::Boolean]),
        ],
        Volatility::Stable,
    )
}

fn sig_format_type() -> Signature {
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Int64, DataType::Int32]),
            TypeSignature::Exact(vec![DataType::Int32, DataType::Int32]),
            TypeSignature::Exact(vec![DataType::Int32, DataType::Int64]),
            TypeSignature::Exact(vec![DataType::UInt32, DataType::Int32]),
            TypeSignature::Exact(vec![DataType::UInt32, DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Int64, DataType::Null]),
            TypeSignature::Exact(vec![DataType::Int32, DataType::Null]),
            TypeSignature::Exact(vec![DataType::UInt32, DataType::Null]),
            TypeSignature::Exact(vec![DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Int32]),
            TypeSignature::Exact(vec![DataType::UInt32]),
            TypeSignature::Exact(vec![DataType::Null, DataType::Int32]),
            TypeSignature::Exact(vec![DataType::Null, DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Null, DataType::Null]),
        ],
        Volatility::Stable,
    )
}

fn sig_pg_get_constraintdef() -> Signature {
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Int32]),
            TypeSignature::Exact(vec![DataType::UInt32]),
            TypeSignature::Exact(vec![DataType::Int64, DataType::Boolean]),
            TypeSignature::Exact(vec![DataType::Int32, DataType::Boolean]),
            TypeSignature::Exact(vec![DataType::UInt32, DataType::Boolean]),
        ],
        Volatility::Stable,
    )
}

fn sig_relation_size() -> Signature {
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Utf8]),
            TypeSignature::Exact(vec![DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Int32]),
            TypeSignature::Exact(vec![DataType::UInt32]),
        ],
        Volatility::Volatile,
    )
}

fn sig_obj_description() -> Signature {
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Int32]),
            TypeSignature::Exact(vec![DataType::UInt32]),
            TypeSignature::Exact(vec![DataType::Int64, DataType::Utf8]),
            TypeSignature::Exact(vec![DataType::Int32, DataType::Utf8]),
            TypeSignature::Exact(vec![DataType::UInt32, DataType::Utf8]),
        ],
        Volatility::Stable,
    )
}

fn sig_col_description() -> Signature {
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Int64, DataType::Int32]),
            TypeSignature::Exact(vec![DataType::Int32, DataType::Int32]),
            TypeSignature::Exact(vec![DataType::Int32, DataType::Int64]),
            TypeSignature::Exact(vec![DataType::UInt32, DataType::Int32]),
            TypeSignature::Exact(vec![DataType::UInt32, DataType::Int64]),
        ],
        Volatility::Stable,
    )
}

fn sig_has_privilege() -> Signature {
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
            TypeSignature::Exact(vec![DataType::Int64, DataType::Utf8]),
            TypeSignature::Exact(vec![DataType::Int32, DataType::Utf8]),
            TypeSignature::Exact(vec![DataType::UInt32, DataType::Utf8]),
        ],
        Volatility::Stable,
    )
}

// ---------------------------------------------------------------------------
// Helper: row count from ColumnarValue slice
// ---------------------------------------------------------------------------

fn num_rows(args: &[ColumnarValue]) -> usize {
    args.iter()
        .filter_map(|a| match a {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1)
}

// ---------------------------------------------------------------------------
// SimpleOidBoolUdf — (any oid-ish arg) -> bool constant
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct SimpleOidBoolUdf {
    name: String,
    value: bool,
    signature: Signature,
}

impl ScalarUDFImpl for SimpleOidBoolUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        let n = num_rows(args);
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![self.value; n]));
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// SimpleOidTextUdf — (any oid-ish arg) -> text constant (non-null)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct SimpleOidTextUdf {
    name: String,
    value: String,
    signature: Signature,
}

impl ScalarUDFImpl for SimpleOidTextUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        let n = num_rows(args);
        let arr: ArrayRef = Arc::new(StringArray::from(vec![self.value.as_str(); n]));
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// EmptyTextMultiArgUdf — (any args) -> "" (empty string, non-null)
// Used for: pg_get_expr, pg_get_indexdef, pg_get_constraintdef
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct EmptyTextMultiArgUdf {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for EmptyTextMultiArgUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        let n = num_rows(args);
        let arr: ArrayRef = Arc::new(StringArray::from(vec![""; n]));
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// NullTextMultiArgUdf — (any args) -> NULL text
// Used for: obj_description, col_description, pg_get_partkeydef
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct NullTextMultiArgUdf {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for NullTextMultiArgUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        let n = num_rows(args);
        let arr: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; n]));
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// FormatTypeUdf — format_type(oid[, int]) -> text
//
// Best-effort: map well-known PG OIDs to their type names; fall back to
// "unknown" for anything we don't recognise.  psql only needs the call to
// not error.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct FormatTypeUdf {
    name: String,
    signature: Signature,
}

fn pg_oid_to_type_name(oid: i64) -> &'static str {
    match oid {
        16 => "boolean",
        17 => "bytea",
        18 => "\"char\"",
        19 => "name",
        20 => "bigint",
        21 => "smallint",
        23 => "integer",
        25 => "text",
        26 => "oid",
        114 => "json",
        700 => "real",
        701 => "double precision",
        790 => "money",
        1042 => "character",
        1043 => "character varying",
        1082 => "date",
        1083 => "time without time zone",
        1114 => "timestamp without time zone",
        1184 => "timestamp with time zone",
        1186 => "interval",
        1266 => "time with time zone",
        1700 => "numeric",
        2278 => "void",
        2950 => "uuid",
        3802 => "jsonb",
        _ => "unknown",
    }
}

impl ScalarUDFImpl for FormatTypeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        let n = num_rows(args);
        // First arg is the OID; ignore the second (typemod).
        let oid_col = args[0].clone().into_array(n)?;
        let mut out: Vec<Option<&str>> = Vec::with_capacity(n);
        for i in 0..n {
            if oid_col.is_null(i) {
                out.push(None);
                continue;
            }
            // OID can arrive as Int32, Int64, or UInt32.
            let oid_val: i64 = match oid_col.data_type() {
                DataType::Int64 => oid_col
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .map(|a| a.value(i))
                    .unwrap_or(0),
                DataType::Int32 => oid_col
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int32Array>()
                    .map(|a| a.value(i) as i64)
                    .unwrap_or(0),
                DataType::UInt32 => oid_col
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::UInt32Array>()
                    .map(|a| a.value(i) as i64)
                    .unwrap_or(0),
                _ => 0,
            };
            out.push(Some(pg_oid_to_type_name(oid_val)));
        }
        let arr: ArrayRef = Arc::new(StringArray::from(out));
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// RelationSizeUdf — pg_total_relation_size / pg_table_size / pg_relation_size
//                   -> bigint : 0
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct RelationSizeUdf {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for RelationSizeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        let n = num_rows(args);
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![0i64; n]));
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// PgEncodingToCharUdf — pg_encoding_to_char(int) -> text : 'UTF8'
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct PgEncodingToCharUdf {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for PgEncodingToCharUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        let n = num_rows(args);
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["UTF8"; n]));
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// CurrentSchemaUdf — current_schema() -> text : 'public'
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct CurrentSchemaUdf {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for CurrentSchemaUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke_no_args(&self, _number_rows: usize) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "public".to_string(),
        ))))
    }
}

// ---------------------------------------------------------------------------
// CurrentSchemasUdf — current_schemas(bool) -> text
//
// In real Postgres this returns `text[]`.  Basin's df↔ws arrow bridge does not
// support List types, so we return the PG text-array literal representation
// (`{pg_catalog,public}`) as a plain Utf8 column instead.  psql and ORMs that
// call `current_schemas(true)` for schema-search-path checks tolerate this
// because they mostly care about the strings inside, not the wire type.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct CurrentSchemasUdf {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for CurrentSchemasUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        // Return as Utf8 (PG text-array literal) instead of List<Utf8> because
        // Basin's df↔ws arrow bridge does not yet handle List types.
        Ok(DataType::Utf8)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        let n = num_rows(args);
        // Return the PG text-array literal form: {pg_catalog,public}
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["{pg_catalog,public}"; n]));
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// HasPrivilegeUdf — has_table_privilege / has_schema_privilege -> bool : true
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct HasPrivilegeUdf {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for HasPrivilegeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    #[allow(deprecated)]
    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        let n = num_rows(args);
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![true; n]));
        Ok(ColumnarValue::Array(arr))
    }
}
