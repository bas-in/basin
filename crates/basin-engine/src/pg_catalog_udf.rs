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

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
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

    // ----------- pg_get_serial_sequence -----------
    // Prisma calls `pg_get_serial_sequence(table, column)` to derive the
    // underlying SEQUENCE name for a SERIAL/BIGSERIAL column. Basin doesn't
    // have real sequences (BIGSERIAL is implemented via per-table catalog
    // counters), so we synthesize the PG naming convention unconditionally:
    //   `public.<table>_<column>_seq`
    // This keeps Prisma's introspection display happy without validating
    // whether the column actually has a sequence.
    let sig_get_serial_seq = sig_pg_get_serial_sequence();
    ctx.register_udf(ScalarUDF::from(PgGetSerialSequenceUdf {
        name: "pg_get_serial_sequence".into(),
        signature: sig_get_serial_seq.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(PgGetSerialSequenceUdf {
        name: "pg_catalog.pg_get_serial_sequence".into(),
        signature: sig_get_serial_seq,
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

    // ----------- pg_advisory_lock family -----------
    // Advisory locks are NOT registered here. They are session-scoped (a lock
    // is owned by a specific session and must be visible as "held" to other
    // sessions), so a stateless stub cannot implement PG semantics. The real,
    // PG-faithful implementations are registered per-session in
    // `session::open` via `crate::advisory_lock::register_advisory_lock_udfs`
    // (BUG #138). See `crates/basin-engine/src/advisory_lock.rs`.

    // ----------- pg_typeof -----------
    // Names the argument's type from its Arrow type. Falls back to PG's own
    // `unknown` for types with no PG spelling (see PgTypeofUdf).
    let sig_any = Signature::variadic_any(Volatility::Stable);
    ctx.register_udf(ScalarUDF::from(PgTypeofUdf {
        signature: sig_any.clone(),
    }));

    // ----------- pg_size_pretty -----------
    let sig_size = Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Int32]),
            TypeSignature::Exact(vec![DataType::UInt32]),
        ],
        Volatility::Immutable,
    );
    ctx.register_udf(ScalarUDF::from(PgSizePrettyUdf {
        signature: sig_size,
    }));

    // ----------- pg_column_size -----------
    ctx.register_udf(ScalarUDF::from(SimpleConstInt64Udf {
        name: "pg_column_size".into(),
        value: 0,
        signature: sig_any.clone(),
    }));

    // ----------- current_user / session_user -----------
    // These are SQL standard keywords; DataFusion doesn't support them natively
    // so we register them as 0-argument scalar UDFs that return a stub string.
    let sig_nullary = Signature::nullary(Volatility::Stable);
    ctx.register_udf(ScalarUDF::from(SimpleConstTextUdf {
        name: "current_user".into(),
        value: "anonymous".into(),
        signature: sig_nullary.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(SimpleConstTextUdf {
        name: "session_user".into(),
        value: "anonymous".into(),
        signature: sig_nullary,
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

/// Signature for `pg_get_serial_sequence(table_name TEXT, column_name TEXT)`.
/// Accepts (Utf8, Utf8) and the NULL-typed variants that the planner may infer
/// when literal NULLs are passed.
fn sig_pg_get_serial_sequence() -> Signature {
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            TypeSignature::Exact(vec![DataType::Null, DataType::Utf8]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Null]),
            TypeSignature::Exact(vec![DataType::Null, DataType::Null]),
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![self.value; n]));
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// SimpleOidTextUdf — (any oid-ish arg) -> text constant (non-null)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let arr: ArrayRef = Arc::new(StringArray::from(vec![self.value.as_str(); n]));
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// EmptyTextMultiArgUdf — (any args) -> "" (empty string, non-null)
// Used for: pg_get_expr, pg_get_indexdef, pg_get_constraintdef
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let arr: ArrayRef = Arc::new(StringArray::from(vec![""; n]));
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// NullTextMultiArgUdf — (any args) -> NULL text
// Used for: obj_description, col_description, pg_get_partkeydef
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![0i64; n]));
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// PgEncodingToCharUdf — pg_encoding_to_char(int) -> text : 'UTF8'
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["UTF8"; n]));
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// CurrentSchemaUdf — current_schema() -> text : 'public'
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        // Return the PG text-array literal form: {pg_catalog,public}
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["{pg_catalog,public}"; n]));
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// HasPrivilegeUdf — has_table_privilege / has_schema_privilege -> bool : true
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![true; n]));
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// PgGetSerialSequenceUdf — pg_get_serial_sequence(table, column) -> text
//
// Returns the synthetic PG sequence name `public.<table>_<column>_seq` for
// any (table, column) pair. Basin has no real sequences (BIGSERIAL is backed
// by per-table catalog counters), but Prisma calls this purely to derive the
// display name during introspection. Returns NULL if either argument is NULL.
//
// Schema-qualified table names like `"my_schema"."users"` are passed through
// unchanged in the prefix (we always emit `public.` as the schema prefix per
// PG convention and don't try to parse the input — Prisma uses the result as
// an opaque display string).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PgGetSerialSequenceUdf {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for PgGetSerialSequenceUdf {
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);

        // args[0] = table_name, args[1] = column_name. Coerce both to arrays.
        let table_col = args[0].clone().into_array(n)?;
        let column_col = args[1].clone().into_array(n)?;

        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            if table_col.is_null(i) || column_col.is_null(i) {
                out.push(None);
                continue;
            }
            let table = match table_col.data_type() {
                DataType::Utf8 => table_col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .map(|a| a.value(i).to_string())
                    .unwrap_or_default(),
                _ => String::new(),
            };
            let column = match column_col.data_type() {
                DataType::Utf8 => column_col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .map(|a| a.value(i).to_string())
                    .unwrap_or_default(),
                _ => String::new(),
            };
            out.push(Some(format!("public.{table}_{column}_seq")));
        }
        let arr: ArrayRef = Arc::new(StringArray::from(out));
        Ok(ColumnarValue::Array(arr))
    }
}

// (Advisory-lock stub UDFs removed — see BUG #138 / advisory_lock.rs. The
// PG-faithful, session-scoped implementations live in that module and are
// registered per-session in session::open.)

// ---------------------------------------------------------------------------
// pg_typeof(any) -> text
// ---------------------------------------------------------------------------

/// PostgreSQL's `regtype` spelling for an Arrow type, or `None` when Basin has
/// no PG name for it.
///
/// These are the *display* names `pg_typeof` prints, not the short
/// `pg_type.typname` spellings `pg_colnames::pg_cast_type_name` uses for cast
/// column names — PostgreSQL prints `double precision`, not `float8`, and
/// `timestamp without time zone`, not `timestamp`. Every entry below was read
/// off a live 18.2 session.
fn pg_typeof_name(dt: &DataType) -> Option<String> {
    let name = match dt {
        DataType::Int8 | DataType::Int16 | DataType::UInt8 => "smallint",
        DataType::Int32 | DataType::UInt16 => "integer",
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => "bigint",
        DataType::Float32 => "real",
        DataType::Float64 => "double precision",
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "text",
        DataType::Boolean => "boolean",
        DataType::Date32 | DataType::Date64 => "date",
        DataType::Time32(_) | DataType::Time64(_) => "time without time zone",
        DataType::Timestamp(_, Some(_)) => "timestamp with time zone",
        DataType::Timestamp(_, None) => "timestamp without time zone",
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "numeric",
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "bytea",
        DataType::Interval(_) => "interval",
        // PG spells an array type as the element type plus `[]`, and its
        // arrays are single-typed however deeply nested:
        // `pg_typeof(ARRAY[[1,2],[3,4]])` is `integer[]`, not `integer[][]`.
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            let mut inner = f.data_type();
            while let DataType::List(g) | DataType::LargeList(g) | DataType::FixedSizeList(g, _) =
                inner
            {
                inner = g.data_type();
            }
            return pg_typeof_name(inner).map(|e| format!("{e}[]"));
        }
        _ => return None,
    };
    Some(name.to_string())
}

/// `pg_typeof(x)` — the name of `x`'s type.
///
/// This answered the constant `"unknown"` for every argument. `unknown` is a
/// real PostgreSQL type, which is what made the stub so quiet: introspection
/// code that branches on `pg_typeof` saw a legitimate-looking answer rather
/// than a failure. Basin does know the type — it is the argument's Arrow type,
/// available on the `ColumnarValue` — so name it.
///
/// **Known divergence, not a bug in this function:** Basin plans an unadorned
/// integer literal as `Int64`, so `pg_typeof(1)` says `bigint` where PostgreSQL
/// says `integer`. The literal's width is decided long before this UDF runs;
/// `pg_typeof(1::int)` and `pg_typeof(col)` on a real `integer` column are both
/// right. The same offset applies to any function keyed on literal width.
///
/// `unknown` survives as the fallback for Arrow types with no PG spelling,
/// which is also what PostgreSQL answers for `pg_typeof(NULL)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PgTypeofUdf {
    signature: Signature,
}

impl ScalarUDFImpl for PgTypeofUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "pg_typeof"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return exec_err!("pg_typeof expects 1 argument, got {}", args.len());
        }
        // The *type* is what is asked for, so a NULL value still names its
        // type: `pg_typeof(NULL::int)` is `integer` on PG, not NULL.
        let name = pg_typeof_name(&args[0].data_type()).unwrap_or_else(|| "unknown".to_string());
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(name))))
    }
}

// ---------------------------------------------------------------------------
// pg_size_pretty(bigint) -> text
// ---------------------------------------------------------------------------

/// `pg_size_pretty(n)` — a byte count in the largest unit that keeps it short.
///
/// This was the constant string `"0 bytes"` for every input, which is a
/// plausible answer for exactly one of them. PostgreSQL's rule (`pg_size_pretty`
/// in `dbsize.c`) is: while the magnitude is at least `10 * 1024` in the current
/// unit, divide by 1024 with **half-up rounding away from zero** and step to the
/// next unit. So the switch happens at 10 kB, not 1 kB, and `1048576` prints as
/// `1024 kB` rather than `1 MB`. Measured on 18.2:
///
/// | n                  | PG          |
/// |--------------------|-------------|
/// | `0`                | `0 bytes`   |
/// | `1023`             | `1023 bytes`|
/// | `10239`            | `10239 bytes` |
/// | `10240`            | `10 kB`     |
/// | `1048576`          | `1024 kB`   |
/// | `10485760`         | `10 MB`     |
/// | `123456789`        | `118 MB`    |
/// | `1073741824`       | `1024 MB`   |
/// | `-1024`            | `-1024 bytes` |
/// | `1125899906842624` | `1024 TB`   |
///
/// Note `-1024`: the threshold is on the magnitude, and the sign rides along.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PgSizePrettyUdf {
    signature: Signature,
}

/// Format `bytes` the way PostgreSQL's `pg_size_pretty` does.
fn pg_size_pretty_text(bytes: i64) -> String {
    const UNITS: [&str; 5] = ["kB", "MB", "GB", "TB", "PB"];
    // i64::MIN has no positive counterpart; widen so the loop below can work
    // on the magnitude without overflowing.
    let mut size = bytes as i128;
    for unit in UNITS {
        if size.abs() < 10 * 1024 {
            return format!("{size} bytes");
        }
        // Half-up, away from zero — PG's `(size + 512) / 1024` for positives,
        // mirrored for negatives.
        size = if size >= 0 {
            (size + 512) / 1024
        } else {
            (size - 512) / 1024
        };
        if size.abs() < 10 * 1024 {
            return format!("{size} {unit}");
        }
    }
    // Ran out of units: PG stops at PB and prints whatever is left there.
    format!("{size} PB")
}

impl ScalarUDFImpl for PgSizePrettyUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "pg_size_pretty"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        if args.len() != 1 {
            return exec_err!("pg_size_pretty expects 1 argument, got {}", args.len());
        }
        let n = num_rows(args);
        let arr = args[0].clone().into_array(n)?;
        let mut out = StringBuilder::with_capacity(n, n * 8);
        for i in 0..n {
            if arr.is_null(i) {
                out.append_null();
                continue;
            }
            let v = ScalarValue::try_from_array(&arr, i)?;
            match v.cast_to(&DataType::Int64) {
                Ok(ScalarValue::Int64(Some(bytes))) => out.append_value(pg_size_pretty_text(bytes)),
                _ => {
                    return exec_err!(
                        "pg_size_pretty: expected an integer byte count, got {:?}",
                        arr.data_type()
                    )
                }
            }
        }
        let arr: ArrayRef = Arc::new(out.finish());
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// SimpleConstTextUdf — (any args) -> text constant
// Used for: pg_typeof, current_user, session_user
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SimpleConstTextUdf {
    name: String,
    value: String,
    signature: Signature,
}

impl ScalarUDFImpl for SimpleConstTextUdf {
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = if args.is_empty() { 1 } else { num_rows(args) };
        let arr: ArrayRef = Arc::new(StringArray::from(vec![self.value.as_str(); n]));
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// SimpleConstInt64Udf — (any args) -> int64 constant
// Used for: pg_column_size
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SimpleConstInt64Udf {
    name: String,
    value: i64,
    signature: Signature,
}

impl ScalarUDFImpl for SimpleConstInt64Udf {
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
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = &args.args;
        let n = num_rows(args);
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![self.value; n]));
        Ok(ColumnarValue::Array(arr))
    }
}

// ---------------------------------------------------------------------------
// Inline unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;

    fn return_field_utf8() -> Arc<Field> {
        Arc::new(Field::new("_", DataType::Utf8, true))
    }

    /// Call `pg_get_serial_sequence(table, column)` directly via UDF impl
    /// and return the single-row text result (or None for NULL).
    fn call_pg_get_serial_sequence(table: Option<&str>, column: Option<&str>) -> Option<String> {
        let udf = PgGetSerialSequenceUdf {
            name: "pg_get_serial_sequence".into(),
            signature: sig_pg_get_serial_sequence(),
        };
        let table_arr: ArrayRef = Arc::new(StringArray::from(vec![table]));
        let column_arr: ArrayRef = Arc::new(StringArray::from(vec![column]));
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(table_arr),
                ColumnarValue::Array(column_arr),
            ],
            arg_fields: vec![],
            number_rows: 1,
            return_field: return_field_utf8(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        match udf.invoke_with_args(args).unwrap() {
            ColumnarValue::Array(arr) => {
                let sa = arr.as_any().downcast_ref::<StringArray>().unwrap();
                if sa.is_null(0) {
                    None
                } else {
                    Some(sa.value(0).to_string())
                }
            }
            ColumnarValue::Scalar(s) => match s {
                ScalarValue::Utf8(v) => v,
                _ => None,
            },
        }
    }

    #[test]
    fn pg_get_serial_sequence_returns_synthetic_name() {
        assert_eq!(
            call_pg_get_serial_sequence(Some("users"), Some("id")),
            Some("public.users_id_seq".to_string())
        );
    }

    #[test]
    fn pg_get_serial_sequence_null_table_returns_null() {
        assert_eq!(call_pg_get_serial_sequence(None, Some("id")), None);
    }

    #[test]
    fn pg_get_serial_sequence_null_column_returns_null() {
        assert_eq!(call_pg_get_serial_sequence(Some("users"), None), None);
    }
}
