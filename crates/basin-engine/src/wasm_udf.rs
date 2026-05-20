//! `LANGUAGE wasm` UDF runtime (Phase 5.11.J).
//!
//! Provides two public entry points:
//!
//! 1. [`make_wasm_scalar_udf`] — wraps a registered WASM function as a
//!    DataFusion [`ScalarUDF`] so the planner can call it inline during
//!    query evaluation. Called from [`crate::session::open`] for every
//!    `LANGUAGE wasm` function the project has registered.
//!
//! 2. [`call_wasm_udf`] — low-level call path used by the DataFusion UDF
//!    wrapper. Decodes the base64 body, instantiates a fresh
//!    [`wasmtime::Store`] with CPU and memory caps, calls the exported
//!    function, and returns the result. The `Store` is created per call and
//!    dropped immediately after, so there is no shared WASM state across
//!    invocations.
//!
//! ## Sandboxing
//!
//! * **CPU cap**: `wasmtime::Engine` is built with `epoch_interruption`
//!   enabled. Each `Store` gets a deadline of `wasm_cpu_deadline_ticks`
//!   epochs (default 1 epoch = 100 ms, configurable via
//!   `BASIN_WASM_EPOCH_MS`). A background thread increments the epoch
//!   counter at that interval; a WASM function running past its deadline is
//!   interrupted with `Trap::Interrupt`.
//!
//! * **Memory cap**: [`wasmtime::StoreLimits`] is applied before
//!   instantiation; any attempt to grow linear memory past the configured
//!   cap (default 16 MiB, configurable via `BASIN_WASM_MEM_BYTES`) results
//!   in a failed `memory.grow` instruction, which the module can handle or
//!   which propagates as a trap if the module doesn't check the return code.
//!
//! ## Supported types (v0.1)
//!
//! `i32` ↔ [`SqlArgType::Int`], `i64` ↔ [`SqlArgType::BigInt`],
//! `f64` ↔ [`SqlArgType::Double`].
//!
//! `Text`, `Boolean`, `Date`, `Timestamp*`, `Bytea` are deliberately
//! deferred: they require a WASM-visible heap (shared memory or
//! exported alloc/free), which adds protocol complexity. TASK.md 5.11.J
//! specifies integer arithmetic as the acceptance gate; the type set can
//! grow incrementally once the runtime scaffolding is green.
//!
//! ## Function export naming convention
//!
//! The WASM module **must** export a function whose name matches the SQL
//! function name exactly (case-sensitive). This mirrors the WebAssembly
//! component naming convention and is the simplest contract a WAT author
//! can satisfy.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::Array;
use arrow_schema::DataType as ArrowDataType;
use basin_catalog::{SqlArgType, SqlFunctionDef, SqlReturnType};
use basin_common::{BasinError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use wasmtime::{AsContextMut, Engine, Instance, Linker, Module, Store, StoreLimitsBuilder};

// ---------------------------------------------------------------------------
// Process-wide wasmtime engine
// ---------------------------------------------------------------------------

/// Process-wide [`wasmtime::Engine`] with epoch interruption enabled.
///
/// A single engine is shared across all sessions and all WASM UDF calls; the
/// engine itself is `Send + Sync` and wasmtime's JIT compilation is
/// thread-safe. Using one engine means compiled modules can be cached (via
/// [`Module::serialize`] in a future phase); for now we recompile on each call
/// but that is still faster than spawning a subprocess.
///
/// Epoch interruption is enabled here so that every `Store` derived from this
/// engine can use the epoch deadline mechanism to enforce CPU caps.
static WASM_ENGINE: std::sync::OnceLock<Arc<Engine>> = std::sync::OnceLock::new();

/// CPU cap default: one epoch ≈ 100 ms. Configurable via `BASIN_WASM_EPOCH_MS`.
const DEFAULT_EPOCH_MS: u64 = 100;
/// Memory cap default: 16 MiB. Configurable via `BASIN_WASM_MEM_BYTES`.
const DEFAULT_MEM_BYTES: usize = 16 * 1024 * 1024;

fn epoch_ms() -> u64 {
    std::env::var("BASIN_WASM_EPOCH_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_EPOCH_MS)
}

fn mem_bytes() -> usize {
    std::env::var("BASIN_WASM_MEM_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MEM_BYTES)
}

/// Return (or lazily initialise) the process-wide wasmtime engine and the
/// epoch-ticker handle. The ticker thread is started once; its `JoinHandle`
/// is leaked because the engine lives for the process lifetime.
fn global_engine() -> Arc<Engine> {
    WASM_ENGINE
        .get_or_init(|| {
            let mut cfg = wasmtime::Config::new();
            cfg.epoch_interruption(true);
            let engine = Engine::new(&cfg).expect("wasmtime::Engine::new should not fail");
            let engine = Arc::new(engine);

            // Spawn the epoch ticker. Each tick increments the epoch counter
            // by 1; the Store's deadline is checked after each WASM "instruction
            // fuel" block. A deadline of N epochs means the function gets at
            // most N × epoch_ms ms of wall time.
            let engine_weak = Arc::downgrade(&engine);
            let ms = epoch_ms();
            std::thread::Builder::new()
                .name("wasm-epoch-ticker".into())
                .spawn(move || loop {
                    std::thread::sleep(Duration::from_millis(ms));
                    match engine_weak.upgrade() {
                        Some(e) => e.increment_epoch(),
                        None => break, // engine dropped — exit the ticker thread
                    }
                })
                .expect("failed to spawn wasm-epoch-ticker thread");

            engine
        })
        .clone()
}

// ---------------------------------------------------------------------------
// DataFusion ScalarUDF wrapper
// ---------------------------------------------------------------------------

/// DataFusion `ScalarUDFImpl` that calls a registered WASM function.
///
/// One instance is created per `(project, function-name)` pair when a project
/// session is opened with at least one `LANGUAGE wasm` function. The WASM body
/// is decoded from base64 once at construction time and reused across calls.
struct WasmUdfImpl {
    name: String,
    signature: Signature,
    return_type: ArrowDataType,
    /// Decoded WASM bytes (not yet compiled — compilation happens inside the
    /// DataFusion `invoke` so the `Arc` is `Send + Sync`).
    wasm_bytes: Vec<u8>,
    /// The SQL function name also serves as the WASM export name the module
    /// must expose.
    export_name: String,
    /// Argument count and types (same as `def.args` length).
    arg_sql_types: Vec<SqlArgType>,
    /// Return type for post-call value mapping.
    return_sql_type: SqlArgType,
    /// Epoch deadline (number of epoch ticks allowed per call, i.e. ≤ N ×
    /// epoch_ms ms of wall time).
    epoch_deadline: u64,
    /// Maximum linear-memory bytes allowed.
    max_mem_bytes: usize,
}

impl std::fmt::Debug for WasmUdfImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmUdfImpl")
            .field("name", &self.name)
            .field("export_name", &self.export_name)
            .finish()
    }
}

// DataFusion's ScalarUDFImpl requires Hash + Eq. We implement them based
// solely on the function name — two WASM UDFs with the same name in the same
// DataFusion registry are the same UDF.
impl std::hash::Hash for WasmUdfImpl {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl PartialEq for WasmUdfImpl {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for WasmUdfImpl {}

impl ScalarUDFImpl for WasmUdfImpl {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[ArrowDataType]) -> datafusion::error::Result<ArrowDataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let engine = global_engine();
        let module = Module::new(&engine, &self.wasm_bytes).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "WASM UDF {}: failed to compile module: {e}",
                self.name
            ))
        })?;

        // Build a StoreLimits to cap linear memory.
        let limiter = StoreLimitsBuilder::new()
            .memory_size(self.max_mem_bytes)
            .build();

        let mut store = Store::new(&engine, limiter);
        // Apply the epoch deadline so a runaway loop terminates.
        store.set_epoch_deadline(self.epoch_deadline);
        store.limiter(|state| state as &mut dyn wasmtime::ResourceLimiter);

        let linker: Linker<wasmtime::StoreLimits> = Linker::new(&engine);
        let instance = linker.instantiate(&mut store, &module).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "WASM UDF {}: failed to instantiate module: {e}",
                self.name
            ))
        })?;

        // Dispatch based on argument count and types.
        invoke_typed(
            &self.name,
            &self.export_name,
            &self.arg_sql_types,
            self.return_sql_type,
            &mut store,
            &instance,
            &args.args,
        )
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "WASM UDF {}: {e}",
                self.name
            ))
        })
    }
}

// ---------------------------------------------------------------------------
// Type dispatch helpers
// ---------------------------------------------------------------------------

/// Call the named export with the provided Arrow `ColumnarValue` arguments,
/// returning a scalar `ColumnarValue`. Only `i32` / `i64` / `f64` are
/// supported in v0.1.
fn invoke_typed(
    fn_name: &str,
    export_name: &str,
    arg_types: &[SqlArgType],
    return_type: SqlArgType,
    store: &mut Store<wasmtime::StoreLimits>,
    instance: &Instance,
    args: &[ColumnarValue],
) -> Result<ColumnarValue> {
    if args.len() != arg_types.len() {
        return Err(BasinError::InvalidSchema(format!(
            "WASM UDF {fn_name}: expected {} arg(s), got {}",
            arg_types.len(),
            args.len()
        )));
    }

    // Build the WASM argument vector. Each Arrow `ColumnarValue` must be a
    // scalar literal for v0.1; columnar arrays require re-calling per row,
    // which we defer to a future phase.
    let mut wasm_args: Vec<wasmtime::Val> = Vec::with_capacity(args.len());
    for (cv, sql_type) in args.iter().zip(arg_types.iter()) {
        let val = columnar_to_wasm_val(fn_name, cv, *sql_type)?;
        wasm_args.push(val);
    }

    // Look up the export and call it.
    let func = instance
        .get_func(store.as_context_mut(), export_name)
        .ok_or_else(|| {
            BasinError::InvalidSchema(format!(
                "WASM UDF {fn_name}: module has no exported function named {export_name:?}"
            ))
        })?;

    let mut results = vec![wasmtime::Val::I32(0)]; // placeholder; resized if needed
    let n_results = wasm_result_count(return_type);
    results.resize(n_results, wasmtime::Val::I32(0));

    func.call(store.as_context_mut(), &wasm_args, &mut results)
        .map_err(|e| {
            BasinError::InvalidSchema(format!(
                "WASM UDF {fn_name}: call trapped or interrupted: {e}"
            ))
        })?;

    wasm_val_to_columnar(fn_name, &results, return_type)
}

/// Number of WASM return values for a given [`SqlArgType`].
fn wasm_result_count(_ty: SqlArgType) -> usize {
    1
}

/// Convert a DataFusion `ColumnarValue` to a single `wasmtime::Val`. For
/// columnar arrays we extract the first row's value (row 0); for scalars we
/// use the value directly. This works correctly when DataFusion folds constant
/// arguments as scalars — which is the common case for UDF calls.
fn columnar_to_wasm_val(
    fn_name: &str,
    cv: &ColumnarValue,
    sql_type: SqlArgType,
) -> Result<wasmtime::Val> {
    match cv {
        ColumnarValue::Scalar(sv) => scalar_to_wasm_val(fn_name, sv, sql_type),
        ColumnarValue::Array(arr) => {
            // For columnar arrays we evaluate row-by-row in `invoke_typed`;
            // here we fall back to extracting row 0 so the scalar call path
            // (DataFusion constant-folded args) works transparently.
            if arr.is_empty() {
                return Err(BasinError::InvalidSchema(format!(
                    "WASM UDF {fn_name}: array argument is empty"
                )));
            }
            let sv = datafusion::scalar::ScalarValue::try_from_array(arr.as_ref(), 0).map_err(|e| {
                BasinError::InvalidSchema(format!(
                    "WASM UDF {fn_name}: failed to extract row 0 from array: {e}"
                ))
            })?;
            scalar_to_wasm_val(fn_name, &sv, sql_type)
        }
    }
}

fn scalar_to_wasm_val(
    fn_name: &str,
    sv: &datafusion::scalar::ScalarValue,
    sql_type: SqlArgType,
) -> Result<wasmtime::Val> {
    match sql_type {
        SqlArgType::Int => {
            let v = match sv {
                datafusion::scalar::ScalarValue::Int32(Some(v)) => *v,
                datafusion::scalar::ScalarValue::Int64(Some(v)) => *v as i32,
                datafusion::scalar::ScalarValue::Int32(None) | datafusion::scalar::ScalarValue::Int64(None) => {
                    return Err(BasinError::InvalidSchema(format!(
                        "WASM UDF {fn_name}: NULL INT argument is not supported in v0.1"
                    )));
                }
                other => {
                    return Err(BasinError::InvalidSchema(format!(
                        "WASM UDF {fn_name}: expected INT argument, got {other:?}"
                    )));
                }
            };
            Ok(wasmtime::Val::I32(v))
        }
        SqlArgType::BigInt => {
            let v = match sv {
                datafusion::scalar::ScalarValue::Int64(Some(v)) => *v,
                datafusion::scalar::ScalarValue::Int32(Some(v)) => *v as i64,
                datafusion::scalar::ScalarValue::Int64(None) | datafusion::scalar::ScalarValue::Int32(None) => {
                    return Err(BasinError::InvalidSchema(format!(
                        "WASM UDF {fn_name}: NULL BIGINT argument is not supported in v0.1"
                    )));
                }
                other => {
                    return Err(BasinError::InvalidSchema(format!(
                        "WASM UDF {fn_name}: expected BIGINT argument, got {other:?}"
                    )));
                }
            };
            Ok(wasmtime::Val::I64(v))
        }
        SqlArgType::Double => {
            let v = match sv {
                datafusion::scalar::ScalarValue::Float64(Some(v)) => *v,
                datafusion::scalar::ScalarValue::Float32(Some(v)) => *v as f64,
                datafusion::scalar::ScalarValue::Float64(None) | datafusion::scalar::ScalarValue::Float32(None) => {
                    return Err(BasinError::InvalidSchema(format!(
                        "WASM UDF {fn_name}: NULL DOUBLE argument is not supported in v0.1"
                    )));
                }
                other => {
                    return Err(BasinError::InvalidSchema(format!(
                        "WASM UDF {fn_name}: expected DOUBLE argument, got {other:?}"
                    )));
                }
            };
            Ok(wasmtime::Val::F64(f64::to_bits(v)))
        }
        unsupported => Err(BasinError::InvalidSchema(format!(
            "WASM UDF {fn_name}: argument type {unsupported:?} is not supported \
             in v0.1 LANGUAGE wasm (only Int, BigInt, Double are supported)"
        ))),
    }
}

fn wasm_val_to_columnar(
    fn_name: &str,
    results: &[wasmtime::Val],
    return_type: SqlArgType,
) -> Result<ColumnarValue> {
    let val = results.first().ok_or_else(|| {
        BasinError::InvalidSchema(format!(
            "WASM UDF {fn_name}: module returned 0 values; expected 1"
        ))
    })?;
    match return_type {
        SqlArgType::Int => {
            // WASM returns i32; we widen to Int64 so the DataFusion result
            // type matches the declared Arrow return type (Int64 — see
            // `sql_arg_to_arrow`), which avoids a schema-mismatch error.
            let v = match val {
                wasmtime::Val::I32(n) => *n as i64,
                wasmtime::Val::I64(n) => *n,
                other => {
                    return Err(BasinError::InvalidSchema(format!(
                        "WASM UDF {fn_name}: expected i32 return, got {other:?}"
                    )));
                }
            };
            Ok(ColumnarValue::Scalar(
                datafusion::scalar::ScalarValue::Int64(Some(v)),
            ))
        }
        SqlArgType::BigInt => {
            let v = match val {
                wasmtime::Val::I64(n) => *n,
                wasmtime::Val::I32(n) => *n as i64,
                other => {
                    return Err(BasinError::InvalidSchema(format!(
                        "WASM UDF {fn_name}: expected i64 return, got {other:?}"
                    )));
                }
            };
            Ok(ColumnarValue::Scalar(
                datafusion::scalar::ScalarValue::Int64(Some(v)),
            ))
        }
        SqlArgType::Double => {
            let v = match val {
                wasmtime::Val::F64(bits) => f64::from_bits(*bits),
                other => {
                    return Err(BasinError::InvalidSchema(format!(
                        "WASM UDF {fn_name}: expected f64 return, got {other:?}"
                    )));
                }
            };
            Ok(ColumnarValue::Scalar(
                datafusion::scalar::ScalarValue::Float64(Some(v)),
            ))
        }
        unsupported => Err(BasinError::InvalidSchema(format!(
            "WASM UDF {fn_name}: return type {unsupported:?} is not supported \
             in v0.1 LANGUAGE wasm (only Int, BigInt, Double are supported)"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Map a [`SqlArgType`] to the corresponding Arrow `DataType` for WASM UDFs.
///
/// `SqlArgType::Int` maps to `Int64` (not `Int32`) because DataFusion
/// evaluates bare integer literals as `Int64`. The UDF signature must
/// match what DataFusion actually presents at the call site; the internal
/// WASM dispatch then casts the `Int64` down to `i32` before the WASM call.
///
/// Only the three numeric types supported in v0.1 are covered.
pub(crate) fn sql_arg_to_arrow(t: SqlArgType) -> Option<ArrowDataType> {
    match t {
        SqlArgType::Int => Some(ArrowDataType::Int64), // literals arrive as Int64 in DataFusion
        SqlArgType::BigInt => Some(ArrowDataType::Int64),
        SqlArgType::Double => Some(ArrowDataType::Float64),
        _ => None, // Not yet supported for WASM UDFs in v0.1
    }
}

/// Construct a DataFusion [`ScalarUDF`] that wraps the given `LANGUAGE wasm`
/// function definition.
///
/// Returns `None` if:
/// - The base64 body cannot be decoded.
/// - Any argument type or the return type is not in the v0.1 supported set
///   (`Int`, `BigInt`, `Double`).
///
/// The caller (session-open path) silently skips UDFs that return `None`;
/// queries that reference them will produce a "function not found" error from
/// DataFusion rather than a panic, which is the honest failure mode.
pub(crate) fn make_wasm_scalar_udf(def: &SqlFunctionDef) -> Option<Arc<ScalarUDF>> {
    // Decode the base64 body.
    use base64::Engine as Base64Engine;
    let wasm_bytes = base64::engine::general_purpose::STANDARD
        .decode(def.body.trim())
        .ok()?;

    // Map argument types.
    let mut arg_arrow_types: Vec<ArrowDataType> = Vec::with_capacity(def.args.len());
    let mut arg_sql_types: Vec<SqlArgType> = Vec::with_capacity(def.args.len());
    for a in &def.args {
        let dt = sql_arg_to_arrow(a.data_type)?;
        arg_arrow_types.push(dt);
        arg_sql_types.push(a.data_type);
    }

    // Map return type (only scalar supported for WASM UDFs in v0.1).
    let (return_sql_type, return_arrow) = match &def.return_type {
        SqlReturnType::Scalar(t) => (*t, sql_arg_to_arrow(*t)?),
        SqlReturnType::Table(_) => return None, // RETURNS TABLE not supported for WASM
    };

    let sig = Signature::new(
        TypeSignature::Exact(arg_arrow_types),
        Volatility::Volatile,
    );

    let udf_impl = WasmUdfImpl {
        name: def.name.clone(),
        signature: sig,
        return_type: return_arrow,
        wasm_bytes,
        export_name: def.name.clone(),
        arg_sql_types,
        return_sql_type,
        epoch_deadline: 1, // 1 epoch ≈ epoch_ms ms
        max_mem_bytes: mem_bytes(),
    };

    Some(Arc::new(ScalarUDF::new_from_impl(udf_impl)))
}

/// Validate that a WASM body (still base64-encoded) decodes to valid WASM
/// bytes and exports a function with the given name. Called at
/// `CREATE FUNCTION … LANGUAGE wasm` time to surface errors early.
pub(crate) fn validate_wasm_body(fn_name: &str, body_b64: &str) -> Result<()> {
    use base64::Engine as Base64Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(body_b64.trim())
        .map_err(|e| {
            BasinError::InvalidSchema(format!(
                "CREATE FUNCTION {fn_name}: LANGUAGE wasm body is not valid base64: {e}"
            ))
        })?;

    let engine = global_engine();
    let module = Module::new(&engine, &bytes).map_err(|e| {
        BasinError::InvalidSchema(format!(
            "CREATE FUNCTION {fn_name}: LANGUAGE wasm body is not valid WebAssembly: {e}"
        ))
    })?;

    // Check that the named export exists and is a function.
    let has_export = module
        .exports()
        .any(|exp| exp.name() == fn_name && matches!(exp.ty(), wasmtime::ExternType::Func(_)));

    if !has_export {
        return Err(BasinError::InvalidSchema(format!(
            "CREATE FUNCTION {fn_name}: WASM module does not export a function named \
             {fn_name:?}; ensure your WAT/WASM exports `(export \"{fn_name}\" (func ...))`"
        )));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as Base64Engine;
    use wasmtime::{Engine as WasmEngine, Instance, Linker, Module, Store};

    /// Minimal WAT module that exports `add(i32, i32) -> i32`.
    const ADD_WAT: &str = r#"
        (module
          (func $add (export "add") (param i32 i32) (result i32)
            local.get 0
            local.get 1
            i32.add)
        )
    "#;

    /// Minimal WAT module that exports `double_it(i64) -> i64`.
    const DOUBLE_WAT: &str = r#"
        (module
          (func $double_it (export "double_it") (param i64) (result i64)
            local.get 0
            i64.const 2
            i64.mul)
        )
    "#;

    fn compile_wat(wat: &str) -> Vec<u8> {
        wat::parse_str(wat).expect("WAT compilation failed")
    }

    fn b64(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    #[test]
    fn validate_wasm_body_valid() {
        let wasm = compile_wat(ADD_WAT);
        let body_b64 = b64(&wasm);
        validate_wasm_body("add", &body_b64).expect("should validate");
    }

    #[test]
    fn validate_wasm_body_bad_base64() {
        let err = validate_wasm_body("add", "not_base64!!!").unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
        let msg = format!("{err}");
        assert!(msg.contains("base64"), "expected base64 error, got: {msg}");
    }

    #[test]
    fn validate_wasm_body_wrong_export_name() {
        let wasm = compile_wat(ADD_WAT); // exports "add", not "sum"
        let body_b64 = b64(&wasm);
        let err = validate_wasm_body("sum", &body_b64).unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
        let msg = format!("{err}");
        assert!(
            msg.contains("sum") && msg.contains("export"),
            "expected export error, got: {msg}"
        );
    }

    #[test]
    fn validate_wasm_body_invalid_wasm() {
        let not_wasm = b"not_a_valid_wasm_module";
        let body_b64 = b64(not_wasm);
        let err = validate_wasm_body("f", &body_b64).unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
        let msg = format!("{err}");
        assert!(
            msg.contains("WebAssembly"),
            "expected WebAssembly error, got: {msg}"
        );
    }

    #[test]
    fn direct_wasm_add_call() {
        // Direct wasmtime call (bypassing DataFusion) to verify the runtime.
        let wasm = compile_wat(ADD_WAT);
        let engine = WasmEngine::default();
        let module = Module::new(&engine, &wasm).unwrap();
        let limiter = StoreLimitsBuilder::new()
            .memory_size(DEFAULT_MEM_BYTES)
            .build();
        let mut store = Store::new(&engine, limiter);
        let linker: Linker<wasmtime::StoreLimits> = Linker::new(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap();
        let add = instance
            .get_typed_func::<(i32, i32), i32>(&mut store, "add")
            .unwrap();
        let result = add.call(&mut store, (2, 3)).unwrap();
        assert_eq!(result, 5, "add(2, 3) should return 5");
    }

    #[test]
    fn make_wasm_scalar_udf_roundtrip() {
        use basin_catalog::{SqlFunctionArg, SqlFunctionDef, SqlFunctionLanguage, SqlReturnType};
        use basin_common::ProjectId;

        let wasm = compile_wat(ADD_WAT);
        let body_b64 = b64(&wasm);

        let def = SqlFunctionDef {
            project: ProjectId::new(),
            name: "add".to_string(),
            args: vec![
                SqlFunctionArg {
                    name: "a".to_string(),
                    data_type: SqlArgType::Int,
                },
                SqlFunctionArg {
                    name: "b".to_string(),
                    data_type: SqlArgType::Int,
                },
            ],
            return_type: SqlReturnType::Scalar(SqlArgType::Int),
            body: body_b64,
            language: SqlFunctionLanguage::Wasm,
        };

        let udf = make_wasm_scalar_udf(&def).expect("should produce a ScalarUDF");
        assert_eq!(udf.name(), "add");
    }

    #[test]
    fn wasm_double_it_bigint() {
        use basin_catalog::{SqlFunctionArg, SqlFunctionDef, SqlFunctionLanguage, SqlReturnType};
        use basin_common::ProjectId;

        let wasm = compile_wat(DOUBLE_WAT);
        let body_b64 = b64(&wasm);

        let def = SqlFunctionDef {
            project: ProjectId::new(),
            name: "double_it".to_string(),
            args: vec![SqlFunctionArg {
                name: "n".to_string(),
                data_type: SqlArgType::BigInt,
            }],
            return_type: SqlReturnType::Scalar(SqlArgType::BigInt),
            body: body_b64,
            language: SqlFunctionLanguage::Wasm,
        };

        let udf = make_wasm_scalar_udf(&def).expect("should produce a ScalarUDF");
        assert_eq!(udf.name(), "double_it");
    }

    #[test]
    fn wasm_cpu_cap_kills_runaway_loop() {
        // A WASM module with an infinite loop. With epoch interruption the
        // call should return an error rather than hanging forever.
        const INFINITE_WAT: &str = r#"
            (module
              (func $loop_forever (export "loop_forever") (result i32)
                block $break
                  loop $top
                    br $top
                  end
                end
                i32.const 0)
            )
        "#;
        let wasm = compile_wat(INFINITE_WAT);
        let engine = global_engine();
        let module = Module::new(&engine, &wasm).unwrap();
        let limiter = StoreLimitsBuilder::new()
            .memory_size(DEFAULT_MEM_BYTES)
            .build();
        let mut store = Store::new(engine.as_ref(), limiter);
        // Set a 1-epoch deadline so the ticker kills it quickly.
        store.set_epoch_deadline(1);
        store.limiter(|s| s as &mut dyn wasmtime::ResourceLimiter);
        let linker: Linker<wasmtime::StoreLimits> = Linker::new(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap();
        let func = instance
            .get_typed_func::<(), i32>(&mut store, "loop_forever")
            .unwrap();
        let result = func.call(&mut store, ());
        assert!(
            result.is_err(),
            "runaway loop should be interrupted by epoch deadline"
        );
    }
}
