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
//! ## Supported types
//!
//! **Numeric (pass-by-value):**
//! `i32` ↔ [`SqlArgType::Int`], `i64` ↔ [`SqlArgType::BigInt`],
//! `f64` ↔ [`SqlArgType::Double`], `i64`-micros ↔ [`SqlArgType::TimestampTz`]
//! and [`SqlArgType::Timestamp`].
//!
//! **Dynamic-size (pass-by-(ptr,len)):**
//! [`SqlArgType::Text`] (UTF-8 bytes) and [`SqlArgType::Bytea`] (raw bytes).
//! These ride the **guest allocator contract** described below.
//!
//! Still deferred: `Boolean`, `Date`, `Jsonb` (stored as Text internally if
//! marshalled), composite rows, arrays.
//!
//! ## Guest allocator contract — (ptr, len) ABI
//!
//! For dynamic-size types the host needs a WASM-visible heap. We adopt the
//! standard `wasm-bindgen` / `Component Model wasi-io` shape:
//!
//! ```text
//! ;; Required exports on the guest module:
//! (memory (export "memory") 1)
//! (func (export "basin_alloc")   (param i32) (result i32))
//! (func (export "basin_dealloc") (param i32 i32))
//! ```
//!
//! * `basin_alloc(n)` returns a pointer into linear memory to `n` writable bytes.
//! * `basin_dealloc(ptr, n)` releases bytes previously returned by `basin_alloc`.
//!
//! ### Call sequence for a function `f(text) -> text`
//!
//! 1. Host evaluates the SQL argument to a UTF-8 `&[u8]`.
//! 2. Host calls `basin_alloc(len)` to get a guest pointer `arg_ptr`.
//! 3. Host writes the bytes into `memory[arg_ptr .. arg_ptr+len]`.
//! 4. Host calls the user function with `(arg_ptr: i32, len: i32) -> i64`
//!    where the `i64` packs `(ret_ptr: i32 high, ret_len: i32 low)`.
//! 5. Host reads `memory[ret_ptr .. ret_ptr+ret_len]` and constructs the
//!    Arrow output.
//! 6. Host calls `basin_dealloc(ret_ptr, ret_len)` to release the result.
//!    (The argument buffer is the guest's to free when it sees fit — by
//!    convention guests free it inside the user function before returning.)
//!
//! Packing the return as a single `i64` keeps the WAT call signature
//! single-value, which is the most portable shape across guest toolchains.
//!
//! ### NULL handling
//!
//! A NULL SQL input arrives as `(ptr=0, len=-1)`. A NULL return is also
//! signalled by `len=-1` (any ptr). Empty strings/bytes use `len=0`.
//!
//! ## Function export naming convention
//!
//! The WASM module **must** export a function whose name matches the SQL
//! function name exactly (case-sensitive). This mirrors the WebAssembly
//! component naming convention and is the simplest contract a WAT author
//! can satisfy.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::builder::{
    BinaryBuilder, Float64Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{
    Array, BinaryArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType as ArrowDataType, TimeUnit};
use basin_catalog::{SqlArgType, SqlFunctionDef, SqlReturnType};
use basin_common::{BasinError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use wasmtime::{AsContextMut, Engine, Instance, Linker, Memory, Module, Store, StoreLimitsBuilder};

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

/// Whether a [`SqlArgType`] travels by-value (a single `wasmtime::Val`) or
/// by-(ptr,len) (two `wasmtime::Val::I32`s, with bytes written to guest
/// linear memory via `basin_alloc`).
fn is_dynamic_bytes(t: SqlArgType) -> bool {
    matches!(t, SqlArgType::Text | SqlArgType::Bytea)
}

/// Drive one call into the WASM module for each row in the inputs, then
/// pack the per-row results into a single Arrow array (or scalar, if every
/// argument was scalar).
///
/// The (ptr, len) ABI for `Text`/`Bytea` is implemented here:
///
/// 1. Resolve the guest's `basin_alloc` / `basin_dealloc` once per call.
/// 2. For each row, build the `wasmtime::Val` argument vector — numeric
///    scalars go through as-is; dynamic bytes go through
///    [`write_bytes_to_guest`] which calls `basin_alloc` and then writes
///    the bytes into linear memory.
/// 3. Call the user export. The signature is `[arg-vals…] -> [ret]` where
///    `ret` is a single `wasmtime::Val::I64` when the return is dynamic
///    (packing `(ptr<<32 | len_as_u32)`), or the natural numeric value
///    otherwise.
/// 4. Decode the result, read the bytes (if dynamic) back from guest memory,
///    free them with `basin_dealloc`, and append to the output builder.
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

    // Look up the export once.
    let func = instance
        .get_func(store.as_context_mut(), export_name)
        .ok_or_else(|| {
            BasinError::InvalidSchema(format!(
                "WASM UDF {fn_name}: module has no exported function named {export_name:?}"
            ))
        })?;

    // If any input arg is dynamic OR the return is dynamic, we need the
    // guest's allocator + memory. Look them up once and pass into the row
    // loop. They are *not* required for purely-numeric calls — back-compat
    // with the legacy minimal WAT modules that don't export `basin_alloc`.
    let needs_alloc = arg_types.iter().copied().any(is_dynamic_bytes)
        || is_dynamic_bytes(return_type);

    let alloc = if needs_alloc {
        Some(instance
            .get_typed_func::<i32, i32>(store.as_context_mut(), "basin_alloc")
            .map_err(|_| {
                BasinError::InvalidSchema(format!(
                    "WASM UDF {fn_name}: module uses dynamic-size types but does not \
                     export `basin_alloc(i32) -> i32`; see basin-engine::wasm_udf docs"
                ))
            })?)
    } else {
        None
    };
    let dealloc = if needs_alloc {
        Some(instance
            .get_typed_func::<(i32, i32), ()>(store.as_context_mut(), "basin_dealloc")
            .map_err(|_| {
                BasinError::InvalidSchema(format!(
                    "WASM UDF {fn_name}: module uses dynamic-size types but does not \
                     export `basin_dealloc(i32, i32)`; see basin-engine::wasm_udf docs"
                ))
            })?)
    } else {
        None
    };
    let memory = if needs_alloc {
        Some(instance
            .get_memory(store.as_context_mut(), "memory")
            .ok_or_else(|| {
                BasinError::InvalidSchema(format!(
                    "WASM UDF {fn_name}: module uses dynamic-size types but does not \
                     export `memory`; see basin-engine::wasm_udf docs"
                ))
            })?)
    } else {
        None
    };

    // Determine row count: any columnar array fixes it; otherwise scalar
    // call (1 logical row) and return scalar.
    let row_count: Option<usize> = args.iter().find_map(|cv| match cv {
        ColumnarValue::Array(a) => Some(a.len()),
        ColumnarValue::Scalar(_) => None,
    });
    let scalar_call = row_count.is_none();
    let rows = row_count.unwrap_or(1);

    let mut output = OutputBuilder::new(return_type, rows);

    for row in 0..rows {
        // Build the argument Vals for this row.
        let mut wasm_args: Vec<wasmtime::Val> = Vec::with_capacity(arg_types.len() * 2);
        for (cv, sql_type) in args.iter().zip(arg_types.iter()) {
            extract_row_into_wasm_args(
                fn_name,
                cv,
                row,
                *sql_type,
                &mut wasm_args,
                store,
                alloc.as_ref(),
                memory.as_ref(),
            )?;
        }

        // Allocate the result slot. Dynamic returns use a single i64 carrying
        // packed (ptr, len); numeric returns use the natural Val.
        let mut results = vec![placeholder_for(return_type)];

        func.call(store.as_context_mut(), &wasm_args, &mut results)
            .map_err(|e| {
                BasinError::InvalidSchema(format!(
                    "WASM UDF {fn_name}: call trapped or interrupted: {e}"
                ))
            })?;

        let ret_val = results.first().ok_or_else(|| {
            BasinError::InvalidSchema(format!(
                "WASM UDF {fn_name}: module returned 0 values; expected 1"
            ))
        })?;

        decode_and_push(
            fn_name,
            ret_val,
            return_type,
            &mut output,
            store,
            memory.as_ref(),
            dealloc.as_ref(),
        )?;
    }

    output.finish(fn_name, scalar_call)
}

/// Placeholder Val matching the return-type's wasm representation, used to
/// size the `results` buffer before `call`.
fn placeholder_for(t: SqlArgType) -> wasmtime::Val {
    match t {
        SqlArgType::Int => wasmtime::Val::I32(0),
        SqlArgType::BigInt | SqlArgType::Timestamp | SqlArgType::TimestampTz => {
            wasmtime::Val::I64(0)
        }
        SqlArgType::Double => wasmtime::Val::F64(0),
        SqlArgType::Text | SqlArgType::Bytea => wasmtime::Val::I64(0), // packed (ptr,len)
        _ => wasmtime::Val::I32(0),
    }
}

/// Extract the value at `row` from `cv` and push the resulting WASM `Val`(s)
/// onto `wasm_args`. Numeric scalars push 1 Val; dynamic bytes push 2 Vals
/// (ptr, len) after allocating + writing to guest memory.
#[allow(clippy::too_many_arguments)]
fn extract_row_into_wasm_args(
    fn_name: &str,
    cv: &ColumnarValue,
    row: usize,
    sql_type: SqlArgType,
    wasm_args: &mut Vec<wasmtime::Val>,
    store: &mut Store<wasmtime::StoreLimits>,
    alloc: Option<&wasmtime::TypedFunc<i32, i32>>,
    memory: Option<&Memory>,
) -> Result<()> {
    // Pull the value at row index. For scalar args, all rows see the same value.
    let sv: datafusion::scalar::ScalarValue = match cv {
        ColumnarValue::Scalar(sv) => sv.clone(),
        ColumnarValue::Array(arr) => {
            datafusion::scalar::ScalarValue::try_from_array(arr.as_ref(), row).map_err(|e| {
                BasinError::InvalidSchema(format!(
                    "WASM UDF {fn_name}: failed to extract row {row} from array: {e}"
                ))
            })?
        }
    };

    if is_dynamic_bytes(sql_type) {
        // NULL → (0, -1). Empty → (alloc_ptr, 0).
        let bytes: Option<Vec<u8>> = match (sql_type, &sv) {
            (SqlArgType::Text, datafusion::scalar::ScalarValue::Utf8(Some(s)))
            | (SqlArgType::Text, datafusion::scalar::ScalarValue::LargeUtf8(Some(s)))
            | (SqlArgType::Text, datafusion::scalar::ScalarValue::Utf8View(Some(s))) => {
                Some(s.as_bytes().to_vec())
            }
            (SqlArgType::Text, datafusion::scalar::ScalarValue::Utf8(None))
            | (SqlArgType::Text, datafusion::scalar::ScalarValue::LargeUtf8(None))
            | (SqlArgType::Text, datafusion::scalar::ScalarValue::Utf8View(None)) => None,
            (SqlArgType::Bytea, datafusion::scalar::ScalarValue::Binary(Some(b)))
            | (SqlArgType::Bytea, datafusion::scalar::ScalarValue::LargeBinary(Some(b)))
            | (SqlArgType::Bytea, datafusion::scalar::ScalarValue::BinaryView(Some(b))) => {
                Some(b.clone())
            }
            (SqlArgType::Bytea, datafusion::scalar::ScalarValue::Binary(None))
            | (SqlArgType::Bytea, datafusion::scalar::ScalarValue::LargeBinary(None))
            | (SqlArgType::Bytea, datafusion::scalar::ScalarValue::BinaryView(None)) => None,
            (_, other) => {
                return Err(BasinError::InvalidSchema(format!(
                    "WASM UDF {fn_name}: expected {sql_type:?} argument, got {other:?}"
                )));
            }
        };
        match bytes {
            None => {
                // NULL marker: ptr=0, len=-1.
                wasm_args.push(wasmtime::Val::I32(0));
                wasm_args.push(wasmtime::Val::I32(-1));
            }
            Some(b) => {
                let (ptr, len) = write_bytes_to_guest(
                    fn_name,
                    store,
                    alloc.expect("dynamic arg requires alloc"),
                    memory.expect("dynamic arg requires memory"),
                    &b,
                )?;
                wasm_args.push(wasmtime::Val::I32(ptr));
                wasm_args.push(wasmtime::Val::I32(len));
            }
        }
        return Ok(());
    }

    // Numeric arms (Int / BigInt / Double / Timestamp / TimestampTz).
    let val = scalar_to_numeric_val(fn_name, &sv, sql_type)?;
    wasm_args.push(val);
    Ok(())
}

/// Numeric-arm extractor — the original v0.1 logic, plus Timestamp /
/// TimestampTz which marshal as `i64` microseconds (Arrow's own representation
/// — no conversion needed).
fn scalar_to_numeric_val(
    fn_name: &str,
    sv: &datafusion::scalar::ScalarValue,
    sql_type: SqlArgType,
) -> Result<wasmtime::Val> {
    match sql_type {
        SqlArgType::Int => match sv {
            datafusion::scalar::ScalarValue::Int32(Some(v)) => Ok(wasmtime::Val::I32(*v)),
            datafusion::scalar::ScalarValue::Int64(Some(v)) => Ok(wasmtime::Val::I32(*v as i32)),
            datafusion::scalar::ScalarValue::Int32(None)
            | datafusion::scalar::ScalarValue::Int64(None) => Err(BasinError::InvalidSchema(
                format!("WASM UDF {fn_name}: NULL INT argument is not supported"),
            )),
            other => Err(BasinError::InvalidSchema(format!(
                "WASM UDF {fn_name}: expected INT argument, got {other:?}"
            ))),
        },
        SqlArgType::BigInt => match sv {
            datafusion::scalar::ScalarValue::Int64(Some(v)) => Ok(wasmtime::Val::I64(*v)),
            datafusion::scalar::ScalarValue::Int32(Some(v)) => Ok(wasmtime::Val::I64(*v as i64)),
            datafusion::scalar::ScalarValue::Int64(None)
            | datafusion::scalar::ScalarValue::Int32(None) => Err(BasinError::InvalidSchema(
                format!("WASM UDF {fn_name}: NULL BIGINT argument is not supported"),
            )),
            other => Err(BasinError::InvalidSchema(format!(
                "WASM UDF {fn_name}: expected BIGINT argument, got {other:?}"
            ))),
        },
        SqlArgType::Double => match sv {
            datafusion::scalar::ScalarValue::Float64(Some(v)) => {
                Ok(wasmtime::Val::F64(f64::to_bits(*v)))
            }
            datafusion::scalar::ScalarValue::Float32(Some(v)) => {
                Ok(wasmtime::Val::F64(f64::to_bits(*v as f64)))
            }
            datafusion::scalar::ScalarValue::Float64(None)
            | datafusion::scalar::ScalarValue::Float32(None) => Err(BasinError::InvalidSchema(
                format!("WASM UDF {fn_name}: NULL DOUBLE argument is not supported"),
            )),
            other => Err(BasinError::InvalidSchema(format!(
                "WASM UDF {fn_name}: expected DOUBLE argument, got {other:?}"
            ))),
        },
        SqlArgType::TimestampTz | SqlArgType::Timestamp => match sv {
            datafusion::scalar::ScalarValue::TimestampMicrosecond(Some(v), _) => {
                Ok(wasmtime::Val::I64(*v))
            }
            datafusion::scalar::ScalarValue::TimestampMicrosecond(None, _) => {
                Err(BasinError::InvalidSchema(format!(
                    "WASM UDF {fn_name}: NULL TIMESTAMP argument is not supported"
                )))
            }
            datafusion::scalar::ScalarValue::Int64(Some(v)) => Ok(wasmtime::Val::I64(*v)),
            other => Err(BasinError::InvalidSchema(format!(
                "WASM UDF {fn_name}: expected TIMESTAMP argument (Int64 micros), got {other:?}"
            ))),
        },
        unsupported => Err(BasinError::InvalidSchema(format!(
            "WASM UDF {fn_name}: argument type {unsupported:?} is not numeric — \
             handled in the dynamic-bytes arm"
        ))),
    }
}

/// Decode the function's return Val into the per-row output builder. For
/// dynamic types this reads from guest memory and calls `basin_dealloc`.
#[allow(clippy::too_many_arguments)]
fn decode_and_push(
    fn_name: &str,
    ret_val: &wasmtime::Val,
    return_type: SqlArgType,
    output: &mut OutputBuilder,
    store: &mut Store<wasmtime::StoreLimits>,
    memory: Option<&Memory>,
    dealloc: Option<&wasmtime::TypedFunc<(i32, i32), ()>>,
) -> Result<()> {
    match return_type {
        SqlArgType::Int => {
            let v = match ret_val {
                wasmtime::Val::I32(n) => *n as i64,
                wasmtime::Val::I64(n) => *n,
                other => {
                    return Err(BasinError::InvalidSchema(format!(
                        "WASM UDF {fn_name}: expected i32 return, got {other:?}"
                    )));
                }
            };
            output.push_int64(v);
        }
        SqlArgType::BigInt => {
            let v = match ret_val {
                wasmtime::Val::I64(n) => *n,
                wasmtime::Val::I32(n) => *n as i64,
                other => {
                    return Err(BasinError::InvalidSchema(format!(
                        "WASM UDF {fn_name}: expected i64 return, got {other:?}"
                    )));
                }
            };
            output.push_int64(v);
        }
        SqlArgType::Double => {
            let v = match ret_val {
                wasmtime::Val::F64(bits) => f64::from_bits(*bits),
                other => {
                    return Err(BasinError::InvalidSchema(format!(
                        "WASM UDF {fn_name}: expected f64 return, got {other:?}"
                    )));
                }
            };
            output.push_float64(v);
        }
        SqlArgType::TimestampTz | SqlArgType::Timestamp => {
            let v = match ret_val {
                wasmtime::Val::I64(n) => *n,
                other => {
                    return Err(BasinError::InvalidSchema(format!(
                        "WASM UDF {fn_name}: expected i64 micros return, got {other:?}"
                    )));
                }
            };
            output.push_timestamp_micros(v);
        }
        SqlArgType::Text | SqlArgType::Bytea => {
            let packed = match ret_val {
                wasmtime::Val::I64(n) => *n,
                other => {
                    return Err(BasinError::InvalidSchema(format!(
                        "WASM UDF {fn_name}: expected packed-(ptr,len) i64 return, got {other:?}"
                    )));
                }
            };
            let (ptr, len) = unpack_ptr_len(packed);
            if len < 0 {
                // NULL return marker.
                output.push_null();
            } else {
                let memory = memory.expect("dynamic return requires memory");
                let dealloc = dealloc.expect("dynamic return requires dealloc");
                let bytes = read_bytes_from_guest(fn_name, store, memory, ptr, len as usize)?;
                if matches!(return_type, SqlArgType::Text) {
                    let s = std::str::from_utf8(&bytes).map_err(|e| {
                        BasinError::InvalidSchema(format!(
                            "WASM UDF {fn_name}: TEXT return is not valid UTF-8: {e}"
                        ))
                    })?;
                    output.push_string(s);
                } else {
                    output.push_binary(&bytes);
                }
                // Best-effort dealloc — if it traps, surface the error so we
                // don't quietly leak guest heap on a misbehaving module.
                dealloc
                    .call(store.as_context_mut(), (ptr, len))
                    .map_err(|e| {
                        BasinError::InvalidSchema(format!(
                            "WASM UDF {fn_name}: basin_dealloc trapped: {e}"
                        ))
                    })?;
            }
        }
        unsupported => {
            return Err(BasinError::InvalidSchema(format!(
                "WASM UDF {fn_name}: return type {unsupported:?} is not supported"
            )));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Guest memory helpers — the (ptr, len) ABI primitives
// ---------------------------------------------------------------------------

/// Allocate `bytes.len()` bytes inside the guest's linear memory via
/// `basin_alloc`, write `bytes` into the returned region, and return the
/// `(ptr, len)` tuple.
///
/// The pointer is a u32-shaped i32 (wasm32 linear-memory addresses fit in
/// 32 bits). The length is a 31-bit non-negative i32 (we reserve `-1` as the
/// NULL sentinel — any input longer than `i32::MAX` is rejected up-front so
/// the sentinel never collides with a real length).
pub(crate) fn write_bytes_to_guest(
    fn_name: &str,
    store: &mut Store<wasmtime::StoreLimits>,
    alloc: &wasmtime::TypedFunc<i32, i32>,
    memory: &Memory,
    bytes: &[u8],
) -> Result<(i32, i32)> {
    if bytes.len() > i32::MAX as usize {
        return Err(BasinError::InvalidSchema(format!(
            "WASM UDF {fn_name}: argument is {} bytes; limit is i32::MAX",
            bytes.len()
        )));
    }
    let len = bytes.len() as i32;
    let ptr = alloc.call(store.as_context_mut(), len).map_err(|e| {
        BasinError::InvalidSchema(format!(
            "WASM UDF {fn_name}: basin_alloc({len}) trapped: {e}"
        ))
    })?;
    if ptr < 0 {
        return Err(BasinError::InvalidSchema(format!(
            "WASM UDF {fn_name}: basin_alloc returned negative pointer {ptr}"
        )));
    }
    memory
        .write(store.as_context_mut(), ptr as usize, bytes)
        .map_err(|e| {
            BasinError::InvalidSchema(format!(
                "WASM UDF {fn_name}: failed writing arg bytes to guest memory: {e}"
            ))
        })?;
    Ok((ptr, len))
}

/// Read `len` bytes from guest linear memory at `ptr` into an owned `Vec<u8>`.
///
/// The bytes are *cloned* — we never hand out aliased slices into a `Store`,
/// because subsequent guest calls can grow / reallocate the linear memory
/// and invalidate any pointer we held.
pub(crate) fn read_bytes_from_guest(
    fn_name: &str,
    store: &mut Store<wasmtime::StoreLimits>,
    memory: &Memory,
    ptr: i32,
    len: usize,
) -> Result<Vec<u8>> {
    if ptr < 0 {
        return Err(BasinError::InvalidSchema(format!(
            "WASM UDF {fn_name}: guest returned negative pointer {ptr}"
        )));
    }
    let mut buf = vec![0u8; len];
    memory
        .read(store.as_context_mut(), ptr as usize, &mut buf)
        .map_err(|e| {
            BasinError::InvalidSchema(format!(
                "WASM UDF {fn_name}: failed reading {len} bytes at ptr {ptr} from guest memory: {e}"
            ))
        })?;
    Ok(buf)
}

/// Unpack a guest-returned i64 into `(ptr, len)`. Pointer in the high 32
/// bits, length in the low 32. Length is interpreted as a signed i32 so
/// `-1` can signal NULL.
pub(crate) fn unpack_ptr_len(packed: i64) -> (i32, i32) {
    let ptr = ((packed as u64) >> 32) as i32;
    let len = (packed as u64 & 0xFFFF_FFFF) as i32;
    (ptr, len)
}

/// Pack `(ptr, len)` into a single i64 for guest returns. Symmetric to
/// [`unpack_ptr_len`]. Provided as a test helper + for the documented
/// guest-side ABI; not called directly by host code.
pub(crate) fn pack_ptr_len(ptr: i32, len: i32) -> i64 {
    (((ptr as u32) as u64) << 32 | ((len as u32) as u64)) as i64
}

// ---------------------------------------------------------------------------
// OutputBuilder — accumulates per-row results into a typed Arrow array.
// ---------------------------------------------------------------------------

/// Per-call output accumulator. The variant is chosen once based on the
/// declared SQL return type; row results are pushed in order, and `finish`
/// converts to a `ColumnarValue` (scalar if the call was scalar, otherwise
/// an Array).
enum OutputBuilder {
    Int64 { b: Int64Builder },
    Float64 { b: Float64Builder },
    Text { b: StringBuilder },
    Bytea { b: BinaryBuilder },
    TimestampTz { b: TimestampMicrosecondBuilder },
    Timestamp { b: TimestampMicrosecondBuilder },
}

impl OutputBuilder {
    fn new(t: SqlArgType, capacity: usize) -> Self {
        match t {
            SqlArgType::Int | SqlArgType::BigInt => {
                OutputBuilder::Int64 { b: Int64Builder::with_capacity(capacity) }
            }
            SqlArgType::Double => {
                OutputBuilder::Float64 { b: Float64Builder::with_capacity(capacity) }
            }
            SqlArgType::Text => OutputBuilder::Text { b: StringBuilder::with_capacity(capacity, capacity * 16) },
            SqlArgType::Bytea => OutputBuilder::Bytea { b: BinaryBuilder::with_capacity(capacity, capacity * 16) },
            SqlArgType::TimestampTz => OutputBuilder::TimestampTz {
                b: TimestampMicrosecondBuilder::with_capacity(capacity).with_timezone("UTC"),
            },
            SqlArgType::Timestamp => OutputBuilder::Timestamp {
                b: TimestampMicrosecondBuilder::with_capacity(capacity),
            },
            // Fallback for any types not covered above — should never be
            // reached because make_wasm_scalar_udf gates on sql_arg_to_arrow.
            _ => OutputBuilder::Int64 { b: Int64Builder::with_capacity(capacity) },
        }
    }

    fn push_int64(&mut self, v: i64) {
        if let OutputBuilder::Int64 { b } = self {
            b.append_value(v);
        }
    }
    fn push_float64(&mut self, v: f64) {
        if let OutputBuilder::Float64 { b } = self {
            b.append_value(v);
        }
    }
    fn push_string(&mut self, s: &str) {
        if let OutputBuilder::Text { b } = self {
            b.append_value(s);
        }
    }
    fn push_binary(&mut self, b_in: &[u8]) {
        if let OutputBuilder::Bytea { b } = self {
            b.append_value(b_in);
        }
    }
    fn push_timestamp_micros(&mut self, v: i64) {
        match self {
            OutputBuilder::TimestampTz { b } => b.append_value(v),
            OutputBuilder::Timestamp { b } => b.append_value(v),
            _ => {}
        }
    }
    fn push_null(&mut self) {
        match self {
            OutputBuilder::Int64 { b } => b.append_null(),
            OutputBuilder::Float64 { b } => b.append_null(),
            OutputBuilder::Text { b } => b.append_null(),
            OutputBuilder::Bytea { b } => b.append_null(),
            OutputBuilder::TimestampTz { b } => b.append_null(),
            OutputBuilder::Timestamp { b } => b.append_null(),
        }
    }

    /// Finalise into a `ColumnarValue`. When the original call was scalar
    /// (every arg was a `Scalar`) we return a `ColumnarValue::Scalar` so
    /// DataFusion's constant-folding remains effective; otherwise an Array.
    fn finish(self, fn_name: &str, scalar_call: bool) -> Result<ColumnarValue> {
        match self {
            OutputBuilder::Int64 { mut b } => {
                let arr: Int64Array = b.finish();
                if scalar_call {
                    let v = if arr.is_null(0) { None } else { Some(arr.value(0)) };
                    Ok(ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Int64(v)))
                } else {
                    Ok(ColumnarValue::Array(Arc::new(arr)))
                }
            }
            OutputBuilder::Float64 { mut b } => {
                let arr: Float64Array = b.finish();
                if scalar_call {
                    let v = if arr.is_null(0) { None } else { Some(arr.value(0)) };
                    Ok(ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Float64(v)))
                } else {
                    Ok(ColumnarValue::Array(Arc::new(arr)))
                }
            }
            OutputBuilder::Text { mut b } => {
                let arr: StringArray = b.finish();
                if scalar_call {
                    let v = if arr.is_null(0) { None } else { Some(arr.value(0).to_string()) };
                    Ok(ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(v)))
                } else {
                    Ok(ColumnarValue::Array(Arc::new(arr)))
                }
            }
            OutputBuilder::Bytea { mut b } => {
                let arr: BinaryArray = b.finish();
                if scalar_call {
                    let v = if arr.is_null(0) { None } else { Some(arr.value(0).to_vec()) };
                    Ok(ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Binary(v)))
                } else {
                    Ok(ColumnarValue::Array(Arc::new(arr)))
                }
            }
            OutputBuilder::TimestampTz { mut b } => {
                let arr: TimestampMicrosecondArray = b.finish();
                if scalar_call {
                    let v = if arr.is_null(0) { None } else { Some(arr.value(0)) };
                    Ok(ColumnarValue::Scalar(
                        datafusion::scalar::ScalarValue::TimestampMicrosecond(v, Some("UTC".into())),
                    ))
                } else {
                    Ok(ColumnarValue::Array(Arc::new(arr)))
                }
            }
            OutputBuilder::Timestamp { mut b } => {
                let arr: TimestampMicrosecondArray = b.finish();
                if scalar_call {
                    let v = if arr.is_null(0) { None } else { Some(arr.value(0)) };
                    Ok(ColumnarValue::Scalar(
                        datafusion::scalar::ScalarValue::TimestampMicrosecond(v, None),
                    ))
                } else {
                    let _ = fn_name; // suppress unused warning when no error branch fires
                    Ok(ColumnarValue::Array(Arc::new(arr)))
                }
            }
        }
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
/// `TimestampTz` carries the `"UTC"` zone tag so DataFusion's planner
/// distinguishes it from the zoneless `Timestamp` variant — matching the
/// shape `info_schema::pg_type_oid_for_arg` uses, so the OID round-trip
/// stays consistent with what `pg_catalog` reports.
pub(crate) fn sql_arg_to_arrow(t: SqlArgType) -> Option<ArrowDataType> {
    match t {
        SqlArgType::Int => Some(ArrowDataType::Int64), // literals arrive as Int64 in DataFusion
        SqlArgType::BigInt => Some(ArrowDataType::Int64),
        SqlArgType::Double => Some(ArrowDataType::Float64),
        SqlArgType::Text => Some(ArrowDataType::Utf8),
        SqlArgType::Bytea => Some(ArrowDataType::Binary),
        SqlArgType::TimestampTz => {
            Some(ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())))
        }
        SqlArgType::Timestamp => Some(ArrowDataType::Timestamp(TimeUnit::Microsecond, None)),
        _ => None, // Boolean / Date / composite — not yet wired for WASM UDFs
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
            version: 1,
            source: None,
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
            version: 1,
            source: None,
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

    // -----------------------------------------------------------------------
    // (ptr, len) ABI primitives — unit tests
    // -----------------------------------------------------------------------

    /// Bump-allocator WAT exposing the documented guest contract:
    ///
    /// * `memory` is exported.
    /// * `basin_alloc(n)` bumps a high-water cursor stored at offset 0.
    /// * `basin_dealloc(ptr, n)` is a no-op (bump allocator never frees).
    /// * `echo_text(ptr, len)` reads UTF-8 bytes, allocates a fresh result,
    ///   copies them back, and returns `(ret_ptr << 32) | ret_len`.
    const ECHO_TEXT_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (data (i32.const 0) "\10\00\00\00") ;; cursor starts at 16 (skip 16-byte header)

          (func $basin_alloc (export "basin_alloc") (param $n i32) (result i32)
            (local $ptr i32)
            (local.set $ptr (i32.load (i32.const 0)))
            (i32.store (i32.const 0) (i32.add (local.get $ptr) (local.get $n)))
            (local.get $ptr))

          (func $basin_dealloc (export "basin_dealloc") (param $ptr i32) (param $n i32))

          (func $memcpy (param $dst i32) (param $src i32) (param $n i32)
            (local $i i32)
            (block $break
              (loop $top
                (br_if $break (i32.eq (local.get $i) (local.get $n)))
                (i32.store8
                  (i32.add (local.get $dst) (local.get $i))
                  (i32.load8_u (i32.add (local.get $src) (local.get $i))))
                (local.set $i (i32.add (local.get $i) (i32.const 1)))
                (br $top))))

          ;; echo_text(ptr, len) -> i64 (packed (ret_ptr<<32) | ret_len)
          (func $echo_text (export "echo_text") (param $ptr i32) (param $len i32) (result i64)
            (local $ret i32)
            (local.set $ret (call $basin_alloc (local.get $len)))
            (call $memcpy (local.get $ret) (local.get $ptr) (local.get $len))
            (i64.or
              (i64.shl (i64.extend_i32_u (local.get $ret)) (i64.const 32))
              (i64.extend_i32_u (local.get $len))))
        )
    "#;

    /// Build an instance of `ECHO_TEXT_WAT` ready for direct host-side
    /// invocation. Helper used by the round-trip + property tests below.
    fn instantiate_echo() -> (Store<wasmtime::StoreLimits>, Instance) {
        let wasm = compile_wat(ECHO_TEXT_WAT);
        let engine = WasmEngine::default();
        let module = Module::new(&engine, &wasm).unwrap();
        let limiter = StoreLimitsBuilder::new()
            .memory_size(DEFAULT_MEM_BYTES)
            .build();
        let mut store = Store::new(&engine, limiter);
        store.limiter(|s| s as &mut dyn wasmtime::ResourceLimiter);
        let linker: Linker<wasmtime::StoreLimits> = Linker::new(&engine);
        let instance = linker.instantiate(&mut store, &module).unwrap();
        (store, instance)
    }

    #[test]
    fn pack_unpack_ptr_len_round_trip() {
        for (ptr, len) in [(0, 0), (1, 1), (16, 5), (0x0100_0000, 1024), (-1, -1)] {
            let packed = pack_ptr_len(ptr, len);
            let (p, l) = unpack_ptr_len(packed);
            assert_eq!((p, l), (ptr, len), "pack/unpack must round-trip");
        }
    }

    #[test]
    fn write_then_read_bytes_round_trip() {
        // Unit test for the (ptr, len) primitives: allocate, write, read,
        // verify identity, dealloc (no-op in this WAT). This is the
        // "write_bytes_to_guest + read_bytes_from_guest round-trip" gate.
        let (mut store, instance) = instantiate_echo();
        let alloc = instance
            .get_typed_func::<i32, i32>(store.as_context_mut(), "basin_alloc")
            .expect("basin_alloc");
        let memory = instance
            .get_memory(store.as_context_mut(), "memory")
            .expect("memory");

        let payload = b"hello, basin";
        let (ptr, len) =
            write_bytes_to_guest("echo", &mut store, &alloc, &memory, payload).expect("write");
        assert_eq!(len as usize, payload.len());
        let out = read_bytes_from_guest("echo", &mut store, &memory, ptr, len as usize)
            .expect("read");
        assert_eq!(out, payload);
    }

    #[test]
    fn write_bytes_random_payloads_round_trip() {
        // Property-ish test: a fixed pool of "random" byte sequences (small +
        // medium + with-null-bytes) all round-trip cleanly through the
        // host->guest->host bytes channel.
        let (mut store, instance) = instantiate_echo();
        let alloc = instance
            .get_typed_func::<i32, i32>(store.as_context_mut(), "basin_alloc")
            .expect("basin_alloc");
        let memory = instance
            .get_memory(store.as_context_mut(), "memory")
            .expect("memory");
        let payloads: Vec<Vec<u8>> = vec![
            vec![],
            b"a".to_vec(),
            (0u8..=255u8).collect(),
            b"with\0embedded\0nulls".to_vec(),
            vec![0xff; 1024],
        ];
        for payload in payloads {
            let (ptr, len) = write_bytes_to_guest("echo", &mut store, &alloc, &memory, &payload)
                .expect("write");
            assert_eq!(len as usize, payload.len());
            // Empty payload still gets a valid (>=0) ptr from the bump allocator.
            assert!(ptr >= 0, "ptr must be non-negative");
            let out = read_bytes_from_guest("echo", &mut store, &memory, ptr, len as usize)
                .expect("read");
            assert_eq!(out, payload, "round-trip mismatch for {} bytes", payload.len());
        }
    }

    #[test]
    fn echo_text_end_to_end_via_user_func() {
        // Drives the *user* function (`echo_text`) end-to-end the same way
        // `invoke_typed` would: write input bytes via basin_alloc, call the
        // function with (ptr, len), decode the packed (ret_ptr, ret_len),
        // read the result, verify identity.
        let (mut store, instance) = instantiate_echo();
        let alloc = instance
            .get_typed_func::<i32, i32>(store.as_context_mut(), "basin_alloc")
            .expect("basin_alloc");
        let memory = instance
            .get_memory(store.as_context_mut(), "memory")
            .expect("memory");
        let echo = instance
            .get_typed_func::<(i32, i32), i64>(store.as_context_mut(), "echo_text")
            .expect("echo_text export");

        let payload = "hello unicode 🦀 with utf-8";
        let (in_ptr, in_len) =
            write_bytes_to_guest("echo_text", &mut store, &alloc, &memory, payload.as_bytes())
                .expect("write");
        let packed = echo
            .call(store.as_context_mut(), (in_ptr, in_len))
            .expect("call");
        let (out_ptr, out_len) = unpack_ptr_len(packed);
        assert_eq!(out_len, in_len);
        let bytes =
            read_bytes_from_guest("echo_text", &mut store, &memory, out_ptr, out_len as usize)
                .expect("read");
        assert_eq!(std::str::from_utf8(&bytes).unwrap(), payload);
    }

    #[test]
    fn sql_arg_to_arrow_covers_new_types() {
        // Regression: the new types must each return a concrete Arrow type
        // so make_wasm_scalar_udf doesn't silently drop the function.
        assert!(matches!(sql_arg_to_arrow(SqlArgType::Text), Some(ArrowDataType::Utf8)));
        assert!(matches!(sql_arg_to_arrow(SqlArgType::Bytea), Some(ArrowDataType::Binary)));
        assert!(matches!(
            sql_arg_to_arrow(SqlArgType::TimestampTz),
            Some(ArrowDataType::Timestamp(TimeUnit::Microsecond, _))
        ));
        assert!(matches!(
            sql_arg_to_arrow(SqlArgType::Timestamp),
            Some(ArrowDataType::Timestamp(TimeUnit::Microsecond, None))
        ));
        // Out-of-scope types still return None so the engine doesn't
        // pretend it supports them.
        assert!(sql_arg_to_arrow(SqlArgType::Boolean).is_none());
        assert!(sql_arg_to_arrow(SqlArgType::Date).is_none());
    }
}
