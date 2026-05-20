//! Glue point for `basin-net` — `net.http_get` / `net.http_post` scalar UDFs.
//!
//! `basin-net` depends on `basin-engine`. These UDFs call `reqwest` directly
//! (same lib as basin-net) without a circular dep. They require
//! `BASIN_NET_ALLOW_ANY_HOST=1` for outgoing HTTP (dev/test only).
//!
//! SQL surface rewritten before DataFusion sees it:
//! - `net.http_get(...)` → `net_http_get(...)`
//! - `net.http_post(...)` → `net_http_post(...)`

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

const DEFAULT_TIMEOUT_SECS: u64 = 30;
const DEFAULT_MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

/// Hook called once from [`crate::Engine::new`]. No-op; registration happens
/// per-session via [`register_net_udfs`].
#[inline]
pub(crate) fn install() {}

/// Register `net_http_get` and `net_http_post` UDFs on `ctx`.
pub(crate) fn register_net_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(NetHttpGetUdf {
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Volatile),
    }));
    ctx.register_udf(ScalarUDF::from(NetHttpPostUdf {
        signature: Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Volatile,
        ),
    }));
}

/// Rewrite `net.http_get(...)` / `net.http_post(...)` to underscore forms.
pub(crate) fn rewrite_net_schema_functions(sql: &str) -> String {
    const TARGETS: &[(&str, &str)] = &[
        ("net.http_get", "net_http_get"),
        ("net.http_post", "net_http_post"),
    ];
    crate::cron_glue::rewrite_schema_dot(sql, TARGETS)
}

#[derive(Debug)]
struct NetHttpGetUdf { signature: Signature }
impl PartialEq for NetHttpGetUdf { fn eq(&self, _o: &Self) -> bool { true } }
impl Eq for NetHttpGetUdf {}
impl std::hash::Hash for NetHttpGetUdf { fn hash<H: std::hash::Hasher>(&self, _s: &mut H) {} }
impl ScalarUDFImpl for NetHttpGetUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "net_http_get" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, number_rows, .. } = args;
        if args.len() != 1 {
            return Err(DataFusionError::Execution(format!("net_http_get expects 1 arg, got {}", args.len())));
        }
        let url = net_scalar_utf8_req(&args[0], "url")?;
        let body = http_get_blocking(&url)?;
        let rows: Vec<Option<&str>> = vec![body.as_deref(); number_rows];
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(rows)) as ArrayRef))
    }
}

#[derive(Debug)]
struct NetHttpPostUdf { signature: Signature }
impl PartialEq for NetHttpPostUdf { fn eq(&self, _o: &Self) -> bool { true } }
impl Eq for NetHttpPostUdf {}
impl std::hash::Hash for NetHttpPostUdf { fn hash<H: std::hash::Hasher>(&self, _s: &mut H) {} }
impl ScalarUDFImpl for NetHttpPostUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "net_http_post" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> { Ok(DataType::Utf8) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let ScalarFunctionArgs { args, number_rows, .. } = args;
        if args.len() < 2 || args.len() > 3 {
            return Err(DataFusionError::Execution(format!("net_http_post expects 2-3 args, got {}", args.len())));
        }
        let url = net_scalar_utf8_req(&args[0], "url")?;
        let body = net_scalar_utf8_opt(&args[1], "body")?.unwrap_or_default();
        let ct = if args.len() == 3 {
            net_scalar_utf8_opt(&args[2], "content_type")?.unwrap_or_else(|| "application/json".to_string())
        } else { "application/json".to_string() };
        let resp = http_post_blocking(&url, body, &ct)?;
        let rows: Vec<Option<&str>> = vec![resp.as_deref(); number_rows];
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(rows)) as ArrayRef))
    }
}

fn timeout_dur() -> Duration {
    Duration::from_secs(std::env::var("BASIN_NET_TIMEOUT_SECS").ok()
        .and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_TIMEOUT_SECS))
}
fn max_body_bytes() -> usize {
    std::env::var("BASIN_NET_MAX_BODY_BYTES").ok()
        .and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_MAX_BODY_BYTES)
}
fn check_allow() -> DFResult<()> {
    if std::env::var("BASIN_NET_ALLOW_ANY_HOST").as_deref() != Ok("1") {
        return Err(DataFusionError::Execution(
            "net UDFs: set BASIN_NET_ALLOW_ANY_HOST=1 to enable outbound HTTP (dev/test only).".into()));
    }
    Ok(())
}

fn http_get_blocking(url: &str) -> DFResult<Option<String>> {
    check_allow()?;
    let url = url.to_string();
    let (timeout, cap) = (timeout_dur(), max_body_bytes());
    tokio::runtime::Handle::current().block_on(async move {
        let c = reqwest::Client::builder().timeout(timeout).user_agent("basin-net/0.1").build()
            .map_err(|e| DataFusionError::Execution(format!("net_http_get: {e}")))?;
        let resp = c.get(&url).send().await
            .map_err(|e| DataFusionError::Execution(format!("net_http_get: {e}")))?;
        let bytes = resp.bytes().await
            .map_err(|e| DataFusionError::Execution(format!("net_http_get body: {e}")))?;
        if bytes.len() > cap { return Err(DataFusionError::Execution(format!("net_http_get: body {} > cap {cap}", bytes.len()))); }
        Ok(Some(String::from_utf8_lossy(&bytes).into_owned()))
    })
}

fn http_post_blocking(url: &str, body: String, content_type: &str) -> DFResult<Option<String>> {
    check_allow()?;
    let url = url.to_string();
    let ct = content_type.to_string();
    let (timeout, cap) = (timeout_dur(), max_body_bytes());
    tokio::runtime::Handle::current().block_on(async move {
        let c = reqwest::Client::builder().timeout(timeout).user_agent("basin-net/0.1").build()
            .map_err(|e| DataFusionError::Execution(format!("net_http_post: {e}")))?;
        let resp = c.post(&url).header("content-type", &ct).body(body).send().await
            .map_err(|e| DataFusionError::Execution(format!("net_http_post: {e}")))?;
        let bytes = resp.bytes().await
            .map_err(|e| DataFusionError::Execution(format!("net_http_post body: {e}")))?;
        if bytes.len() > cap { return Err(DataFusionError::Execution(format!("net_http_post: body {} > cap {cap}", bytes.len()))); }
        Ok(Some(String::from_utf8_lossy(&bytes).into_owned()))
    })
}

fn net_scalar_utf8_req(col: &ColumnarValue, name: &str) -> DFResult<String> {
    net_scalar_utf8_opt(col, name)?
        .ok_or_else(|| DataFusionError::Execution(format!("net arg {name} must not be NULL")))
}

fn net_scalar_utf8_opt(col: &ColumnarValue, name: &str) -> DFResult<Option<String>> {
    match col {
        ColumnarValue::Scalar(sv) => match sv {
            ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) | ScalarValue::Utf8View(v) => Ok(v.clone()),
            other => Err(DataFusionError::Execution(format!("net arg {name} expected TEXT, got {other:?}"))),
        },
        ColumnarValue::Array(arr) => {
            use datafusion::arrow::array::Array;
            let arr = arr.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Execution(format!("net arg {name} expected TEXT array")))?;
            if arr.is_null(0) { Ok(None) } else { Ok(Some(arr.value(0).to_string())) }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rewrite_net_http_get() {
        assert_eq!(rewrite_net_schema_functions("SELECT net.http_get('https://example.com')"),
            "SELECT net_http_get('https://example.com')");
    }
    #[test]
    fn rewrite_net_http_post() {
        assert_eq!(rewrite_net_schema_functions(r#"SELECT net.http_post('https://example.com', '{"k":1}')"#),
            r#"SELECT net_http_post('https://example.com', '{"k":1}')"#);
    }
    #[test]
    fn rewrite_net_case_insensitive() {
        assert_eq!(rewrite_net_schema_functions("SELECT NET.HTTP_GET('https://example.com')"),
            "SELECT net_http_get('https://example.com')");
    }
    #[test]
    fn rewrite_net_leaves_non_matching() {
        assert_eq!(rewrite_net_schema_functions("SELECT my_net.http_get('https://example.com')"),
            "SELECT my_net.http_get('https://example.com')");
    }
}
