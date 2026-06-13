//! Functions client — Basin's two function-invocation surfaces.
//!
//! Routes verified:
//! - `ANY /fn/v1/:name`          server.rs:290, handler `fn_handler.rs`
//!   (`any_fn_handler`) — HTTP-handler-shape Wasm functions; the guest's
//!   response (status, headers, body) is proxied back verbatim.
//! - `POST /rest/v1/rpc/:fn_name` server.rs:288, handler `rpc.rs` (`post_rpc`)
//!   — catalog SQL / Wasm UDFs. Body is a JSON object of named args; scalar
//!   results unwrap to the bare value, table results return an array of rows.
//!
//! Both are bearer-authenticated like every other route.

use std::collections::HashMap;

use reqwest::Method;
use serde_json::Value;

use crate::error::{ApiError, BasinError};
use crate::http::Transport;
use crate::types::FunctionInvokeResult;

/// Functions client. Obtain via [`Client::functions`](crate::Client::functions).
#[derive(Clone)]
pub struct FunctionsClient {
    transport: Transport,
}

impl FunctionsClient {
    pub(crate) fn new(transport: Transport) -> Self {
        Self { transport }
    }

    /// `POST /rest/v1/rpc/:fn_name` — call a catalog UDF.
    ///
    /// `args` is a JSON object of named arguments (missing declared args become
    /// NULL). Returns the bare scalar for single-value results, or an array of
    /// rows for `RETURNS TABLE`.
    pub async fn rpc(&self, fn_name: &str, args: Option<&Value>) -> Result<Value, BasinError> {
        let empty = Value::Object(serde_json::Map::new());
        let body = args.unwrap_or(&empty);
        let path = format!("/rest/v1/rpc/{}", crate::http::urlencode(fn_name));
        Ok(self
            .transport
            .request_json_opt(Method::POST, &path, &[], Some(body), true)
            .await?
            .unwrap_or(Value::Null))
    }

    /// `ANY /fn/v1/:name` — invoke an HTTP-handler function.
    ///
    /// The function's response is proxied verbatim, so non-2xx statuses are
    /// **not** automatically errors — unless the body is Basin's own error
    /// envelope (auth 401, unknown function 404, invocation failure 500), in
    /// which case a [`BasinError::Api`] is returned.
    ///
    /// `body` may be `None`, a raw byte payload, or a JSON value (set the
    /// matching `content-type` via `headers` when sending raw bytes).
    pub async fn invoke(
        &self,
        name: &str,
        method: Method,
        body: Option<InvokeBody>,
        headers: &[(String, String)],
    ) -> Result<FunctionInvokeResult, BasinError> {
        let path = format!("/fn/v1/{}", crate::http::urlencode(name));

        // The transport adds `content-type: application/json` for a JSON body
        // automatically; for a raw body the caller sets it via `headers`.
        let (json_body, raw_body): (Option<Value>, Option<Vec<u8>>) = match body {
            None => (None, None),
            Some(InvokeBody::Json(v)) => (Some(v), None),
            Some(InvokeBody::Raw(bytes)) => (None, Some(bytes)),
        };

        let resp = self
            .transport
            .request(
                method,
                &path,
                &[],
                json_body.as_ref(),
                raw_body,
                headers,
                true,
            )
            .await;

        // `request` already turned a non-2xx envelope into Err(Api). But a
        // function may legitimately return a non-2xx status with a non-envelope
        // body; the transport surfaces those as Err too, so we re-classify.
        match resp {
            Ok(response) => Self::build_result(response).await,
            Err(BasinError::Api(e)) => {
                // If the envelope was a real Basin error code, propagate it;
                // otherwise reconstruct the proxied result from the error parts.
                if e.code.starts_with("E_") {
                    Err(BasinError::Api(e))
                } else {
                    Ok(reconstruct_from_api_error(e))
                }
            }
            Err(other) => Err(other),
        }
    }

    async fn build_result(response: reqwest::Response) -> Result<FunctionInvokeResult, BasinError> {
        let status = response.status().as_u16();
        let mut headers = HashMap::new();
        for (k, v) in response.headers().iter() {
            if let Ok(val) = v.to_str() {
                headers.insert(k.as_str().to_string(), val.to_string());
            }
        }
        let ct = headers
            .get("content-type")
            .cloned()
            .unwrap_or_default();
        let text = response.text().await.map_err(BasinError::from)?;
        let data = if ct.contains("json") && !text.is_empty() {
            serde_json::from_str(&text).unwrap_or(Value::String(text))
        } else {
            Value::String(text)
        };
        Ok(FunctionInvokeResult {
            status,
            headers,
            data,
        })
    }
}

/// Body payload for [`FunctionsClient::invoke`].
#[derive(Debug, Clone)]
pub enum InvokeBody {
    /// A JSON value (serialised with `content-type: application/json`).
    Json(Value),
    /// A raw byte payload (set `content-type` yourself via `headers`).
    Raw(Vec<u8>),
}

fn reconstruct_from_api_error(e: ApiError) -> FunctionInvokeResult {
    FunctionInvokeResult {
        status: e.status,
        headers: HashMap::new(),
        data: Value::String(e.message),
    }
}
