//! client — thin HTTP wrapper around the v1 + admin/v1 + auth/v1 API.
//!
//! reqwest (blocking, rustls) does the transport; serde shapes the
//! envelopes. The wrapper centralises bearer-header injection,
//! content-type negotiation, error mapping (the cloud's typed-code
//! envelope decodes into [`ApiError`]), and the `{data,error}` success
//! unwrap.

use std::time::Duration;

use reqwest::blocking::Client as HttpClient;
use reqwest::Method;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::{msg, ApiError, CliResult};

/// version is stamped at build time via the `BASIN_VERSION` env var
/// (set by cargo-dist / CI); uninjected it falls back to the Cargo
/// package version, then "dev".
pub fn version() -> &'static str {
    match option_env!("BASIN_VERSION") {
        Some(v) if !v.is_empty() => v,
        _ => env!("CARGO_PKG_VERSION"),
    }
}

/// Client is a single base-URL + bearer-token combo. Long-lived per CLI
/// invocation; safe to reuse for streaming reads.
pub struct Client {
    pub base_url: String,
    pub token: String,
    http: HttpClient,
    user_agent: String,
}

/// DEFAULT_TIMEOUT mirrors Go's 30s default request timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

impl Client {
    /// new binds the base URL and bearer token. Caller is responsible
    /// for ensuring base URL is non-empty.
    pub fn new(base: &str, token: &str) -> Client {
        let http = HttpClient::builder()
            .timeout(DEFAULT_TIMEOUT)
            .build()
            .expect("build reqwest client");
        Client {
            base_url: base.trim_end_matches('/').to_string(),
            token: token.to_string(),
            http,
            user_agent: format!("basin-cli/{}", version()),
        }
    }

    /// do_json executes an HTTP request and decodes the JSON response
    /// body into `T`. Returns [`ApiError`] on non-2xx.
    pub fn do_json<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> CliResult<T> {
        let text = self.send(method, path, body, None)?;
        unwrap_envelope(&text)
    }

    /// do_json_timeout is do_json with a per-request timeout override
    /// (used by short-fuse calls like `/v1/version`).
    pub fn do_json_timeout<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        timeout: Duration,
    ) -> CliResult<T> {
        let text = self.send(method, path, body, Some(timeout))?;
        unwrap_envelope(&text)
    }

    /// do_noout executes a request and discards the response body
    /// (the `out == nil` path in Go's `do`).
    pub fn do_noout(&self, method: Method, path: &str, body: Option<Value>) -> CliResult<()> {
        self.send(method, path, body, None)?;
        Ok(())
    }

    /// send performs the request, returning the response body string on
    /// 2xx or an [`ApiError`] on non-2xx.
    fn send(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        timeout: Option<Duration>,
    ) -> CliResult<String> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self
            .http
            .request(method, &url)
            .header("Accept", "application/json")
            .header("User-Agent", &self.user_agent);
        if !self.token.is_empty() {
            req = req.bearer_auth(&self.token);
        }
        if let Some(b) = body {
            req = req.header("Content-Type", "application/json").json(&b);
        }
        if let Some(t) = timeout {
            req = req.timeout(t);
        }
        let resp = req.send()?;
        let status = resp.status();
        let text = resp.text()?;
        if status.is_success() {
            return Ok(text);
        }
        Err(Box::new(parse_api_error(status.as_u16(), &text)))
    }
}

/// unwrap_envelope decodes `text` into `T`, tolerating the cloud's
/// `{"data":{…},"error":null}` wrapper. We try the wrapped shape first
/// and fall back to a flat decode for forward/back compat (test mocks,
/// public bootstrap routes). A null/absent `data` falls through to flat
/// so a `{"data":null}` 204-shape doesn't blank out the caller's struct.
pub fn unwrap_envelope<T: DeserializeOwned>(text: &str) -> CliResult<T> {
    let raw: Value =
        serde_json::from_str(text).map_err(|e| msg(format!("decode response: {e}")))?;
    if let Some(data) = raw.get("data") {
        if !data.is_null() {
            return Ok(serde_json::from_value(data.clone())?);
        }
    }
    Ok(serde_json::from_value(raw)?)
}

/// parse_api_error builds an [`ApiError`] from a non-2xx response body,
/// trying the nested `{"error":{code,message}}` shape first, then a flat
/// `{code,message}`, then the raw trimmed body as the message.
pub fn parse_api_error(status: u16, body: &str) -> ApiError {
    let mut ae = ApiError {
        http_status: status,
        code: String::new(),
        message: String::new(),
    };
    if body.is_empty() {
        return ae;
    }
    if let Ok(v) = serde_json::from_str::<Value>(body) {
        if let Some(nested) = v.get("error") {
            let code = nested.get("code").and_then(|x| x.as_str()).unwrap_or("");
            let m = nested.get("message").and_then(|x| x.as_str()).unwrap_or("");
            if !code.is_empty() || !m.is_empty() {
                ae.code = code.to_string();
                ae.message = m.to_string();
                return ae;
            }
        }
        let code = v.get("code").and_then(|x| x.as_str()).unwrap_or("");
        let m = v.get("message").and_then(|x| x.as_str()).unwrap_or("");
        if !code.is_empty() || !m.is_empty() {
            ae.code = code.to_string();
            ae.message = m.to_string();
            return ae;
        }
    }
    ae.message = body.trim().to_string();
    ae
}

/// query_string builds a "?k=v&k=v" suffix, skipping empty values so
/// callers can pass a sparse list. Keys are sorted for stable output.
pub fn query_string(kv: &[(&str, &str)]) -> String {
    let mut pairs: Vec<(&str, &str)> = kv.iter().filter(|(_, v)| !v.is_empty()).copied().collect();
    if pairs.is_empty() {
        return String::new();
    }
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    let enc: Vec<String> = pairs
        .iter()
        .map(|(k, v)| format!("{}={}", urlencode(k), urlencode(v)))
        .collect();
    format!("?{}", enc.join("&"))
}

/// urlencode is a minimal application/x-www-form-urlencoded escaper for
/// query values (RFC 3986 unreserved set stays literal).
pub fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::as_api_error;
    use crate::testutil::{Req, Resp, TestServer};
    use crate::types::QueryResult;
    use serde::Deserialize;
    use std::error::Error as StdError;

    #[derive(Debug, Deserialize)]
    struct TestPayload {
        foo: String,
    }

    #[test]
    fn unwraps_data_envelope() {
        let q: QueryResult = unwrap_envelope(
            r#"{"data":{"columns":["x"],"rows":[[1]],"elapsed_ms":3},"error":null}"#,
        )
        .unwrap();
        assert_eq!(q.columns, vec!["x"]);
        assert_eq!(q.elapsed_ms, 3);
    }

    #[test]
    fn falls_back_to_flat() {
        let q: QueryResult = unwrap_envelope(r#"{"columns":["y"],"elapsed_ms":1}"#).unwrap();
        assert_eq!(q.columns, vec!["y"]);
    }

    #[test]
    fn null_data_falls_through_to_flat() {
        // A `{"data":null}` 204-shape must not blank a flat decode target.
        let q: QueryResult = unwrap_envelope(r#"{"data":null}"#).unwrap();
        assert!(q.columns.is_empty());
    }

    #[test]
    fn nested_error_envelope() {
        let ae = parse_api_error(409, r#"{"error":{"code":"conflict","message":"taken"}}"#);
        assert_eq!(ae.code, "conflict");
        assert_eq!(ae.message, "taken");
        assert_eq!(ae.http_status, 409);
    }

    #[test]
    fn flat_error_then_raw_fallback() {
        let flat = parse_api_error(400, r#"{"code":"bad","message":"nope"}"#);
        assert_eq!(flat.code, "bad");
        let raw = parse_api_error(500, "boom");
        assert_eq!(raw.message, "boom");
        assert!(raw.code.is_empty());
    }

    #[test]
    fn query_string_sorts_and_skips_empty() {
        assert_eq!(query_string(&[]), "");
        assert_eq!(
            query_string(&[("b", "2"), ("a", "1"), ("c", "")]),
            "?a=1&b=2"
        );
        assert_eq!(query_string(&[("q", "a b")]), "?q=a+b");
    }

    // ── do_json: envelope unwrap / flat fallback / error mapping ────────────

    #[test]
    fn do_json_unwraps_data_envelope() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"data":{"foo":"bar"},"error":null}"#)
        });
        let c = Client::new(&srv.url, "tok");
        let out: TestPayload = c.do_json(Method::GET, "/v1/probe", None).unwrap();
        assert_eq!(out.foo, "bar");
    }

    #[test]
    fn do_json_flat_fallback() {
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"foo":"baz"}"#));
        let c = Client::new(&srv.url, "tok");
        let out: TestPayload = c.do_json(Method::GET, "/v1/probe", None).unwrap();
        assert_eq!(out.foo, "baz");
    }

    #[test]
    fn do_json_maps_error_envelope() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(409, r#"{"error":{"code":"conflict","message":"taken"}}"#)
        });
        let c = Client::new(&srv.url, "tok");
        let err = c
            .do_json::<TestPayload>(Method::POST, "/v1/probe", None)
            .unwrap_err();
        let ae = as_api_error(err.as_ref()).expect("ApiError downcast");
        assert_eq!(ae.code, "conflict");
        assert_eq!(ae.message, "taken");
        assert_eq!(ae.http_status, 409);
    }

    // ── do_json_timeout: timeout fires ──────────────────────────────────────

    #[test]
    fn do_json_timeout_fires() {
        // Stub sleeps 5s before responding; client's 200ms per-request
        // timeout must trip before the server writes back.
        let srv = TestServer::start(|_req: &Req| {
            std::thread::sleep(Duration::from_secs(5));
            Resp::ok(r#"{"foo":"late"}"#)
        });
        let c = Client::new(&srv.url, "tok");
        let err = c
            .do_json_timeout::<TestPayload>(
                Method::GET,
                "/v1/slow",
                None,
                Duration::from_millis(200),
            )
            .unwrap_err();
        // reqwest's top-level message is "error sending request"; the
        // timeout flavour lives on the source chain, so walk it.
        let mut chain = String::new();
        chain.push_str(&err.to_string());
        let mut cur: Option<&(dyn StdError + 'static)> = err.source();
        while let Some(s) = cur {
            chain.push_str(" :: ");
            chain.push_str(&s.to_string());
            cur = s.source();
        }
        let chain = chain.to_lowercase();
        assert!(
            chain.contains("timeout")
                || chain.contains("elapsed")
                || chain.contains("timed out"),
            "expected timeout-ish error, got chain: {chain}"
        );
    }

    // ── do_noout: 2xx empty Ok / 5xx error mapping ──────────────────────────

    #[test]
    fn do_noout_2xx_empty_body_is_ok() {
        let srv = TestServer::start(|_req: &Req| Resp::status(204, String::new()));
        let c = Client::new(&srv.url, "tok");
        c.do_noout(Method::DELETE, "/v1/thing/1", None).unwrap();
    }

    #[test]
    fn do_noout_5xx_propagates_api_error() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(500, r#"{"error":{"code":"internal","message":"boom"}}"#)
        });
        let c = Client::new(&srv.url, "tok");
        let err = c
            .do_noout(Method::POST, "/v1/thing", None)
            .unwrap_err();
        let ae = as_api_error(err.as_ref()).expect("ApiError downcast");
        assert_eq!(ae.code, "internal");
        assert_eq!(ae.message, "boom");
        assert_eq!(ae.http_status, 500);
    }
}
