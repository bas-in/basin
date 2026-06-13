//! Minimal HTTP transport shared by every sub-client.
//!
//! Wraps a [`reqwest::Client`] with:
//! - Bearer-token injection (an auto-refreshed session token beats the static
//!   key)
//! - error-envelope decoding into [`BasinError`]
//! - URL building with ordered query-pair lists (`Vec<(String, String)>`),
//!   matching Basin's repeatable-key filter grammar.

use std::sync::Arc;

use reqwest::{Client, Method, Response};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::{decode_error_envelope, BasinError};

/// An async callable that yields the current Bearer token, if any.
///
/// The auth client wires one of these into the transport so every request uses
/// the freshest (possibly just-refreshed) session token.
pub(crate) type TokenGetter = Arc<
    dyn Fn() -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Option<String>, BasinError>> + Send>,
        > + Send
        + Sync,
>;

/// Shared HTTP transport.
#[derive(Clone)]
pub(crate) struct Transport {
    pub(crate) base_url: String,
    pub(crate) key: Option<String>,
    client: Client,
    token_getter: Arc<std::sync::RwLock<Option<TokenGetter>>>,
}

impl Transport {
    pub(crate) fn new(base_url: String, key: Option<String>, client: Client) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            key,
            client,
            token_getter: Arc::new(std::sync::RwLock::new(None)),
        }
    }

    /// Install the token getter (called once by the client after construction).
    pub(crate) fn set_token_getter(&self, getter: TokenGetter) {
        if let Ok(mut guard) = self.token_getter.write() {
            *guard = Some(getter);
        }
    }

    /// Resolve the Bearer token: session token if available, else the static
    /// key.
    pub(crate) async fn bearer(&self) -> Result<Option<String>, BasinError> {
        let getter = self
            .token_getter
            .read()
            .ok()
            .and_then(|g| g.clone());
        if let Some(getter) = getter {
            if let Some(tok) = getter().await? {
                return Ok(Some(tok));
            }
        }
        Ok(self.key.clone())
    }

    /// Build a full URL from a path and an ordered list of query pairs.
    pub(crate) fn build_url(&self, path: &str, query: &[(String, String)]) -> String {
        let mut url = format!("{}{}", self.base_url, path);
        if !query.is_empty() {
            url.push('?');
            let encoded: Vec<String> = query
                .iter()
                .map(|(k, v)| format!("{}={}", urlencode(k), urlencode(v)))
                .collect();
            url.push_str(&encoded.join("&"));
        }
        url
    }

    /// Issue a request and return the raw [`Response`], decoding non-2xx
    /// responses into [`BasinError::Api`].
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn request(
        &self,
        method: Method,
        path: &str,
        query: &[(String, String)],
        json_body: Option<&Value>,
        raw_body: Option<Vec<u8>>,
        extra_headers: &[(String, String)],
        auth: bool,
    ) -> Result<Response, BasinError> {
        let url = self.build_url(path, query);
        let mut req = self.client.request(method, &url);

        if auth {
            if let Some(tok) = self.bearer().await? {
                req = req.bearer_auth(tok);
            }
        }
        for (k, v) in extra_headers {
            req = req.header(k.as_str(), v.as_str());
        }
        if let Some(raw) = raw_body {
            req = req.body(raw);
        } else if let Some(body) = json_body {
            req = req
                .header("content-type", "application/json")
                .body(serde_json::to_vec(body).map_err(|e| BasinError::Decode(e.to_string()))?);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| BasinError::Network(e.to_string()))?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let text = resp.text().await.unwrap_or_default();
            return Err(BasinError::Api(decode_error_envelope(status, &text)));
        }
        Ok(resp)
    }

    /// Issue a request and decode the JSON body into `T`. A 204 / empty body
    /// decodes as `T::default`-equivalent only when `T` permits it; prefer
    /// [`request_json_opt`](Self::request_json_opt) when an empty body is valid.
    pub(crate) async fn request_json<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        query: &[(String, String)],
        json_body: Option<&Value>,
        auth: bool,
    ) -> Result<T, BasinError> {
        let resp = self
            .request(method, path, query, json_body, None, &[], auth)
            .await?;
        let text = resp.text().await.map_err(BasinError::from)?;
        serde_json::from_str(&text).map_err(|e| BasinError::Decode(e.to_string()))
    }

    /// Like [`request_json`](Self::request_json) but tolerates a 204 / empty
    /// body, returning `None` in that case.
    pub(crate) async fn request_json_opt<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        query: &[(String, String)],
        json_body: Option<&Value>,
        auth: bool,
    ) -> Result<Option<T>, BasinError> {
        let resp = self
            .request(method, path, query, json_body, None, &[], auth)
            .await?;
        if resp.status().as_u16() == 204 {
            return Ok(None);
        }
        let text = resp.text().await.map_err(BasinError::from)?;
        if text.is_empty() {
            return Ok(None);
        }
        serde_json::from_str(&text)
            .map(Some)
            .map_err(|e| BasinError::Decode(e.to_string()))
    }

    /// Issue a request and ignore the body entirely (used for `{ ok }` /
    /// 204 responses where the caller does not need the payload).
    pub(crate) async fn request_discard(
        &self,
        method: Method,
        path: &str,
        query: &[(String, String)],
        json_body: Option<&Value>,
        auth: bool,
    ) -> Result<(), BasinError> {
        self.request(method, path, query, json_body, None, &[], auth)
            .await?;
        Ok(())
    }
}

/// Percent-encode a query component. Encodes everything that is not an
/// unreserved character so Basin's `op.value` filter grammar survives intact.
pub(crate) fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Percent-encode each segment of an object path, preserving `/` separators.
pub(crate) fn encode_object_path(path: &str) -> String {
    path.split('/')
        .map(urlencode)
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_url_encodes_filter_grammar() {
        let t = Transport::new("http://h/".into(), None, Client::new());
        let q = vec![
            ("select".to_string(), "id,total".to_string()),
            ("status".to_string(), "eq.paid".to_string()),
        ];
        let url = t.build_url("/rest/v1/orders", &q);
        assert_eq!(
            url,
            "http://h/rest/v1/orders?select=id%2Ctotal&status=eq.paid"
        );
    }

    #[test]
    fn base_url_trailing_slash_trimmed() {
        let t = Transport::new("http://h///".into(), None, Client::new());
        assert_eq!(t.base_url, "http://h");
    }

    #[test]
    fn object_path_preserves_slashes() {
        assert_eq!(encode_object_path("a b/c.png"), "a%20b/c.png");
    }
}
