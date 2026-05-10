//! Shared test fixtures: a tiny axum HTTP server that lets each test
//! script its own response sequence and capture the requests it received.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Router;

/// Captured request for test assertions.
#[derive(Clone, Debug)]
pub struct CapturedRequest {
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

/// Behaviour script. Each call to the handler pops the front of
/// `responses`; if empty, the trailing `default` is used. Captured
/// requests are appended to `requests` in arrival order.
#[derive(Default)]
pub struct ScriptedServerState {
    pub responses: std::collections::VecDeque<u16>,
    pub default: u16,
    pub requests: Vec<CapturedRequest>,
}

#[derive(Clone)]
pub struct ServerHandle {
    pub addr: SocketAddr,
    pub state: Arc<Mutex<ScriptedServerState>>,
}

impl ServerHandle {
    pub fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{}", self.addr.port(), path)
    }

    pub fn requests(&self) -> Vec<CapturedRequest> {
        self.state.lock().unwrap().requests.clone()
    }

    pub fn push_response(&self, status: u16) {
        self.state.lock().unwrap().responses.push_back(status);
    }
}

async fn handler(
    State(state): State<Arc<Mutex<ScriptedServerState>>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let captured = CapturedRequest {
        headers: headers
            .iter()
            .filter_map(|(k, v)| {
                v.to_str()
                    .ok()
                    .map(|vs| (k.as_str().to_lowercase(), vs.to_string()))
            })
            .collect(),
        body: body.to_vec(),
    };
    let status = {
        let mut s = state.lock().unwrap();
        s.requests.push(captured);
        s.responses.pop_front().unwrap_or(s.default)
    };
    StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
}

/// Spawn a server with `default` as the response for every request the
/// scripted queue doesn't cover. Returns a [`ServerHandle`].
pub async fn spawn_server(default_status: u16) -> ServerHandle {
    let state = Arc::new(Mutex::new(ScriptedServerState {
        responses: std::collections::VecDeque::new(),
        default: default_status,
        requests: Vec::new(),
    }));
    let app = Router::new().fallback(handler).with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app.into_make_service()).await;
    });
    // Sanity wait so the server is bound before the test issues requests.
    tokio::task::yield_now().await;
    ServerHandle { addr, state }
}

/// Wait until `predicate` returns true, polling every 5ms up to `timeout`.
pub async fn wait_for<F: FnMut() -> bool>(mut predicate: F, timeout: std::time::Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if predicate() {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    predicate()
}

/// Generic helper: turn a request capture's body into a JSON value.
pub fn body_as_json(req: &CapturedRequest) -> serde_json::Value {
    serde_json::from_slice(&req.body).expect("body is valid JSON")
}
