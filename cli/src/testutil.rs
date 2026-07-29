//! testutil — a minimal in-process HTTP stub (the Rust analogue of Go's
//! httptest.Server) plus a config-dir sandbox. Test-only.
//!
//! TestServer binds to 127.0.0.1:0, hands back its base URL, and serves
//! each request with a caller-supplied handler. It speaks just enough
//! HTTP/1.1 (request line + Content-Length body + `Connection: close`
//! response) to satisfy reqwest's blocking client.

#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

/// Captured request passed to a [`TestServer`] handler.
pub struct Req {
    pub method: String,
    pub path: String,
    pub body: String,
}

/// Canned response a handler returns: HTTP status + JSON body string.
pub struct Resp {
    pub status: u16,
    pub body: String,
}

impl Resp {
    pub fn ok(body: impl Into<String>) -> Resp {
        Resp {
            status: 200,
            body: body.into(),
        }
    }
    pub fn status(status: u16, body: impl Into<String>) -> Resp {
        Resp {
            status,
            body: body.into(),
        }
    }
}

/// TestServer keeps the listener thread alive for the test's duration.
pub struct TestServer {
    pub url: String,
}

impl TestServer {
    /// start spins a stub server whose handler runs for every request.
    /// The handler must be `Send + Sync` since it runs on the accept
    /// thread.
    pub fn start<H>(handler: H) -> TestServer
    where
        H: Fn(&Req) -> Resp + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub server");
        let addr = listener.local_addr().expect("local addr");
        let url = format!("http://{addr}");
        let handler = Arc::new(handler);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let h = Arc::clone(&handler);
                // Handle each connection inline; tests are sequential and
                // short, so no per-connection thread is needed.
                let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));
                let mut request_line = String::new();
                if reader.read_line(&mut request_line).is_err() || request_line.is_empty() {
                    continue;
                }
                let mut parts = request_line.split_whitespace();
                let method = parts.next().unwrap_or("").to_string();
                let path = parts.next().unwrap_or("").to_string();
                // Read headers, tracking Content-Length.
                let mut content_length = 0usize;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).is_err() {
                        break;
                    }
                    let trimmed = line.trim_end();
                    if trimmed.is_empty() {
                        break;
                    }
                    if let Some(v) = trimmed.to_ascii_lowercase().strip_prefix("content-length:") {
                        content_length = v.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0u8; content_length];
                if content_length > 0 {
                    let _ = reader.read_exact(&mut body);
                }
                let req = Req {
                    method,
                    path,
                    body: String::from_utf8_lossy(&body).to_string(),
                };
                let resp = h(&req);
                let payload = format!(
                    "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    resp.status,
                    resp.body.len(),
                    resp.body
                );
                let _ = stream.write_all(payload.as_bytes());
                let _ = stream.flush();
            }
        });
        TestServer { url }
    }
}

/// with_temp_config_dir points $XDG_CONFIG_HOME at a fresh temp dir for
/// the duration of the returned guard, so config reads/writes don't
/// touch the developer's real ~/.config/basin. Returns the guard; drop
/// it (end of test) to restore the previous value.
///
/// XDG_CONFIG_HOME is a process-global env var, so parallel tests can
/// race on it (test A sets it to /tmp/A; test B sets it to /tmp/B
/// mid-write, and test A's rename target now lives under /tmp/B). The
/// guard holds a process-wide [`Mutex`] for its lifetime, serialising
/// every config-touching test. Drop releases the mutex.
pub struct ConfigDirGuard {
    prev: Option<std::ffi::OsString>,
    _dir: std::path::PathBuf,
    _lock: MutexGuard<'static, ()>,
}

impl Drop for ConfigDirGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => std::env::set_var("XDG_CONFIG_HOME", v),
            None => std::env::remove_var("XDG_CONFIG_HOME"),
        }
    }
}

fn config_dir_mutex() -> &'static Mutex<()> {
    static M: OnceLock<Mutex<()>> = OnceLock::new();
    M.get_or_init(|| Mutex::new(()))
}

pub fn with_temp_config_dir() -> ConfigDirGuard {
    // Take the lock BEFORE touching the env var so concurrent tests serialise
    // cleanly. A poisoned mutex still works here — we don't share state, just
    // a critical section, so recover from the poison.
    let lock = config_dir_mutex()
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let prev = std::env::var_os("XDG_CONFIG_HOME");
    let dir = std::env::temp_dir().join(format!("basin-test-{}", unique()));
    std::fs::create_dir_all(&dir).expect("create temp config dir");
    std::env::set_var("XDG_CONFIG_HOME", &dir);
    ConfigDirGuard {
        prev,
        _dir: dir,
        _lock: lock,
    }
}

fn unique() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
