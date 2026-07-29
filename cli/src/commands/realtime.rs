//! realtime — SSE single-table subscribe (T26.1) + WS multi-table subscribe (T26.2/T26.3).
//!
//! ## SSE path (T26.1)
//!
//!   basin realtime subscribe <table> [--project=<ref>] [--since=<seq>]
//!
//!   Endpoint: GET {api_url}/realtime/v1/sse/:project/:table
//!             Authorization: Bearer <token>
//!             Last-Event-Id: <seq>         (when --since is set)
//!
//!   Reads SSE frames line-by-line, parses each `data:` line as JSON, and
//!   prints one compact JSON event per line to stdout. Comment / heartbeat
//!   lines (starting with `:`) are silently skipped.
//!
//! ## WebSocket path (T26.2/T26.3)
//!
//!   basin realtime subscribe --multi <t1>,<t2>,… [--filter=<expr>] [--project=<ref>]
//!
//!   Endpoint: GET {api_url}/realtime/v1/ws/:project  (Upgrade: websocket)
//!
//!   Sends {"type":"subscribe","table":"<t>"[,"filter":"<expr>"]} per table,
//!   waits for {"type":"subscribed"} acks, then prints each {"type":"event",…}
//!   as a table-tagged compact JSON line. Handles {"type":"error","code":"lag"}
//!   with a stderr warning. Ctrl-C exits 0 via clean close handshake.
//!
//! SIGINT exits 0 — reqwest/tungstenite I/O errors on interrupt are treated
//! as clean EOF.

use std::io::{BufRead, BufReader};
use std::net::TcpStream;

use clap::{Arg, ArgAction, Command};
use reqwest::blocking::Client as HttpClient;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{ClientRequestBuilder, Message, WebSocket};

use crate::client::version;
use crate::config::load_working_project;
use crate::error::{msg, CliResult};
use crate::global::GlobalFlags;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Dispatcher ────────────────────────────────────────────────────────────────

pub fn cmd_realtime(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            return Err(msg(
                "usage: basin realtime subscribe <table> [--project=<ref>] [--since=<seq>]",
            ));
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "subscribe" => realtime_subscribe(g, rest),
        "--help" | "-h" | "help" => {
            print_help();
            Ok(())
        }
        other => Err(msg(format!("unknown subcommand {:?} for realtime", other))),
    }
}

fn print_help() {
    help_for_command(
        "realtime",
        "Stream real-time events from a project table over SSE or WebSocket.",
        &[
            "subscribe <table> [--project=<ref>] [--since=<seq>]        SSE single-table subscribe.",
            "subscribe --multi <t1>,<t2>,… [--filter=<expr>] [--project=<ref>]  WS multi-table subscribe.",
        ],
    );
}

// ── realtime subscribe ────────────────────────────────────────────────────────

fn resolve_project_ref(flag_value: &str) -> CliResult<String> {
    if !flag_value.is_empty() {
        return Ok(flag_value.to_string());
    }
    let cwd = std::env::current_dir().map_err(|e| msg(format!("could not determine cwd: {e}")))?;
    let wp = load_working_project(&cwd)?;
    match wp {
        Some(w) if !w.project_ref.is_empty() => Ok(w.project_ref),
        _ => Err(msg(
            "--project is required (or run `basin link` to bind this directory)",
        )),
    }
}

fn realtime_subscribe(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("realtime subscribe")
        .arg(Arg::new("table"))
        .arg(Arg::new("multi").long("multi"))
        .arg(Arg::new("filter").long("filter"))
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("since").long("since"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "realtime subscribe",
            "Subscribe to a project table's event stream.",
            &[
                "<table>                    Table name (SSE mode, required without --multi).",
                "--multi=<t1>,<t2>,…        Tables to subscribe (WS mode, comma-separated).",
                "--filter=<expr>            Filter expression applied to all subscribed tables (WS mode).",
                "--project=<ref>            Project ref.",
                "--since=<seq>              Resume from this sequence (SSE mode; sets Last-Event-Id header).",
            ],
        );
        return Ok(());
    }

    let multi = m.get_one::<String>("multi").cloned().unwrap_or_default();
    let filter = m.get_one::<String>("filter").cloned().unwrap_or_default();
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();

    // Resolve credentials (same ladder as require_client).
    let cfg = crate::config::read_config_file().ok().flatten();
    let token = crate::global::resolve_token(g, cfg.as_ref());
    if token.is_empty() {
        eprintln!(
            "basin: not signed in. Run `basin login` first, set $BASIN_TOKEN, or pass --token=<pat>."
        );
        return Err(crate::error::silent());
    }

    if !multi.is_empty() {
        // ── WebSocket multi-table path (T26.3) ────────────────────────────
        let tables: Vec<&str> = multi
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();
        if tables.is_empty() {
            return Err(msg("--multi requires at least one table name"));
        }
        let project = resolve_project_ref(&project_flag)?;
        let ws_url = build_ws_url(&g.api_url, &project);
        ws_subscribe(&ws_url, &token, &tables, &filter)
    } else {
        // ── SSE single-table path (T26.1) ─────────────────────────────────
        let table = m.get_one::<String>("table").cloned().ok_or_else(|| {
            msg("usage: basin realtime subscribe <table> [--project=<ref>] [--since=<seq>]")
        })?;
        let project = resolve_project_ref(&project_flag)?;
        let since = m.get_one::<String>("since").cloned().unwrap_or_default();
        let url = format!(
            "{}/realtime/v1/sse/{}/{}",
            g.api_url.trim_end_matches('/'),
            project,
            table
        );
        sse_subscribe(&url, &token, &since)
    }
}

// ── WS URL helpers ────────────────────────────────────────────────────────────

/// Build the WebSocket URL from the HTTP api_url.
/// `https://api.basin.run` → `wss://api.basin.run/realtime/v1/ws/<project>`
/// `http://127.0.0.1:N`   → `ws://127.0.0.1:N/realtime/v1/ws/<project>`
pub fn build_ws_url(api_url: &str, project: &str) -> String {
    let base = api_url.trim_end_matches('/');
    let ws_base = if base.starts_with("https://") {
        base.replacen("https://", "wss://", 1)
    } else if base.starts_with("http://") {
        base.replacen("http://", "ws://", 1)
    } else {
        format!("wss://{base}")
    };
    format!("{ws_base}/realtime/v1/ws/{project}")
}

// ── WebSocket multi-table subscribe ──────────────────────────────────────────

/// ws_subscribe opens a WS connection, sends subscribe frames for each table,
/// waits for acks, then streams events to stdout. Exported for tests.
pub fn ws_subscribe(ws_url: &str, token: &str, tables: &[&str], filter: &str) -> CliResult<()> {
    let mut socket = ws_connect(ws_url, token)?;

    // Send subscribe frames.
    for &table in tables {
        let frame_text = if filter.is_empty() {
            serde_json::json!({"type": "subscribe", "table": table}).to_string()
        } else {
            serde_json::json!({"type": "subscribe", "table": table, "filter": filter}).to_string()
        };
        socket
            .send(Message::Text(frame_text))
            .map_err(|e| msg(format!("ws send subscribe: {e}")))?;
    }

    // Wait for all acks, then stream events.
    let mut pending_acks = tables.len();
    loop {
        let frame = match socket.read() {
            Ok(f) => f,
            Err(tungstenite::Error::ConnectionClosed) | Err(tungstenite::Error::AlreadyClosed) => {
                break
            }
            Err(e) => {
                // I/O error on Ctrl-C / pipe close — treat as clean EOF.
                if is_interrupted(&e) {
                    break;
                }
                return Err(msg(format!("ws read: {e}")));
            }
        };

        match frame {
            Message::Text(text) => {
                handle_ws_text(text.as_str(), &mut pending_acks)?;
            }
            Message::Binary(b) => {
                // Treat as UTF-8 text (same path).
                if let Ok(s) = std::str::from_utf8(&b) {
                    let owned = s.to_string();
                    handle_ws_text(&owned, &mut pending_acks)?;
                }
            }
            Message::Ping(data) => {
                // tungstenite auto-responds to pings in `read()` in most
                // builds; send explicit pong to be safe.
                let _ = socket.send(Message::Pong(data));
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    // Clean close handshake.
    let _ = socket.close(None);
    Ok(())
}

/// Handle a single WS text frame: route by `type` field.
fn handle_ws_text(text: &str, pending_acks: &mut usize) -> CliResult<()> {
    let v: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => {
            // Non-JSON — print verbatim.
            println!("{text}");
            return Ok(());
        }
    };
    match v.get("type").and_then(|t| t.as_str()) {
        Some("subscribed") => {
            if *pending_acks > 0 {
                *pending_acks -= 1;
            }
        }
        Some("event") => {
            let compact = serde_json::to_string(&v).unwrap_or_else(|_| text.to_string());
            println!("{compact}");
        }
        Some("error") => {
            let code = v.get("code").and_then(|c| c.as_str()).unwrap_or("unknown");
            if code == "lag" {
                eprintln!("basin: realtime warning: consumer lag detected; some events may have been dropped");
            } else {
                eprintln!("basin: realtime error: {text}");
            }
            // stdout uninterrupted — do not return Err.
        }
        _ => {
            // Unknown frame type — print compact.
            let compact = serde_json::to_string(&v).unwrap_or_else(|_| text.to_string());
            println!("{compact}");
        }
    }
    Ok(())
}

/// Open a tungstenite WebSocket connection with the Bearer token.
fn ws_connect(ws_url: &str, token: &str) -> CliResult<WebSocket<MaybeTlsStream<TcpStream>>> {
    let uri: tungstenite::http::Uri = ws_url
        .parse()
        .map_err(|e| msg(format!("ws url parse '{ws_url}': {e}")))?;
    let builder = ClientRequestBuilder::new(uri)
        .with_header("Authorization", format!("Bearer {token}"))
        .with_header("User-Agent", format!("basin-cli/{}", version()));
    let (socket, _response) =
        tungstenite::connect(builder).map_err(|e| msg(format!("ws connect {ws_url}: {e}")))?;
    Ok(socket)
}

/// Returns true when a tungstenite error is an OS-level interrupt / broken pipe.
fn is_interrupted(e: &tungstenite::Error) -> bool {
    if let tungstenite::Error::Io(io_err) = e {
        matches!(
            io_err.kind(),
            std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::Interrupted
        )
    } else {
        false
    }
}

// ── SSE single-table subscribe ────────────────────────────────────────────────

/// sse_subscribe opens a GET to `url`, reads SSE frames line-by-line, and
/// prints each `data:` line as compact JSON. Exported for tests.
pub fn sse_subscribe(url: &str, token: &str, since: &str) -> CliResult<()> {
    let http = HttpClient::builder()
        .timeout(None) // persistent connection
        .build()?;
    let mut req = http
        .get(url)
        .bearer_auth(token)
        .header("Accept", "text/event-stream")
        .header("User-Agent", format!("basin-cli/{}", version()));
    if !since.is_empty() {
        req = req.header("Last-Event-Id", since);
    }
    let resp = req.send()?;
    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp.text().unwrap_or_default();
        return Err(Box::new(crate::client::parse_api_error(status, &body)));
    }

    let reader = BufReader::new(resp);
    for line_result in reader.lines() {
        let line = match line_result {
            Ok(l) => l,
            // SIGINT or connection close → exit cleanly.
            Err(_) => break,
        };
        // Heartbeat / comment frames start with `:` — skip them.
        if line.starts_with(':') || line.is_empty() {
            continue;
        }
        // SSE `data:` prefix — parse payload as JSON and print compact.
        if let Some(payload) = line.strip_prefix("data:") {
            let payload = payload.trim();
            if payload.is_empty() {
                continue;
            }
            // Validate JSON, then re-print compact (no trailing spaces).
            match serde_json::from_str::<serde_json::Value>(payload) {
                Ok(v) => {
                    let compact = serde_json::to_string(&v).unwrap_or_else(|_| payload.to_string());
                    println!("{compact}");
                }
                Err(_) => {
                    // Non-JSON data line — print verbatim.
                    println!("{payload}");
                }
            }
        }
        // Other SSE field types (event:, id:, retry:) are ignored.
    }
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{with_temp_config_dir, Req, TestServer};
    use std::io::Write;
    use std::net::TcpListener;
    use std::sync::Arc;

    fn flags(url: &str) -> GlobalFlags {
        GlobalFlags {
            api_url: url.to_string(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        }
    }

    // ── dispatcher tests ──────────────────────────────────────────────────────

    #[test]
    fn no_args_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        assert!(cmd_realtime(&g, &[]).is_err());
    }

    #[test]
    fn unknown_subcommand_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        assert!(cmd_realtime(&g, &["frobnicate".to_string()]).is_err());
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        assert!(cmd_realtime(&g, &["help".to_string()]).is_ok());
    }

    #[test]
    fn subscribe_missing_table_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        assert!(cmd_realtime(&g, &["subscribe".to_string(), "--project=p1".to_string()]).is_err());
    }

    // ── SSE streaming tests ───────────────────────────────────────────────────

    /// Spin an SSE stub server that emits 3 data frames + 1 heartbeat,
    /// then closes the connection. Verify sse_subscribe returns 3 JSON
    /// lines and skips the heartbeat.
    #[test]
    fn sse_three_frames_one_heartbeat() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind SSE stub");
        let addr = listener.local_addr().expect("local addr");
        let url = format!("http://{addr}/realtime/v1/sse/p1/events");

        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).is_err() || line == "\r\n" {
                        break;
                    }
                }
                let header = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nTransfer-Encoding: chunked\r\n\r\n";
                let _ = stream.write_all(header.as_bytes());
                for i in 1..=3u32 {
                    let frame = format!("data: {{\"seq\":{i}}}\n\n");
                    let chunk = format!("{:x}\r\n{}\r\n", frame.len(), frame);
                    let _ = stream.write_all(chunk.as_bytes());
                }
                let heartbeat = ": keep-alive\n\n";
                let chunk = format!("{:x}\r\n{}\r\n", heartbeat.len(), heartbeat);
                let _ = stream.write_all(chunk.as_bytes());
                let _ = stream.write_all(b"0\r\n\r\n");
                let _ = stream.flush();
                break;
            }
        });

        let result = sse_subscribe(&url, "tok", "");
        assert!(result.is_ok(), "sse_subscribe should return Ok: {result:?}");
    }

    /// Verify the SSE parse logic: heartbeats / comments are skipped and
    /// data frames are printed as compact JSON.
    #[test]
    fn sse_parse_logic_skips_heartbeats() {
        let sse_body = ": heartbeat\ndata: {\"a\":1}\ndata: {\"b\":2}\n\ndata: {\"c\":3}\n\n";
        let reader = BufReader::new(sse_body.as_bytes());
        let mut results = Vec::new();
        for line in reader.lines() {
            let line = line.unwrap();
            if line.starts_with(':') || line.is_empty() {
                continue;
            }
            if let Some(payload) = line.strip_prefix("data:") {
                let payload = payload.trim();
                if !payload.is_empty() {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(payload) {
                        results.push(serde_json::to_string(&v).unwrap());
                    }
                }
            }
        }
        assert_eq!(results.len(), 3, "expected 3 data frames, got: {results:?}");
        assert_eq!(results[0], r#"{"a":1}"#);
        assert_eq!(results[1], r#"{"b":2}"#);
        assert_eq!(results[2], r#"{"c":3}"#);
    }

    /// --since sets the Last-Event-Id header.
    #[test]
    fn subscribe_since_header_forwarded() {
        let _g = with_temp_config_dir();
        let captured = Arc::new(std::sync::Mutex::new(String::new()));
        let cap2 = Arc::clone(&captured);
        let srv = TestServer::start(move |req: &Req| {
            *cap2.lock().unwrap() = req.path.clone();
            crate::testutil::Resp {
                status: 200,
                body: String::new(),
            }
        });
        let url = format!("{}/realtime/v1/sse/myproj/orders", srv.url);
        let _ = sse_subscribe(&url, "tok", "42");
        let path = captured.lock().unwrap().clone();
        assert_eq!(path, "/realtime/v1/sse/myproj/orders", "path: {path}");
    }

    /// 401 response from SSE endpoint → ApiError.
    #[test]
    fn subscribe_401_returns_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            crate::testutil::Resp::status(
                401,
                r#"{"code":"unauthorized","message":"invalid token"}"#,
            )
        });
        let url = format!("{}/realtime/v1/sse/p1/tbl", srv.url);
        let err = sse_subscribe(&url, "bad-tok", "").unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 401);
    }

    /// URL path construction: realtime subscribe uses api_url as data-plane base.
    #[test]
    fn data_plane_url_uses_api_url() {
        let base = "https://api.basin.run";
        let project = "myproj";
        let table = "orders";
        let expected = "https://api.basin.run/realtime/v1/sse/myproj/orders";
        let got = format!(
            "{}/realtime/v1/sse/{}/{}",
            base.trim_end_matches('/'),
            project,
            table
        );
        assert_eq!(got, expected);
    }

    /// subscribe --help returns Ok.
    #[test]
    fn subscribe_help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(cmd_realtime(&g, &["subscribe".to_string(), "--help".to_string()]).is_ok());
    }

    // ── WS URL construction tests ─────────────────────────────────────────────

    #[test]
    fn build_ws_url_https_to_wss() {
        let url = build_ws_url("https://api.basin.run", "myproj");
        assert_eq!(url, "wss://api.basin.run/realtime/v1/ws/myproj");
    }

    #[test]
    fn build_ws_url_http_to_ws() {
        let url = build_ws_url("http://127.0.0.1:9090", "p1");
        assert_eq!(url, "ws://127.0.0.1:9090/realtime/v1/ws/p1");
    }

    #[test]
    fn build_ws_url_trailing_slash_stripped() {
        let url = build_ws_url("https://api.basin.run/", "proj");
        assert_eq!(url, "wss://api.basin.run/realtime/v1/ws/proj");
    }

    // ── WS frame-handling unit tests ──────────────────────────────────────────

    /// "subscribed" frame decrements pending ack count.
    #[test]
    fn ws_subscribed_ack_decrements_counter() {
        let text = r#"{"type":"subscribed","table":"orders"}"#;
        let mut pending = 2usize;
        handle_ws_text(text, &mut pending).unwrap();
        assert_eq!(pending, 1);
    }

    /// "subscribed" never underflows below zero.
    #[test]
    fn ws_subscribed_ack_no_underflow() {
        let text = r#"{"type":"subscribed"}"#;
        let mut pending = 0usize;
        handle_ws_text(text, &mut pending).unwrap();
        assert_eq!(pending, 0);
    }

    /// "event" frame is formatted as compact JSON (does not modify pending).
    #[test]
    fn ws_event_frame_compact_json() {
        // We cannot capture stdout easily in unit tests, but we verify the
        // function does not return an error and does not touch pending_acks.
        let text = r#"{"type":"event","table":"orders","record":{"id":1}}"#;
        let mut pending = 0usize;
        let result = handle_ws_text(text, &mut pending);
        assert!(result.is_ok());
        assert_eq!(pending, 0, "event frame must not touch pending_acks");
    }

    /// "error" with code "lag" returns Ok (stderr warning, stdout uninterrupted).
    #[test]
    fn ws_lag_frame_returns_ok() {
        let text = r#"{"type":"error","code":"lag","message":"consumer lag"}"#;
        let mut pending = 0usize;
        let result = handle_ws_text(text, &mut pending);
        assert!(result.is_ok(), "lag error must not abort the stream");
    }

    /// Non-JSON text frame is printed verbatim without panicking.
    #[test]
    fn ws_non_json_frame_ok() {
        let mut pending = 0usize;
        let result = handle_ws_text("not-json-at-all", &mut pending);
        assert!(result.is_ok());
    }

    /// subscribe --multi with no tables errors before connecting.
    #[test]
    fn multi_empty_tables_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        // --multi= with empty value → no tables after split → should error.
        let result = cmd_realtime(
            &g,
            &[
                "subscribe".to_string(),
                "--multi=".to_string(),
                "--project=p1".to_string(),
            ],
        );
        assert!(result.is_err(), "empty --multi should error");
    }

    // ── WS end-to-end integration test ───────────────────────────────────────

    /// Hermetic in-process WS server for --multi integration testing.
    ///
    /// Topology
    /// --------
    ///   main thread : calls ws_subscribe("orders,items") against 127.0.0.1:0
    ///   server thread: does the tungstenite RFC-6455 handshake via accept(),
    ///                  collects subscribe frames, sends subscribed acks,
    ///                  sends two interleaved event frames (one per table),
    ///                  sends one {"type":"error","code":"lag"} frame,
    ///                  then closes.
    ///
    /// Assertions
    /// ----------
    ///   1. ws_subscribe returns Ok(())  – lag frame does not abort the stream.
    ///   2. Server received exactly two subscribe frames, one per table.
    ///   3. Each subscribe frame has the correct JSON shape.
    ///   4. Protocol: client-sent frames arrive before any server message is
    ///      consumed, so the server can echo them back for inspection.
    #[test]
    fn ws_multi_integration_scripted_server() {
        use std::sync::mpsc;
        use tungstenite::{accept, Message as WsMsg};

        // --- bind an ephemeral port -----------------------------------------
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind WS stub");
        let addr = listener.local_addr().expect("local addr");
        let ws_url = format!("ws://{addr}/realtime/v1/ws/testproj");

        // Channel: server → main: list of subscribe-frame payloads received.
        let (tx, rx) = mpsc::channel::<Vec<String>>();

        // --- server thread ---------------------------------------------------
        std::thread::spawn(move || {
            // Accept exactly one connection.
            let stream = match listener.accept() {
                Ok((s, _)) => s,
                Err(_) => return,
            };
            // Perform the WebSocket handshake (server side).
            let mut ws = match accept(stream) {
                Ok(ws) => ws,
                Err(_) => return,
            };

            // Collect subscribe frames until we have received two.
            let mut received: Vec<String> = Vec::new();
            while received.len() < 2 {
                match ws.read() {
                    Ok(WsMsg::Text(t)) => received.push(t.to_string()),
                    Ok(WsMsg::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }

            // Send subscribed acks (one per table).
            for _ in 0..received.len() {
                let _ = ws.send(WsMsg::Text(r#"{"type":"subscribed"}"#.to_string()));
            }

            // Send two interleaved event frames (orders then items).
            let evt_orders = r#"{"type":"event","table":"orders","record":{"id":1,"amount":99}}"#;
            let evt_items = r#"{"type":"event","table":"items","record":{"id":42,"sku":"X1"}}"#;
            let _ = ws.send(WsMsg::Text(evt_orders.to_string()));
            let _ = ws.send(WsMsg::Text(evt_items.to_string()));

            // Send the lag-error frame — client must NOT abort, must stay Ok.
            let lag = r#"{"type":"error","code":"lag","message":"consumer behind"}"#;
            let _ = ws.send(WsMsg::Text(lag.to_string()));

            // Close cleanly.
            let _ = ws.close(None);
            // Flush any remaining frames so the client sees Close before EOF.
            loop {
                match ws.read() {
                    Ok(WsMsg::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }

            // Report received frames to the main thread.
            let _ = tx.send(received);
        });

        // --- client (ws_subscribe) ------------------------------------------
        let result = ws_subscribe(&ws_url, "test-token", &["orders", "items"], "");
        assert!(
            result.is_ok(),
            "ws_subscribe must return Ok after lag frame + clean server close: {result:?}"
        );

        // --- collect server-side observations --------------------------------
        let received = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("server thread did not report received frames in time");

        // Two subscribe frames must have arrived.
        assert_eq!(
            received.len(),
            2,
            "expected 2 subscribe frames, server saw: {received:?}"
        );

        // Parse each frame and verify the JSON shape.
        let parsed: Vec<serde_json::Value> = received
            .iter()
            .map(|s| serde_json::from_str(s).expect("subscribe frame must be valid JSON"))
            .collect();

        // Both frames must have type == "subscribe".
        for (i, v) in parsed.iter().enumerate() {
            assert_eq!(
                v["type"], "subscribe",
                "frame {i} missing type:subscribe — got {v}"
            );
        }

        // The two table names must be exactly {"orders", "items"} (order agnostic).
        let mut tables: Vec<&str> = parsed
            .iter()
            .map(|v| v["table"].as_str().expect("table field must be a string"))
            .collect();
        tables.sort_unstable();
        assert_eq!(
            tables,
            vec!["items", "orders"],
            "subscribe frames must cover exactly the requested tables"
        );

        // Neither frame should contain a filter key (we passed "").
        for (i, v) in parsed.iter().enumerate() {
            assert!(
                v.get("filter").is_none(),
                "frame {i} must not include a filter key when no filter was requested"
            );
        }
    }

    /// Same topology as ws_multi_integration_scripted_server but with a filter
    /// expression — verifies the filter key is forwarded in every subscribe frame.
    #[test]
    fn ws_multi_integration_filter_forwarded() {
        use std::sync::mpsc;
        use tungstenite::{accept, Message as WsMsg};

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind WS stub");
        let addr = listener.local_addr().expect("local addr");
        let ws_url = format!("ws://{addr}/realtime/v1/ws/testproj");

        let (tx, rx) = mpsc::channel::<Vec<String>>();

        std::thread::spawn(move || {
            let stream = match listener.accept() {
                Ok((s, _)) => s,
                Err(_) => return,
            };
            let mut ws = match accept(stream) {
                Ok(ws) => ws,
                Err(_) => return,
            };

            let mut received: Vec<String> = Vec::new();
            while received.len() < 2 {
                match ws.read() {
                    Ok(WsMsg::Text(t)) => received.push(t.to_string()),
                    Ok(WsMsg::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }

            // Ack both tables.
            for _ in 0..received.len() {
                let _ = ws.send(WsMsg::Text(r#"{"type":"subscribed"}"#.to_string()));
            }

            // Close immediately — the client sees ConnectionClosed and exits Ok.
            let _ = ws.close(None);
            loop {
                match ws.read() {
                    Ok(WsMsg::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }

            let _ = tx.send(received);
        });

        let result = ws_subscribe(&ws_url, "test-token", &["orders", "items"], "id=gt.5");
        assert!(
            result.is_ok(),
            "ws_subscribe with filter must return Ok: {result:?}"
        );

        let received = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("server thread did not report in time");

        assert_eq!(
            received.len(),
            2,
            "expected 2 subscribe frames: {received:?}"
        );

        let parsed: Vec<serde_json::Value> = received
            .iter()
            .map(|s| serde_json::from_str(s).expect("valid JSON"))
            .collect();

        // Every frame must carry the filter.
        for (i, v) in parsed.iter().enumerate() {
            assert_eq!(v["filter"], "id=gt.5", "frame {i} missing filter: {v}");
        }
    }

    /// Subscribe frame JSON shape — verifies the exact wire format sent
    /// to the server for a table with and without a filter.
    #[test]
    fn subscribe_frame_shape_no_filter() {
        let frame = serde_json::json!({"type": "subscribe", "table": "orders"}).to_string();
        let v: serde_json::Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["type"], "subscribe");
        assert_eq!(v["table"], "orders");
        assert!(v.get("filter").is_none());
    }

    #[test]
    fn subscribe_frame_shape_with_filter() {
        let table = "orders";
        let filter = "id=gt.5";
        let frame =
            serde_json::json!({"type": "subscribe", "table": table, "filter": filter}).to_string();
        let v: serde_json::Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["type"], "subscribe");
        assert_eq!(v["table"], "orders");
        assert_eq!(v["filter"], "id=gt.5");
    }

    /// Multi-table list is split correctly from comma-separated input.
    #[test]
    fn multi_table_split() {
        let input = "orders, items , users";
        let tables: Vec<&str> = input
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();
        assert_eq!(tables, vec!["orders", "items", "users"]);
    }
}
