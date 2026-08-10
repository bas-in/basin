//! Wedge-forensics / perf-forensics debug listener (#72, #78).
//!
//! Runs on a DEDICATED OS thread with its OWN single-thread tokio runtime,
//! bound to a SEPARATE port (`BASIN_DEBUG_PORT`). It holds a clone of the MAIN
//! multi-thread runtime's `Handle` so it can introspect that runtime from the
//! outside — the whole point is that this thread keeps answering when every
//! main-runtime worker is blocked (the #72 all-queries-hang shape), and that
//! its CPU profiler samples OS threads independently of the tokio scheduler
//! (the #78 where-does-ingest-CPU-go question).
//!
//! Endpoints:
//!   GET /debug/health   -> liveness of THIS thread (always answers).
//!   GET /debug/runtime  -> tokio `Handle::metrics()` of the MAIN runtime
//!                          (workers, alive tasks, blocking threads, queue
//!                          depths — needs `--cfg tokio_unstable`, set
//!                          workspace-wide in `.cargo/config.toml`). A wedged
//!                          runtime shows saturated workers + a deep
//!                          injection queue here. (`Handle::dump()` task
//!                          backtraces additionally need the Linux-only
//!                          `tokio_taskdump` cfg — deliberately not used so
//!                          local macOS builds keep compiling.)
//!   GET /debug/threads  -> pprof CPU-profile flamegraph (SVG) of the whole
//!                          process, sampled for `BASIN_DEBUG_PROFILE_SECS`
//!                          (default 10 s; override per request with
//!                          `?seconds=N`, capped at 120).
//!
//! Env gate: no-op (thread never spawned) unless `BASIN_DEBUG_PORT` is set.
//! `BASIN_DEBUG_BIND` defaults to 127.0.0.1 — reach it via `fly ssh console`;
//! never expose it publicly.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use tokio::net::TcpListener;
use tokio::runtime::Handle;

/// Everything the debug endpoints need to introspect the live process.
#[derive(Clone)]
pub struct DebugState {
    /// Clone of the MAIN multi-thread runtime's Handle. `Handle::dump()` on
    /// this reports the async task backtraces of THAT runtime even though it
    /// is called from this thread's own current-thread runtime.
    pub main_handle: Handle,
}

/// Spawn the debug listener on a dedicated OS thread. Returns immediately;
/// `None` unless `BASIN_DEBUG_PORT` parses to a port.
pub fn spawn_debug_listener(main_handle: Handle) -> Option<std::thread::JoinHandle<()>> {
    let port: u16 = std::env::var("BASIN_DEBUG_PORT")
        .ok()
        .and_then(|s| s.trim().parse().ok())?;
    let bind = std::env::var("BASIN_DEBUG_BIND").unwrap_or_else(|_| "127.0.0.1".to_string());
    let addr = format!("{bind}:{port}");

    let state = DebugState { main_handle };

    std::thread::Builder::new()
        .name("basin-debug".into())
        .spawn(move || {
            // OWN current-thread runtime, isolated from the main pool.
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("basin-debug: failed to build runtime: {e}");
                    return;
                }
            };
            rt.block_on(async move {
                let app = Router::new()
                    .route("/debug/health", get(health))
                    .route("/debug/runtime", get(runtime_metrics))
                    .route("/debug/threads", get(profile_threads))
                    .with_state(Arc::new(state));
                match TcpListener::bind(&addr).await {
                    Ok(l) => {
                        eprintln!("basin-debug: listening on {addr}");
                        let _ = axum::serve(l, app).await;
                    }
                    Err(e) => eprintln!("basin-debug: bind {addr} failed: {e}"),
                }
            });
        })
        .ok()
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "debug-thread-alive\n")
}

/// Scheduler-level metrics of the MAIN runtime. Read from this thread via the
/// cloned Handle — works (and stays responsive) even when every main worker
/// is blocked, which is exactly when the numbers are interesting: saturated
/// `num_workers` with a deep `global_queue_depth` and maxed
/// `num_blocking_threads` is the worker-starvation signature.
async fn runtime_metrics(State(state): State<Arc<DebugState>>) -> impl IntoResponse {
    let m = state.main_handle.metrics();
    let body = format!(
        "num_workers: {}\nnum_alive_tasks: {}\nnum_blocking_threads: {}\n\
         num_idle_blocking_threads: {}\nglobal_queue_depth: {}\n\
         spawned_tasks_count: {}\nblocking_queue_depth: {}\n",
        m.num_workers(),
        m.num_alive_tasks(),
        m.num_blocking_threads(),
        m.num_idle_blocking_threads(),
        m.global_queue_depth(),
        m.spawned_tasks_count(),
        m.blocking_queue_depth(),
    );
    (StatusCode::OK, body)
}

#[derive(serde::Deserialize)]
struct ProfileParams {
    seconds: Option<u64>,
}

/// pprof CPU profile of the WHOLE PROCESS (all OS threads, including the main
/// runtime's workers). Samples via SIGPROF + stack walking, independent of
/// the tokio scheduler, so it works even when the main runtime cannot
/// schedule anything. Returns a flamegraph SVG.
async fn profile_threads(Query(p): Query<ProfileParams>) -> impl IntoResponse {
    let secs: u64 = p
        .seconds
        .or_else(|| {
            std::env::var("BASIN_DEBUG_PROFILE_SECS")
                .ok()
                .and_then(|s| s.trim().parse().ok())
        })
        .unwrap_or(10)
        .min(120);
    // The guard + report build are blocking; run them on the debug runtime's
    // blocking pool so /debug/health stays responsive mid-sample.
    let svg = tokio::task::spawn_blocking(move || {
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(100)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .map_err(|e| format!("profiler build: {e}"))?;
        std::thread::sleep(Duration::from_secs(secs));
        let report = guard.report().build().map_err(|e| format!("report: {e}"))?;
        let mut buf = Vec::new();
        report
            .flamegraph(&mut buf)
            .map_err(|e| format!("flamegraph: {e}"))?;
        Ok::<_, String>(buf)
    })
    .await;

    match svg {
        Ok(Ok(bytes)) => {
            (StatusCode::OK, [("content-type", "image/svg+xml")], bytes).into_response()
        }
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("join: {e}")).into_response(),
    }
}
