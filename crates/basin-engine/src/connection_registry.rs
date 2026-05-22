//! Per-project connection/session registry for `pg_stat_activity` (Phase 5.23.C).
//!
//! ## Design
//!
//! `pg_stat_activity` shows "current" session state: pid, state ("active" /
//! "idle"), current query text, query_start timestamp, etc. This module
//! provides a lightweight process-wide snapshot of active sessions so that
//! `PgStatActivityLiveProvider` (in `info_schema_provider.rs`) can serve live
//! data rather than a static stub.
//!
//! ## Per-project cost discipline
//!
//! The registry is a single `Arc<Mutex<HashMap<ProjectId, Vec<SessionInfo>>>>`.
//! Idle projects contribute O(0) entries (missing key). The `Mutex` guard is
//! never held across an `await` — only across short synchronous mutations.
//!
//! ## Thread safety
//!
//! `std::sync::Mutex` is used (not `tokio::sync::Mutex`) because all mutations
//! are synchronous and brief: a `Vec::push` on connect, a field update per
//! execute, and a `Vec::retain` on disconnect.
//!
//! ## pg_cancel_backend wiring (Phase 5.31.A)
//!
//! Each session gets an `Arc<tokio::sync::Notify>` at connect time.  The
//! engine stores a pid → Notify map alongside the session list.  When
//! `pg_cancel_backend(pid)` is called:
//!
//!   1. `ConnectionRegistry::notify_cancel(pid)` is called.
//!   2. It marks the pid in `CancelBackendRegistry` (catalog-side bool).
//!   3. It calls `notify.notify_one()` on the per-pid `Notify`.
//!   4. The executor's `exec_select` selects on `cancel_notify.notified()`
//!      alongside the DataFusion collect future; when the notification fires
//!      it drops the DataFusion future and returns `BasinError::QueryCanceled`
//!      (SQLSTATE 57014).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use basin_catalog::info_schema::CancelBackendRegistry;
use basin_common::ProjectId;

/// Snapshot of one session's state as visible via `pg_stat_activity`.
#[derive(Clone, Debug)]
pub struct SessionInfo {
    /// Synthetic backend pid (monotonically increasing counter per process).
    pub pid: i32,
    /// Current session state. `"active"` while a query is running,
    /// `"idle"` between commands, `"idle in transaction"` inside a BEGIN.
    pub state: String,
    /// The most-recently-executed query text (params redacted in v0.1).
    pub query: Option<String>,
    /// When the most-recently-executed query started (`None` if idle).
    pub query_start: Option<Instant>,
    /// When this session connected (backend_start).
    pub backend_start: Instant,
    /// Application name — set by the client or defaulted to `"basin"`.
    pub application_name: String,
    /// Phase 5.31.A: notification handle for `pg_cancel_backend`.
    /// Notified when another session calls `pg_cancel_backend(this.pid)`.
    /// The executor's `exec_select` awaits this alongside the DataFusion
    /// collect future and returns SQLSTATE 57014 when it fires.
    pub cancel_notify: Arc<tokio::sync::Notify>,
}

impl SessionInfo {
    fn new(pid: i32) -> Self {
        Self {
            pid,
            state: "active".to_string(),
            query: None,
            query_start: None,
            backend_start: Instant::now(),
            application_name: "basin".to_string(),
            cancel_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

/// Process-wide registry of open sessions, keyed by project.
///
/// Cheap to clone — just an `Arc` ref-count bump.
#[derive(Clone, Default)]
pub struct ConnectionRegistry {
    inner: Arc<Mutex<HashMap<ProjectId, Vec<SessionInfo>>>>,
    /// Monotonically-increasing pid counter (shared across all projects).
    next_pid: Arc<std::sync::atomic::AtomicI32>,
    /// Phase 5.31.A: process-wide pid → Notify map for `pg_cancel_backend`.
    /// Parallel to the `inner` session list; keyed by pid (process-wide, not
    /// per-project) so `notify_cancel(pid)` can find the notify in O(1)
    /// without iterating the per-project session lists.
    cancel_notifies: Arc<Mutex<HashMap<i32, Arc<tokio::sync::Notify>>>>,
    /// Phase 5.31.A: catalog-side bool registry for `pg_cancel_backend`.
    /// Shared with the `pg_cancel_backend` UDF registered per-session so
    /// the UDF can call `cancel_registry.cancel(pid)`.
    pub(crate) cancel_registry: CancelBackendRegistry,
}

impl ConnectionRegistry {
    /// Create a new, empty registry.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            next_pid: Arc::new(std::sync::atomic::AtomicI32::new(1)),
            cancel_notifies: Arc::new(Mutex::new(HashMap::new())),
            cancel_registry: CancelBackendRegistry::new(),
        }
    }

    /// Register a new session for `project`. Returns a [`ConnectionHandle`]
    /// that deregisters on drop (session close).
    pub fn connect(&self, project: &ProjectId) -> ConnectionHandle {
        let pid = self
            .next_pid
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let info = SessionInfo::new(pid);
        // Phase 5.31.A: capture the cancel notify before moving `info` into
        // the registry so we can return it through the ConnectionHandle.
        let cancel_notify = info.cancel_notify.clone();
        {
            let mut map = self.inner.lock().expect("ConnectionRegistry mutex poisoned");
            map.entry(*project).or_default().push(info);
        }
        // Register the pid in the cancel_notifies map and the catalog registry.
        {
            let mut cn = self.cancel_notifies.lock().expect("cancel_notifies poisoned");
            cn.insert(pid, cancel_notify.clone());
        }
        self.cancel_registry.register(pid);
        ConnectionHandle {
            registry: self.inner.clone(),
            cancel_notifies: self.cancel_notifies.clone(),
            cancel_registry: self.cancel_registry.clone(),
            project: *project,
            pid,
            cancel_notify,
        }
    }

    /// Phase 5.31.A: signal cancellation for `pid`.
    ///
    /// Marks the pid as cancelled in the `CancelBackendRegistry` (so the
    /// catalog-side `pg_cancel_backend` function returns `true`) and fires
    /// the per-session `Notify` so the executor's `tokio::select!` branch
    /// unblocks and returns SQLSTATE 57014.
    ///
    /// Returns `true` if the pid was found (i.e. session is still live),
    /// `false` if the pid is unknown (session already disconnected).
    pub fn notify_cancel(&self, pid: i32) -> bool {
        let notified = self.cancel_registry.cancel(pid);
        if notified {
            let cn = self.cancel_notifies.lock().expect("cancel_notifies poisoned");
            if let Some(notify) = cn.get(&pid) {
                notify.notify_one();
            }
        }
        notified
    }

    /// Update the `query` and `state` for the session identified by `pid`
    /// within `project`. Called at the start of each `execute()` invocation.
    pub fn set_query(&self, project: &ProjectId, pid: i32, query: Option<&str>) {
        let mut map = self.inner.lock().expect("ConnectionRegistry mutex poisoned");
        if let Some(entries) = map.get_mut(project) {
            for entry in entries.iter_mut() {
                if entry.pid == pid {
                    entry.query = query.map(|q| q.to_string());
                    entry.query_start = query.map(|_| Instant::now());
                    entry.state = if query.is_some() {
                        "active".to_string()
                    } else {
                        "idle".to_string()
                    };
                    break;
                }
            }
        }
    }

    /// Snapshot all session entries for `project`.
    ///
    /// Called from `PgStatActivityLiveProvider::scan()` — holds the lock
    /// only for the `clone()` call, then releases immediately.
    pub fn snapshot(&self, project: &ProjectId) -> Vec<SessionInfo> {
        let map = self.inner.lock().expect("ConnectionRegistry mutex poisoned");
        map.get(project).cloned().unwrap_or_default()
    }
}

/// RAII guard that removes a session entry from the registry when dropped.
///
/// Obtained from [`ConnectionRegistry::connect`]. Dropped when the
/// `ProjectSession` is dropped (end of the session lifecycle).
pub struct ConnectionHandle {
    registry: Arc<Mutex<HashMap<ProjectId, Vec<SessionInfo>>>>,
    /// Phase 5.31.A: shared cancel_notifies map; entry removed on drop.
    cancel_notifies: Arc<Mutex<HashMap<i32, Arc<tokio::sync::Notify>>>>,
    /// Phase 5.31.A: catalog-side bool registry; deregistered on drop.
    cancel_registry: CancelBackendRegistry,
    project: ProjectId,
    /// The pid of the session this handle owns.
    pub pid: i32,
    /// Phase 5.31.A: the per-session cancel notification handle.
    /// Held here so it stays alive for the duration of the session and the
    /// executor can clone an `Arc` reference from `ProjectSession`.
    pub cancel_notify: Arc<tokio::sync::Notify>,
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        if let Ok(mut map) = self.registry.lock() {
            let pid = self.pid;
            if let Some(entries) = map.get_mut(&self.project) {
                entries.retain(|e| e.pid != pid);
                if entries.is_empty() {
                    map.remove(&self.project);
                }
            }
        }
        // Phase 5.31.A: remove from cancel_notifies and deregister from the
        // catalog-side registry so the pid slot is not accidentally signalled
        // after the session closes.
        if let Ok(mut cn) = self.cancel_notifies.lock() {
            cn.remove(&self.pid);
        }
        self.cancel_registry.deregister(self.pid);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::ProjectId;

    #[test]
    fn connect_and_snapshot() {
        let reg = ConnectionRegistry::new();
        let project = ProjectId::new();

        assert!(reg.snapshot(&project).is_empty(), "no sessions initially");

        let handle = reg.connect(&project);
        let snap = reg.snapshot(&project);
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].state, "active");
        let pid = handle.pid;

        reg.set_query(&project, pid, Some("SELECT 1"));
        let snap2 = reg.snapshot(&project);
        assert_eq!(snap2[0].query.as_deref(), Some("SELECT 1"));

        drop(handle);
        assert!(reg.snapshot(&project).is_empty(), "removed on drop");
    }

    #[test]
    fn two_projects_isolated() {
        let reg = ConnectionRegistry::new();
        let p1 = ProjectId::new();
        let p2 = ProjectId::new();

        let _h1 = reg.connect(&p1);
        let _h2 = reg.connect(&p2);

        assert_eq!(reg.snapshot(&p1).len(), 1);
        assert_eq!(reg.snapshot(&p2).len(), 1);
    }
}
