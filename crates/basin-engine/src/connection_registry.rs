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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

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
}

impl ConnectionRegistry {
    /// Create a new, empty registry.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            next_pid: Arc::new(std::sync::atomic::AtomicI32::new(1)),
        }
    }

    /// Register a new session for `project`. Returns a [`ConnectionHandle`]
    /// that deregisters on drop (session close).
    pub fn connect(&self, project: &ProjectId) -> ConnectionHandle {
        let pid = self
            .next_pid
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let info = SessionInfo::new(pid);
        {
            let mut map = self.inner.lock().expect("ConnectionRegistry mutex poisoned");
            map.entry(*project).or_default().push(info);
        }
        ConnectionHandle {
            registry: self.inner.clone(),
            project: *project,
            pid,
        }
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
    project: ProjectId,
    /// The pid of the session this handle owns.
    pub pid: i32,
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
