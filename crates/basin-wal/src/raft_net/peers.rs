//! Node-id → address resolution for the raft transport.
//!
//! v1 is a static registry parsed from `BASIN_RAFT_PEERS`. [`PeerRegistry`]
//! is the trait seam so the cloud can inject a service-discovery backend
//! (DNS-SRV, a control-plane lookup, fly.io 6PN, …) without touching the
//! client. The client only ever asks "what URI for node N?".

use std::collections::BTreeMap;
use std::sync::Arc;

use basin_common::{BasinError, Result};

/// Resolves a raft `node_id` to a connectable URI.
///
/// Implementations must be cheap to call on every (re)connect — the client
/// resolves lazily on first use and again after a channel is dropped, so a
/// discovery backend can return an updated address after a peer moves.
pub trait PeerRegistry: Send + Sync + 'static {
    /// Return the base URI for `node_id` (e.g. `http://10.0.0.4:7001`), or
    /// `None` if the node is unknown. `None` is treated by the client as
    /// "unreachable" (openraft backs off), NOT as a hard error — membership
    /// can race ahead of discovery.
    fn resolve(&self, node_id: u64) -> Option<String>;
}

impl<T: PeerRegistry + ?Sized> PeerRegistry for Arc<T> {
    fn resolve(&self, node_id: u64) -> Option<String> {
        (**self).resolve(node_id)
    }
}

/// Static `node_id → uri` map. The v1 backing for [`PeerRegistry`].
#[derive(Clone, Debug, Default)]
pub struct StaticPeers {
    map: BTreeMap<u64, String>,
}

impl StaticPeers {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    /// Insert / overwrite a peer. `addr` may be a bare `host:port` (scheme is
    /// defaulted to `http://`) or a full URI.
    pub fn insert(&mut self, node_id: u64, addr: impl Into<String>) -> &mut Self {
        self.map.insert(node_id, normalize_uri(addr.into()));
        self
    }

    pub fn with(mut self, node_id: u64, addr: impl Into<String>) -> Self {
        self.insert(node_id, addr);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Parse the `BASIN_RAFT_PEERS` env-var format:
    /// `1@host:port,2@host:port,…`. Whitespace around entries is trimmed.
    /// Each entry is `id@addr`; `addr` may carry a scheme or not.
    ///
    /// Errors on a malformed entry (missing `@`, non-numeric id, empty addr)
    /// rather than silently dropping peers — a typo in cluster config should
    /// fail loudly at startup, not surface as a node that never replicates.
    pub fn parse(spec: &str) -> Result<Self> {
        let mut map = BTreeMap::new();
        for raw in spec.split(',') {
            let entry = raw.trim();
            if entry.is_empty() {
                continue;
            }
            let (id_str, addr) = entry.split_once('@').ok_or_else(|| {
                BasinError::wal(format!(
                    "BASIN_RAFT_PEERS entry '{entry}' missing '@' (want id@host:port)"
                ))
            })?;
            let id: u64 = id_str.trim().parse().map_err(|_| {
                BasinError::wal(format!(
                    "BASIN_RAFT_PEERS entry '{entry}' has non-numeric node id '{id_str}'"
                ))
            })?;
            let addr = addr.trim();
            if addr.is_empty() {
                return Err(BasinError::wal(format!(
                    "BASIN_RAFT_PEERS entry '{entry}' has empty address"
                )));
            }
            if map.insert(id, normalize_uri(addr.to_string())).is_some() {
                return Err(BasinError::wal(format!(
                    "BASIN_RAFT_PEERS has duplicate node id {id}"
                )));
            }
        }
        Ok(Self { map })
    }

    /// Parse from the `BASIN_RAFT_PEERS` environment variable. Empty / unset
    /// yields an empty registry (single-node bootstrap is legal).
    pub fn from_env() -> Result<Self> {
        match std::env::var("BASIN_RAFT_PEERS") {
            Ok(v) => Self::parse(&v),
            Err(std::env::VarError::NotPresent) => Ok(Self::new()),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err(BasinError::wal("BASIN_RAFT_PEERS is not valid UTF-8"))
            }
        }
    }
}

impl PeerRegistry for StaticPeers {
    fn resolve(&self, node_id: u64) -> Option<String> {
        self.map.get(&node_id).cloned()
    }
}

/// Default a bare `host:port` to an `http://` URI; leave anything with a
/// scheme untouched (so a future mTLS config can pass `https://…`).
fn normalize_uri(addr: String) -> String {
    if addr.contains("://") {
        addr
    } else {
        format!("http://{addr}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_spec() {
        let p = StaticPeers::parse("1@host-a:7001,2@host-b:7002,3@host-c:7003").unwrap();
        assert_eq!(p.len(), 3);
        assert_eq!(p.resolve(1).as_deref(), Some("http://host-a:7001"));
        assert_eq!(p.resolve(2).as_deref(), Some("http://host-b:7002"));
        assert_eq!(p.resolve(3).as_deref(), Some("http://host-c:7003"));
        assert_eq!(p.resolve(4), None);
    }

    #[test]
    fn tolerates_whitespace_and_trailing_comma() {
        let p = StaticPeers::parse(" 1@a:1 , 2@b:2 , ").unwrap();
        assert_eq!(p.len(), 2);
    }

    #[test]
    fn preserves_explicit_scheme() {
        let p = StaticPeers::parse("1@https://secure:7001").unwrap();
        assert_eq!(p.resolve(1).as_deref(), Some("https://secure:7001"));
    }

    #[test]
    fn rejects_missing_at() {
        assert!(StaticPeers::parse("1host:7001").is_err());
    }

    #[test]
    fn rejects_non_numeric_id() {
        assert!(StaticPeers::parse("a@host:7001").is_err());
    }

    #[test]
    fn rejects_empty_addr() {
        assert!(StaticPeers::parse("1@").is_err());
    }

    #[test]
    fn rejects_duplicate_id() {
        assert!(StaticPeers::parse("1@a:1,1@b:2").is_err());
    }

    #[test]
    fn empty_spec_is_empty_registry() {
        let p = StaticPeers::parse("").unwrap();
        assert!(p.is_empty());
    }

    #[test]
    fn builder_normalizes() {
        let p = StaticPeers::new().with(7, "127.0.0.1:9000");
        assert_eq!(p.resolve(7).as_deref(), Some("http://127.0.0.1:9000"));
    }
}
