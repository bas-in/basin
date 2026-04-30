//! Map a pgwire `user` parameter to a `TenantId`.
//!
//! The PoC ships a `StaticTenantResolver` driven by a `HashMap`; production
//! deployments will swap in a network-backed resolver (etcd/FDB lookup,
//! tenant-control-plane RPC, etc.). The trait is async because any real
//! implementation will hit the network.

use std::collections::HashMap;

use async_trait::async_trait;
use basin_common::{BasinError, Result, TenantId};

/// Resolves a pgwire `user` to a `TenantId`. Implementations must be safe to
/// share across connections; `&self` is the only handle the router holds.
#[async_trait]
pub trait TenantResolver: Send + Sync {
    async fn resolve(&self, username: &str) -> Result<TenantId>;
}

/// In-memory resolver keyed on username -> tenant id. Useful for the PoC and
/// for tests; not a production tool.
#[derive(Debug, Clone, Default)]
pub struct StaticTenantResolver {
    map: HashMap<String, TenantId>,
}

impl StaticTenantResolver {
    pub fn new(map: HashMap<String, TenantId>) -> Self {
        Self { map }
    }

    pub fn with_entry(mut self, user: impl Into<String>, tenant: TenantId) -> Self {
        self.map.insert(user.into(), tenant);
        self
    }
}

#[async_trait]
impl TenantResolver for StaticTenantResolver {
    async fn resolve(&self, username: &str) -> Result<TenantId> {
        self.map.get(username).copied().ok_or_else(|| {
            BasinError::not_found(format!("no tenant mapped for user {username:?}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn static_resolver_lookup() {
        let t = TenantId::new();
        let mut map = HashMap::new();
        map.insert("alice".to_owned(), t);
        let r = StaticTenantResolver::new(map);

        assert_eq!(r.resolve("alice").await.unwrap(), t);
        let err = r.resolve("bob").await.unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)), "got {err:?}");
    }
}
