//! Row-level security for `storage.objects` (Phase 5.17.C).
//!
//! This module is the storage-tier analogue of `basin-engine/src/rls.rs`.
//! Rather than DataFusion plan injection (which would require pulling in the
//! full engine stack), it evaluates policies directly against the `Object`
//! struct fields at the HTTP handler layer — a thin predicate filter that
//! mirrors the SQL semantics the engine provides for regular tables.
//!
//! ## Policy model
//!
//! An [`ObjectPolicy`] has:
//! - A **command** (SELECT / INSERT / UPDATE / DELETE / ALL) — only SELECT
//!   and DELETE are enforced at the object layer in v1 (INSERT sets the owner
//!   field; UPDATE is not a storage concept).
//! - A **USING predicate** — one of the supported built-in predicates (not
//!   arbitrary SQL; arbitrary SQL would require a full engine session).
//! - An optional **role list** — empty means PUBLIC.
//!
//! Supported predicates (Phase 5.17.C):
//!
//! | Expression | Semantics |
//! |---|---|
//! | `owner = auth.uid()` | Object was uploaded by this user. |
//! | `TRUE` | Always passes (open policy). |
//! | `FALSE` | Never passes (deny-all). |
//!
//! Multiple policies on the same command combine with OR (Postgres permissive
//! semantics). No applicable policy → deny-all (Postgres default).
//!
//! ## Public-bucket bypass
//!
//! When the bucket's `public` flag is `true`, read (SELECT) policies are
//! bypassed entirely. The caller must check `bucket.public` before calling
//! [`check_object_access`].

use std::collections::HashMap;
use std::sync::RwLock;

use uuid::Uuid;

use basin_common::ProjectId;

use crate::model::Object;

// ---------------------------------------------------------------------------
// Policy types
// ---------------------------------------------------------------------------

/// Which DML commands this policy applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectPolicyCommand {
    All,
    Select,
    Insert,
    Update,
    Delete,
}

/// Built-in USING predicates supported by the storage RLS layer.
///
/// These are intentionally narrow — full SQL expression evaluation would
/// require a DataFusion session, which lives in `basin-engine`. The predicates
/// cover the use-cases spelled out in ADR 0021.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectUsing {
    /// `owner = auth.uid()` — object was uploaded by the current user.
    OwnerEqAuthUid,
    /// `TRUE` — always passes. Used for open / public policies.
    True,
    /// `FALSE` — never passes. Used for deny-all policies.
    False,
    /// `role = auth.role()` — restrict to a named role.
    RoleEqAuthRole(String),
}

impl ObjectUsing {
    /// Parse the canonical USING expression strings that appear in
    /// `CREATE POLICY … USING (…)` statements or are constructed directly.
    ///
    /// Accepted forms (case-insensitive, whitespace-normalised):
    ///
    /// - `"owner = auth.uid()"` / `"auth.uid() = owner"`
    /// - `"true"` / `"false"`
    pub fn parse(s: &str) -> Option<Self> {
        let norm = s.split_whitespace().collect::<Vec<_>>().join(" ");
        let lower = norm.to_ascii_lowercase();
        if lower == "true" {
            Some(Self::True)
        } else if lower == "false" {
            Some(Self::False)
        } else if lower == "owner = auth.uid()" || lower == "auth.uid() = owner" {
            Some(Self::OwnerEqAuthUid)
        } else {
            None
        }
    }

    /// Evaluate this predicate against `obj` for the given principal UUID
    /// (`auth_uid`) and role name (`auth_role`).
    ///
    /// Returns `true` if the object passes the predicate.
    pub fn evaluate(&self, obj: &Object, auth_uid: Option<Uuid>, auth_role: &str) -> bool {
        match self {
            ObjectUsing::OwnerEqAuthUid => {
                // NULL = NULL is not TRUE in SQL — both must be non-null and equal.
                match (obj.owner, auth_uid) {
                    (Some(owner), Some(uid)) => owner == uid,
                    _ => false,
                }
            }
            ObjectUsing::True => true,
            ObjectUsing::False => false,
            ObjectUsing::RoleEqAuthRole(role) => auth_role == role,
        }
    }
}

/// An RLS policy entry for `storage.objects`.
#[derive(Debug, Clone)]
pub struct ObjectPolicy {
    pub name: String,
    /// Commands this policy applies to. `All` matches every command.
    pub command: ObjectPolicyCommand,
    /// Roles this policy applies to. Empty = PUBLIC (all roles).
    pub applies_to_roles: Vec<String>,
    /// The USING predicate.
    pub using: ObjectUsing,
}

impl ObjectPolicy {
    /// Return `true` if this policy applies to `command` for `role`.
    pub fn matches_command_and_role(&self, command: ObjectPolicyCommand, role: &str) -> bool {
        let cmd_match = matches!(self.command, ObjectPolicyCommand::All) || self.command == command;
        let role_match =
            self.applies_to_roles.is_empty() || self.applies_to_roles.iter().any(|r| r == role);
        cmd_match && role_match
    }
}

// ---------------------------------------------------------------------------
// Per-project, per-table RLS store
// ---------------------------------------------------------------------------

/// In-memory store for storage.objects RLS policies (one per project).
///
/// Stored in `BlobStore` alongside the object catalog. Keyed on
/// `(project_id, table_name)`. In Phase 5.17.C only `"objects"` is
/// used; the key is forward-compatible.
#[derive(Debug, Default)]
pub struct ObjectRlsStore {
    /// `(project_str, table_name)` → `(rls_enabled, policies)`
    inner: RwLock<HashMap<(String, String), (bool, Vec<ObjectPolicy>)>>,
}

impl ObjectRlsStore {
    pub fn new() -> Self {
        Self::default()
    }

    // --- Admin DDL helpers ---

    /// Enable / disable RLS for `table` in `project`.
    pub fn set_enabled(&self, project: &ProjectId, table: &str, enabled: bool) {
        let mut guard = self.inner.write().unwrap();
        let entry = guard
            .entry((project.to_string(), table.to_string()))
            .or_insert_with(|| (false, Vec::new()));
        entry.0 = enabled;
    }

    /// Create a policy for `table` in `project`.
    ///
    /// Returns `false` if a policy with the same name already exists.
    pub fn create_policy(&self, project: &ProjectId, table: &str, policy: ObjectPolicy) -> bool {
        let mut guard = self.inner.write().unwrap();
        let entry = guard
            .entry((project.to_string(), table.to_string()))
            .or_insert_with(|| (false, Vec::new()));
        if entry.1.iter().any(|p| p.name == policy.name) {
            return false;
        }
        entry.1.push(policy);
        true
    }

    /// Drop a policy by name. Returns `true` if found and removed.
    pub fn drop_policy(&self, project: &ProjectId, table: &str, name: &str) -> bool {
        let mut guard = self.inner.write().unwrap();
        if let Some(entry) = guard.get_mut(&(project.to_string(), table.to_string())) {
            let before = entry.1.len();
            entry.1.retain(|p| p.name != name);
            return entry.1.len() < before;
        }
        false
    }

    // --- Query helpers ---

    /// Return `(rls_enabled, applicable_policies)` for `command` / `role`.
    pub fn applicable(
        &self,
        project: &ProjectId,
        table: &str,
        command: ObjectPolicyCommand,
        role: &str,
    ) -> (bool, Vec<ObjectPolicy>) {
        let guard = self.inner.read().unwrap();
        if let Some((enabled, policies)) = guard.get(&(project.to_string(), table.to_string())) {
            if !enabled {
                return (false, Vec::new());
            }
            let applicable: Vec<ObjectPolicy> = policies
                .iter()
                .filter(|p| p.matches_command_and_role(command, role))
                .cloned()
                .collect();
            (*enabled, applicable)
        } else {
            (false, Vec::new())
        }
    }
}

// ---------------------------------------------------------------------------
// RLS check helpers (called from storage route handlers)
// ---------------------------------------------------------------------------

/// Context passed from the JWT claims into the RLS check.
#[derive(Debug, Clone)]
pub struct CallerCtx {
    /// `auth.uid()` — the user UUID from the JWT, `None` for anonymous.
    pub uid: Option<Uuid>,
    /// `auth.role()` — `"authenticated"` for JWT sessions, `"anon"` for
    /// anonymous / no-JWT requests.
    pub role: String,
}

impl CallerCtx {
    pub fn authenticated(uid: Uuid) -> Self {
        Self {
            uid: Some(uid),
            role: "authenticated".into(),
        }
    }

    pub fn anonymous() -> Self {
        Self {
            uid: None,
            role: "anon".into(),
        }
    }
}

/// Evaluate the RLS policies for `command` against `obj`.
///
/// Returns `Ok(())` if access is permitted, `Err(reason)` if denied.
///
/// Semantics (Postgres permissive model):
/// - If RLS is off → permit.
/// - If RLS is on, zero applicable policies → deny (default-deny).
/// - If RLS is on, ≥1 applicable policy → permit if *any* policy's USING
///   evaluates to TRUE for this object (OR-merge).
pub fn check_object_access(
    rls_enabled: bool,
    applicable: &[ObjectPolicy],
    obj: &Object,
    caller: &CallerCtx,
    command: ObjectPolicyCommand,
) -> Result<(), String> {
    if !rls_enabled {
        return Ok(());
    }
    if applicable.is_empty() {
        return Err(format!(
            "new row violates row-level security policy for table \"storage.objects\": \
             no policy applies for command {:?}",
            command
        ));
    }
    // OR-merge: pass if any applicable policy's USING is true.
    for p in applicable {
        if p.using.evaluate(obj, caller.uid, &caller.role) {
            return Ok(());
        }
    }
    Err(format!(
        "row-level security policy denied access to object \"{}\"",
        obj.path
    ))
}

/// Filter a list of objects, keeping only those that pass the RLS check.
pub fn filter_objects(
    rls_enabled: bool,
    applicable: &[ObjectPolicy],
    objects: Vec<Object>,
    caller: &CallerCtx,
) -> Vec<Object> {
    if !rls_enabled || applicable.is_empty() {
        if !rls_enabled {
            return objects;
        }
        // RLS on, no applicable policy → deny all
        return Vec::new();
    }
    objects
        .into_iter()
        .filter(|obj| {
            applicable
                .iter()
                .any(|p| p.using.evaluate(obj, caller.uid, &caller.role))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{BucketId, Object};
    use basin_common::ProjectId;
    use uuid::Uuid;

    fn make_object(owner: Option<Uuid>) -> Object {
        let bucket_id = BucketId::new();
        Object::new(
            bucket_id,
            "test/path.txt",
            10,
            None,
            serde_json::Value::Null,
            owner,
            "etag",
        )
    }

    #[test]
    fn owner_eq_auth_uid_passes_when_owner_matches() {
        let uid = Uuid::new_v4();
        let obj = make_object(Some(uid));
        let caller = CallerCtx::authenticated(uid);
        let policy = ObjectPolicy {
            name: "own_objects".into(),
            command: ObjectPolicyCommand::Select,
            applies_to_roles: vec![],
            using: ObjectUsing::OwnerEqAuthUid,
        };
        let result =
            check_object_access(true, &[policy], &obj, &caller, ObjectPolicyCommand::Select);
        assert!(result.is_ok());
    }

    #[test]
    fn owner_eq_auth_uid_denied_when_owner_differs() {
        let uid_a = Uuid::new_v4();
        let uid_b = Uuid::new_v4();
        let obj = make_object(Some(uid_a));
        let caller = CallerCtx::authenticated(uid_b);
        let policy = ObjectPolicy {
            name: "own_objects".into(),
            command: ObjectPolicyCommand::Select,
            applies_to_roles: vec![],
            using: ObjectUsing::OwnerEqAuthUid,
        };
        let result =
            check_object_access(true, &[policy], &obj, &caller, ObjectPolicyCommand::Select);
        assert!(result.is_err());
    }

    #[test]
    fn no_policy_applies_deny_all() {
        let obj = make_object(None);
        let caller = CallerCtx::anonymous();
        let result = check_object_access(true, &[], &obj, &caller, ObjectPolicyCommand::Select);
        assert!(result.is_err());
    }

    #[test]
    fn rls_off_bypasses_check() {
        let obj = make_object(None);
        let caller = CallerCtx::anonymous();
        // Even with no policies, RLS off → permit.
        let result = check_object_access(false, &[], &obj, &caller, ObjectPolicyCommand::Select);
        assert!(result.is_ok());
    }

    #[test]
    fn filter_objects_keeps_matching_rows() {
        let uid = Uuid::new_v4();
        let obj_mine = make_object(Some(uid));
        let obj_other = make_object(Some(Uuid::new_v4()));
        let caller = CallerCtx::authenticated(uid);
        let policy = ObjectPolicy {
            name: "own_objects".into(),
            command: ObjectPolicyCommand::Select,
            applies_to_roles: vec![],
            using: ObjectUsing::OwnerEqAuthUid,
        };
        let filtered = filter_objects(true, &[policy], vec![obj_mine.clone(), obj_other], &caller);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].owner, Some(uid));
    }

    #[test]
    fn object_rls_store_roundtrip() {
        let store = ObjectRlsStore::new();
        let project = ProjectId::new();

        // Not enabled by default.
        let (enabled, _) = store.applicable(
            &project,
            "objects",
            ObjectPolicyCommand::Select,
            "authenticated",
        );
        assert!(!enabled);

        store.set_enabled(&project, "objects", true);
        let policy = ObjectPolicy {
            name: "p1".into(),
            command: ObjectPolicyCommand::All,
            applies_to_roles: vec![],
            using: ObjectUsing::OwnerEqAuthUid,
        };
        assert!(store.create_policy(&project, "objects", policy.clone()));
        // Duplicate name is rejected.
        assert!(!store.create_policy(&project, "objects", policy));

        let (enabled, applicable) = store.applicable(
            &project,
            "objects",
            ObjectPolicyCommand::Select,
            "authenticated",
        );
        assert!(enabled);
        assert_eq!(applicable.len(), 1);

        // Drop removes the policy.
        assert!(store.drop_policy(&project, "objects", "p1"));
        let (_, applicable) = store.applicable(
            &project,
            "objects",
            ObjectPolicyCommand::Select,
            "authenticated",
        );
        assert!(applicable.is_empty());
    }

    #[test]
    fn using_parse_canonical_forms() {
        assert_eq!(ObjectUsing::parse("true"), Some(ObjectUsing::True));
        assert_eq!(ObjectUsing::parse("TRUE"), Some(ObjectUsing::True));
        assert_eq!(ObjectUsing::parse("false"), Some(ObjectUsing::False));
        assert_eq!(
            ObjectUsing::parse("owner = auth.uid()"),
            Some(ObjectUsing::OwnerEqAuthUid)
        );
        assert_eq!(
            ObjectUsing::parse("owner  =  auth.uid()"),
            Some(ObjectUsing::OwnerEqAuthUid)
        );
        assert_eq!(ObjectUsing::parse("garbage"), None);
    }
}
