//! Object-key construction. Centralized so project prefix enforcement is
//! impossible to bypass — all writers and listers must go through these
//! helpers.

use basin_common::{PartitionKey, ProjectId, TableName};
use chrono::{DateTime, Datelike, Utc};
use object_store::path::Path as ObjectPath;
use ulid::Ulid;

use crate::tier::Tier;

/// Path under which all project data lives.
pub(crate) const PROJECTS_SEGMENT: &str = "projects";
const TABLES_SEGMENT: &str = "tables";

/// Build the absolute object key for one new data file in the hot tier.
///
/// Layout:
/// `{root?}/projects/{project}/tables/{table}/data/{partition}/yyyy/mm/dd/{ulid}.parquet`
///
/// The partition segment is included so files for different partition keys
/// don't collide, even though pruning by partition is currently a list-time
/// concern.
pub(crate) fn data_file_key(
    root: Option<&ObjectPath>,
    project: &ProjectId,
    table: &TableName,
    partition: &PartitionKey,
    now: DateTime<Utc>,
    file_id: Ulid,
) -> ObjectPath {
    data_file_key_in_tier(root, project, table, partition, now, file_id, Tier::Hot)
}

/// Tier-aware variant of [`data_file_key`]. Cold-tier writes land under
/// `tables/{t}/cold/...` so a single object-store lifecycle rule on the
/// `cold/` prefix can flip the underlying storage class to the infrequent-access
/// tier (S3-IA on AWS S3, Tigris infrequent-access, or provider equivalent).
pub(crate) fn data_file_key_in_tier(
    root: Option<&ObjectPath>,
    project: &ProjectId,
    table: &TableName,
    partition: &PartitionKey,
    now: DateTime<Utc>,
    file_id: Ulid,
    tier: Tier,
) -> ObjectPath {
    let mut p = root.cloned().unwrap_or_else(|| ObjectPath::from(""));
    p = p.child(PROJECTS_SEGMENT);
    p = p.child(project.as_prefix());
    p = p.child(TABLES_SEGMENT);
    p = p.child(table.as_str());
    p = p.child(tier.segment());
    let part = partition.as_str();
    if part.is_empty() {
        p = p.child(PartitionKey::DEFAULT);
    } else {
        // `/` inside a PartitionKey is a Hive-style path separator (see
        // `PartitionKey::new` validation): callers pass `year=2026/month=04`
        // and we expand each segment into its own directory level so listings
        // and prefix-pruning work without percent-decoding.
        for segment in part.split('/') {
            p = p.child(segment);
        }
    }
    p = p.child(format!("{:04}", now.year()));
    p = p.child(format!("{:02}", now.month()));
    p = p.child(format!("{:02}", now.day()));
    p.child(format!("{file_id}.parquet"))
}

/// Prefix that all of one project+table's hot-tier data files live under.
/// Used by listing.
#[cfg(test)]
pub(crate) fn table_data_prefix(
    root: Option<&ObjectPath>,
    project: &ProjectId,
    table: &TableName,
) -> ObjectPath {
    table_tier_prefix(root, project, table, Tier::Hot)
}

/// Prefix that all of one project+table's files in a storage tier live under.
pub(crate) fn table_tier_prefix(
    root: Option<&ObjectPath>,
    project: &ProjectId,
    table: &TableName,
    tier: Tier,
) -> ObjectPath {
    let mut p = root.cloned().unwrap_or_else(|| ObjectPath::from(""));
    p = p.child(PROJECTS_SEGMENT);
    p = p.child(project.as_prefix());
    p = p.child(TABLES_SEGMENT);
    p = p.child(table.as_str());
    p.child(tier.segment())
}

/// Translate a hot-tier object key into its cold-tier sibling: replace the
/// single `tables/<t>/data/` segment with `tables/<t>/cold/`. Used by the
/// compactor when migrating an existing file between tiers.
///
/// Returns `None` if the input doesn't follow the canonical layout (i.e. has
/// no `tables/<t>/data/` segment) — callers treat that as a no-op.
pub(crate) fn rewrite_to_cold(path: &ObjectPath) -> Option<ObjectPath> {
    let s = path.as_ref();
    let needle = "/tables/";
    let mut search_from = 0usize;
    while let Some(idx) = s[search_from..].find(needle) {
        let abs = search_from + idx + needle.len();
        let rest = &s[abs..];
        let next_slash = rest.find('/')?;
        let tier_slot = abs + next_slash + 1;
        let tier_rest = &s[tier_slot..];
        let tier_end = tier_rest.find('/')?;
        if &tier_rest[..tier_end] == "data" {
            let mut out = String::with_capacity(s.len());
            out.push_str(&s[..tier_slot]);
            out.push_str("cold");
            out.push_str(&tier_rest[tier_end..]);
            return Some(ObjectPath::from(out));
        }
        search_from = abs;
    }
    // Bucket-relative form (no leading slash before `tables/`).
    if s.starts_with("tables/") {
        let after = "tables/".len();
        let rest = &s[after..];
        let next_slash = rest.find('/')?;
        let tier_slot = after + next_slash + 1;
        let tier_rest = &s[tier_slot..];
        let tier_end = tier_rest.find('/')?;
        if &tier_rest[..tier_end] == "data" {
            let mut out = String::with_capacity(s.len());
            out.push_str(&s[..tier_slot]);
            out.push_str("cold");
            out.push_str(&tier_rest[tier_end..]);
            return Some(ObjectPath::from(out));
        }
    }
    None
}

/// Prefix that all of one project's data lives under. Used for the safety-net
/// invariant test.
#[cfg(test)]
pub(crate) fn project_prefix(root: Option<&ObjectPath>, project: &ProjectId) -> ObjectPath {
    let mut p = root.cloned().unwrap_or_else(|| ObjectPath::from(""));
    p = p.child(PROJECTS_SEGMENT);
    p.child(project.as_prefix())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn key_layout_includes_project_and_date() {
        let project = ProjectId::new();
        let table = TableName::new("orders").unwrap();
        let part = PartitionKey::new("region:us").unwrap();
        let now = Utc.with_ymd_and_hms(2026, 4, 30, 12, 0, 0).unwrap();
        let id = Ulid::new();
        let key = data_file_key(None, &project, &table, &part, now, id);
        let s = key.as_ref();
        let expected_prefix =
            format!("projects/{project}/tables/orders/data/region:us/2026/04/30/{id}.parquet");
        assert_eq!(s, expected_prefix);
    }

    #[test]
    fn key_with_root_prefix() {
        let project = ProjectId::new();
        let table = TableName::new("t").unwrap();
        let part = PartitionKey::default_key();
        let root = ObjectPath::from("warehouse");
        let key = data_file_key(
            Some(&root),
            &project,
            &table,
            &part,
            Utc::now(),
            Ulid::new(),
        );
        assert!(key.as_ref().starts_with("warehouse/projects/"));
    }

    /// "Fuzz-ish": across many random combinations the produced path always
    /// begins with `projects/{project}/`. This is the load-bearing isolation
    /// invariant for this crate.
    #[test]
    fn path_helper_always_includes_project_prefix() {
        for _ in 0..256 {
            let project = ProjectId::new();
            let table_name = format!("t{}", Ulid::new().to_string().to_lowercase());
            let table = TableName::new(&table_name[..table_name.len().min(63)]).unwrap();
            let part = if Ulid::new().0 % 2 == 0 {
                PartitionKey::default_key()
            } else {
                PartitionKey::new(format!("p{}", Ulid::new())).unwrap()
            };
            let key = data_file_key(None, &project, &table, &part, Utc::now(), Ulid::new());
            let prefix = format!("projects/{project}/");
            assert!(
                key.as_ref().starts_with(&prefix),
                "key {} missing project prefix {}",
                key,
                prefix
            );

            let listed = table_data_prefix(None, &project, &table);
            assert!(listed.as_ref().starts_with(&prefix));

            let tprefix = project_prefix(None, &project);
            assert_eq!(tprefix.as_ref(), format!("projects/{project}"));
        }
    }
}
