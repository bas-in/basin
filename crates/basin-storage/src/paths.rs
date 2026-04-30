//! Object-key construction. Centralized so tenant prefix enforcement is
//! impossible to bypass — all writers and listers must go through these
//! helpers.

use basin_common::{PartitionKey, TableName, TenantId};
use chrono::{DateTime, Datelike, Utc};
use object_store::path::Path as ObjectPath;
use ulid::Ulid;

/// Path under which all tenant data lives.
pub(crate) const TENANTS_SEGMENT: &str = "tenants";
const TABLES_SEGMENT: &str = "tables";
const DATA_SEGMENT: &str = "data";

/// Build the absolute object key for one new data file.
///
/// Layout:
/// `{root?}/tenants/{tenant}/tables/{table}/data/{partition}/yyyy/mm/dd/{ulid}.parquet`
///
/// The partition segment is included so files for different partition keys
/// don't collide, even though pruning by partition is currently a list-time
/// concern.
pub(crate) fn data_file_key(
    root: Option<&ObjectPath>,
    tenant: &TenantId,
    table: &TableName,
    partition: &PartitionKey,
    now: DateTime<Utc>,
    file_id: Ulid,
) -> ObjectPath {
    let mut p = root.cloned().unwrap_or_else(|| ObjectPath::from(""));
    p = p.child(TENANTS_SEGMENT);
    p = p.child(tenant.as_prefix());
    p = p.child(TABLES_SEGMENT);
    p = p.child(table.as_str());
    p = p.child(DATA_SEGMENT);
    let part = partition.as_str();
    let part_segment = if part.is_empty() {
        PartitionKey::DEFAULT
    } else {
        part
    };
    p = p.child(part_segment);
    p = p.child(format!("{:04}", now.year()));
    p = p.child(format!("{:02}", now.month()));
    p = p.child(format!("{:02}", now.day()));
    p.child(format!("{file_id}.parquet"))
}

/// Prefix that all of one tenant+table's data files live under. Used by
/// listing.
pub(crate) fn table_data_prefix(
    root: Option<&ObjectPath>,
    tenant: &TenantId,
    table: &TableName,
) -> ObjectPath {
    let mut p = root.cloned().unwrap_or_else(|| ObjectPath::from(""));
    p = p.child(TENANTS_SEGMENT);
    p = p.child(tenant.as_prefix());
    p = p.child(TABLES_SEGMENT);
    p = p.child(table.as_str());
    p.child(DATA_SEGMENT)
}

/// Prefix that all of one tenant's data lives under. Used for the safety-net
/// invariant test.
#[cfg(test)]
pub(crate) fn tenant_prefix(root: Option<&ObjectPath>, tenant: &TenantId) -> ObjectPath {
    let mut p = root.cloned().unwrap_or_else(|| ObjectPath::from(""));
    p = p.child(TENANTS_SEGMENT);
    p.child(tenant.as_prefix())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn key_layout_includes_tenant_and_date() {
        let tenant = TenantId::new();
        let table = TableName::new("orders").unwrap();
        let part = PartitionKey::new("region:us").unwrap();
        let now = Utc.with_ymd_and_hms(2026, 4, 30, 12, 0, 0).unwrap();
        let id = Ulid::new();
        let key = data_file_key(None, &tenant, &table, &part, now, id);
        let s = key.as_ref();
        let expected_prefix = format!(
            "tenants/{tenant}/tables/orders/data/region:us/2026/04/30/{id}.parquet"
        );
        assert_eq!(s, expected_prefix);
    }

    #[test]
    fn key_with_root_prefix() {
        let tenant = TenantId::new();
        let table = TableName::new("t").unwrap();
        let part = PartitionKey::default_key();
        let root = ObjectPath::from("warehouse");
        let key = data_file_key(
            Some(&root),
            &tenant,
            &table,
            &part,
            Utc::now(),
            Ulid::new(),
        );
        assert!(key.as_ref().starts_with("warehouse/tenants/"));
    }

    /// "Fuzz-ish": across many random combinations the produced path always
    /// begins with `tenants/{tenant}/`. This is the load-bearing isolation
    /// invariant for this crate.
    #[test]
    fn path_helper_always_includes_tenant_prefix() {
        for _ in 0..256 {
            let tenant = TenantId::new();
            let table_name = format!("t{}", Ulid::new().to_string().to_lowercase());
            let table = TableName::new(&table_name[..table_name.len().min(63)]).unwrap();
            let part = if Ulid::new().0 % 2 == 0 {
                PartitionKey::default_key()
            } else {
                PartitionKey::new(format!("p{}", Ulid::new())).unwrap()
            };
            let key = data_file_key(
                None,
                &tenant,
                &table,
                &part,
                Utc::now(),
                Ulid::new(),
            );
            let prefix = format!("tenants/{tenant}/");
            assert!(
                key.as_ref().starts_with(&prefix),
                "key {} missing tenant prefix {}",
                key,
                prefix
            );

            let listed = table_data_prefix(None, &tenant, &table);
            assert!(listed.as_ref().starts_with(&prefix));

            let tprefix = tenant_prefix(None, &tenant);
            assert_eq!(tprefix.as_ref(), format!("tenants/{tenant}"));
        }
    }
}
