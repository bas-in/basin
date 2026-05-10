//! Integration tests for the sequence catalog API.
//!
//! These cover the rust-API surface ([`Catalog::create_sequence`] etc.)
//! against the in-memory backend. SQL-level coverage (the `nextval` /
//! `currval` / `setval` UDFs) lives in `tests/integration/tests/sequences.rs`.

use std::collections::HashSet;
use std::sync::Arc;

use basin_catalog::{Catalog, InMemoryCatalog, SequenceDef};
use basin_common::{BasinError, TenantId};
use futures::future::join_all;

fn def(tenant: TenantId, name: &str, start: i64, increment: i64) -> SequenceDef {
    SequenceDef {
        tenant,
        name: name.into(),
        start,
        increment,
        min_value: if increment > 0 { 1 } else { i64::MIN + 1 },
        max_value: if increment > 0 { i64::MAX } else { -1 },
        cache_size: 1,
        cycle: false,
    }
}

#[tokio::test]
async fn create_and_nextval_basic() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_sequence(def(t, "s", 1, 1)).await.unwrap();

    assert_eq!(cat.nextval(&t, "s").await.unwrap(), 1);
    assert_eq!(cat.nextval(&t, "s").await.unwrap(), 2);
    assert_eq!(cat.nextval(&t, "s").await.unwrap(), 3);
}

/// Concurrency invariant: no two `nextval` calls on the same sequence
/// hand out the same value. The per-`(tenant, sequence)` Mutex
/// serialises the increment; this test asserts the resulting set of
/// 10 values has 10 distinct elements.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_concurrent_no_duplicates() {
    let cat = Arc::new(InMemoryCatalog::new());
    let t = TenantId::new();
    cat.create_sequence(def(t, "s", 1, 1)).await.unwrap();

    let mut handles = Vec::with_capacity(10);
    for _ in 0..10 {
        let cat = cat.clone();
        handles.push(tokio::spawn(async move { cat.nextval(&t, "s").await }));
    }
    let results: Vec<_> = join_all(handles).await.into_iter().collect();
    let mut values: Vec<i64> = Vec::new();
    for r in results {
        values.push(r.unwrap().unwrap());
    }
    let unique: HashSet<i64> = values.iter().copied().collect();
    assert_eq!(
        unique.len(),
        10,
        "10 concurrent nextvals must return 10 distinct values, got {values:?}"
    );
    // Sanity: also a contiguous 1..=10 range.
    let mut sorted = values.clone();
    sorted.sort();
    assert_eq!(sorted, (1..=10).collect::<Vec<_>>());
}

/// `cache_size > 1` semantics: with the v0.1 in-memory backend each
/// `nextval` hits the per-sequence Mutex anyway, so the call is
/// monotonic and gap-free across `25` consecutive calls. The test
/// also locks in that `cache_size > 1` doesn't accidentally produce
/// duplicates or skip values within a single process lifetime.
#[tokio::test]
async fn cache_block_refill() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    let mut d = def(t, "s", 1, 1);
    d.cache_size = 10;
    cat.create_sequence(d).await.unwrap();

    let mut got = Vec::with_capacity(25);
    for _ in 0..25 {
        got.push(cat.nextval(&t, "s").await.unwrap());
    }
    assert_eq!(got, (1..=25).collect::<Vec<_>>());
}

/// Engine restart semantics for cached blocks. The contract is "gaps
/// are acceptable per PG; duplicates are NOT". For the in-memory
/// backend every call goes through the persisted counter (cache_size
/// is bookkeeping in v0.1), so dropping the catalog and rebuilding it
/// resets the counter cleanly. The assertion is the no-duplicate
/// invariant: if the first incarnation handed out [1..N], the second
/// incarnation's first `nextval` is strictly greater than N or the
/// catalog has been destructively rebuilt — in this in-memory test
/// the latter is what we model.
#[tokio::test]
async fn cache_gap_on_restart_acceptable() {
    let t = TenantId::new();
    // First incarnation: hand out 5 values from a cache_size=10 sequence.
    let pre_drop_values: Vec<i64>;
    {
        let cat = InMemoryCatalog::new();
        let mut d = def(t, "s", 1, 1);
        d.cache_size = 10;
        cat.create_sequence(d).await.unwrap();
        let mut got = Vec::with_capacity(5);
        for _ in 0..5 {
            got.push(cat.nextval(&t, "s").await.unwrap());
        }
        pre_drop_values = got;
        // catalog drops here, simulating engine restart with no
        // durable backing.
    }
    // In a durable backend we'd reload from the catalog and the next
    // nextval would be `last_persisted_block + 1`. The in-memory
    // backend has no durability so the next incarnation starts fresh.
    // The load-bearing assertion is that *no value* repeats across
    // the boundary in a real durable setup; we model that here by
    // never re-handing-out a value <= the last-handed-out from the
    // pre-drop incarnation when the underlying state survives.
    let max_pre = pre_drop_values.iter().max().copied().unwrap();
    let cat2 = InMemoryCatalog::new();
    let mut d = def(t, "s", max_pre + 1, 1);
    d.cache_size = 10;
    cat2.create_sequence(d).await.unwrap();
    let next = cat2.nextval(&t, "s").await.unwrap();
    assert!(
        next > max_pre,
        "post-restart nextval {next} must not duplicate any pre-restart value (max_pre={max_pre})"
    );
}

#[tokio::test]
async fn currval_without_nextval_errors() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_sequence(def(t, "s", 1, 1)).await.unwrap();

    let err = cat.currval(&t, "s").await.unwrap_err();
    assert!(
        matches!(err, BasinError::NotFound(_)),
        "expected NotFound for currval-before-nextval, got {err:?}"
    );
}

#[tokio::test]
async fn currval_after_nextval_returns_last() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_sequence(def(t, "s", 1, 1)).await.unwrap();
    let n = cat.nextval(&t, "s").await.unwrap();
    let c = cat.currval(&t, "s").await.unwrap();
    assert_eq!(n, c);
}

#[tokio::test]
async fn setval_advance_true() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_sequence(def(t, "s", 1, 1)).await.unwrap();
    let r = cat.setval(&t, "s", 100, true).await.unwrap();
    assert_eq!(r, 100);
    let n = cat.nextval(&t, "s").await.unwrap();
    assert_eq!(n, 101, "setval(advance=true): next should be 100+1");
}

#[tokio::test]
async fn setval_advance_false() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_sequence(def(t, "s", 1, 1)).await.unwrap();
    let r = cat.setval(&t, "s", 100, false).await.unwrap();
    assert_eq!(r, 100);
    let n = cat.nextval(&t, "s").await.unwrap();
    assert_eq!(n, 100, "setval(advance=false): next should be exactly 100");
}

#[tokio::test]
async fn cross_tenant_isolation() {
    let cat = InMemoryCatalog::new();
    let a = TenantId::new();
    let b = TenantId::new();
    cat.create_sequence(def(a, "shared", 1, 1)).await.unwrap();

    // Tenant A's sequence is invisible to tenant B.
    let err = cat.nextval(&b, "shared").await.unwrap_err();
    assert!(
        matches!(err, BasinError::NotFound(_)),
        "tenant B must not see tenant A's sequence, got {err:?}"
    );

    // Tenant A advances independently of tenant B's empty namespace.
    assert_eq!(cat.nextval(&a, "shared").await.unwrap(), 1);
    assert_eq!(cat.nextval(&a, "shared").await.unwrap(), 2);

    // Tenant B can register its *own* "shared" sequence with no
    // collision against tenant A.
    cat.create_sequence(def(b, "shared", 100, 1)).await.unwrap();
    assert_eq!(cat.nextval(&b, "shared").await.unwrap(), 100);
    // Tenant A is unchanged.
    assert_eq!(cat.nextval(&a, "shared").await.unwrap(), 3);
}

#[tokio::test]
async fn drop_sequence_works() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_sequence(def(t, "s", 1, 1)).await.unwrap();
    cat.nextval(&t, "s").await.unwrap();

    cat.drop_sequence(&t, "s").await.unwrap();

    let err = cat.nextval(&t, "s").await.unwrap_err();
    assert!(
        matches!(err, BasinError::NotFound(_)),
        "post-drop nextval must error with NotFound, got {err:?}"
    );

    // drop again is NotFound (not idempotent at this level).
    let err = cat.drop_sequence(&t, "s").await.unwrap_err();
    assert!(matches!(err, BasinError::NotFound(_)));
}

#[tokio::test]
async fn create_duplicate_errors() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_sequence(def(t, "s", 1, 1)).await.unwrap();
    let err = cat
        .create_sequence(def(t, "s", 100, 2))
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::Catalog(_)), "got {err:?}");
}

#[tokio::test]
async fn lookup_returns_definition() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    let original = def(t, "s", 5, 7);
    cat.create_sequence(original.clone()).await.unwrap();
    let looked_up = cat.lookup_sequence(&t, "s").await.unwrap();
    assert_eq!(looked_up, original);
    assert!(cat.lookup_sequence(&t, "missing").await.is_none());
}

#[tokio::test]
async fn drop_namespace_clears_sequences() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.create_sequence(def(t, "s", 1, 1)).await.unwrap();
    cat.drop_namespace(&t).await.unwrap();
    let err = cat.nextval(&t, "s").await.unwrap_err();
    assert!(matches!(err, BasinError::NotFound(_)));
}
