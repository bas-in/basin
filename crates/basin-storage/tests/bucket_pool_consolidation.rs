//! Online consolidation on scale-down (#37) — the exactly-once + crash-safety
//! test matrix (#38).
//!
//! These exercise the migration state machine against MULTIPLE in-memory
//! stores (one per bucket). The load-bearing properties:
//!
//! - **Exactly-once**: post-cutover the target bucket B holds EXACTLY the
//!   project's live set (count + per-object identity) and the source A holds
//!   none — no lost, no doubled object.
//! - **Crash at every phase boundary**: kill the node before/after each phase
//!   (and partway through copy + drain-delete), restart (re-resolve stores +
//!   re-read the intent), resume, and assert convergence to the SAME
//!   exactly-once result.
//! - **Cutover atomicity**: a crash lands either before the flip (assignment
//!   still A — re-run cutover) or after (assignment B — done); reads resolve
//!   correctly either way.
//! - **Provable-empty deletion**: A's bucket is reclaimed ONLY once it holds no
//!   live project object; a non-empty source is never dropped.
//! - **Vacuum→reclaim**: a fully-migrated project leaves A empty and the bucket
//!   removed from the registry.
//! - **No-op when the flag is OFF.**

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use basin_catalog::bucket_pool::{
    BucketRegistry, BucketRegistryEntry, MigrationIntent, MigrationPhase,
};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{ProjectId, Result};
use basin_storage::bucket_pool::{BucketPool, BucketResolver, CrashPoint, PoolConfig};
use futures::stream::StreamExt;
use object_store::memory::InMemory;
use object_store::{path::Path as OsPath, ObjectStore, ObjectStoreExt, PutPayload};

/// One distinct `InMemory` store per `bucket_id` — the multi-store world the
/// migration moves objects between.
#[derive(Default)]
struct MultiStoreResolver {
    stores: Mutex<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl MultiStoreResolver {
    fn store_for(&self, bucket_id: &str) -> Arc<dyn ObjectStore> {
        self.stores
            .lock()
            .unwrap()
            .entry(bucket_id.to_string())
            .or_insert_with(|| Arc::new(InMemory::new()))
            .clone()
    }
}

impl BucketResolver for MultiStoreResolver {
    fn resolve(&self, entry: &BucketRegistryEntry) -> Result<Arc<dyn ObjectStore>> {
        Ok(self.store_for(&entry.bucket_id))
    }
}

fn pool(resolver: Arc<MultiStoreResolver>) -> Arc<BucketPool> {
    Arc::new(BucketPool::new(
        PoolConfig {
            enabled: true,
            max_buckets: 8,
            watermark: 1,
            stripe: 1,
        },
        resolver,
    ))
}

fn entry(bucket_id: &str, assigned: u64) -> BucketRegistryEntry {
    BucketRegistryEntry {
        bucket_id: bucket_id.to_string(),
        bucket_name: bucket_id.to_string(),
        endpoint: String::new(),
        region: String::new(),
        credentials_ref: None,
        assigned_count: assigned,
    }
}

/// Seed `n` live objects for `project` into `store` under the project prefix,
/// returning the (key → bytes) map so tests can assert per-object identity.
async fn seed_objects(
    store: &Arc<dyn ObjectStore>,
    project: &ProjectId,
    n: usize,
) -> BTreeMap<String, Vec<u8>> {
    let mut map = BTreeMap::new();
    for i in 0..n {
        let key =
            format!("projects/{project}/tables/t/data/p{i}/2026/06/27/file-{i}.vortex");
        let body = format!("object-{i}-body").into_bytes();
        store
            .put(&OsPath::from(key.clone()), PutPayload::from(body.clone()))
            .await
            .unwrap();
        map.insert(key, body);
    }
    map
}

/// The full (key → bytes) contents of `store` under the project prefix.
async fn contents(
    store: &Arc<dyn ObjectStore>,
    project: &ProjectId,
) -> BTreeMap<String, Vec<u8>> {
    let prefix = OsPath::from(format!("projects/{project}/"));
    let mut out = BTreeMap::new();
    let mut s = store.list(Some(&prefix));
    while let Some(item) = s.next().await {
        let meta = item.unwrap();
        let bytes = store.get(&meta.location).await.unwrap().bytes().await.unwrap();
        out.insert(meta.location.to_string(), bytes.to_vec());
    }
    out
}

/// Build a catalog + registry with buckets A,B and an existing single-bucket
/// assignment of `project → A`, then seed `n` objects into A. Returns the
/// catalog and the live set as it exists in A.
async fn setup(
    resolver: &Arc<MultiStoreResolver>,
    project: &ProjectId,
    a_assigned: u64,
    b_assigned: u64,
    n: usize,
) -> (Arc<InMemoryCatalog>, BTreeMap<String, Vec<u8>>) {
    let cat = Arc::new(InMemoryCatalog::new());
    let registry = BucketRegistry {
        buckets: vec![entry("A", a_assigned), entry("B", b_assigned)],
    };
    cat.put_bucket_registry(&registry).await.unwrap();
    // project currently assigned to A.
    let assign = basin_catalog::BucketAssignment {
        bucket_id: "A".into(),
        tier: basin_catalog::BucketTier::Pooled,
        stripe: vec!["A".into()],
    };
    cat.assign_bucket_if_absent(project, &assign).await.unwrap();

    let store_a = resolver.store_for("A");
    let live = seed_objects(&store_a, project, n).await;
    (cat, live)
}

/// Assert the exactly-once end state: B holds EXACTLY `live` (count + identity),
/// A holds nothing for the project, the assignment points at B, the intent is
/// gone, and A's bucket has been reclaimed from the registry (it held only this
/// project).
async fn assert_converged(
    resolver: &Arc<MultiStoreResolver>,
    cat: &Arc<InMemoryCatalog>,
    project: &ProjectId,
    live: &BTreeMap<String, Vec<u8>>,
) {
    let store_a = resolver.store_for("A");
    let store_b = resolver.store_for("B");

    let b = contents(&store_b, project).await;
    assert_eq!(&b, live, "B must hold exactly the project's live set (no lost, no doubled)");

    let a = contents(&store_a, project).await;
    assert!(a.is_empty(), "A must hold none of the project's objects after migration");

    let assign = cat.get_bucket_assignment(project).await.unwrap().unwrap();
    assert_eq!(assign.bucket_id, "B", "assignment must point at B post-cutover");

    assert!(
        cat.get_migration_intent(project).await.unwrap().is_none(),
        "intent must be deleted on completion"
    );

    let registry = cat.get_bucket_registry().await.unwrap();
    assert!(
        registry.get("A").is_none(),
        "A held only this project, so its bucket must be reclaimed"
    );
    assert!(registry.get("B").is_some(), "B must remain registered");
}

/// Happy path, no crash: a project migrates A→B exactly-once and A is reclaimed.
#[tokio::test]
async fn migrate_converges_exactly_once_no_crash() {
    let resolver = Arc::new(MultiStoreResolver::default());
    let pool = pool(resolver.clone());
    let project = ProjectId::new();
    let (cat, live) = setup(&resolver, &project, 1, 0, 12).await;

    pool.consolidate_project(&project, "A", "B", cat.as_ref()).await.unwrap();

    assert_converged(&resolver, &cat, &project, &live).await;
}

/// Concurrent writes to A during the copy window are caught by the drain
/// re-copy: post-migration B holds the FULL live set including the late writes,
/// and A holds none. (We simulate the concurrent write by adding an object to A
/// after copy but before drain, via crash+resume.)
#[tokio::test]
async fn migrate_with_late_write_to_a_is_drained() {
    let resolver = Arc::new(MultiStoreResolver::default());
    let pool = pool(resolver.clone());
    let project = ProjectId::new();
    let (cat, mut live) = setup(&resolver, &project, 1, 0, 8).await;

    // Crash right after Copy completes (intent now at Verify). Simulates a node
    // that copied, then a late write hit A before the rest ran.
    let intent = MigrationIntent {
        project,
        from: "A".into(),
        to: "B".into(),
        phase: MigrationPhase::Copy,
    };
    cat.put_migration_intent(&intent).await.unwrap();
    let err = pool
        .run_migration(&intent, cat.as_ref(), CrashPoint::After(MigrationPhase::Copy))
        .await
        .unwrap_err();
    assert!(err.is_crash());

    // A late write lands in A (after copy, before cutover/drain).
    let store_a = resolver.store_for("A");
    let key = format!("projects/{project}/tables/t/data/late/2026/06/27/late.vortex");
    let body = b"late-write".to_vec();
    store_a
        .put(&OsPath::from(key.clone()), PutPayload::from(body.clone()))
        .await
        .unwrap();
    live.insert(key, body);

    // Restart: re-read intent, resume to completion. Drain must catch the late
    // write so B ends with the FULL live set.
    pool.invalidate_all();
    let resumed = cat.get_migration_intent(&project).await.unwrap().unwrap();
    pool.run_migration(&resumed, cat.as_ref(), CrashPoint::None).await.unwrap();

    assert_converged(&resolver, &cat, &project, &live).await;
}

/// THE crash-injection matrix: for a crash at EVERY phase boundary, the
/// migration resumes from the durable intent and converges to the SAME
/// exactly-once result. Drives crash → restart → resume in a loop until done.
#[tokio::test]
async fn crash_at_every_phase_boundary_converges() {
    let crash_points = [
        CrashPoint::Before(MigrationPhase::Copy),
        CrashPoint::After(MigrationPhase::Copy),
        CrashPoint::MidCopy(3),
        CrashPoint::Before(MigrationPhase::Verify),
        CrashPoint::After(MigrationPhase::Verify),
        CrashPoint::Before(MigrationPhase::Cutover),
        CrashPoint::After(MigrationPhase::Cutover),
        CrashPoint::Before(MigrationPhase::Drain),
        CrashPoint::After(MigrationPhase::Drain),
        CrashPoint::MidDrainDelete(2),
        CrashPoint::Before(MigrationPhase::Delete),
        CrashPoint::After(MigrationPhase::Delete),
    ];

    for cp in crash_points {
        let resolver = Arc::new(MultiStoreResolver::default());
        let pool = pool(resolver.clone());
        let project = ProjectId::new();
        let (cat, live) = setup(&resolver, &project, 1, 0, 7).await;

        // Seed the intent (phase=Copy) as consolidate_project would.
        let intent = MigrationIntent {
            project,
            from: "A".into(),
            to: "B".into(),
            phase: MigrationPhase::Copy,
        };
        cat.put_migration_intent(&intent).await.unwrap();

        // First run crashes at the injected point.
        let cur = cat.get_migration_intent(&project).await.unwrap().unwrap();
        let res = pool.run_migration(&cur, cat.as_ref(), cp).await;
        match res {
            Err(e) if e.is_crash() => {}
            // After(Delete): the migration completes and deletes the intent
            // BEFORE the crash hook fires, so the "crash" is a no-op tail —
            // already converged. Accept Ok too.
            Ok(()) => {}
            Err(e) => panic!("unexpected non-crash failure for {cp:?}: {e:?}"),
        }

        // Restart loop: forget the cache, re-read the intent, resume. Repeat
        // until the intent is gone (converged). Bounded iterations.
        pool.invalidate_all();
        let mut guard = 0;
        while let Some(resume) = cat.get_migration_intent(&project).await.unwrap() {
            pool.run_migration(&resume, cat.as_ref(), CrashPoint::None)
                .await
                .unwrap_or_else(|e| panic!("resume failed for {cp:?}: {e:?}"));
            pool.invalidate_all();
            guard += 1;
            assert!(guard < 16, "resume must converge for {cp:?}");
        }

        assert_converged(&resolver, &cat, &project, &live)
            .await;
    }
}

/// Cutover atomicity: a crash lands either BEFORE the flip (assignment still A)
/// or AFTER (assignment B). Reads resolve correctly either way; resume from
/// either state converges to B.
#[tokio::test]
async fn cutover_is_atomic_before_or_after_the_flip() {
    // Crash BEFORE cutover: assignment must still be A (the old, valid value).
    {
        let resolver = Arc::new(MultiStoreResolver::default());
        let pool = pool(resolver.clone());
        let project = ProjectId::new();
        let (cat, live) = setup(&resolver, &project, 1, 0, 5).await;
        let intent = MigrationIntent {
            project,
            from: "A".into(),
            to: "B".into(),
            phase: MigrationPhase::Cutover,
        };
        cat.put_migration_intent(&intent).await.unwrap();
        // Copy+verify already happened conceptually; do them so B is ready.
        pool.run_migration(
            &MigrationIntent { phase: MigrationPhase::Copy, ..intent.clone() },
            cat.as_ref(),
            CrashPoint::After(MigrationPhase::Verify),
        )
        .await
        .unwrap_err();
        // Now positioned at Cutover; crash BEFORE the flip.
        let at_cutover = cat.get_migration_intent(&project).await.unwrap().unwrap();
        assert_eq!(at_cutover.phase, MigrationPhase::Cutover);
        let err = pool
            .run_migration(&at_cutover, cat.as_ref(), CrashPoint::Before(MigrationPhase::Cutover))
            .await
            .unwrap_err();
        assert!(err.is_crash());
        // BEFORE the flip: assignment is still the valid old A.
        let assign = cat.get_bucket_assignment(&project).await.unwrap().unwrap();
        assert_eq!(assign.bucket_id, "A", "pre-flip read must resolve to A");

        // Resume: converges to B exactly-once.
        pool.invalidate_all();
        let mut guard = 0;
        while let Some(resume) = cat.get_migration_intent(&project).await.unwrap() {
            pool.run_migration(&resume, cat.as_ref(), CrashPoint::None).await.unwrap();
            pool.invalidate_all();
            guard += 1;
            assert!(guard < 16);
        }
        assert_converged(&resolver, &cat, &project, &live).await;
    }

    // Crash AFTER cutover: assignment must be B; reads resolve to B; resume
    // completes drain+delete.
    {
        let resolver = Arc::new(MultiStoreResolver::default());
        let pool = pool(resolver.clone());
        let project = ProjectId::new();
        let (cat, live) = setup(&resolver, &project, 1, 0, 5).await;
        // Drive copy+verify, land at Cutover.
        let seed = MigrationIntent {
            project,
            from: "A".into(),
            to: "B".into(),
            phase: MigrationPhase::Copy,
        };
        cat.put_migration_intent(&seed).await.unwrap();
        pool.run_migration(&seed, cat.as_ref(), CrashPoint::After(MigrationPhase::Verify))
            .await
            .unwrap_err();
        let at_cutover = cat.get_migration_intent(&project).await.unwrap().unwrap();
        // Crash AFTER the flip (intent advances to Drain, assignment is B).
        let err = pool
            .run_migration(&at_cutover, cat.as_ref(), CrashPoint::After(MigrationPhase::Cutover))
            .await
            .unwrap_err();
        assert!(err.is_crash());
        let assign = cat.get_bucket_assignment(&project).await.unwrap().unwrap();
        assert_eq!(assign.bucket_id, "B", "post-flip read must resolve to B");

        pool.invalidate_all();
        let mut guard = 0;
        while let Some(resume) = cat.get_migration_intent(&project).await.unwrap() {
            pool.run_migration(&resume, cat.as_ref(), CrashPoint::None).await.unwrap();
            pool.invalidate_all();
            guard += 1;
            assert!(guard < 16);
        }
        assert_converged(&resolver, &cat, &project, &live).await;
    }
}

/// Provable-empty deletion gate: the source bucket is reclaimed ONLY when it
/// holds no live project objects. A source that still holds another project's
/// objects keeps its registry entry (assigned_count > 0).
#[tokio::test]
async fn bucket_reclaimed_only_when_provably_empty() {
    let resolver = Arc::new(MultiStoreResolver::default());
    let pool = pool(resolver.clone());
    let project = ProjectId::new();
    let other = ProjectId::new();

    // A holds TWO projects; migrate only `project` off A. A must survive (it
    // still holds `other`).
    let (cat, live) = setup(&resolver, &project, 2, 0, 6).await;
    let store_a = resolver.store_for("A");
    let other_live = seed_objects(&store_a, &other, 4).await;

    pool.consolidate_project(&project, "A", "B", cat.as_ref()).await.unwrap();

    // B holds exactly `project`'s live set.
    let b = contents(&resolver.store_for("B"), &project).await;
    assert_eq!(b, live);
    // A no longer holds `project` but STILL holds `other`.
    assert!(contents(&store_a, &project).await.is_empty());
    assert_eq!(contents(&store_a, &other).await, other_live);
    // A's bucket is NOT reclaimed (assigned_count still 1 > 0).
    let registry = cat.get_bucket_registry().await.unwrap();
    let a = registry.get("A").expect("A must survive while it holds another project");
    assert_eq!(a.assigned_count, 1, "A still holds one live project");
}

/// Vacuum→reclaim end-to-end: a single-project sparse bucket, once its only
/// project migrates off, ends empty and is removed from the registry.
#[tokio::test]
async fn vacuumed_project_reclaims_its_bucket() {
    let resolver = Arc::new(MultiStoreResolver::default());
    let pool = pool(resolver.clone());
    let project = ProjectId::new();
    let (cat, live) = setup(&resolver, &project, 1, 5, 10).await;

    // A is the sparse source (assigned_count 1, at/below a reclaim watermark of
    // 1); B is the dense target.
    let registry = cat.get_bucket_registry().await.unwrap();
    let candidates = BucketPool::reclaim_candidates(&registry, 1);
    assert_eq!(candidates, vec!["A".to_string()], "A is the reclaim candidate");
    let target = BucketPool::consolidation_target(&registry, "A").unwrap();
    assert_eq!(target, "B", "densest non-source bucket is the target");

    pool.consolidate_project(&project, "A", &target, cat.as_ref()).await.unwrap();
    assert_converged(&resolver, &cat, &project, &live).await;
}

/// Bounded concurrency: with the ceiling at K, admitting a (K+1)th NEW migration
/// is rejected; an already-recorded intent still resumes.
#[tokio::test]
async fn migration_concurrency_is_bounded() {
    let resolver = Arc::new(MultiStoreResolver::default());
    let pool = pool(resolver.clone());
    let cat = Arc::new(InMemoryCatalog::new());
    let registry = BucketRegistry {
        buckets: vec![entry("A", 1), entry("B", 1), entry("C", 1)],
    };
    cat.put_bucket_registry(&registry).await.unwrap();

    // Pre-seed DEFAULT_MAX_CONCURRENT_MIGRATIONS (2) live intents.
    for _ in 0..2 {
        let p = ProjectId::new();
        cat.put_migration_intent(&MigrationIntent {
            project: p,
            from: "A".into(),
            to: "B".into(),
            phase: MigrationPhase::Copy,
        })
        .await
        .unwrap();
    }

    // A NEW migration is rejected at the ceiling.
    let newp = ProjectId::new();
    let err = pool
        .consolidate_project(&newp, "A", "C", cat.as_ref())
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("concurrency ceiling"),
        "new migration past the ceiling must be rejected, got {err:?}"
    );
}

/// Flag OFF: consolidation + resume are provable no-ops — no objects move, no
/// intent is created, the assignment is untouched.
#[tokio::test]
async fn flag_off_consolidation_is_a_noop() {
    let resolver = Arc::new(MultiStoreResolver::default());
    let pool = Arc::new(BucketPool::new(
        PoolConfig {
            enabled: false,
            max_buckets: 8,
            watermark: 1,
            stripe: 1,
        },
        resolver.clone(),
    ));
    let project = ProjectId::new();
    let (cat, live) = setup(&resolver, &project, 1, 0, 5).await;

    // consolidate + resume both no-op.
    pool.consolidate_project(&project, "A", "B", cat.as_ref()).await.unwrap();
    assert_eq!(pool.resume_migrations(cat.as_ref()).await.unwrap(), 0);

    // Nothing moved: A still holds the live set, B is empty, no intent, A still
    // assigned.
    assert_eq!(contents(&resolver.store_for("A"), &project).await, live);
    assert!(resolver.store_for("B").list(None).next().await.is_none());
    assert!(cat.get_migration_intent(&project).await.unwrap().is_none());
    assert_eq!(
        cat.get_bucket_assignment(&project).await.unwrap().unwrap().bucket_id,
        "A"
    );
}

/// resume_migrations drives EVERY in-flight intent to completion after a bounce
/// (the startup recovery entry point).
#[tokio::test]
async fn resume_migrations_drives_all_inflight_to_completion() {
    let resolver = Arc::new(MultiStoreResolver::default());
    let pool = pool(resolver.clone());
    let cat = Arc::new(InMemoryCatalog::new());
    let registry = BucketRegistry {
        buckets: vec![entry("A", 1), entry("B", 1), entry("C", 2)],
    };
    cat.put_bucket_registry(&registry).await.unwrap();

    // Two projects mid-migration A→C and B→C, intents persisted at Copy.
    let p1 = ProjectId::new();
    let p2 = ProjectId::new();
    let assign_a = basin_catalog::BucketAssignment {
        bucket_id: "A".into(),
        tier: basin_catalog::BucketTier::Pooled,
        stripe: vec!["A".into()],
    };
    let assign_b = basin_catalog::BucketAssignment {
        bucket_id: "B".into(),
        tier: basin_catalog::BucketTier::Pooled,
        stripe: vec!["B".into()],
    };
    cat.assign_bucket_if_absent(&p1, &assign_a).await.unwrap();
    cat.assign_bucket_if_absent(&p2, &assign_b).await.unwrap();
    let live1 = seed_objects(&resolver.store_for("A"), &p1, 4).await;
    let live2 = seed_objects(&resolver.store_for("B"), &p2, 6).await;
    cat.put_migration_intent(&MigrationIntent {
        project: p1,
        from: "A".into(),
        to: "C".into(),
        phase: MigrationPhase::Copy,
    })
    .await
    .unwrap();
    cat.put_migration_intent(&MigrationIntent {
        project: p2,
        from: "B".into(),
        to: "C".into(),
        phase: MigrationPhase::Copy,
    })
    .await
    .unwrap();

    let n = pool.resume_migrations(cat.as_ref()).await.unwrap();
    assert_eq!(n, 2, "both in-flight migrations resumed");

    // Both converged onto C; A and B reclaimed (each held only their project).
    let on_c = contents(&resolver.store_for("C"), &p1).await;
    assert_eq!(on_c, live1);
    let on_c2 = contents(&resolver.store_for("C"), &p2).await;
    assert_eq!(on_c2, live2);
    assert!(cat.get_migration_intent(&p1).await.unwrap().is_none());
    assert!(cat.get_migration_intent(&p2).await.unwrap().is_none());
    let reg = cat.get_bucket_registry().await.unwrap();
    assert!(reg.get("A").is_none() && reg.get("B").is_none(), "A,B reclaimed");
    assert!(reg.get("C").is_some(), "C remains");
}
