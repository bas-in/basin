//! Security regression test — #53 P1 realtime cross-subscription leak.
//!
//! **Issue:** Two projects (A, B) both subscribe to a realtime channel named
//! "events". Project A INSERTs a row (publishes an event). Project B's
//! subscriber must receive NOTHING within 500 ms — cross-project isolation.
//!
//! **Failure mode being tested:** If `ChannelKey` is keyed only on the table
//! name (not on `(project_id, table_name)`), project B's subscriber would
//! receive project A's events — a security P1 cross-project data leak.
//!
//! **Harness:** pure in-process; mirrors `realtime_differential.rs`.
//!
//! # Run command
//!
//! ```text
//! cargo test -p basin-integration-tests --test sec_realtime_isolation
//! ```

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use basin_common::{ChangeEvent, ChangeEventSink, ChangeOp, ProjectId, TableName};
use basin_realtime::{ChannelKey, RealtimeSink};
use chrono::Utc;

// ---------------------------------------------------------------------------
// #53 P1 — realtime cross-subscription leak
// ---------------------------------------------------------------------------

/// Security regression — #53 P1: cross-project subscription isolation.
///
/// Two projects (A, B) each subscribe to a channel named "events" on the same
/// `RealtimeSink`. Project A publishes an INSERT event. Project B's subscriber
/// must receive NO events within 500 ms.
///
/// If the channel key is not project-scoped (keyed only on table name), project
/// B would receive project A's event — a P1 security breach.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_project_realtime_subscription_isolation() {
    let project_a = ProjectId::new();
    let project_b = ProjectId::new();
    let channel_name = "events";

    let sink = RealtimeSink::new();

    // Subscribe project A and project B to identically-named channels.
    let key_a = ChannelKey::new(project_a, TableName::new(channel_name).unwrap());
    let key_b = ChannelKey::new(project_b, TableName::new(channel_name).unwrap());

    let mut rx_a = sink.registry().subscribe(key_a);
    let mut rx_b = sink.registry().subscribe(key_b);

    // Project A publishes an INSERT event (simulates a row INSERT in project A's
    // "events" table).
    let ev_a = ChangeEvent {
        project: project_a,
        table: TableName::new(channel_name).unwrap(),
        op: ChangeOp::Insert,
        before: None,
        after: Some(serde_json::json!({"id": 1, "owner": "project_a", "secret": "a-secret"})),
        committed_at: Utc::now(),
        seq: 1,
        causation_user: None,
    };
    sink.publish(&ev_a)
        .await
        .expect("project A publish must succeed");

    // Project A MUST receive its own event.
    let got_a = rx_a
        .try_recv()
        .expect("project A subscriber must receive its own INSERT event");
    assert_eq!(
        got_a.project, project_a,
        "project A subscriber received an event from the wrong project"
    );
    assert!(
        rx_a.try_recv().is_err(),
        "project A subscriber must not receive more than one event"
    );

    // Project B MUST NOT receive project A's event.
    //
    // We give the delivery path up to 500 ms via a tokio timeout — try_recv()
    // alone would only check the instant after publish, but the delivery may
    // be slightly asynchronous. If a leaked event arrives within the window
    // the test fails loudly.
    let b_received: Result<Arc<ChangeEvent>, _> =
        tokio::time::timeout(Duration::from_millis(500), async {
            // Poll rx_b; block until an event arrives or the outer timeout fires.
            loop {
                if let Ok(ev) = rx_b.try_recv() {
                    return ev;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

    match b_received {
        Err(_timeout) => {
            // Good: no event arrived within 500 ms — isolation holds.
            println!(
                "[sec_realtime_isolation] PASS: project B received no events \
                 from project A within 500 ms"
            );
        }
        Ok(leaked) => {
            // If a leak is found, mark the failure clearly.
            panic!(
                "SECURITY P1 (#53): cross-project subscription leak detected — \
                 project B received an event from project A.\n\
                 Leaked event: project={:?}, table={:?}, seq={}",
                leaked.project, leaked.table, leaked.seq
            );
        }
    }
}

/// Belt-and-braces: verify that `ChannelKey` distinguishes projects even when
/// the table name is identical. Two keys with the same table but different
/// project IDs must NOT be equal.
#[test]
fn channel_key_is_project_scoped() {
    let project_a = ProjectId::new();
    let project_b = ProjectId::new();
    let table = TableName::new("events").unwrap();

    let key_a = ChannelKey::new(project_a, table.clone());
    let key_b = ChannelKey::new(project_b, table.clone());

    assert_ne!(
        key_a, key_b,
        "SECURITY P1 (#53): ChannelKey must include project_id in equality — \
         two keys with the same table but different projects must not be equal"
    );

    let key_a2 = ChannelKey::new(project_a, table.clone());
    assert_eq!(
        key_a, key_a2,
        "ChannelKey must equal itself (same project + table)"
    );
}

/// Fan-out within the same project is unaffected by cross-project isolation.
/// Multiple subscribers on project A's "events" channel all receive project A's
/// events — isolation does not break same-project fan-out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_project_fanout_unaffected_by_isolation() {
    let project = ProjectId::new();
    let table_name = "events";

    let sink = RealtimeSink::new();
    let key = || ChannelKey::new(project, TableName::new(table_name).unwrap());

    let mut rx1 = sink.registry().subscribe(key());
    let mut rx2 = sink.registry().subscribe(key());
    let mut rx3 = sink.registry().subscribe_filtered(key(), None);

    let ev = ChangeEvent {
        project,
        table: TableName::new(table_name).unwrap(),
        op: ChangeOp::Insert,
        before: None,
        after: Some(serde_json::json!({"id": 42})),
        committed_at: Utc::now(),
        seq: 1,
        causation_user: None,
    };
    sink.publish(&ev).await.expect("publish must succeed");

    // All three same-project subscribers must receive the event.
    let e1 = rx1
        .try_recv()
        .expect("rx1 (same-project SSE) must receive event");
    assert_eq!(e1.seq, 1, "rx1: wrong seq");

    let e2 = rx2
        .try_recv()
        .expect("rx2 (same-project SSE dup) must receive event");
    assert_eq!(e2.seq, 1, "rx2: wrong seq");

    let e3 = rx3
        .try_recv()
        .expect("rx3 (same-project WS) must receive event");
    assert_eq!(e3.seq, 1, "rx3: wrong seq");

    println!(
        "[sec_realtime_isolation] PASS: same-project fan-out unaffected; \
         all 3 subscribers received seq=1"
    );
}
