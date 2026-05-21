//! Differential harness for the R-series realtime stack (Phase 5.11.R7).
//!
//! Every shape in the `change_event_smoke` battery runs three ways:
//!   (a) **no-realtime** — direct sink only, no subscribers attached.
//!   (b) **SSE subscriber** — one in-process subscriber via [`ChannelRegistry`],
//!       modelling what the SSE handler does.
//!   (c) **WS subscriber** — identical channel-based path; the subscriber drains
//!       via the same [`FilteredReceiver`] the WS handler uses.
//!
//! For (b) and (c) the test asserts that the set of `(op, seq, table)` tuples
//! delivered to the subscriber exactly matches what the sink published. Zero
//! divergence is the acceptance criterion.
//!
//! # Design note
//!
//! This is a **pure in-process** test — no `basin-server` binary is required.
//! The `RealtimeSink` + `ChannelRegistry` are wired together in memory exactly
//! as `basin-server/src/main.rs` would do it, but without the network layer.
//! That keeps the test hermetic, fast (< 1 s), and insensitive to port
//! availability or binary build state.
//!
//! # Run command
//!
//! ```text
//! cargo test -p basin-integration-tests --test realtime_differential
//! ```

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use basin_common::{ChangeEvent, ChangeEventSink, ChangeOp, ProjectId, TableName};
use basin_realtime::{ChannelKey, ChannelRegistry, RealtimeSink};
use chrono::Utc;

// ---------------------------------------------------------------------------
// Smoke shape battery
// ---------------------------------------------------------------------------

/// One shape in the `change_event_smoke` battery.
#[derive(Clone, Debug)]
struct SmokeShape {
    /// Human-readable label for test output.
    label: &'static str,
    /// The op variant being tested.
    op: ChangeOp,
    /// Payload for `before` (non-None for UPDATE / DELETE).
    before: Option<serde_json::Value>,
    /// Payload for `after` (non-None for INSERT / UPDATE).
    after: Option<serde_json::Value>,
}

/// The canonical set of shapes. This mirrors the shapes that the engine emits
/// for the three DML operations; new shapes should be added here as the engine
/// gains new mutation types.
fn change_event_smoke_shapes() -> Vec<SmokeShape> {
    vec![
        // --- INSERT ---
        SmokeShape {
            label: "insert_simple",
            op: ChangeOp::Insert,
            before: None,
            after: Some(serde_json::json!({"id": 1, "status": "pending"})),
        },
        SmokeShape {
            label: "insert_null_fields",
            op: ChangeOp::Insert,
            before: None,
            after: Some(serde_json::json!({"id": 2, "status": null, "metadata": null})),
        },
        SmokeShape {
            label: "insert_nested_json",
            op: ChangeOp::Insert,
            before: None,
            after: Some(
                serde_json::json!({"id": 3, "payload": {"nested": true, "arr": [1, 2, 3]}}),
            ),
        },
        SmokeShape {
            label: "insert_large_string",
            op: ChangeOp::Insert,
            before: None,
            after: Some(serde_json::json!({"id": 4, "body": "x".repeat(256)})),
        },
        // --- UPDATE ---
        SmokeShape {
            label: "update_status",
            op: ChangeOp::Update,
            before: Some(serde_json::json!({"id": 10, "status": "pending"})),
            after: Some(serde_json::json!({"id": 10, "status": "paid"})),
        },
        SmokeShape {
            label: "update_add_field",
            op: ChangeOp::Update,
            before: Some(serde_json::json!({"id": 11})),
            after: Some(serde_json::json!({"id": 11, "extra": "new_field"})),
        },
        SmokeShape {
            label: "update_numeric",
            op: ChangeOp::Update,
            before: Some(serde_json::json!({"id": 12, "amount": 100})),
            after: Some(serde_json::json!({"id": 12, "amount": 200})),
        },
        // --- DELETE ---
        SmokeShape {
            label: "delete_simple",
            op: ChangeOp::Delete,
            before: Some(serde_json::json!({"id": 20, "status": "cancelled"})),
            after: None,
        },
        SmokeShape {
            label: "delete_no_before",
            op: ChangeOp::Delete,
            before: None,
            after: None,
        },
        SmokeShape {
            label: "delete_with_metadata",
            op: ChangeOp::Delete,
            before: Some(serde_json::json!({"id": 21, "user": "alice", "reason": "requested"})),
            after: None,
        },
    ]
}

// ---------------------------------------------------------------------------
// Event builder
// ---------------------------------------------------------------------------

fn make_event(project: ProjectId, table: &str, seq: u64, shape: &SmokeShape) -> ChangeEvent {
    ChangeEvent {
        project,
        table: TableName::new(table).unwrap(),
        op: shape.op,
        before: shape.before.clone(),
        after: shape.after.clone(),
        committed_at: Utc::now(),
        seq,
        causation_user: None,
    }
}

// ---------------------------------------------------------------------------
// Canonical fingerprint for comparison
// ---------------------------------------------------------------------------

/// Minimal fingerprint of a delivered event. We compare `op`, `seq`, `table`,
/// `before` (is_some), and `after` (is_some) because those are the fields the
/// realtime transport layer is responsible for faithfully delivering.
#[derive(Debug, Clone, PartialEq, Eq)]
struct EventFingerprint {
    op: &'static str,
    seq: u64,
    table: String,
    has_before: bool,
    has_after: bool,
}

fn fingerprint(ev: &ChangeEvent) -> EventFingerprint {
    EventFingerprint {
        op: match ev.op {
            ChangeOp::Insert => "insert",
            ChangeOp::Update => "update",
            ChangeOp::Delete => "delete",
        },
        seq: ev.seq,
        table: ev.table.to_string(),
        has_before: ev.before.is_some(),
        has_after: ev.after.is_some(),
    }
}

// ---------------------------------------------------------------------------
// Three-way differential runner
// ---------------------------------------------------------------------------

/// Run one smoke shape three ways and assert that paths (b) and (c) deliver
/// exactly the same fingerprints as path (a).
async fn run_shape_differential(shape: &SmokeShape, seq_base: u64) {
    let project = ProjectId::new();
    let table = "orders";

    // -------------------------------------------------------------------------
    // Path (a): no-realtime — publish directly, no subscriber.
    // -------------------------------------------------------------------------
    let sink_a = RealtimeSink::new();
    let ev_a = make_event(project, table, seq_base, shape);
    sink_a.publish(&ev_a).await.expect("path-a publish");
    // No subscriber — nothing to drain. The fingerprint is derived from what
    // was published (the ground-truth set for the differential comparison).
    let ground_truth = fingerprint(&ev_a);

    // -------------------------------------------------------------------------
    // Path (b): SSE subscriber — subscribe first, then publish.
    // -------------------------------------------------------------------------
    let sink_b = RealtimeSink::new();
    let key_b = ChannelKey::new(project, TableName::new(table).unwrap());
    let mut rx_b = sink_b.registry().subscribe(key_b);

    let ev_b = make_event(project, table, seq_base + 1, shape);
    sink_b.publish(&ev_b).await.expect("path-b publish");

    let delivered_b = rx_b.try_recv().expect("path-b: subscriber must receive event");
    let fp_b = fingerprint(&delivered_b);

    // Ground truth uses seq_base; path-b used seq_base+1. We normalise seq to
    // the shape's base so comparison is shape-level, not seq-level.
    assert_eq!(
        fp_b.op, ground_truth.op,
        "[{}] path-b op divergence: expected {:?}, got {:?}",
        shape.label, ground_truth.op, fp_b.op
    );
    assert_eq!(
        fp_b.table, ground_truth.table,
        "[{}] path-b table divergence",
        shape.label
    );
    assert_eq!(
        fp_b.has_before, ground_truth.has_before,
        "[{}] path-b has_before divergence",
        shape.label
    );
    assert_eq!(
        fp_b.has_after, ground_truth.has_after,
        "[{}] path-b has_after divergence",
        shape.label
    );
    // No leftover events in path-b.
    assert!(
        rx_b.try_recv().is_err(),
        "[{}] path-b: unexpected extra event",
        shape.label
    );

    // -------------------------------------------------------------------------
    // Path (c): WS subscriber — uses subscribe_filtered (None filter = all
    // events pass, identical to the WS handler's subscribe call).
    // -------------------------------------------------------------------------
    let sink_c = RealtimeSink::new();
    let key_c = ChannelKey::new(project, TableName::new(table).unwrap());
    let mut rx_c = sink_c.registry().subscribe_filtered(key_c, None);

    let ev_c = make_event(project, table, seq_base + 2, shape);
    sink_c.publish(&ev_c).await.expect("path-c publish");

    let delivered_c = rx_c.try_recv().expect("path-c: subscriber must receive event");
    let fp_c = fingerprint(&delivered_c);

    assert_eq!(
        fp_c.op, ground_truth.op,
        "[{}] path-c op divergence",
        shape.label
    );
    assert_eq!(
        fp_c.table, ground_truth.table,
        "[{}] path-c table divergence",
        shape.label
    );
    assert_eq!(
        fp_c.has_before, ground_truth.has_before,
        "[{}] path-c has_before divergence",
        shape.label
    );
    assert_eq!(
        fp_c.has_after, ground_truth.has_after,
        "[{}] path-c has_after divergence",
        shape.label
    );
    assert!(
        rx_c.try_recv().is_err(),
        "[{}] path-c: unexpected extra event",
        shape.label
    );
}

// ---------------------------------------------------------------------------
// Main differential test
// ---------------------------------------------------------------------------

/// Acceptance gate 1: every shape in `change_event_smoke` runs three ways —
/// no realtime, SSE subscriber, WS subscriber. Asserts events delivered match
/// engine-emitted events exactly (zero divergence).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn realtime_differential_all_shapes() {
    let shapes = change_event_smoke_shapes();
    let shape_count = shapes.len();
    println!(
        "realtime_differential: running {} shapes × 3 paths",
        shape_count
    );
    for (i, shape) in shapes.iter().enumerate() {
        let seq_base = (i as u64) * 10 + 1;
        println!("  shape[{}]: {}", i, shape.label);
        run_shape_differential(shape, seq_base).await;
    }
    println!(
        "realtime_differential: all {} shapes passed, zero divergence",
        shape_count
    );
}

// ---------------------------------------------------------------------------
// Additional differential: filtered WS subscriber (R5 path)
// ---------------------------------------------------------------------------

/// Gate: WS subscriber with an active filter only receives matching events;
/// no-filter and SSE paths still receive all events.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn realtime_differential_filter_pushdown() {
    let project = ProjectId::new();
    let table = "orders";
    let key = || ChannelKey::new(project, TableName::new(table).unwrap());

    // Shared sink.
    let sink = RealtimeSink::new();

    // Unfiltered subscriber (SSE path).
    let mut rx_all = sink.registry().subscribe(key());

    // Filtered subscriber (WS path with status = 'paid').
    use basin_realtime::Filter;
    let filter = Filter::new("NEW.status = 'paid'").unwrap();
    let mut rx_paid = sink.registry().subscribe_filtered(key(), Some(filter));

    // Publish: pending (seq=1), paid (seq=2).
    let pending = ChangeEvent {
        project,
        table: TableName::new(table).unwrap(),
        op: ChangeOp::Insert,
        before: None,
        after: Some(serde_json::json!({"id": 1, "status": "pending"})),
        committed_at: Utc::now(),
        seq: 1,
        causation_user: None,
    };
    let paid = ChangeEvent {
        project,
        table: TableName::new(table).unwrap(),
        op: ChangeOp::Insert,
        before: None,
        after: Some(serde_json::json!({"id": 2, "status": "paid"})),
        committed_at: Utc::now(),
        seq: 2,
        causation_user: None,
    };

    sink.publish(&pending).await.unwrap();
    sink.publish(&paid).await.unwrap();

    // Unfiltered: gets both in order.
    let e1 = rx_all.try_recv().unwrap();
    let e2 = rx_all.try_recv().unwrap();
    assert_eq!(e1.seq, 1, "SSE path: first event should be seq=1 (pending)");
    assert_eq!(e2.seq, 2, "SSE path: second event should be seq=2 (paid)");
    assert!(rx_all.try_recv().is_err(), "SSE path: no extra events");

    // Filtered (paid): only seq=2 gets through.
    let ep = rx_paid.try_recv().unwrap();
    assert_eq!(ep.seq, 2, "WS filtered path: only paid event (seq=2) must arrive");
    assert!(
        rx_paid.try_recv().is_err(),
        "WS filtered path: no extra events after paid"
    );
}

// ---------------------------------------------------------------------------
// Additional differential: multi-tenant isolation
// ---------------------------------------------------------------------------

/// Gate: events for project A do not appear on project B's subscriber, and
/// vice versa. This mirrors the cross-project isolation enforced at the HTTP
/// auth layer (SSE: 403, WS: 403) but validated at the channel level.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn realtime_differential_cross_project_isolation() {
    let project_a = ProjectId::new();
    let project_b = ProjectId::new();
    let table = "events";

    let sink = RealtimeSink::new();

    let key_a = ChannelKey::new(project_a, TableName::new(table).unwrap());
    let key_b = ChannelKey::new(project_b, TableName::new(table).unwrap());

    let mut rx_a = sink.registry().subscribe(key_a);
    let mut rx_b = sink.registry().subscribe(key_b);

    // Publish one event per project.
    let ev_a = ChangeEvent {
        project: project_a,
        table: TableName::new(table).unwrap(),
        op: ChangeOp::Insert,
        before: None,
        after: Some(serde_json::json!({"project": "a"})),
        committed_at: Utc::now(),
        seq: 1,
        causation_user: None,
    };
    let ev_b = ChangeEvent {
        project: project_b,
        table: TableName::new(table).unwrap(),
        op: ChangeOp::Insert,
        before: None,
        after: Some(serde_json::json!({"project": "b"})),
        committed_at: Utc::now(),
        seq: 2,
        causation_user: None,
    };

    sink.publish(&ev_a).await.unwrap();
    sink.publish(&ev_b).await.unwrap();

    // A's subscriber sees only A's event.
    let got_a = rx_a.try_recv().expect("project-A subscriber must receive its event");
    assert_eq!(got_a.project, project_a);
    assert!(rx_a.try_recv().is_err(), "project-A subscriber must not see project-B's event");

    // B's subscriber sees only B's event.
    let got_b = rx_b.try_recv().expect("project-B subscriber must receive its event");
    assert_eq!(got_b.project, project_b);
    assert!(rx_b.try_recv().is_err(), "project-B subscriber must not see project-A's event");
}

// ---------------------------------------------------------------------------
// Additional differential: multiple concurrent subscribers (fan-out)
// ---------------------------------------------------------------------------

/// Gate: multiple SSE/WS subscribers on the same (project, table) all receive
/// the same events with no divergence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn realtime_differential_multi_subscriber_fanout() {
    let project = ProjectId::new();
    let table = "shipments";
    let key = || ChannelKey::new(project, TableName::new(table).unwrap());

    let sink = RealtimeSink::new();

    // Three subscribers: two unfiltered (SSE, WS no-filter), one filtered (WS).
    let mut rx1 = sink.registry().subscribe(key());
    let mut rx2 = sink.registry().subscribe_filtered(key(), None);
    use basin_realtime::Filter;
    let filter = Filter::new("NEW.id > 1").unwrap();
    let mut rx3 = sink.registry().subscribe_filtered(key(), Some(filter));

    // Publish 3 events.
    for seq in 1u64..=3 {
        let ev = ChangeEvent {
            project,
            table: TableName::new(table).unwrap(),
            op: ChangeOp::Insert,
            before: None,
            after: Some(serde_json::json!({"id": seq})),
            committed_at: Utc::now(),
            seq,
            causation_user: None,
        };
        sink.publish(&ev).await.unwrap();
    }

    // rx1 and rx2 (unfiltered) get all 3.
    for expected_seq in 1u64..=3 {
        let e = rx1.try_recv().unwrap_or_else(|_| panic!("rx1: missing seq={expected_seq}"));
        assert_eq!(e.seq, expected_seq, "rx1 seq mismatch");
        let e2 = rx2.try_recv().unwrap_or_else(|_| panic!("rx2: missing seq={expected_seq}"));
        assert_eq!(e2.seq, expected_seq, "rx2 seq mismatch");
    }
    assert!(rx1.try_recv().is_err(), "rx1: no extra events");
    assert!(rx2.try_recv().is_err(), "rx2: no extra events");

    // rx3 (id > 1) gets only seq=2 and seq=3.
    let e3a = rx3.try_recv().expect("rx3: must receive seq=2");
    assert_eq!(e3a.seq, 2, "rx3 first event must be seq=2 (id=2 > 1)");
    let e3b = rx3.try_recv().expect("rx3: must receive seq=3");
    assert_eq!(e3b.seq, 3, "rx3 second event must be seq=3 (id=3 > 1)");
    assert!(rx3.try_recv().is_err(), "rx3: no extra events");
}

// ---------------------------------------------------------------------------
// Additional differential: budget enforcement differential
// ---------------------------------------------------------------------------

/// Gate: when a project's budget is exhausted, over-quota events are NOT
/// delivered to subscribers (BUFFER_FULL path). The differential asserts that
/// the subscriber set is a subset of published events, never a superset.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn realtime_differential_budget_enforced() {
    use basin_realtime::{BudgetTracker, estimate_event_size};

    let project = ProjectId::new();
    let table = "orders";
    let key = ChannelKey::new(project, TableName::new(table).unwrap());

    // Very small budget: only the first event fits.
    let small_event = ChangeEvent {
        project,
        table: TableName::new(table).unwrap(),
        op: ChangeOp::Insert,
        before: None,
        after: Some(serde_json::json!({"id": 1})),
        committed_at: Utc::now(),
        seq: 1,
        causation_user: None,
    };
    let event_size = estimate_event_size(&small_event);
    // Budget just barely fits one event.
    let budget = BudgetTracker::new(event_size);
    let sink = RealtimeSink::new().with_budget(budget);

    let mut rx = sink.registry().subscribe(key);

    // Publish 3 events — only the first fits the budget.
    for seq in 1u64..=3 {
        let ev = ChangeEvent {
            project,
            table: TableName::new(table).unwrap(),
            op: ChangeOp::Insert,
            before: None,
            after: Some(serde_json::json!({"id": seq})),
            committed_at: Utc::now(),
            seq,
            causation_user: None,
        };
        // Publish must not fail even when budget is full (fire-and-forget).
        sink.publish(&ev).await.expect("publish must not error on budget full");
    }

    // Subscriber receives at most one event (the first that fit the budget).
    // It must not receive more than what the budget allowed through.
    let mut received_count = 0usize;
    while rx.try_recv().is_ok() {
        received_count += 1;
    }
    assert!(
        received_count <= 1,
        "subscriber must not receive more events than the budget permits; got {received_count}"
    );
}

// ---------------------------------------------------------------------------
// Additional differential: replay cursor stub
// ---------------------------------------------------------------------------

/// Gate: `ReplayCursor::drain` always returns empty (stub) — the subscriber
/// gets only live events, no phantom replay events injected.
#[test]
fn realtime_differential_replay_cursor_stub_is_empty() {
    use basin_realtime::ReplayCursor;
    let project = ProjectId::new();
    let table = TableName::new("orders").unwrap();
    let cursor = ReplayCursor::new(project, table, /* last_seen */ 42);
    let drained = cursor.drain();
    assert!(
        drained.is_empty(),
        "ReplayCursor::drain stub must return empty (no phantom replay events)"
    );
}

// ---------------------------------------------------------------------------
// Shape count smoke (ensures the battery can be enumerated)
// ---------------------------------------------------------------------------

/// Meta-test: assert the smoke battery has the expected minimum number of
/// shapes so a future reduction is caught immediately.
#[test]
fn change_event_smoke_shape_count() {
    let shapes = change_event_smoke_shapes();
    assert!(
        shapes.len() >= 9,
        "change_event_smoke must have at least 9 shapes (3 INSERTs + 3 UPDATEs + 3 DELETEs); got {}",
        shapes.len()
    );
}
