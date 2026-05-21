//! Integration tests for the [`HandlerHarness`] (Phase 5.11.W2).
//!
//! Exercises the HTTP-handler component shape end-to-end:
//!
//!   WIT (basin-fn.wit, `basin-functions-handler` world)
//!     → bindgen! (host-trait + linker glue in `crate::handler`)
//!       → HandlerHarness::new (compile + pre-link)
//!         → HandlerHarness::handle (Store + instantiate + call_handle)
//!
//! The component encodes a minimal `handle(request) -> response` export in
//! WAT. The four host imports (`query`/`http`/`log`/`secret`) are wired the
//! same way as the W1 `ComponentHarness` — they share the same
//! `FunctionHost` trait impls.

use std::sync::Arc;

use basin_fn::engine::{InvocationContext, QueryRow};
use basin_fn::{HandlerHarness, HandlerRequest, QueryExecutor};

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

/// Minimal WAT component that exports `basin:functions/handler#handle`.
///
/// Returns `result::ok(response { status: 200, headers: [], body: [] })`
/// without touching the request — the goal is to validate the full
/// instantiation + canon-lift pipeline for the W2 world.
///
/// The discriminant 0 = `ok` for the `result<response, string>` return; the
/// inner response body is a list with length 0 (offset 0). Headers list is
/// also empty.
const HANDLER_WAT: &str = r#"
(component
  (core module $core_m
    (memory (export "memory") 1)

    ;; cabi_realloc: required by canon lift / lower. Returns the requested
    ;; offset rounded up — the smallest correct implementation is a bump
    ;; allocator anchored at offset 16 (leave the first 16 bytes free for
    ;; canonical-ABI scratch).
    (global $bump (mut i32) (i32.const 16))
    ;; Bump allocator that honours the canonical-ABI alignment contract:
    ;; wasmtime's host validates that realloc's return is `$align`-aligned
    ;; on every call (lifting fails with "realloc return: result not aligned"
    ;; otherwise). After each allocation we round the bump up to the next
    ;; multiple of `$align` before claiming the offset, then advance past
    ;; `$new_sz`. Since strings/lists may request align=1,2,4,8 the rounding
    ;; uses the standard `(x + align - 1) & ~(align - 1)` formula.
    (func (export "cabi_realloc")
        (param $old i32) (param $old_sz i32) (param $align i32) (param $new_sz i32)
        (result i32)
      (local $ret i32)
      ;; ret = (bump + align - 1) & ~(align - 1)
      (local.set $ret
        (i32.and
          (i32.add (global.get $bump) (i32.sub (local.get $align) (i32.const 1)))
          (i32.xor (i32.sub (local.get $align) (i32.const 1)) (i32.const -1))))
      (global.set $bump (i32.add (local.get $ret) (local.get $new_sz)))
      (local.get $ret)
    )

    ;; handle_inner: ignores the lowered request and writes a response with
    ;; status=200, empty headers list, empty body list.
    ;;
    ;; The canonical-ABI lowering of `request` (record { method: string,
    ;; path: string, headers: list<...>, body: list<u8> }) flattens to eight
    ;; i32 core params: (method_ptr, method_len, path_ptr, path_len,
    ;; headers_ptr, headers_len, body_ptr, body_len). We ignore them all.
    ;;
    ;; The return is a single i32 pointer to the lowered
    ;; `result<response, string>` record. We point at a 64-byte scratch
    ;; region pre-zeroed with status=200; every list-length field is 0 so
    ;; canon lift never dereferences the pointer fields.
    (func (export "handle_inner")
        (param $method_ptr i32) (param $method_len i32)
        (param $path_ptr   i32) (param $path_len   i32)
        (param $headers_ptr i32) (param $headers_len i32)
        (param $body_ptr   i32) (param $body_len   i32)
        (result i32)
      (local $out i32)
      ;; Reserve 64 bytes of scratch for the result record.
      (local.set $out (i32.const 1024))

      ;; Zero out 64 bytes at $out.
      (i32.store (i32.add (local.get $out) (i32.const  0)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const  4)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const  8)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const 12)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const 16)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const 20)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const 24)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const 28)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const 32)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const 36)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const 40)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const 44)) (i32.const 0))

      ;; Write status = 200 at offset 4 (after the discriminant word).
      (i32.store16 (i32.add (local.get $out) (i32.const 4)) (i32.const 200))

      ;; Return pointer to the result record.
      (local.get $out)
    )
  )
  (core instance $core_i (instantiate $core_m))

  ;; Named component-level types. Wasmtime >= 44 (wasmparser >= 0.246) tightens
  ;; the "all types referred to by an exported func must be named" rule
  ;; (validate_and_register_named_types in wasmparser::validator::component),
  ;; so the request / response records must be top-level type declarations
  ;; with explicit exported names rather than inline anonymous records on the
  ;; export's signature. Headers/body lists stay anonymous — only the
  ;; outermost defined types need exported names.
  (type $request (record
    (field "method" string)
    (field "path" string)
    (field "headers" (list (tuple string string)))
    (field "body" (list u8))))
  (export $request-name "request" (type $request))
  (type $response (record
    (field "status" u16)
    (field "headers" (list (tuple string string)))
    (field "body" (list u8))))
  (export $response-name "response" (type $response))

  ;; Lift the core-Wasm `handle_inner` to the component-level `handle` export.
  ;; The world exports `handle` as a bare func (same export style as W1's
  ;; `run`), so this is a single top-level `(func (export "handle") …)` with a
  ;; canon lift — no interface-instance plumbing needed.
  (func (export "handle")
    (param "req" $request-name)
    (result (result $response-name (error string)))
    (canon lift
      (core func $core_i "handle_inner")
      (memory $core_i "memory")
      (realloc (func $core_i "cabi_realloc"))
    )
  )
)
"#;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// `HandlerHarness::new` compiles the WAT component and pre-links the four
/// W1 host imports for the W2 world.
#[test]
fn handler_harness_compiles_minimal_component() {
    let wasm_bytes = wat::parse_str(HANDLER_WAT).expect("WAT compiles");
    let _harness = HandlerHarness::new(&wasm_bytes).expect("harness instantiates");
}

/// `HandlerHarness::handle` calls into the guest, which returns a 200
/// response with no body — the canonical "the handler ABI works" assertion.
#[test]
fn handler_harness_handle_returns_200() {
    let wasm_bytes = wat::parse_str(HANDLER_WAT).expect("WAT compiles");
    let harness = HandlerHarness::new(&wasm_bytes).expect("harness instantiates");

    let ctx = InvocationContext::mock();
    let req = HandlerRequest {
        method: "GET".to_string(),
        path: "/fn/v1/echo".to_string(),
        headers: vec![("x-test".to_string(), "1".to_string())],
        body: b"hello".to_vec(),
    };

    let resp = harness.handle(ctx, req).expect("handle runs without trap");
    assert_eq!(resp.status, 200, "expected 200 from the handler component");
    assert!(resp.body.is_empty(), "minimal handler returns empty body");
}

/// The same `HandlerHarness` is callable repeatedly; each call gets a fresh
/// `Store`. The W1 harness has the same guarantee.
#[test]
fn handler_harness_handle_is_reentrant() {
    let wasm_bytes = wat::parse_str(HANDLER_WAT).expect("WAT compiles");
    let harness = HandlerHarness::new(&wasm_bytes).expect("harness instantiates");

    for i in 0..3 {
        let ctx = InvocationContext::mock();
        let req = HandlerRequest {
            method: "POST".to_string(),
            path: format!("/fn/v1/echo?n={i}"),
            headers: vec![],
            body: vec![i as u8],
        };
        let resp = harness.handle(ctx, req).expect("handle runs");
        assert_eq!(resp.status, 200, "call {i} should succeed");
    }
}

/// `FunctionStore` is wired so the `basin-rest` integration test can build a
/// no-deps in-memory store. This test exercises the trait shape rather than
/// the runtime; the `/fn/v1/:name` integration test (in
/// `tests/integration/tests/fn_handler.rs`) covers the wired call.
#[test]
fn function_store_trait_object_safe() {
    use basin_common::ProjectId;
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct InMemStore {
        by_name: Mutex<HashMap<(ProjectId, String), Arc<HandlerHarness>>>,
    }

    impl basin_fn::FunctionStore for InMemStore {
        fn lookup(
            &self,
            project: ProjectId,
            name: &str,
        ) -> Option<(Arc<HandlerHarness>, InvocationContext)> {
            let guard = self.by_name.lock().unwrap();
            guard
                .get(&(project, name.to_string()))
                .cloned()
                .map(|h| (h, InvocationContext::mock()))
        }
    }

    let store: Arc<dyn basin_fn::FunctionStore> = Arc::new(InMemStore {
        by_name: Mutex::new(HashMap::new()),
    });
    assert!(store.lookup(ProjectId::new(), "missing").is_none());
}

/// The `QueryExecutor` plumbed through `InvocationContext` is the same
/// trait used by the W1 `ComponentHarness`. Confirms there's no API drift.
#[test]
fn handler_harness_accepts_custom_executor() {
    struct EmptyExecutor;
    impl QueryExecutor for EmptyExecutor {
        fn exec_sql(&self, _sql: &str) -> Result<Vec<QueryRow>, String> {
            Ok(Vec::new())
        }
    }
    let wasm_bytes = wat::parse_str(HANDLER_WAT).expect("WAT compiles");
    let harness = HandlerHarness::new(&wasm_bytes).expect("harness instantiates");

    let ctx = InvocationContext::with_executor(Arc::new(EmptyExecutor));
    let req = HandlerRequest {
        method: "GET".into(),
        path: "/fn/v1/anything".into(),
        headers: vec![],
        body: vec![],
    };
    let resp = harness.handle(ctx, req).expect("handle");
    assert_eq!(resp.status, 200);
}
