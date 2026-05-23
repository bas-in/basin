//! Run an async test body on a dedicated thread with a large stack.
//!
//! Some integration tests overflow the default thread stack under the
//! `dev` profile because basin-engine's async DataFusion plan visitors
//! produce very large future state machines when unoptimized. The
//! `#[tokio::test]` macro (and its `multi_thread` flavor) inherits
//! tokio's 2 MiB worker stack which is too small for those futures in
//! dev. This helper spawns a fresh OS thread with 8 MiB of stack and
//! drives a single-threaded tokio runtime on it via `block_on`.
//!
//! Usage:
//!
//! ```ignore
//! #[test]
//! fn my_async_test() {
//!     basin_integration_tests::big_stack::run(async {
//!         // ...async test body...
//!     });
//! }
//! ```
//!
//! The helper panics in the caller if the spawned thread panics, so
//! test assertion failures behave identically to a normal `#[test]`.

/// Run `fut` to completion on a new OS thread with an 8 MiB stack,
/// using a current-thread tokio runtime.
pub fn run<F>(fut: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let handle = std::thread::Builder::new()
        .name("basin-big-stack".into())
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build current-thread tokio runtime");
            rt.block_on(fut);
        })
        .expect("spawn big-stack thread");

    if let Err(payload) = handle.join() {
        std::panic::resume_unwind(payload);
    }
}
