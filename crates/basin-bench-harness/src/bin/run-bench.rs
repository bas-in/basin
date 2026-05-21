//! `run-bench` — CLI front-end for [`basin_bench_harness::BenchSuite`].
//!
//! Invoked by `scripts/run-bench.sh`. Usage:
//!
//! ```sh
//! cargo run -p basin-bench-harness --bin run-bench --release -- <profile|all> [--fast]
//! ```
//!
//! - `<profile>`: one of the names in `basin_bench_harness::profiles::all()`
//!   (e.g. `vortex-vs-parquet`, `wasm`, `noisy-neighbor`, `multi-instance`),
//!   or `all`.
//! - `--fast`: shrink iteration counts so the whole matrix fits the CI
//!   <5min budget. Without it the profile's full baseline runs.
//!
//! Writes per-shape JSON sidecars into `benchmark/<data_slug>/` (so the
//! existing `index_*.html` dashboards render them) and prints the Markdown
//! summary to stdout.

use basin_bench_harness::runner::BenchSuite;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let profile = args.get(1).map(|s| s.as_str()).unwrap_or("all");
    let fast = args.iter().any(|a| a == "--fast");

    eprintln!(
        "run-bench: profile={profile} fast={fast} (writing JSON sidecars to benchmark/data*/)"
    );

    let results = BenchSuite::run(profile, move |cfg| {
        if fast {
            // CI subset: cap iterations and dataset so the whole matrix is
            // < 5 min. Each shape stays statistically meaningful at 200
            // samples for p99, 2 samples in the p999 bucket.
            cfg.workload.iterations = cfg.workload.iterations.min(200);
            cfg.workload.table_rows = cfg.workload.table_rows.min(20_000);
            if let Some(n) = cfg.workload.noisy.as_mut() {
                n.quiet_projects = n.quiet_projects.min(4);
            }
        }
    })
    .await?;

    print!("{}", BenchSuite::render_markdown(&results));

    let failed = results.iter().filter(|r| !r.passed).count();
    if failed > 0 {
        eprintln!("run-bench: {failed} shape(s) failed");
        std::process::exit(1);
    }
    Ok(())
}
