# Contract-replay fixtures

Each `*.json` here drives one run of `cargo test --test contract`. The runner
spawns the built `basin` binary against an in-process HTTP stub, plays back
`steps[]` in order, and fails on method/path drift.

Schema: `{ name, description, steps: [{request:{method,path}, response:{status,body}}], command: [string], expect_stdout_contains?: [string], expect_stderr_contains?: [string], expect_exit_code?: u8 }`.
Defaults: `expect_exit_code` is `0`; `BASIN_API` + `BASIN_TOKEN` are injected.

To add a fixture: drop a new `*.json` file, capture the request shape from
`src/commands/<name>.rs` (or a real cloud call), and re-run the test — no
Rust edits needed.

Record-mode (auto-generating fixtures from a live `basin-cloud`) is the
gated follow-up half of TASKS 363.
