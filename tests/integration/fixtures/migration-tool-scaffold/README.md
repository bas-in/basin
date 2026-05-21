# Migration-Tool Scaffold Fixture

This fixture provides a docker-compose template for spinning up an ephemeral
Basin instance (exposing pgwire) against which third-party SQL migration tools
can be tested.

## Files

- `docker-compose.template.yml` — Basin server service template. Two
  placeholders must be substituted before use: `{{PGWIRE_PORT}}` (a free host
  port) and `{{PROJECT_NAME}}` (the pgwire username / project slug).

## How per-tool tests use this

Each per-tool integration test (5.25.B Flyway, 5.25.C golang-migrate, 5.25.D
Diesel, 5.25.E sqlx, 5.25.F Prisma) follows this protocol:

1. **Render the template** — substitute `{{PGWIRE_PORT}}` with a free
   ephemeral port and `{{PROJECT_NAME}}` with a short slug (e.g. `"alice"`).
   Write the rendered YAML to a `tempfile::TempDir`.

2. **Launch Basin** via `docker-compose -f <tmpdir>/docker-compose.yml up -d`
   (or, for in-process testing that does not need docker, call
   `migration_tool_common::spawn_basin_server()` directly and skip docker).

3. **Construct a DSN** — `postgres://<PROJECT_NAME>:any@127.0.0.1:<PORT>/basin`

4. **Run the migration tool** against that DSN using `tokio::process::Command`
   or a native Rust crate.

5. **Assert** using the helpers in `tests/migration_tool_common.rs`:
   - `assert_schema_matches_snapshot` — introspect the live schema and diff it
     against an expected `SchemaSnapshot`.
   - `run_crud_battery` — execute a set of CRUD queries and assert results.

6. **Teardown** — `docker-compose … down -v`, or drop the in-process server
   handle. The tempdir is deleted automatically when the `TempDir` guard drops.

## Notes

- The docker-compose fixture is optional. Tests that use `spawn_basin_server()`
  in-process (via `basin_router::run_until_bound`) need no docker at all and
  run faster in CI. Docker compose is useful when the migration tool itself
  requires a persistent TCP endpoint that must outlive a single Rust async task.

- Tests that require docker should guard with a `docker info` check and emit
  a `[skip]` line + return `Ok(())` when docker is unavailable, matching the
  convention used by `smoke_pgx` and `compare_server_lifecycle`.
