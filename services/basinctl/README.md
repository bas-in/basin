# basinctl

Small admin CLI for a running Basin pgwire endpoint.

```sh
export BASIN_URL=postgres://alice@127.0.0.1:5433/basin
basinctl ping                        # OK in 3 ms
basinctl ping postgres://h:5433/db   # positional override
basinctl projects                     # prints current_user
basinctl tables                      # one table name per line
basinctl query "SELECT 1, 'hi'"      # padded | separated rows
basinctl version                     # basinctl 0.0.1 (<sha>)
basinctl reset-auth --yes            # drop every basin_auth_* table
basinctl import-from-postgres \
  --source postgres://app@db:5432/app   # migrate schema + data into BASIN_URL
```

`--url` overrides `BASIN_URL`. On error, prints to stderr and exits non-zero.

`import-from-postgres` is the onboarding tool: it enumerates the source via
`information_schema`, translates DDL to Basin's dialect (serial → BIGINT
identity; uuid/jsonb/citext/vector pass through; triggers, plpgsql
functions, and exotic constraints are skipped with a loud per-object
report), creates the tables in FK order, then streams rows per table via
binary COPY (CSV fallback for types Basin's binary COPY rejects) with
`--jobs N` parallel tables and per-table row-count verification. Full flag
set, type-mapping table, and skip-report semantics: `docs/import.md`.

`reset-auth` is destructive: it drops basin-auth's catalog tables. See the
"Resetting basin-auth state" section in `docs/deployment.md` for the full
flag set (`--yes`, `--engine-url`, `--include-project-creds`).

## WASM function toolchain — `basinctl fn`

Scaffold, compile, and deploy Basin Wasm handler functions. Functions run as
WASI Preview 2 components inside the Basin engine under per-project CPU,
memory, and wall-clock caps (see `docs/functions.md`).

### Quickstart

```sh
# 1. Prerequisites (one-time)
rustup target add wasm32-wasip1
cargo install cargo-component

# 2. Scaffold a new function project
basinctl fn new hello
cd hello/

# 3. Edit src/lib.rs to implement your handler, then build
basinctl fn build
#   → target/wasm32-wasip1/release/hello.wasm

# 4. Deploy to a running Basin server
export BASIN_ADMIN_TOKEN=<your-admin-bearer-token>
basinctl fn deploy hello

# 5. Invoke
curl -H "Authorization: Bearer $JWT" http://localhost:5434/fn/v1/hello
```

### `basinctl fn new <name> [--lang rust]`

Creates a `<name>/` directory with:

| File | Purpose |
|---|---|
| `Cargo.toml` | `crate-type = ["cdylib"]`, `wasm32-wasip1` target, `wit-bindgen` dep |
| `src/lib.rs` | Echo-handler example implementing `handle(req) -> result<response, string>` |
| `.cargo/config.toml` | `[build] target = "wasm32-wasip1"` |
| `.gitignore` | `/target` |
| `README.md` | Host ABI reference, build prerequisites, deploy instructions |

The scaffold implements the `basin-functions-handler` WIT world from
`crates/basin-fn/wit/basin-fn.wit`. The four host imports (`query`, `http`,
`log`, `secret`) are demonstrated in the example handler.

Only `--lang rust` is supported for now. TypeScript via ComponentizeJS is
planned.

### `basinctl fn build [path]`

Shells out to `cargo build --target wasm32-wasip1 --release` in the function
project directory (`path`, default: `.`). Prints the artifact path on success:

```
target/wasm32-wasip1/release/<name>.wasm
```

Set `BASINCTL_EXEC_BUILD=1` to actually execute the build. Without it, the
command prints the cargo invocation and the expected artifact path without
running it (useful in constrained environments).

Prerequisites that must be installed separately:

```sh
rustup target add wasm32-wasip1
cargo install cargo-component   # wraps wasm-tools component new
```

### `basinctl fn deploy <name> [--path <file.wasm>] [--token <jwt>] [--rest-url <url>]`

Uploads the compiled `.wasm` to `POST /admin/v1/functions/deploy` on the Basin
REST server. Auth, URL resolution, and defaults:

| Knob | Flag | Env | Default |
|---|---|---|---|
| Admin bearer token | `--token` | `BASIN_ADMIN_TOKEN` | — (required) |
| REST server base URL | `--rest-url` | `BASIN_REST_URL`* | `http://127.0.0.1:5434` |
| Path to `.wasm` | `--path` | — | `target/wasm32-wasip1/release/<name>.wasm` |

\* `BASIN_REST_URL` is not currently read automatically; pass `--rest-url` or
rely on the default. Full env-var support can be added once the deploy surface
is stabilised.

The deploy endpoint stores the component bytes (base64-encoded) in the server's
`FunctionRegistry` (in-process, per-project). A redeploy of the same `name`
atomically increments the version counter and invalidates the compiled
`HandlerHarness` cache so the new component takes effect on the next request.

**Server-side gap:** The `FunctionRegistry` in `basin-rest` is an in-process
store that does not persist across server restarts. A catalog-backed
persistence layer (see `services/basin-server/src/fn_runtime.rs` and the W6
`CatalogFunctionStore`) is the right long-term home. Track this as a follow-up
before promoting the toolchain to `stable`.

### ABI contract

| Element | Value |
|---|---|
| WIT world | `basin-functions-handler` (in `crates/basin-fn/wit/basin-fn.wit`) |
| Target triple | `wasm32-wasip1` |
| Wasm format | Component model (WASI Preview 2) |
| Exported entrypoint | `handle(req: request) -> result<response, string>` |
| Invocation URL | `ANY /fn/v1/<name>` |
| Auth | JWT Bearer in `Authorization` header (project-scoped) |

Full reference: `docs/functions.md`.
