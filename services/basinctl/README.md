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
basinctl fn                          # WASM functions toolchain roadmap (stub)
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
