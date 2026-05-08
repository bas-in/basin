# basinctl

Small admin CLI for a running Basin pgwire endpoint.

```sh
export BASIN_URL=postgres://alice@127.0.0.1:5433/basin
basinctl ping                        # OK in 3 ms
basinctl ping postgres://h:5433/db   # positional override
basinctl tenants                     # prints current_user
basinctl tables                      # one table name per line
basinctl query "SELECT 1, 'hi'"      # padded | separated rows
basinctl version                     # basinctl 0.0.1 (<sha>)
```

`--url` overrides `BASIN_URL`. On error, prints to stderr and exits non-zero.
