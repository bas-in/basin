# Repo metadata to paste on GitHub (Settings → General + Topics)

## Repo description (visible under the repo name; max 350 chars)

> Multi-project Postgres-compatible database on object storage. Per-project
> prefix isolation, ~$0.10/project/month at scale, drop-in pgwire works with
> psql / tokio-postgres / asyncpg / JDBC / any ORM. Native vector search,
> ZSTD Parquet, Iceberg catalog. Auth + REST included. Apache-2.0, Rust.

(323 chars)

## Topics (paste in Settings → General → Topics; first 5–7 are most visible)

Recommended order — leading with high-traffic tags, then niche-specific:

```
postgres
database
rust
multi-project
s3
object-storage
vector-database
iceberg
parquet
pgwire
postgres-compatible
analytics
oltp
saas
data-lake
postgrest
hnsw
self-hosted
apache-2-0
```

Top 7 (the ones GitHub renders inline above the description) should be:

```
postgres database rust multi-project s3 vector-database iceberg
```

## Website (Settings → General → Website)

Set to the cloud-product landing page once it ships. Until then leave
unset OR point at the rendered benchmark dashboard:

```
(leave blank)
```

## Social preview image

Use `basin.svg` (already at repo root) rendered at 1280×640. The
existing svg works as the social card; rasterize with `rsvg-convert` /
`inkscape` or just upload the svg through GitHub's image picker.
