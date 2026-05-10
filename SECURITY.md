# Security Policy

## Reporting a vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Instead, email the maintainers (or use GitHub's private vulnerability
reporting at `Security → Advisories → Report a vulnerability` on the
repo). Include:

- Affected versions (or `main` SHA)
- A description of the vulnerability
- Steps to reproduce
- Suggested fix if you have one
- Whether you'd like public credit in the advisory

We aim to:

- Acknowledge receipt within **48 hours**
- Triage and respond with a plan within **7 days**
- Ship a patch within **14 days** for high-severity issues

## Supported versions

Pre-1.0, only the latest minor release receives security patches.
Customers running older releases should bump.

| Version       | Supported                |
|---------------|--------------------------|
| `0.x` latest  | ✅ patches + advisories  |
| `0.x` older   | ❌ upgrade to latest     |

## Security invariants

These are tested in `tests/integration/tests/security.rs` and the suite
must remain green for any release to ship. If you find a way to violate
any of these, that's a P0 vulnerability.

- **Cross-tenant data isolation.** No SQL path on a `TenantSession`
  exposes another tenant's data. Bucket prefix isolation is enforced at
  the `basin-storage` API boundary, not at call sites.
- **RLS bypass via UNION / CTE / subquery.** RLS predicate injection
  walks every query shape that can hide a `TableScan`:
  `SetExpr::SetOperation` (UNION/INTERSECT/EXCEPT), `query.with` CTEs,
  `TableFactor::Derived` subqueries, `TableFactor::NestedJoin`, and
  expression-embedded subqueries (EXISTS / IN / scalar).
- **pgwire SQL injection.** Both simple-query and extended-bind paths
  parameterise correctly; literal values arrive in `Bind` slots, not
  spliced into SQL strings.
- **Path injection.** `TableName`, `TenantId`, `PartitionKey` constructors
  reject `..`, `/`, and other traversal characters. `BASIN_COPY_PATH_ALLOWLIST`
  defaults to deny-all for `COPY … FROM/TO '<path>'`.
- **Cross-tenant fork.** `Catalog::fork_table` is structurally
  same-tenant only; cross-tenant fork is rejected at the API.
- **Rate limit enforcement.** pgwire and basin-net both enforce
  per-tenant rate limits via `governor` token-buckets; bucket-empty maps
  to PG SQLSTATE `53400`.

## Secret handling

- **Never log full SQL with parameter values.** Logs use parameter
  redaction; raw `Bind` values are debug-only behind a non-default
  feature flag.
- **bcrypt password hashes** for pgwire credentials (per [ADR 0005](./docs/decisions/0005-auth-system.md))
- **JWT auth** with HS256 signing; secret loaded from env var, never
  committed
- **TLS** via rustls (aws-lc-rs); no custom crypto

## Dependencies

CI runs `cargo audit --deny warnings` on every push. Vulnerable crates
break the build. Upgrade promptly; we don't carry unpatched
vulnerabilities into releases.
