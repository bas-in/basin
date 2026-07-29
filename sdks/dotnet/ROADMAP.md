# basin-dotnet — Roadmap

The shape this SDK is reaching for: a first-class .NET citizen in the Basin SDK
family — idiomatic async/await throughout, zero heavy dependencies in the core
path, and full parity with the reference surface (Python + Rust) on the
capabilities that matter most to .NET consumers: typed rows, columnar reads via
Arrow IPC, streaming large result sets, and a reliable realtime change feed.

v0.1 shipped with the initial monorepo split and covers auth, the PostgREST
query builder, storage, realtime WebSocket, RPC/HTTP functions, Arrow IPC, and
the typed error model. Everything below is forward work.

---

## 0.2 — NDJSON streaming result iterator

The single remaining streaming gap for basin-dotnet per the SDK parity matrix.
The engine promotes responses over ~1 MiB or 10,000 rows to NDJSON
(`?stream=true`, one row per line, trailing `{"_basin_next_cursor":"…"}`
sentinel). The infrastructure for this already exists in `StreamResult`
(`src/Basin/Types.cs`), including cursor extraction and the `IAsyncEnumerable`
interface — but it is not yet wired to the public `QueryBuilder` API in a way
that callers can discover.

Outcome:
- `client.Table(t).Select(…).StreamAsync()` is now the documented, tested,
  public primary path for large result sets, not a secondary note in the query
  docs. The existing `StreamResult` type already implements this — the work is
  ensuring it is covered by tests and the README quickstart shows it
  alongside `RunAsync`.
- The builder's `RunAsync` path detects a `Content-Type: application/x-ndjson`
  response (auto-promoted large result) and falls back to NDJSON parsing rather
  than failing or silently truncating. This removes the surprise for callers who
  never read the streaming docs but send a query that happens to cross the size
  threshold.
- Add at least one unit test for each code path using a `MockHandler` that
  serves NDJSON with a trailing cursor line.

---

## 0.3 — NuGet publish + package hygiene

The `Basin.Sdk` package ID is set in `Basin.csproj` but nothing has been
published to nuget.org yet. This milestone gets the package there and ensures
consumers have a clean install experience.

- `dotnet pack` target verified in CI; `*.nupkg` artifact uploaded as a CI
  artifact on every push to `main`.
- NuGet publish step on tagged releases (GitHub Actions `workflow_dispatch` or
  tag trigger), gated behind an `NUGET_API_KEY` secret.
- `<RepositoryUrl>` updated to the standalone repo URL (currently points at the
  monorepo).
- `<PackageReadmeFile>` wired so nuget.org renders `README.md` in the package
  listing.
- `<PackageTags>` set: `basin database postgresql realtime storage`.
- Include XML doc comments in the package (`<GenerateDocumentationFile>true</GenerateDocumentationFile>`
  already set) so IntelliSense surfaces summaries without requiring source.

---

## 0.4 — Typed row deserialization codegen

The `QueryResult.Into<T>()` helper is a good escape hatch, but it requires
callers to write their own record types by hand and match JSON property names
with `[JsonPropertyName]` attributes. The engine ships
`GET /rest/v1/_openapi.json` — a per-project OpenAPI 3.0.3 document
auto-generated from the Arrow schema of every table.

Outcome:
- `dotnet tool install -g Basin.Sdk.Cli` (separate package): a CLI tool that
  fetches the OpenAPI doc and emits `BasinTypes.g.cs` with strongly-typed
  `Row` / `Insert` / `Update` records per table, wired to match the engine's
  JSON property names.
- No runtime dependency on the generated file — it is just source. Callers can
  use `result.Into<Products.Row>()` instead of writing the record themselves.

---

## 0.5 — Admin routes

The engine exposes operator-grade routes under `/admin/v1/*` for provisioning
per-project pgwire credentials and rotating them. Today callers must use the
`RequestAsync` escape hatch.

Outcome:
- `AdminClient` added to `BasinClient` (accessible via `client.Admin`).
- `client.Admin.Projects.ProvisionAsync(projectId)` →
  `{ ConnectionString }`.
- `client.Admin.Projects.RotateCredentialsAsync(pgwireUser)` →
  `{ ConnectionString }`.
- `client.Admin.Projects.ListCredentialsAsync(projectId)` →
  `IReadOnlyList<CredentialDescriptor>` (metadata only — no plaintext hashes).
- Calls 401 cleanly when the session lacks `is_admin`; typed
  `BasinApiException("E_FORBIDDEN", …)` so callers can route to a
  permissions-gate UI.

---

## 0.6 — DX and resilience polish

Smaller items that compound into "this SDK feels production-grade."

- **Retry + exponential backoff** on transient failures (network errors, 5xx,
  429 with `Retry-After`). Configurable via a `RetryPolicy` on the builder;
  opt-out per-call via a `CancellationToken` with a deadline. Defaults: 3
  attempts, starting at 200 ms.
- **`TreatWarningsAsErrors` turned on** in `Basin.csproj` and all existing
  warnings resolved. Currently `false` as a bootstrapping concession.
- **`net9.0` TFM added** alongside `net8.0` once .NET 9 reaches LTS status and
  the GitHub-hosted runner images ship it.
- **XML doc audit**: every `public` type and member in `src/Basin/` has a
  `<summary>`. Currently ~85 % covered.
- **Benchmarks**: a `BenchmarkDotNet` micro-benchmark for `QueryResult.Into<T>`
  and `StreamResult` to gate against accidental allocations on hot paths.
- **Samples project** under `samples/`: a self-contained console app that
  demonstrates auth, query, storage, and realtime against a local engine, so
  new contributors can verify the SDK works end-to-end without standing up
  infrastructure.

---

## 1.0 — Stable public API

Once 0.2–0.6 land:

- Public API review: no `sealed` regressions, no breaking rename since 0.1.
- `<Version>1.0.0</Version>` in `Basin.csproj` with a semantic-versioning
  commitment going forward.
- CHANGELOG.md tracking breaking changes between versions.
- Integration test suite runnable against a real basin-engine container image
  (Docker Compose setup in `tests/Integration/`).

---

## Known gaps vs. the parity matrix

For context, the SDK parity matrix at
[`basin/docs/sdk-parity.md`](https://github.com/vul-os/basin/blob/main/docs/sdk-parity.md)
identifies one dotnet-specific gap and several uniform absences across all SDKs:

**Dotnet-specific (actionable):**
- NDJSON streaming result iterator — not yet surfaced as a documented public
  path. Addressed in **0.2** above.

**Uniform absences across all ten SDKs (server/contract gaps, not SDK gaps):**
- Transactions / `BEGIN`–`COMMIT` — not exposed by the `basin-rest` surface.
- `COPY` / bulk-insert path — not exposed by `basin-rest`.
- Vector / similarity search — planned engine feature (v0.3+).
- Raw SQL `execute()` — not exposed by `basin-rest`.

These are not recommendations for basin-dotnet specifically; they are blocked
on the engine roadmap.
