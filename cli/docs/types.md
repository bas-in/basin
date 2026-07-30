---
title: "Type Generation (basin gen types)"
description: "Generate TypeScript, Go, or Python type definitions from your Basin Cloud project schema using information_schema."
---

# Type Generation — `basin gen types`

`basin gen types` queries `information_schema.columns` via
`POST /v1/projects/{ref}/sql/query` and emits typed database interfaces or structs
for your application code. No new cloud endpoint is needed — the CLI assembles
source code from the same `information_schema` a developer can query themselves.

The type-mapping opinions (nullable forms, JSONB representation, vector dimensions)
live in the `type_table()` mapping in
[`src/commands/gen.rs`](../src/commands/gen.rs); each language has its own
`emit_*` function in the same file. (Those two responsibilities lived in
`gen_types_map.go` and `cmd_gen.go` in the Go implementation this command was
ported from; neither file exists any more.)

---

## When to regenerate

Regenerate types whenever the schema changes:

```sh
basin db push               # apply the migration
basin gen types typescript  # emit updated types
```

Automate in CI by adding a step after `basin db push`. In local development, the
`--watch` flag (planned, not yet implemented) will watch `./basin/migrations/` and
re-emit automatically on every successful push.

---

## Usage

```
basin gen types <lang> [--project=<ref>] [--schema=public]
                        [--output=<path>] [--package=<name>]
```

| Flag | Default | Description |
|---|---|---|
| `<lang>` | required | Target language: `typescript`, `go`, or `python`. |
| `--project=<ref>` | from `./basin/config.toml` | Project ref. |
| `--schema=<name>` | `public` | Postgres schema to introspect. |
| `--output=<path>` | stdout | Write generated source to a file. |
| `--package=<name>` | `database` | Go package name (Go target only). |
| `--watch` | `false` | Re-emit on migration push. (Stub — planned.) |
| `--json` | `false` | Emit metadata `{ "lang", "tables", "columns", "output_path" }` to stderr. |

Aliases: `ts` for `typescript`, `py` for `python`.

---

## Per-language opinions

### TypeScript

Output is a collection of `export interface Table_<TableName>` declarations
followed by a single `export interface Database { Tables: { ... } }` wrapper
that mirrors the Supabase type-generation convention.

Nullable columns use the union form `T | null`:

```typescript
export interface Table_Users {
  id: string;           // uuid NOT NULL
  email: string | null; // text NULL
  created_at: string;   // timestamp with time zone NOT NULL
}
```

JSONB columns map to `Record<string, unknown>`.
Vector columns map to `number[]` with a comment indicating the declared dimension.

Run with:

```sh
basin gen types typescript --output=src/types/database.ts
```

### Go

Output is a Go source file with one struct per table. Column names are converted to
PascalCase field names. Both `json:"..."` and `db:"..."` tags are emitted, the
latter for compatibility with `database/sql` + `jmoiron/sqlx`.

Nullable columns use pointer types (`*T`):

```go
type Users struct {
    ID        [16]byte         `json:"id" db:"id"`
    Email     *string          `json:"email" db:"email"`
    CreatedAt time.Time        `json:"created_at" db:"created_at"`
    Metadata  json.RawMessage  `json:"metadata" db:"metadata"`
}
```

The `--package=<name>` flag sets the `package` declaration (default: `database`).
Import statements for `time`, `encoding/json`, etc. are deduplicated and emitted
automatically.

Run with:

```sh
basin gen types go --output=./internal/schema/database.go --package=schema
```

### Python

Output is a Pydantic-v2 `BaseModel` subclass per table. Nullable columns use
`Optional[T]`:

```python
class Users(BaseModel):
    id: UUID
    email: Optional[str]
    created_at: datetime
    metadata: Optional[Any]
```

Import statements (`from pydantic import BaseModel`, `from uuid import UUID`, etc.)
are collected from all mapped types and deduplicated at the top of the file.

Run with:

```sh
basin gen types python --output=./app/models/database.py
```

---

## Engine type → target type mapping table

The full mapping is defined in
the `type_table()` mapping in [`src/commands/gen.rs`](../src/commands/gen.rs).
This table is the canonical source of
truth; the doc below is a summary. The `MapType(pg PgType, lang LangTarget)`
function returns the mapping; callers fall back to `string` / `any` / `Any` with a
`// WARNING: unknown pg type` comment in the generated file when a type is absent.

Only Postgres types marked ✅ in `../basin/CAPABILITIES.md` are included.
Types marked 🚫 (MONEY, XML) or ◻️ (planned) are intentionally absent so generated
code never silently claims support for an unsupported column type.

| Postgres type (`information_schema.data_type`) | TypeScript | Go | Python |
|---|---|---|---|
| `boolean` / `bool` | `boolean` / `boolean \| null` | `bool` / `*bool` | `bool` / `Optional[bool]` |
| `smallint` / `int2` | `number` / `number \| null` | `int16` / `*int16` | `int` / `Optional[int]` |
| `integer` / `int4` | `number` / `number \| null` | `int32` / `*int32` | `int` / `Optional[int]` |
| `bigint` / `int8` | `bigint` / `bigint \| null` | `int64` / `*int64` | `int` / `Optional[int]` |
| `real` / `float4` | `number` / `number \| null` | `float32` / `*float32` | `float` / `Optional[float]` |
| `double precision` / `float8` | `number` / `number \| null` | `float64` / `*float64` | `float` / `Optional[float]` |
| `numeric` / `decimal` | `string` / `string \| null` | `string` / `*string` | `Decimal` / `Optional[Decimal]` |
| `text` / `character varying` / `varchar` / `character` / `char` | `string` / `string \| null` | `string` / `*string` | `str` / `Optional[str]` |
| `bytea` | `Uint8Array` / `Uint8Array \| null` | `[]byte` / `[]byte` | `bytes` / `Optional[bytes]` |
| `uuid` | `string` / `string \| null` | `[16]byte` / `*[16]byte` | `UUID` / `Optional[UUID]` |
| `jsonb` / `json` | `Record<string, unknown>` / `Record<string, unknown> \| null` | `json.RawMessage` / `json.RawMessage` | `Any` / `Optional[Any]` |
| `timestamp with time zone` / `timestamptz` / `timestamp without time zone` / `timestamp` | `string` / `string \| null` | `time.Time` / `*time.Time` | `datetime` / `Optional[datetime]` |
| `date` | `string` / `string \| null` | `time.Time` / `*time.Time` | `date` / `Optional[date]` |
| `time without time zone` / `time with time zone` / `time` | `string` / `string \| null` | `string` / `*string` | `time` / `Optional[time]` |
| `interval` | `string` / `string \| null` | `string` / `*string` | `str` / `Optional[str]` |
| `vector` | `number[]` / `number[] \| null` | `[]float32` / `[]float32` | `list[float]` / `Optional[list[float]]` |

**Notes on specific types:**

- **`bigint` in TypeScript**: mapped to `bigint` (the JS primitive), not `number`,
  to preserve full int64 precision. Callers doing JSON serialisation should be aware
  that `JSON.stringify` does not support `bigint` directly — use `String(val)` or
  a custom replacer.
- **`numeric`/`decimal` in Go**: mapped to `string` rather than a `decimal.Decimal`
  library type to honour the stdlib-only constraint. Callers doing arithmetic should
  use `math/big.Rat` or a third-party lib in their own layer.
- **`bytea` / `json.RawMessage` in Go**: these are slice types whose zero value is
  `nil`, which marshals as `null` — so the non-nullable and nullable forms are
  identical in the Go target.
- **`uuid` in Go**: mapped to `[16]byte`, the stdlib representation (no `google/uuid`
  dep). Convert with `uuid.UUID(val)` if you use `google/uuid`.
- **`vector`**: `information_schema` returns the bare type `vector` without the
  dimension suffix `(N)`. The emitter strips `(N)` before the `MapType` lookup; the
  dimension is emitted as a comment in the generated file.

---

## `--watch` future plans

`--watch` re-emits on every successful `basin db push`. The planned implementation
watches `./basin/migrations/` with a debounced file-system watcher and re-runs the
full generate+write cycle on each new file. It is currently a stub that prints
`watch not yet implemented` and exits 0.

The flag is documented here so type-generation tooling (IDE plugins, Makefile targets)
can adopt the canonical flag name now and benefit from the implementation when it lands.

---

## Golden file maintenance

Test snapshots live in `testdata/expected.{ts,go,py}`. The test suite compares
`gen types <lang>` output against these golden files character-by-character. When
you update the type-mapping table or an emitter:

1. Run `go test -run TestGenTypes -update-golden ./...` (flag not yet wired — update
   the golden files manually for now).
2. Review the diff against the golden file carefully: each type change touches every
   table that uses it.
3. Commit the updated golden file alongside the type-map change so the CI suite
   stays green.

---

## Tests covering this surface

- **`cmd_gen_test.go`** — TypeScript / Go / Python emitters, nullable mapping,
  JSONB mapping, `vector` mapping, snapshot tests against `testdata/expected.{ts,go,py}`.
- **`type_table()` in `src/commands/gen.rs`** — the mapping table itself. Every type in the table is
  exercised by the emitter tests.

---

*Cross-links: [db-workflow.md](./db-workflow.md) · [branches.md](./branches.md)*
