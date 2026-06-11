/**
 * `.from(table)` — REST query builder for `/rest/v1/:table`.
 *
 * Route source (verified): crates/basin-rest/src/server.rs:243-249 —
 * `GET|POST|PATCH|DELETE /rest/v1/:table`.
 *
 * The filter grammar is Basin's PostgREST-*style* dialect, implemented in
 * crates/basin-rest/src/parser.rs (`parse_query` / `parse_op`):
 *
 * - `select=<cols,csv>` (identifiers only — no embedded resources/renames)
 * - `<col>=<op>.<value>` with ops `eq|neq|gt|gte|lt|lte|in|is`
 *   (`in.(a,b,c)` parenthesised; `is.null` / `is.notnull`)
 * - `order=<col>[.asc|.desc][,…]`, `limit=N`, `offset=N`
 * - `cursor=<token>` keyset pagination (id-based, v0.1), `stream=true` NDJSON
 *
 * It is NOT full PostgREST: no `or=`, no `not.`, no `like|ilike`, no embedded
 * `select` trees, no `Prefer: return=representation`. Filters AND together.
 *
 * Response shapes (crates/basin-rest/src/routes/data.rs):
 * - plain GET → JSON array of rows
 * - GET with `limit` or `cursor` → `{ rows, next_cursor }`
 * - large results / `?stream=true` → NDJSON, final line
 *   `{"_basin_next_cursor": "…"}` when paginating
 * - POST → 201, `{ ok, tag }` (or rows); PATCH/DELETE → `{ ok, tag }`
 * - DELETE may surface 501 `E_ENGINE_UNSUPPORTED`
 */

import type { ClientContext } from "./http.js";
import { requestJson, requestRaw } from "./http.js";
import type { ExecTag, Page } from "./types.js";

type Scalar = string | number | boolean | null;

function literal(v: Scalar): string {
  if (v === null) return "null";
  return String(v);
}

export type Row = Record<string, unknown>;

/** Normalized result of a GET: rows plus the cursor when paginating. */
export interface QueryResult<T> {
  rows: T[];
  nextCursor: string | null;
}

export class QueryBuilder<T extends Row = Row>
  implements PromiseLike<QueryResult<T>>
{
  #ctx: ClientContext;
  #table: string;
  #query: Array<[string, string]> = [];
  #paginated = false;

  constructor(ctx: ClientContext, table: string) {
    this.#ctx = ctx;
    this.#table = table;
  }

  /** `select=<cols>` projection; omit or pass `*` for all columns. */
  select(columns?: string | string[]): this {
    const cols = Array.isArray(columns) ? columns.join(",") : (columns ?? "*");
    this.#query.push(["select", cols]);
    return this;
  }

  eq(column: string, value: Scalar): this {
    this.#query.push([column, `eq.${literal(value)}`]);
    return this;
  }

  neq(column: string, value: Scalar): this {
    this.#query.push([column, `neq.${literal(value)}`]);
    return this;
  }

  gt(column: string, value: Scalar): this {
    this.#query.push([column, `gt.${literal(value)}`]);
    return this;
  }

  gte(column: string, value: Scalar): this {
    this.#query.push([column, `gte.${literal(value)}`]);
    return this;
  }

  lt(column: string, value: Scalar): this {
    this.#query.push([column, `lt.${literal(value)}`]);
    return this;
  }

  lte(column: string, value: Scalar): this {
    this.#query.push([column, `lte.${literal(value)}`]);
    return this;
  }

  /** `<col>=in.(a,b,c)` — parenthesised list per parser.rs `parse_in_list`. */
  in(column: string, values: Scalar[]): this {
    this.#query.push([column, `in.(${values.map(literal).join(",")})`]);
    return this;
  }

  /** `<col>=is.null` / `<col>=is.notnull`. */
  is(column: string, value: "null" | "notnull"): this {
    this.#query.push([column, `is.${value}`]);
    return this;
  }

  /** `order=<col>.asc|desc` (repeatable). */
  order(column: string, opts: { ascending?: boolean } = {}): this {
    const dir = opts.ascending === false ? "desc" : "asc";
    this.#query.push(["order", `${column}.${dir}`]);
    return this;
  }

  /** `limit=N`. Switches the GET response to `{ rows, next_cursor }`. */
  limit(n: number): this {
    this.#query.push(["limit", String(n)]);
    this.#paginated = true;
    return this;
  }

  offset(n: number): this {
    this.#query.push(["offset", String(n)]);
    return this;
  }

  /** Resume keyset pagination from a `next_cursor` token. */
  cursor(token: string): this {
    this.#query.push(["cursor", token]);
    this.#paginated = true;
    return this;
  }

  // -------------------------------------------------------------------------
  // Execution
  // -------------------------------------------------------------------------

  /** Execute as GET and normalize both response shapes. */
  async run(): Promise<QueryResult<T>> {
    const body = await requestJson<unknown>(
      this.#ctx,
      "GET",
      `/rest/v1/${this.#table}`,
      { query: this.#query },
    );
    return normalizeGet<T>(body);
  }

  /** Execute as GET and return rows only. */
  async rows(): Promise<T[]> {
    return (await this.run()).rows;
  }

  /**
   * Execute as a paginated GET (`{ rows, next_cursor }` shape). Identical to
   * `run()` but documents intent when iterating with `.cursor()`.
   */
  async page(): Promise<Page<T>> {
    const r = await this.run();
    return { rows: r.rows, next_cursor: r.nextCursor };
  }

  /**
   * Execute as GET with `?stream=true` (NDJSON). Yields rows; a trailing
   * `{"_basin_next_cursor": …}` line is captured, not yielded.
   */
  async *stream(): AsyncGenerator<T, string | null, void> {
    const query: Array<[string, string]> = [...this.#query, ["stream", "true"]];
    const res = await requestRaw(this.#ctx, "GET", `/rest/v1/${this.#table}`, {
      query,
    });
    const text = await res.text();
    let nextCursor: string | null = null;
    for (const line of text.split("\n")) {
      const trimmed = line.trim();
      if (trimmed.length === 0) continue;
      const parsed = JSON.parse(trimmed) as Record<string, unknown>;
      if (typeof parsed["_basin_next_cursor"] === "string") {
        nextCursor = parsed["_basin_next_cursor"];
        continue;
      }
      yield parsed as T;
    }
    return nextCursor;
  }

  /** `POST /rest/v1/:table` — insert one object or an array (201). */
  async insert(values: Row | Row[]): Promise<ExecTag | T[]> {
    return requestJson(this.#ctx, "POST", `/rest/v1/${this.#table}`, {
      body: values,
    });
  }

  /** `PATCH /rest/v1/:table?<filters>` — update rows matching the filters. */
  async update(values: Row): Promise<ExecTag | T[]> {
    return requestJson(this.#ctx, "PATCH", `/rest/v1/${this.#table}`, {
      query: this.#query,
      body: values,
    });
  }

  /**
   * `DELETE /rest/v1/:table?<filters>`. May throw `E_ENGINE_UNSUPPORTED`
   * (501) on engines without DELETE support — see data.rs `delete_table`.
   */
  async delete(): Promise<ExecTag | T[]> {
    return requestJson(this.#ctx, "DELETE", `/rest/v1/${this.#table}`, {
      query: this.#query,
    });
  }

  /** Awaiting the builder executes the GET (`await client.from('t').eq(…)`). */
  then<R1 = QueryResult<T>, R2 = never>(
    onfulfilled?: ((value: QueryResult<T>) => R1 | PromiseLike<R1>) | null,
    onrejected?: ((reason: unknown) => R2 | PromiseLike<R2>) | null,
  ): PromiseLike<R1 | R2> {
    return this.run().then(onfulfilled, onrejected);
  }
}

function normalizeGet<T>(body: unknown): QueryResult<T> {
  if (Array.isArray(body)) {
    return { rows: body as T[], nextCursor: null };
  }
  if (typeof body === "object" && body !== null && "rows" in body) {
    const wrapped = body as { rows: T[]; next_cursor: string | null };
    return { rows: wrapped.rows ?? [], nextCursor: wrapped.next_cursor ?? null };
  }
  // `{ok, tag}` empty-result shape (ExecResult::Empty) → no rows.
  return { rows: [], nextCursor: null };
}
