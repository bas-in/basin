/**
 * PostgrestQueryBuilder — chainable, thenable. Each filter / modifier
 * mutates internal state; awaiting executes the HTTP call.
 *
 * Shape mirrors @supabase/postgrest-js so app code that does
 * `supabase.from('t').select('*').eq('a', 1)` ports directly.
 *
 * The SDK calls basin-engine DIRECTLY (no basin-cloud hop). The engine
 * speaks the PostgREST dialect at `/rest/v1/:table`:
 *  - GET    /rest/v1/{table}    — select
 *  - POST   /rest/v1/{table}    — insert / upsert
 *  - PATCH  /rest/v1/{table}    — update
 *  - DELETE /rest/v1/{table}    — delete
 *  - HEAD   /rest/v1/{table}    — exists / count probes
 *
 * The engine may wrap responses in `{data, error}`; this builder
 * tolerates both the envelope and a flat-shape fallback (matches the
 * unwrap heuristic in src/auth/client.ts).
 */

import { BasinError } from "../errors.js";
import type { AuthClient } from "../auth/client.js";
import type { CountOption, PostgrestResponse, PostgrestSingleResponse } from "./types.js";

interface PostgrestDeps {
  url: string;
  headers: Record<string, string>;
  fetch: typeof fetch;
  auth: AuthClient;
}

type PendingMethod = "GET" | "POST" | "PATCH" | "DELETE";

interface PendingBody {
  method: PendingMethod;
  body?: unknown;
}

type ReturningMode = "representation" | "minimal";

type AcceptMode = "json" | "single" | "csv" | "geojson";

export class PostgrestQueryBuilder<T> implements PromiseLike<PostgrestResponse<T>> {
  #deps: PostgrestDeps;
  #search: URLSearchParams = new URLSearchParams();
  #pending: PendingBody = { method: "GET" };
  #single: "row" | "maybe" | null = null;
  #count: CountOption | null = null;
  #returning: ReturningMode = "representation";
  #accept: AcceptMode = "json";
  #explain = false;
  #explainFormat: "text" | "json" = "text";
  #asOf: string | null = null;
  #vector = false;
  #extraHeaders: Record<string, string> = {};

  constructor(deps: PostgrestDeps) {
    this.#deps = deps;
  }

  // ─── select / mutations ────────────────────────────────────────────

  /**
   * Set the column-list and optional count strategy.
   *
   * Chained AFTER an insert / update / upsert / delete, `.select()`
   * does NOT switch the HTTP method back to GET — it just narrows the
   * column projection on the mutation response (matches supabase-js
   * semantics). Chained on its own it's the natural read entry point.
   */
  select(columns = "*", opts?: { count?: CountOption }): this {
    this.#search.set("select", columns);
    if (opts?.count) this.#count = opts.count;
    return this;
  }

  insert(rows: Partial<T> | Partial<T>[], opts?: { upsert?: boolean }): this {
    this.#pending = { method: "POST", body: rows };
    if (opts?.upsert) this.#search.set("on_conflict", "*");
    return this;
  }

  update(values: Partial<T>): this {
    this.#pending = { method: "PATCH", body: values };
    return this;
  }

  upsert(rows: Partial<T> | Partial<T>[], opts?: { onConflict?: string }): this {
    this.#pending = { method: "POST", body: rows };
    if (opts?.onConflict) this.#search.set("on_conflict", opts.onConflict);
    return this;
  }

  delete(): this {
    this.#pending = { method: "DELETE" };
    return this;
  }

  // ─── filters ───────────────────────────────────────────────────────

  eq(column: string, value: unknown): this {
    return this.#filter(column, "eq", encodeFilterValue(value));
  }

  neq(column: string, value: unknown): this {
    return this.#filter(column, "neq", encodeFilterValue(value));
  }

  gt(column: string, value: unknown): this {
    return this.#filter(column, "gt", encodeFilterValue(value));
  }

  gte(column: string, value: unknown): this {
    return this.#filter(column, "gte", encodeFilterValue(value));
  }

  lt(column: string, value: unknown): this {
    return this.#filter(column, "lt", encodeFilterValue(value));
  }

  lte(column: string, value: unknown): this {
    return this.#filter(column, "lte", encodeFilterValue(value));
  }

  like(column: string, pattern: string): this {
    return this.#filter(column, "like", encodeFilterValue(pattern));
  }

  ilike(column: string, pattern: string): this {
    return this.#filter(column, "ilike", encodeFilterValue(pattern));
  }

  is(column: string, value: null | boolean): this {
    return this.#filter(column, "is", value === null ? "null" : String(value));
  }

  in(column: string, values: unknown[]): this {
    return this.#filter(column, "in", `(${values.map(encodeFilterValue).join(",")})`);
  }

  contains(column: string, value: unknown): this {
    return this.#filter(column, "cs", encodeContainmentValue(value));
  }

  containedBy(column: string, value: unknown): this {
    return this.#filter(column, "cd", encodeContainmentValue(value));
  }

  rangeGt(column: string, range: string): this {
    return this.#filter(column, "sr", encodeFilterValue(range));
  }

  rangeGte(column: string, range: string): this {
    return this.#filter(column, "nxl", encodeFilterValue(range));
  }

  rangeLt(column: string, range: string): this {
    return this.#filter(column, "sl", encodeFilterValue(range));
  }

  rangeLte(column: string, range: string): this {
    return this.#filter(column, "nxr", encodeFilterValue(range));
  }

  rangeAdjacent(column: string, range: string): this {
    return this.#filter(column, "adj", encodeFilterValue(range));
  }

  overlaps(column: string, value: unknown): this {
    return this.#filter(column, "ov", encodeContainmentValue(value));
  }

  textSearch(
    column: string,
    query: string,
    opts?: { type?: "plain" | "phrase" | "websearch"; config?: string },
  ): this {
    const typeMap: Record<string, string> = {
      plain: "plfts",
      phrase: "phfts",
      websearch: "wfts",
    };
    const op = (opts?.type && typeMap[opts.type]) ?? "fts";
    const cfg = opts?.config ? `(${opts.config})` : "";
    return this.#filter(column, `${op}${cfg}`, encodeFilterValue(query));
  }

  match(query: Record<string, unknown>): this {
    for (const [k, v] of Object.entries(query)) {
      this.eq(k, v);
    }
    return this;
  }

  not(column: string, operator: string, value: unknown): this {
    const encoded =
      Array.isArray(value)
        ? `(${value.map(encodeFilterValue).join(",")})`
        : value === null
          ? "null"
          : encodeFilterValue(value);
    return this.#filter(column, `not.${operator}`, encoded);
  }

  or(filters: string, opts?: { foreignTable?: string }): this {
    const key = opts?.foreignTable ? `${opts.foreignTable}.or` : "or";
    this.#search.append(key, `(${filters})`);
    return this;
  }

  filter(column: string, operator: string, value: unknown): this {
    const encoded =
      Array.isArray(value)
        ? `(${value.map(encodeFilterValue).join(",")})`
        : value === null
          ? "null"
          : encodeFilterValue(value);
    return this.#filter(column, operator, encoded);
  }

  // ─── modifiers ─────────────────────────────────────────────────────

  order(column: string, opts: { ascending?: boolean; nullsFirst?: boolean } = {}): this {
    const dir = opts.ascending === false ? "desc" : "asc";
    const nulls = opts.nullsFirst ? ".nullsfirst" : "";
    this.#search.append("order", `${column}.${dir}${nulls}`);
    return this;
  }

  limit(n: number): this {
    this.#search.set("limit", String(n));
    return this;
  }

  range(from: number, to: number): this {
    this.#search.set("offset", String(from));
    this.#search.set("limit", String(to - from + 1));
    return this;
  }

  single(): PostgrestSinglePromise<T> {
    this.#single = "row";
    this.#accept = "single";
    return this as unknown as PostgrestSinglePromise<T>;
  }

  maybeSingle(): PostgrestSinglePromise<T> {
    this.#single = "maybe";
    this.#accept = "single";
    return this as unknown as PostgrestSinglePromise<T>;
  }

  csv(): PostgrestStringPromise {
    this.#accept = "csv";
    return this as unknown as PostgrestStringPromise;
  }

  geojson(): PostgrestStringPromise {
    this.#accept = "geojson";
    return this as unknown as PostgrestStringPromise;
  }

  /**
   * Return the EXPLAIN plan for the current query as a string.
   * `format: 'text'` (default) returns human-readable plan text;
   * `format: 'json'` returns the JSON plan as a string (call
   * `JSON.parse(data)` to access the tree). `analyze` runs the plan
   * with timings; `verbose` adds extra detail (qualifications,
   * aliases, etc.).
   *
   * The body is returned verbatim regardless of format so the typed
   * response (`PostgrestStringPromise`) stays uniform.
   */
  explain(opts?: { analyze?: boolean; verbose?: boolean; format?: "text" | "json" }): PostgrestStringPromise {
    this.#explain = true;
    const parts: string[] = [];
    if (opts?.analyze) parts.push("analyze");
    if (opts?.verbose) parts.push("verbose");
    if (parts.length > 0) this.#search.set("options", parts.join("|"));
    this.#explainFormat = opts?.format ?? "text";
    return this as unknown as PostgrestStringPromise;
  }

  /**
   * Control the cloud's `Prefer: return=` header. Default is
   * `representation` — the server echoes the affected rows. Pass
   * `'minimal'` for write-only operations where the response body is
   * irrelevant; saves bandwidth + skips the JSON serialisation cost.
   */
  returning(mode: ReturningMode): this {
    this.#returning = mode;
    return this;
  }

  // Prefer is merged (comma-joined) rather than overwritten because
  // basin-rest reads multiple directives from a single Prefer header and
  // the SDK may already have set return= or count= before the caller
  // adds their own (e.g. tx=rollback). All other keys the caller wins.
  headers(extra: Record<string, string>): this {
    this.#extraHeaders = { ...this.#extraHeaders, ...extra };
    return this;
  }

  // ─── basin extensions ──────────────────────────────────────────────

  /**
   * Vector similarity search — basin-rest extension. Maps to
   * `ORDER BY <column> <-> $1 LIMIT k`.
   *
   * Not yet supported on basin-rest's PostgREST surface; the method
   * records the call and surfaces a `not_implemented` BasinError on
   * execute so app code can be written today + flip on once basin-rest
   * grows the operator. Tracking: TASKS.md Tier 2 vector ops.
   */
  vectorSearch(
    _column: string,
    _query: number[] | string,
    _opts?: { distance?: "cosine" | "l2" | "ip"; limit?: number },
  ): this {
    this.#vector = true;
    return this;
  }

  /**
   * Engine-snapshot read — basin v0.2 MVCC placeholder. Records the
   * snapshot id; on execute throws `not_implemented` until basin v0.2
   * ships the AS OF SNAPSHOT syntax. Consumers can write the call site
   * today and flip on later with no SDK upgrade.
   */
  asOf(snapshotId: string): this {
    this.#asOf = snapshotId;
    return this;
  }

  // ─── execution (thenable) ──────────────────────────────────────────

  then<TResult1 = PostgrestResponse<T>, TResult2 = never>(
    onfulfilled?:
      | ((value: PostgrestResponse<T>) => TResult1 | PromiseLike<TResult1>)
      | null
      | undefined,
    onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null | undefined,
  ): Promise<TResult1 | TResult2> {
    return this.#execute().then(onfulfilled, onrejected);
  }

  stream(): AsyncIterable<T> {
    this.#search.set("stream", "true");
    const deps = this.#deps;
    const search = this.#search;
    const buildHeaders = this.#buildHeaders.bind(this);
    const pending = this.#pending;

    return {
      [Symbol.asyncIterator](): AsyncIterator<T> {
        const qs = search.toString();
        const url = qs ? `${deps.url}?${qs}` : deps.url;
        const headers = buildHeaders();
        const init: RequestInit = { method: pending.method, headers };
        if (pending.body !== undefined) {
          init.body = JSON.stringify(pending.body);
        }

        let readerState: ReadableStreamDefaultReader<string> | null = null;
        let done = false;
        let lineBuffer = "";
        let fetchError: unknown = null;
        let fetchResponse: Response | null = null;
        let fetchReady: Promise<void> | null = null;

        function initFetch(): Promise<void> {
          if (fetchReady) return fetchReady;
          fetchReady = deps.fetch(url, init).then(
            (res) => { fetchResponse = res; },
            (err) => { fetchError = err; },
          );
          return fetchReady;
        }

        return {
          async next(): Promise<IteratorResult<T>> {
            if (done) return { value: undefined as unknown as T, done: true };

            await initFetch();

            if (fetchError !== null) {
              done = true;
              throw new BasinError("network", networkErrorMessage(fetchError));
            }

            const res = fetchResponse!;

            if (!res.ok) {
              done = true;
              const text = await res.text().catch(() => "");
              throw new BasinError(
                errorCodeForStatus(res.status),
                text || `request failed (HTTP ${res.status})`,
                res.status,
              );
            }

            if (!readerState) {
              if (!res.body) {
                done = true;
                return { value: undefined as unknown as T, done: true };
              }
              const textStream = res.body
                .pipeThrough(new TextDecoderStream());
              readerState = textStream.getReader();
            }

            while (true) {
              const newlineIdx = lineBuffer.indexOf("\n");
              if (newlineIdx !== -1) {
                const line = lineBuffer.slice(0, newlineIdx).trimEnd();
                lineBuffer = lineBuffer.slice(newlineIdx + 1);
                if (!line) continue;
                if (line.includes("_basin_next_cursor")) {
                  try { JSON.parse(line); } catch { /* not a valid sentinel */ }
                  const parsed = (() => { try { return JSON.parse(line) as Record<string, unknown>; } catch { return null; } })();
                  if (parsed && "_basin_next_cursor" in parsed) {
                    done = true;
                    return { value: undefined as unknown as T, done: true };
                  }
                }
                let row: T;
                try {
                  row = JSON.parse(line) as T;
                } catch (e) {
                  done = true;
                  throw new BasinError("invalid_response", `failed to parse NDJSON line: ${e instanceof Error ? e.message : String(e)}`);
                }
                return { value: row, done: false };
              }

              let chunk: ReadableStreamReadResult<string>;
              try {
                chunk = await readerState.read();
              } catch (e) {
                done = true;
                throw new BasinError("network", networkErrorMessage(e));
              }

              if (chunk.done) {
                const remaining = lineBuffer.trimEnd();
                lineBuffer = "";
                done = true;
                if (!remaining) return { value: undefined as unknown as T, done: true };
                if (remaining.includes("_basin_next_cursor")) {
                  const parsed = (() => { try { return JSON.parse(remaining) as Record<string, unknown>; } catch { return null; } })();
                  if (parsed && "_basin_next_cursor" in parsed) {
                    return { value: undefined as unknown as T, done: true };
                  }
                }
                let row: T;
                try {
                  row = JSON.parse(remaining) as T;
                } catch (e) {
                  throw new BasinError("invalid_response", `failed to parse NDJSON line: ${e instanceof Error ? e.message : String(e)}`);
                }
                return { value: row, done: false };
              }

              lineBuffer += chunk.value;
            }
          },

          async return(): Promise<IteratorResult<T>> {
            done = true;
            if (readerState) {
              try { await readerState.cancel(); } catch { /* ignore */ }
            }
            return { value: undefined as unknown as T, done: true };
          },
        };
      },
    };
  }

  // ── private ────────────────────────────────────────────────────────

  #filter(column: string, operator: string, encodedValue: string): this {
    this.#search.append(column, `${operator}.${encodedValue}`);
    return this;
  }

  #buildHeaders(): Record<string, string> {
    const headers: Record<string, string> = { ...this.#deps.headers };

    // RLS: attach the current session's bearer token when one exists.
    // The anon `apikey` header stays — basin-rest reads both (apikey
    // identifies the project; bearer identifies the user for RLS).
    //
    // After a successful `basin.auth.signInWithPassword(...)`, the engine
    // evaluates auth.uid() / auth.role() / auth.jwt() in SQL as the
    // signed-in user's identity, which means RLS policies like:
    //
    //   CREATE POLICY "users see own rows" ON items
    //     FOR ALL USING (owner_id = auth.uid());
    //
    // automatically filter rows for the signed-in user — no extra code
    // needed on the query builder side beyond passing the bearer token.
    // Unsigned / anon callers see auth.role() = 'anon' and auth.uid() = null.
    const session = this.#deps.auth.getSession();
    if (session?.access_token) {
      headers["Authorization"] = `Bearer ${session.access_token}`;
    }

    // Accept header per output mode.
    if (this.#accept === "single") {
      headers["Accept"] = "application/vnd.pgrst.object+json";
    } else if (this.#accept === "csv") {
      headers["Accept"] = "text/csv";
    } else if (this.#accept === "geojson") {
      headers["Accept"] = "application/geo+json";
    } else if (this.#explain) {
      headers["Accept"] =
        this.#explainFormat === "json"
          ? "application/vnd.pgrst.plan+json"
          : "application/vnd.pgrst.plan";
    }

    // Prefer header — combine return= and count= when both are set.
    // GETs are pure selects → no return= directive (the body always is
    // the rows). Mutations (POST/PATCH/DELETE) carry return= so the
    // caller can opt into representation (default) or minimal.
    const prefer: string[] = [];
    if (this.#pending.method !== "GET") {
      prefer.push(`return=${this.#returning}`);
    }
    if (this.#count) {
      prefer.push(`count=${this.#count}`);
    }
    if (prefer.length > 0) {
      headers["Prefer"] = prefer.join(",");
    }

    // Merge caller-supplied extra headers. Prefer is comma-joined so
    // basin-rest sees all directives; every other key the caller wins.
    for (const [key, value] of Object.entries(this.#extraHeaders)) {
      if (key.toLowerCase() === "prefer" && headers["Prefer"]) {
        headers["Prefer"] = `${headers["Prefer"]},${value}`;
      } else {
        headers[key] = value;
      }
    }

    return headers;
  }

  async #execute(): Promise<PostgrestResponse<T>> {
    // Basin-extension stubs surface as not_implemented errors before
    // we hit the network — consumers can write the call site today.
    if (this.#vector) {
      return {
        data: null,
        error: new BasinError(
          "not_implemented",
          "vectorSearch is not yet supported by basin-rest; lands when the PostgREST surface grows the `<->` operator",
        ),
        count: null,
        status: 0,
      };
    }
    if (this.#asOf) {
      return {
        data: null,
        error: new BasinError(
          "not_implemented",
          "AS OF SNAPSHOT lands in basin v0.2",
        ),
        count: null,
        status: 0,
      };
    }

    const qs = this.#search.toString();
    const url = qs ? `${this.#deps.url}?${qs}` : this.#deps.url;
    const headers = this.#buildHeaders();

    const init: RequestInit = {
      method: this.#pending.method,
      headers,
    };
    if (this.#pending.body !== undefined) {
      init.body = JSON.stringify(this.#pending.body);
    }

    let res: Response;
    try {
      res = await this.#deps.fetch(url, init);
    } catch (e) {
      return {
        data: null,
        error: new BasinError("network", networkErrorMessage(e)),
        count: null,
        status: 0,
      };
    }

    const count = parseContentRangeCount(res.headers.get("Content-Range"));

    // basin engine v0.1 returns 501 for DELETE. Surface a clear error
    // rather than a confusing raw 501 — other methods with 501 are
    // genuine server bugs and flow through the generic error path below.
    if (res.status === 501 && this.#pending.method === "DELETE") {
      return {
        data: null,
        error: new BasinError(
          "not_implemented",
          "DELETE in basin REST v0.1 is not implemented yet — see basin engine v0.2 roadmap",
          501,
        ),
        count: null,
        status: 501,
      };
    }

    // 204 No Content — common for return=minimal mutations.
    if (res.status === 204) {
      return { data: null, error: null, count, status: res.status };
    }

    // NDJSON auto-promoted by the engine (>1 MiB or >10k rows).
    const contentType = res.headers.get("Content-Type") ?? "";
    if (contentType.includes("application/x-ndjson") || contentType.includes("application/jsonl")) {
      let text: string;
      try {
        text = await res.text();
      } catch (e) {
        return {
          data: null,
          error: new BasinError("invalid_response", networkErrorMessage(e), res.status),
          count: null,
          status: res.status,
        };
      }
      if (!res.ok) {
        return {
          data: null,
          error: new BasinError(errorCodeForStatus(res.status), `request failed (HTTP ${res.status})`, res.status),
          count: null,
          status: res.status,
        };
      }
      const lines = text.split("\n").filter((l) => l.trim() !== "");
      const parsed: unknown[] = [];
      for (const line of lines) {
        let obj: unknown;
        try {
          obj = JSON.parse(line);
        } catch {
          return {
            data: null,
            error: new BasinError("invalid_response", `NDJSON line is not valid JSON: ${line}`, res.status),
            count: null,
            status: res.status,
          };
        }
        parsed.push(obj);
      }
      let nextCursor: string | null = null;
      if (parsed.length > 0) {
        const last = parsed[parsed.length - 1];
        if (last && typeof last === "object" && !Array.isArray(last) && "_basin_next_cursor" in (last as object)) {
          nextCursor = (last as Record<string, unknown>)["_basin_next_cursor"] as string | null;
          parsed.pop();
        }
      }
      return { data: parsed as T[], error: null, count, status: res.status, nextCursor };
    }

    // CSV / GeoJSON / explain — return the raw text body as data. For
    // explain(json), the body is JSON but consumers want the unparsed
    // plan string; pass it through verbatim and let them JSON.parse if
    // they really need an object.
    if (
      this.#accept === "csv" ||
      this.#accept === "geojson" ||
      this.#explain
    ) {
      let text: string;
      try {
        text = await res.text();
      } catch (e) {
        return {
          data: null,
          error: new BasinError("invalid_response", networkErrorMessage(e), res.status),
          count: null,
          status: res.status,
        };
      }
      if (!res.ok) {
        return {
          data: null,
          error: new BasinError(
            errorCodeForStatus(res.status),
            text || `request failed (HTTP ${res.status})`,
            res.status,
          ),
          count: null,
          status: res.status,
        };
      }
      return {
        data: text as unknown as T[],
        error: null,
        count,
        status: res.status,
      };
    }

    const parsed = await unwrapPostgrestBody(res);
    if (parsed.kind === "error") {
      return { data: null, error: parsed.error, count: null, status: res.status };
    }

    if (this.#single) {
      const payload = parsed.body;
      // PostgREST returns the object directly with the
      // `application/vnd.pgrst.object+json` Accept header — but mock
      // servers / flat-shape fallbacks may have given us an array. Be
      // tolerant either way.
      let row: T | null;
      if (Array.isArray(payload)) {
        if (payload.length === 0) {
          if (this.#single === "row") {
            return {
              data: null,
              error: new BasinError("not_found", "single() expected one row, got zero", res.status),
              count,
              status: res.status,
            };
          }
          row = null;
        } else if (payload.length > 1) {
          return {
            data: null,
            error: new BasinError(
              "invalid_response",
              `single() expected one row, got ${payload.length}`,
              res.status,
            ),
            count,
            status: res.status,
          };
        } else {
          row = payload[0] as T;
        }
      } else {
        row = payload == null ? null : (payload as T);
      }
      return { data: row as unknown as T[], error: null, count, status: res.status };
    }

    const rows = Array.isArray(parsed.body) ? (parsed.body as T[]) : parsed.body === null ? null : ([parsed.body] as T[]);
    return { data: rows, error: null, count, status: res.status };
  }
}

interface PostgrestSinglePromise<T> extends PromiseLike<PostgrestSingleResponse<T>> {}

interface PostgrestStringPromise extends PromiseLike<{ data: string | null; error: BasinError | null; status: number }> {}

// ── module-level helpers (pure, testable) ──────────────────────────

type ParsedRestResp =
  | { kind: "ok"; body: unknown }
  | { kind: "error"; error: BasinError };

/**
 * Reads + unwraps the cloud's `{data, error}` envelope from a REST
 * response. Tolerant — if the body isn't wrapped (e.g. PostgREST
 * directly or hand-mocked test servers), treats the body as the
 * payload. Mirrors the heuristic in src/auth/client.ts's
 * `unwrapAuthBody`.
 */
async function unwrapPostgrestBody(res: Response): Promise<ParsedRestResp> {
  let raw: unknown;
  try {
    raw = await res.json();
  } catch {
    if (res.ok) {
      // Empty/non-JSON body on success — common with `Prefer: return=minimal`.
      return { kind: "ok", body: null };
    }
    return {
      kind: "error",
      error: new BasinError(
        "invalid_response",
        `REST response was not JSON (HTTP ${res.status})`,
        res.status,
      ),
    };
  }

  // Cloud envelope: {data, error}. Be tolerant of flat PostgREST
  // shape — if the body isn't wrapped, treat it as the payload. The
  // envelope is recognised by the presence of BOTH `data` and `error`
  // keys (matches httpserver.WriteJSON's shape); a flat row that
  // happens to have a `data` column won't be misclassified.
  let payload: unknown;
  let envelopeError: { code?: string; message?: string; details?: unknown } | null | undefined;
  if (raw && typeof raw === "object" && !Array.isArray(raw)) {
    const obj = raw as Record<string, unknown>;
    const wrapped = "data" in obj && "error" in obj;
    if (wrapped) {
      payload = obj["data"];
      envelopeError = obj["error"] as typeof envelopeError;
    } else {
      payload = raw;
    }
  } else {
    payload = raw;
  }

  if (!res.ok) {
    const restError = !envelopeError && raw && typeof raw === "object" && !Array.isArray(raw)
      ? (raw as { code?: string; message?: string; details?: unknown })
      : envelopeError ?? null;
    return {
      kind: "error",
      error: new BasinError(
        restError?.code ?? errorCodeForStatus(res.status),
        restError?.message ?? `request failed (HTTP ${res.status})`,
        res.status,
        restError?.details,
      ),
    };
  }

  if (envelopeError) {
    return {
      kind: "error",
      error: new BasinError(
        envelopeError.code ?? errorCodeForStatus(res.status),
        envelopeError.message ?? `request failed (HTTP ${res.status})`,
        res.status,
        envelopeError,
      ),
    };
  }

  return { kind: "ok", body: payload };
}

function parseContentRangeCount(header: string | null): number | null {
  if (!header) return null;
  // Format: "0-9/100" or "*/100" or "0-9/*". Total after the slash.
  const slash = header.indexOf("/");
  if (slash < 0) return null;
  const total = header.slice(slash + 1).trim();
  if (!total || total === "*") return null;
  const n = Number(total);
  return Number.isFinite(n) ? n : null;
}

function encodeFilterValue(value: unknown): string {
  if (value === null) return "null";
  // Don't pre-encode; URLSearchParams.toString() handles URL-encoding
  // when the query string is rendered. Returning the raw string keeps
  // PostgREST operator semantics intact (commas, dots, parens are
  // delimiters in `in.(a,b)`, `fts(english)`, etc. — URLSearchParams's
  // encoding of these is still accepted by basin-rest).
  if (typeof value === "string") return value;
  return String(value);
}

/**
 * Encode a JSON/array value for `cs.`/`cd.`/`ov.` operators. Arrays
 * land as PostgREST's `{a,b,c}` syntax; objects pass through as JSON.
 */
function encodeContainmentValue(value: unknown): string {
  if (Array.isArray(value)) {
    return `{${value.map((v) => (typeof v === "string" ? v : String(v))).join(",")}}`;
  }
  if (value && typeof value === "object") {
    return encodeURIComponent(JSON.stringify(value));
  }
  return encodeFilterValue(value);
}

function errorCodeForStatus(status: number): string {
  if (status === 401) return "unauthorized";
  if (status === 403) return "forbidden";
  if (status === 404) return "not_found";
  if (status === 406) return "not_acceptable";
  if (status === 409) return "conflict";
  if (status === 429) return "rate_limited";
  if (status >= 500) return "internal";
  return "invalid_request";
}

function networkErrorMessage(e: unknown): string {
  if (e instanceof Error && e.message) return e.message;
  return "network error reaching rest endpoint";
}
