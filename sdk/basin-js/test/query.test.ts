import { describe, expect, it } from "vitest";

import { BasinApiError, createClient } from "../src/index.js";
import { FetchMock, jsonResponse } from "./helpers.js";

const URL_BASE = "http://basin.test";

function client(fetch: FetchMock) {
  return createClient(URL_BASE, "key", { fetch: fetch.impl });
}

function queryOf(url: string): URLSearchParams {
  return new URL(url).searchParams;
}

describe("from() query builder", () => {
  it("GET builds parser.rs filter grammar: select/eq/gte/order/limit/offset", async () => {
    const fetch = new FetchMock().queueJson({ rows: [{ id: 1 }], next_cursor: null });
    await client(fetch)
      .from("users")
      .select(["id", "name"])
      .gte("age", 21)
      .eq("active", true)
      .order("age", { ascending: false })
      .limit(10)
      .offset(5)
      .run();

    const call = fetch.last;
    expect(call.method).toBe("GET");
    expect(call.url.startsWith(`${URL_BASE}/rest/v1/users?`)).toBe(true);
    const q = queryOf(call.url);
    expect(q.get("select")).toBe("id,name");
    expect(q.get("age")).toBe("gte.21");
    expect(q.get("active")).toBe("eq.true");
    expect(q.get("order")).toBe("age.desc");
    expect(q.get("limit")).toBe("10");
    expect(q.get("offset")).toBe("5");
    expect(call.headers["authorization"]).toBe("Bearer key");
  });

  it("in() and is() render the parenthesised / null forms", async () => {
    const fetch = new FetchMock().queueJson([]);
    await client(fetch)
      .from("orders")
      .in("status", ["new", "paid"])
      .is("deleted_at", "null")
      .rows();
    const q = queryOf(fetch.last.url);
    expect(q.get("status")).toBe("in.(new,paid)");
    expect(q.get("deleted_at")).toBe("is.null");
  });

  it("plain GET array response normalizes to rows with null cursor", async () => {
    const fetch = new FetchMock().queueJson([{ id: 1 }, { id: 2 }]);
    const result = await client(fetch).from("users").select();
    expect(result.rows).toEqual([{ id: 1 }, { id: 2 }]);
    expect(result.nextCursor).toBeNull();
  });

  it("paginated GET exposes next_cursor and cursor() round-trips it", async () => {
    const fetch = new FetchMock()
      .queueJson({ rows: [{ id: 1 }], next_cursor: "abc123" })
      .queueJson({ rows: [{ id: 2 }], next_cursor: null });
    const c = client(fetch);

    const page1 = await c.from("users").limit(1).page();
    expect(page1.next_cursor).toBe("abc123");

    const page2 = await c.from("users").limit(1).cursor("abc123").page();
    expect(queryOf(fetch.last.url).get("cursor")).toBe("abc123");
    expect(page2.rows).toEqual([{ id: 2 }]);
    expect(page2.next_cursor).toBeNull();
  });

  it("insert POSTs the body and returns the {ok, tag} envelope", async () => {
    const fetch = new FetchMock().queueJson({ ok: true, tag: "INSERT 0 2" }, 201);
    const res = await client(fetch)
      .from("users")
      .insert([{ name: "a" }, { name: "b" }]);
    expect(res).toEqual({ ok: true, tag: "INSERT 0 2" });
    expect(fetch.last.method).toBe("POST");
    expect(fetch.last.url).toBe(`${URL_BASE}/rest/v1/users`);
    expect(JSON.parse(fetch.last.body!)).toEqual([{ name: "a" }, { name: "b" }]);
    expect(fetch.last.headers["content-type"]).toBe("application/json");
  });

  it("update PATCHes with filters in the query string", async () => {
    const fetch = new FetchMock().queueJson({ ok: true, tag: "UPDATE 1" });
    await client(fetch).from("users").eq("id", 7).update({ name: "z" });
    expect(fetch.last.method).toBe("PATCH");
    expect(queryOf(fetch.last.url).get("id")).toBe("eq.7");
    expect(JSON.parse(fetch.last.body!)).toEqual({ name: "z" });
  });

  it("delete sends DELETE with filters; E_ENGINE_UNSUPPORTED is typed", async () => {
    const fetch = new FetchMock().queueJson(
      { code: "E_ENGINE_UNSUPPORTED", message: "DELETE not supported by engine" },
      501,
    );
    const err = await client(fetch)
      .from("users")
      .eq("id", 7)
      .delete()
      .catch((e) => e);
    expect(fetch.last.method).toBe("DELETE");
    expect(err).toBeInstanceOf(BasinApiError);
    expect((err as BasinApiError).code).toBe("E_ENGINE_UNSUPPORTED");
    expect((err as BasinApiError).status).toBe(501);
  });

  it("stream() sets stream=true, parses NDJSON, captures the trailing cursor", async () => {
    const ndjson =
      '{"id":1}\n{"id":2}\n{"_basin_next_cursor":"tok"}\n';
    const fetch = new FetchMock().queue(
      new Response(ndjson, {
        status: 200,
        headers: { "content-type": "application/x-ndjson" },
      }),
    );
    const gen = client(fetch).from("big").stream();
    const rows: unknown[] = [];
    let cursor: string | null = null;
    for (;;) {
      const next = await gen.next();
      if (next.done) {
        cursor = next.value;
        break;
      }
      rows.push(next.value);
    }
    expect(queryOf(fetch.last.url).get("stream")).toBe("true");
    expect(rows).toEqual([{ id: 1 }, { id: 2 }]);
    expect(cursor).toBe("tok");
  });

  it("rpc POSTs named args to /rest/v1/rpc/:fn and unwraps the scalar", async () => {
    const fetch = new FetchMock().queue(jsonResponse(42));
    const result = await client(fetch).rpc<number>("add", { a: 40, b: 2 });
    expect(result).toBe(42);
    expect(fetch.last.url).toBe(`${URL_BASE}/rest/v1/rpc/add`);
    expect(fetch.last.method).toBe("POST");
    expect(JSON.parse(fetch.last.body!)).toEqual({ a: 40, b: 2 });
  });
});
