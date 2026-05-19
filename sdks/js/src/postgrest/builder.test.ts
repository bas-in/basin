import { describe, expect, it } from "vitest";
import { createClient } from "../client.js";
import { BasinError } from "../errors.js";

// Build a `fetch` stub that returns a single canned response. The
// closure captures the request so individual tests can assert on URL,
// method, headers, body. Mirrors src/auth/client.test.ts's helper.
function stubFetch(
  resp: { status?: number; body?: unknown; bodyText?: string; headers?: Record<string, string> },
  captured?: { request?: Request },
): typeof fetch {
  return async (input: Request | string | URL, init?: RequestInit): Promise<Response> => {
    if (captured) {
      captured.request = new Request(input as RequestInfo, init);
    }
    const status = resp.status ?? 200;
    const headers = {
      "Content-Type": "application/json",
      ...resp.headers,
    };
    // Response constructor rejects a body on 204/205/304. Pass null for
    // those statuses so `new Response("", { status: 204 })` doesn't throw.
    const noBodyStatus = status === 204 || status === 205 || status === 304;
    const body = noBodyStatus
      ? null
      : (resp.bodyText ?? (resp.body !== undefined ? JSON.stringify(resp.body) : ""));
    return new Response(body, { status, headers });
  };
}

function newClient(fetchImpl: typeof fetch) {
  return createClient("https://api.basin.run", "anon-key", { fetch: fetchImpl });
}

describe("postgrest filters → URL shape", () => {
  async function urlFor(build: (b: ReturnType<typeof newClient>["from"]) => unknown): Promise<string> {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ body: [] }, captured));
    const builder = build(basin.from);
    await builder;
    return captured.request?.url ?? "";
  }

  it("eq appends column=eq.value", async () => {
    const url = await urlFor((from) => from("t").select("*").eq("name", "john"));
    expect(url).toContain("name=eq.john");
    expect(url).toContain("select=");
  });

  it("neq appends column=neq.value", async () => {
    const url = await urlFor((from) => from("t").select("*").neq("a", 1));
    expect(url).toContain("a=neq.1");
  });

  it("gt / gte / lt / lte serialise to their PostgREST ops", async () => {
    const url = await urlFor((from) =>
      from("t").select("*").gt("a", 1).gte("b", 2).lt("c", 3).lte("d", 4),
    );
    expect(url).toContain("a=gt.1");
    expect(url).toContain("b=gte.2");
    expect(url).toContain("c=lt.3");
    expect(url).toContain("d=lte.4");
  });

  it("like + ilike serialise their pattern", async () => {
    const url = await urlFor((from) => from("t").select("*").like("name", "%foo%").ilike("name", "%bar%"));
    // URLSearchParams encodes % as %25.
    expect(url).toContain("name=like.");
    expect(url).toContain("name=ilike.");
  });

  it("is.null serialises to is.null (not is.'null')", async () => {
    const url = await urlFor((from) => from("t").select("*").is("deleted_at", null));
    expect(url).toContain("deleted_at=is.null");
  });

  it("is.true / is.false serialise to booleans", async () => {
    const url = await urlFor((from) => from("t").select("*").is("active", true).is("frozen", false));
    expect(url).toContain("active=is.true");
    expect(url).toContain("frozen=is.false");
  });

  it("in serialises to in.(a,b,c)", async () => {
    const url = await urlFor((from) => from("t").select("*").in("id", [1, 2, 3]));
    // URLSearchParams encodes commas as %2C and parens stay.
    expect(decodeURIComponent(url)).toContain("id=in.(1,2,3)");
  });

  it("contains serialises to cs.{a,b,c}", async () => {
    const url = await urlFor((from) => from("t").select("*").contains("tags", ["red", "blue"]));
    expect(decodeURIComponent(url)).toContain("tags=cs.{red,blue}");
  });

  it("containedBy serialises to cd.{a,b,c}", async () => {
    const url = await urlFor((from) => from("t").select("*").containedBy("tags", ["red", "blue"]));
    expect(decodeURIComponent(url)).toContain("tags=cd.{red,blue}");
  });

  it("rangeGt / rangeGte / rangeLt / rangeLte / rangeAdjacent map to PostgREST ops", async () => {
    const url = await urlFor((from) =>
      from("t")
        .select("*")
        .rangeGt("r", "[1,10)")
        .rangeGte("r", "[1,10)")
        .rangeLt("r", "[1,10)")
        .rangeLte("r", "[1,10)")
        .rangeAdjacent("r", "[1,10)"),
    );
    const decoded = decodeURIComponent(url);
    expect(decoded).toContain("r=sr.[1,10)");
    expect(decoded).toContain("r=nxl.[1,10)");
    expect(decoded).toContain("r=sl.[1,10)");
    expect(decoded).toContain("r=nxr.[1,10)");
    expect(decoded).toContain("r=adj.[1,10)");
  });

  it("overlaps serialises to ov.{a,b}", async () => {
    const url = await urlFor((from) => from("t").select("*").overlaps("tags", ["a", "b"]));
    expect(decodeURIComponent(url)).toContain("tags=ov.{a,b}");
  });

  it("textSearch defaults to fts", async () => {
    const url = await urlFor((from) => from("t").select("*").textSearch("body", "hello"));
    expect(decodeURIComponent(url)).toContain("body=fts.hello");
  });

  it("textSearch with type=plain → plfts", async () => {
    const url = await urlFor((from) => from("t").select("*").textSearch("body", "hi", { type: "plain" }));
    expect(decodeURIComponent(url)).toContain("body=plfts.hi");
  });

  it("textSearch with config attaches (config) inside the op", async () => {
    const url = await urlFor((from) => from("t").select("*").textSearch("body", "hi", { type: "websearch", config: "english" }));
    expect(decodeURIComponent(url)).toContain("body=wfts(english).hi");
  });

  it("match emits one eq per key", async () => {
    const url = await urlFor((from) => from("t").select("*").match({ a: 1, b: 2 }));
    expect(url).toContain("a=eq.1");
    expect(url).toContain("b=eq.2");
  });

  it("not prepends not. to the operator", async () => {
    const url = await urlFor((from) => from("t").select("*").not("a", "eq", 1));
    expect(url).toContain("a=not.eq.1");
  });

  it("not with array uses (a,b) syntax", async () => {
    const url = await urlFor((from) => from("t").select("*").not("id", "in", [1, 2]));
    expect(decodeURIComponent(url)).toContain("id=not.in.(1,2)");
  });

  it("or appends an or=(...) param", async () => {
    const url = await urlFor((from) => from("t").select("*").or("a.eq.1,b.eq.2"));
    expect(decodeURIComponent(url)).toContain("or=(a.eq.1,b.eq.2)");
  });

  it("or with foreignTable scopes the key", async () => {
    const url = await urlFor((from) => from("t").select("*").or("a.eq.1", { foreignTable: "join" }));
    expect(decodeURIComponent(url)).toContain("join.or=(a.eq.1)");
  });

  it("filter is the generic escape hatch", async () => {
    const url = await urlFor((from) => from("t").select("*").filter("a", "eq", 1));
    expect(url).toContain("a=eq.1");
  });
});

describe("postgrest modifiers", () => {
  it("order with default direction is asc", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ body: [] }, captured));
    await basin.from("t").select("*").order("name");
    expect(decodeURIComponent(captured.request?.url ?? "")).toContain("order=name.asc");
  });

  it("order ascending:false → desc", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ body: [] }, captured));
    await basin.from("t").select("*").order("name", { ascending: false });
    expect(decodeURIComponent(captured.request?.url ?? "")).toContain("order=name.desc");
  });

  it("limit and range", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ body: [] }, captured));
    await basin.from("t").select("*").range(0, 9);
    const url = captured.request?.url ?? "";
    expect(url).toContain("offset=0");
    expect(url).toContain("limit=10");
  });

  it("single() sets Accept: application/vnd.pgrst.object+json", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ body: { id: 1, name: "x" } }, captured));
    const { data, error } = await basin.from<{ id: number; name: string }>("t").select("*").eq("id", 1).single();
    expect(error).toBeNull();
    expect(data?.id).toBe(1);
    expect(captured.request?.headers.get("Accept")).toBe("application/vnd.pgrst.object+json");
  });

  it("maybeSingle() also sets the single Accept header", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ body: { id: 1 } }, captured));
    await basin.from("t").select("*").maybeSingle();
    expect(captured.request?.headers.get("Accept")).toBe("application/vnd.pgrst.object+json");
  });

  it("single() returns not_found error when array is empty", async () => {
    const basin = newClient(stubFetch({ body: [] }));
    const { data, error } = await basin.from("t").select("*").eq("id", 99).single();
    expect(data).toBeNull();
    expect(error?.code).toBe("not_found");
  });

  it("maybeSingle() returns null data when array is empty", async () => {
    const basin = newClient(stubFetch({ body: [] }));
    const { data, error } = await basin.from("t").select("*").eq("id", 99).maybeSingle();
    expect(data).toBeNull();
    expect(error).toBeNull();
  });

  it("csv() sets Accept: text/csv and returns the raw body as data", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch(
      { bodyText: "id,name\n1,x\n", headers: { "Content-Type": "text/csv" } },
      captured,
    ));
    const { data, error } = await basin.from("t").select("*").csv();
    expect(error).toBeNull();
    expect(data).toContain("id,name");
    expect(captured.request?.headers.get("Accept")).toBe("text/csv");
  });

  it("geojson() sets Accept: application/geo+json and returns text", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch(
      { bodyText: '{"type":"FeatureCollection","features":[]}', headers: { "Content-Type": "application/geo+json" } },
      captured,
    ));
    const { data, error } = await basin.from("t").select("*").geojson();
    expect(error).toBeNull();
    expect(data).toContain("FeatureCollection");
    expect(captured.request?.headers.get("Accept")).toBe("application/geo+json");
  });

  it("explain() defaults to text format with vnd.pgrst.plan Accept", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch(
      { bodyText: "Aggregate (cost=0..1)", headers: { "Content-Type": "application/vnd.pgrst.plan" } },
      captured,
    ));
    const { data, error } = await basin.from("t").select("*").explain();
    expect(error).toBeNull();
    expect(data).toContain("Aggregate");
    expect(captured.request?.headers.get("Accept")).toBe("application/vnd.pgrst.plan");
  });
});

describe("postgrest count + pagination", () => {
  it("select with count: 'exact' adds Prefer: count=exact", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ body: [] }, captured));
    await basin.from("t").select("*", { count: "exact" });
    expect(captured.request?.headers.get("Prefer")).toContain("count=exact");
  });

  it("parses Content-Range header into count field", async () => {
    const basin = newClient(stubFetch({
      body: [{ id: 1 }],
      headers: { "Content-Range": "0-0/42" },
    }));
    const { count } = await basin.from("t").select("*", { count: "exact" });
    expect(count).toBe(42);
  });

  it("Content-Range with */N still parses N", async () => {
    const basin = newClient(stubFetch({
      body: [],
      headers: { "Content-Range": "*/17" },
    }));
    const { count } = await basin.from("t").select("*", { count: "exact" });
    expect(count).toBe(17);
  });
});

describe("postgrest mutations", () => {
  it("insert POSTs the rows as JSON body", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ body: [{ id: 1, name: "x" }], status: 201 }, captured));
    const { data, error } = await basin.from<{ id: number; name: string }>("t").insert({ name: "x" });
    expect(error).toBeNull();
    expect(captured.request?.method).toBe("POST");
    const body = await captured.request?.json();
    expect(body).toEqual({ name: "x" });
    expect(data).toEqual([{ id: 1, name: "x" }]);
  });

  it("insert with upsert:true sets on_conflict=*", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ body: [], status: 201 }, captured));
    await basin.from("t").insert({ a: 1 }, { upsert: true });
    expect(captured.request?.url).toContain("on_conflict=");
  });

  it("update PATCHes with the values body + carries the filter chain", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ body: [] }, captured));
    await basin.from("t").update({ name: "new" }).eq("id", 1);
    expect(captured.request?.method).toBe("PATCH");
    expect(captured.request?.url).toContain("id=eq.1");
    const body = await captured.request?.json();
    expect(body).toEqual({ name: "new" });
  });

  it("upsert posts rows + onConflict column name", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ body: [] }, captured));
    await basin.from("t").upsert({ id: 1, name: "x" }, { onConflict: "id" });
    expect(captured.request?.method).toBe("POST");
    expect(captured.request?.url).toContain("on_conflict=id");
    const body = await captured.request?.json();
    expect(body).toEqual({ id: 1, name: "x" });
  });

  it("delete DELETEs with the filter chain", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ status: 204 }, captured));
    const { data, error } = await basin.from("t").delete().eq("id", 99);
    expect(error).toBeNull();
    expect(data).toBeNull();
    expect(captured.request?.method).toBe("DELETE");
    expect(captured.request?.url).toContain("id=eq.99");
  });
});

describe("postgrest returning()", () => {
  it("defaults to Prefer: return=representation on mutations", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ body: [] }, captured));
    await basin.from("t").insert({ a: 1 });
    expect(captured.request?.headers.get("Prefer")).toContain("return=representation");
  });

  it("returning('minimal') sets Prefer: return=minimal", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ status: 204 }, captured));
    await basin.from("t").insert({ a: 1 }).returning("minimal");
    expect(captured.request?.headers.get("Prefer")).toContain("return=minimal");
  });

  it("204 No Content returns {data:null, error:null}", async () => {
    const basin = newClient(stubFetch({ status: 204 }));
    const { data, error } = await basin.from("t").insert({ a: 1 }).returning("minimal");
    expect(data).toBeNull();
    expect(error).toBeNull();
  });
});

describe("postgrest RLS auth header injection", () => {
  it("attaches Authorization: Bearer <access_token> when a session exists", async () => {
    const captured: { request?: Request } = {};
    const basin = createClient("https://api.basin.run", "anon-key", {
      fetch: async (input: Request | string | URL, init?: RequestInit): Promise<Response> => {
        const url = typeof input === "string" ? input : (input as URL | Request).toString();
        if (url.endsWith("/signin")) {
          return new Response(
            JSON.stringify({
              data: {
                user: { id: "u1", email: "x@y.z" },
                session: {
                  access_token: "rls-at",
                  refresh_token: "rt",
                  expires_at: "2030-01-01T00:00:00Z",
                },
              },
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          );
        }
        captured.request = new Request(input as RequestInfo, init);
        return new Response(JSON.stringify([]), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      },
    });
    await basin.auth.signInWithPassword({ email: "x@y.z", password: "p" });
    await basin.from("t").select("*");
    expect(captured.request?.headers.get("Authorization")).toBe("Bearer rls-at");
    expect(captured.request?.headers.get("apikey")).toBe("anon-key");
  });

  it("omits Authorization when no session is active", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ body: [] }, captured));
    await basin.from("t").select("*");
    expect(captured.request?.headers.get("Authorization")).toBeNull();
    expect(captured.request?.headers.get("apikey")).toBe("anon-key");
  });
});

describe("postgrest envelope unwrap", () => {
  it("unwraps {data:[...], error:null} cloud envelope", async () => {
    const basin = newClient(stubFetch({
      body: { data: [{ id: 1 }, { id: 2 }], error: null },
    }));
    const { data, error } = await basin.from("t").select("*");
    expect(error).toBeNull();
    expect(data).toEqual([{ id: 1 }, { id: 2 }]);
  });

  it("falls back to flat array for hand-mocked test servers", async () => {
    const basin = newClient(stubFetch({
      body: [{ id: 1 }],
    }));
    const { data, error } = await basin.from("t").select("*");
    expect(error).toBeNull();
    expect(data).toEqual([{ id: 1 }]);
  });

  it("surfaces typed envelope error as BasinError", async () => {
    const basin = newClient(stubFetch({
      status: 401,
      body: { data: null, error: { code: "unauthorized", message: "Token missing." } },
    }));
    const { data, error } = await basin.from("t").select("*");
    expect(data).toBeNull();
    expect(error).toBeInstanceOf(BasinError);
    expect(error?.code).toBe("unauthorized");
    expect(error?.status).toBe(401);
  });

  it("collapses fetch reject to network error", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => {
        throw new Error("offline");
      },
    });
    const { data, error } = await basin.from("t").select("*");
    expect(data).toBeNull();
    expect(error?.code).toBe("network");
    expect(error?.message).toContain("offline");
  });

  it("surfaces flat PostgREST error shape on non-2xx", async () => {
    const basin = newClient(stubFetch({
      status: 400,
      body: { code: "PGRST116", message: "JSON object requested, multiple rows returned" },
    }));
    const { data, error } = await basin.from("t").select("*").single();
    expect(data).toBeNull();
    expect(error?.code).toBe("PGRST116");
    expect(error?.status).toBe(400);
  });
});

describe("postgrest basin extensions", () => {
  it("vectorSearch() returns not_implemented BasinError", async () => {
    const basin = newClient(stubFetch({ body: [] }));
    const { data, error } = await basin.from("t").select("*").vectorSearch("embedding", [0.1, 0.2, 0.3]);
    expect(data).toBeNull();
    expect(error?.code).toBe("not_implemented");
    expect(error?.message).toContain("vectorSearch");
  });

  it("asOf() returns not_implemented BasinError", async () => {
    const basin = newClient(stubFetch({ body: [] }));
    const { data, error } = await basin.from("t").select("*").asOf("snap-123");
    expect(data).toBeNull();
    expect(error?.code).toBe("not_implemented");
    expect(error?.message).toContain("v0.2");
  });

  it("vectorSearch short-circuits before the network", async () => {
    let called = false;
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => {
        called = true;
        throw new Error("should not be called");
      },
    });
    await basin.from("t").select("*").vectorSearch("embedding", [0.1]);
    expect(called).toBe(false);
  });
});

describe("postgrest NDJSON auto-streaming", () => {
  it("3 rows + cursor sentinel → data is rows, nextCursor is set", async () => {
    const ndjson = '{"id":1}\n{"id":2}\n{"id":3}\n{"_basin_next_cursor":"abc"}\n';
    const basin = newClient(stubFetch({ bodyText: ndjson, headers: { "Content-Type": "application/x-ndjson" } }));
    const { data, error, nextCursor } = await basin.from<{ id: number }>("t").select("*");
    expect(error).toBeNull();
    expect(data).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
    expect(nextCursor).toBe("abc");
  });

  it("0 rows + cursor sentinel → data is [], nextCursor is set", async () => {
    const ndjson = '{"_basin_next_cursor":"xyz"}\n';
    const basin = newClient(stubFetch({ bodyText: ndjson, headers: { "Content-Type": "application/x-ndjson" } }));
    const { data, error, nextCursor } = await basin.from("t").select("*");
    expect(error).toBeNull();
    expect(data).toEqual([]);
    expect(nextCursor).toBe("xyz");
  });

  it("rows with no cursor sentinel → nextCursor is null", async () => {
    const ndjson = '{"id":1}\n{"id":2}\n';
    const basin = newClient(stubFetch({ bodyText: ndjson, headers: { "Content-Type": "application/x-ndjson" } }));
    const { data, error, nextCursor } = await basin.from<{ id: number }>("t").select("*");
    expect(error).toBeNull();
    expect(data).toEqual([{ id: 1 }, { id: 2 }]);
    expect(nextCursor).toBeNull();
  });
});

describe("postgrest URL construction", () => {
  it("targets /rest/v1/<table> (engine-direct, no /v1 prefix)", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ body: [] }, captured));
    await basin.from("products").select("*");
    expect(captured.request?.url.startsWith("https://api.basin.run/rest/v1/products")).toBe(true);
    // Confirm the legacy /v1/rest/v1 prefix is NOT used.
    expect(captured.request?.url).not.toContain("/v1/rest/v1/");
  });
});

describe("postgrest .stream()", () => {
  function makeNdjsonStream(lines: string[]): ReadableStream<Uint8Array> {
    const enc = new TextEncoder();
    return new ReadableStream<Uint8Array>({
      start(controller) {
        for (const line of lines) {
          controller.enqueue(enc.encode(line + "\n"));
        }
        controller.close();
      },
    });
  }

  function stubStreamFetch(
    body: ReadableStream<Uint8Array>,
    captured?: { request?: Request },
  ): typeof fetch {
    return async (input: Request | string | URL, init?: RequestInit): Promise<Response> => {
      if (captured) {
        captured.request = new Request(input as RequestInfo, init);
      }
      return new Response(body, {
        status: 200,
        headers: { "Content-Type": "application/x-ndjson" },
      });
    };
  }

  it("yields 3 rows in order and completes cleanly with cursor sentinel skipped", async () => {
    const rows = [{ id: 1 }, { id: 2 }, { id: 3 }];
    const lines = [
      ...rows.map((r) => JSON.stringify(r)),
      JSON.stringify({ _basin_next_cursor: "tok_abc" }),
    ];
    const basin = newClient(stubStreamFetch(makeNdjsonStream(lines)));
    const collected: unknown[] = [];
    for await (const row of basin.from("events").select().stream()) {
      collected.push(row);
    }
    expect(collected).toEqual(rows);
  });

  it("sets stream=true on the URL when .stream() is called", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubStreamFetch(makeNdjsonStream([]), captured));
    for await (const _ of basin.from("events").select().stream()) {
      /* drain */
    }
    const url = captured.request?.url ?? "";
    expect(url).toContain("stream=true");
  });

  it("rejects with a network-shaped BasinError when the stream errors mid-flight", async () => {
    const enc = new TextEncoder();
    let controller!: ReadableStreamDefaultController<Uint8Array>;
    const body = new ReadableStream<Uint8Array>({
      start(c) { controller = c; },
    });
    const basin = newClient(stubStreamFetch(body));
    const iter = basin.from("events").select().stream()[Symbol.asyncIterator]();

    controller.enqueue(enc.encode(JSON.stringify({ id: 1 }) + "\n"));
    const first = await iter.next();
    expect(first.done).toBe(false);
    expect((first.value as { id: number }).id).toBe(1);

    controller.error(new Error("connection reset"));
    await expect(iter.next()).rejects.toMatchObject({
      code: "network",
      message: expect.stringContaining("connection reset"),
    });
  });

  it("rejects with invalid_response when an NDJSON line fails to parse", async () => {
    const enc = new TextEncoder();
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(enc.encode("not valid json\n"));
        controller.close();
      },
    });
    const basin = newClient(stubStreamFetch(body));
    const iter = basin.from("events").select().stream()[Symbol.asyncIterator]();
    await expect(iter.next()).rejects.toMatchObject({
      code: "invalid_response",
    });
  });
});

describe("postgrest .cursor()", () => {
  it("puts cursor=<token> on the URL", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ body: [] }, captured));
    await basin.from("t").select("*").cursor("abc");
    expect(captured.request?.url).toContain("cursor=abc");
  });

  it("chained with eq + limit preserves all three params", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ body: [] }, captured));
    await basin.from("t").select("*").eq("status", "active").limit(10).cursor("xyz");
    const url = captured.request?.url ?? "";
    expect(url).toContain("status=eq.active");
    expect(url).toContain("limit=10");
    expect(url).toContain("cursor=xyz");
  });

  it("last cursor wins (URLSearchParams.set semantics)", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ body: [] }, captured));
    await basin.from("t").select("*").cursor("a").cursor("b");
    const url = captured.request?.url ?? "";
    expect(url).toContain("cursor=b");
    expect(url).not.toContain("cursor=a");
  });

  it("cursor() with no argument is a no-op", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ body: [] }, captured));
    await basin.from("t").select("*").cursor();
    expect(captured.request?.url).not.toContain("cursor=");
  });
});

describe("postgrest .paginate()", () => {
  function stubSequenceFetch(
    pages: Array<{ rows: unknown[]; next_cursor: string | null }>,
    captured?: { requests: Request[] },
  ): typeof fetch {
    let call = 0;
    return async (input: Request | string | URL, init?: RequestInit): Promise<Response> => {
      if (captured) {
        captured.requests.push(new Request(input as RequestInfo, init));
      }
      const page = pages[call++];
      if (!page) throw new Error("unexpected extra fetch call");
      return new Response(JSON.stringify(page), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    };
  }

  it("3 pages happy path — yields all 6 rows in order then terminates", async () => {
    const pages = [
      { rows: [{ id: 1 }, { id: 2 }], next_cursor: "a" },
      { rows: [{ id: 3 }, { id: 4 }], next_cursor: "b" },
      { rows: [{ id: 5 }, { id: 6 }], next_cursor: null },
    ];
    const basin = newClient(stubSequenceFetch(pages));
    const collected: unknown[] = [];
    for await (const row of basin.from<{ id: number }>("t").select("*").paginate()) {
      collected.push(row);
    }
    expect(collected).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }, { id: 6 }]);
  });

  it("single page — terminates after that page without a second fetch", async () => {
    let calls = 0;
    const basin = newClient(async () => {
      calls++;
      return new Response(JSON.stringify({ rows: [{ id: 1 }, { id: 2 }, { id: 3 }], next_cursor: null }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    });
    const collected: unknown[] = [];
    for await (const row of basin.from("t").select("*").paginate()) {
      collected.push(row);
    }
    expect(collected).toHaveLength(3);
    expect(calls).toBe(1);
  });

  it("empty first page — yields nothing and terminates cleanly", async () => {
    const basin = newClient(async () =>
      new Response(JSON.stringify({ rows: [], next_cursor: null }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
    const collected: unknown[] = [];
    for await (const row of basin.from("t").select("*").paginate()) {
      collected.push(row);
    }
    expect(collected).toHaveLength(0);
  });

  it("mid-pagination error — iterator rejects with BasinError-shaped error", async () => {
    const pages = [
      { rows: [{ id: 1 }, { id: 2 }], next_cursor: "a" },
    ];
    let call = 0;
    const basin = newClient(async () => {
      if (call++ === 0) {
        return new Response(JSON.stringify(pages[0]), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      throw new Error("connection dropped");
    });
    const iter = basin.from("t").select("*").paginate()[Symbol.asyncIterator]();
    await iter.next();
    await iter.next();
    await expect(iter.next()).rejects.toMatchObject({
      code: "network",
      message: expect.stringContaining("connection dropped"),
    });
  });

  it("cursor param is sent on subsequent pages", async () => {
    const captured: { requests: Request[] } = { requests: [] };
    const pages = [
      { rows: [{ id: 1 }], next_cursor: "a" },
      { rows: [{ id: 2 }], next_cursor: null },
    ];
    const basin = newClient(stubSequenceFetch(pages, captured));
    for await (const _ of basin.from("t").select("*").paginate()) {
      /* drain */
    }
    expect(captured.requests).toHaveLength(2);
    const page2Url = decodeURIComponent(captured.requests[1]?.url ?? "");
    expect(page2Url).toContain("cursor=a");
  });
});

describe("T-027: DELETE 501 wrap", () => {
  it("wraps engine 501 on DELETE as not_implemented BasinError", async () => {
    const basin = newClient(stubFetch({ status: 501, bodyText: "Not Implemented" }));
    const { data, error } = await basin.from("t").delete().eq("id", 1);
    expect(data).toBeNull();
    expect(error?.code).toBe("not_implemented");
    expect(error?.message).toContain("DELETE");
    expect(error?.message).toContain("v0.2");
    expect(error?.status).toBe(501);
  });

  it("does NOT wrap 501 on GET as not_implemented (surfaces as internal)", async () => {
    const basin = newClient(stubFetch({ status: 501, body: { code: "internal", message: "server bug" } }));
    const { data, error } = await basin.from("t").select("*");
    expect(data).toBeNull();
    expect(error?.code).not.toBe("not_implemented");
    expect(error?.status).toBe(501);
  });
});

describe("T-043: .headers() Prefer pass-through", () => {
  it("caller-provided Prefer header reaches the fetch", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ status: 204 }, captured));
    await basin.from("t").insert({ a: 1 }).headers({ Prefer: "tx=rollback" });
    const prefer = captured.request?.headers.get("Prefer") ?? "";
    expect(prefer).toContain("tx=rollback");
  });

  it("SDK-set Prefer (return=representation) is preserved when caller adds their own", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ status: 204 }, captured));
    await basin.from("t").insert({ a: 1 }).headers({ Prefer: "tx=rollback" });
    const prefer = captured.request?.headers.get("Prefer") ?? "";
    expect(prefer).toContain("return=representation");
    expect(prefer).toContain("tx=rollback");
  });

  it("non-Prefer caller headers overwrite SDK headers", async () => {
    const captured: { request?: Request } = {};
    const basin = newClient(stubFetch({ body: [] }, captured));
    await basin.from("t").select("*").headers({ "X-Custom": "hello" });
    expect(captured.request?.headers.get("X-Custom")).toBe("hello");
  });
});

describe("T-044: terminal coverage — csv / geojson / explain(json)", () => {
  it("csv() returns raw string body and sets Accept: text/csv", async () => {
    const captured: { request?: Request } = {};
    const csvBody = "id,name\n1,alice\n2,bob\n";
    const basin = newClient(
      stubFetch({ bodyText: csvBody, headers: { "Content-Type": "text/csv" } }, captured),
    );
    const { data, error } = await basin.from("t").select("*").csv();
    expect(error).toBeNull();
    expect(typeof data).toBe("string");
    expect(data).toBe(csvBody);
    expect(captured.request?.headers.get("Accept")).toBe("text/csv");
  });

  it("geojson() returns raw string body and sets Accept: application/geo+json", async () => {
    const captured: { request?: Request } = {};
    const geoBody = '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":null,"properties":{}}]}';
    const basin = newClient(
      stubFetch({ bodyText: geoBody, headers: { "Content-Type": "application/geo+json" } }, captured),
    );
    const { data, error } = await basin.from("t").select("*").geojson();
    expect(error).toBeNull();
    expect(typeof data).toBe("string");
    expect(data).toBe(geoBody);
    expect(captured.request?.headers.get("Accept")).toBe("application/geo+json");
  });

  it("explain() with format:json returns raw plan string and sets Accept: application/vnd.pgrst.plan+json", async () => {
    const captured: { request?: Request } = {};
    const planBody = '[{"Plan":{"Node Type":"Seq Scan","Relation Name":"t"}}]';
    const basin = newClient(
      stubFetch(
        { bodyText: planBody, headers: { "Content-Type": "application/vnd.pgrst.plan+json" } },
        captured,
      ),
    );
    const { data, error } = await basin.from("t").select("*").explain({ format: "json" });
    expect(error).toBeNull();
    expect(typeof data).toBe("string");
    expect(data).toBe(planBody);
    expect(captured.request?.headers.get("Accept")).toBe("application/vnd.pgrst.plan+json");
  });
});
