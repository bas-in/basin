/**
 * Tests for QueryBuilder.toArrow() — Arrow IPC transport.
 *
 * Server contract (crates/basin-rest/src/arrow_ipc.rs):
 *   GET /rest/v1/:table  Accept: application/vnd.apache.arrow.stream
 *   → Content-Type: application/vnd.apache.arrow.stream
 *   → X-Basin-Next-Cursor: <cursor>  (absent when no next page)
 *   → X-Basin-Row-Count: <count>
 *   → Body: Arrow IPC stream
 */

import { tableFromJSON, tableToIPC, type Table } from "apache-arrow";
import { beforeEach, describe, expect, it } from "vitest";
import { createClient } from "../src/index.js";
import { FetchMock } from "./helpers.js";

const ARROW_MIME = "application/vnd.apache.arrow.stream";
const URL_BASE = "http://basin.test";

/** Build an Arrow IPC stream fixture from a list of row objects. */
function makeArrowFixture(rows: Record<string, unknown>[]): Uint8Array {
  const table = tableFromJSON(rows);
  return tableToIPC(table, "stream");
}

/** Wrap Arrow IPC bytes as a Response with the correct content type. */
function arrowResponse(
  bytes: Uint8Array,
  extra: Record<string, string> = {},
  status = 200,
): Response {
  return new Response(bytes, {
    status,
    headers: {
      "content-type": ARROW_MIME,
      ...extra,
    },
  });
}

function makeClient(fetch: FetchMock) {
  return createClient(URL_BASE, "key", { fetch: fetch.impl });
}

describe("QueryBuilder.toArrow()", () => {
  it("sends Accept: application/vnd.apache.arrow.stream header", async () => {
    const fixture = makeArrowFixture([{ id: 1, name: "Alice" }]);
    const fetch = new FetchMock().queue(arrowResponse(fixture));
    await makeClient(fetch).from("orders").toArrow();

    const call = fetch.last;
    expect(call.headers["accept"]).toBe(ARROW_MIME);
    expect(call.method).toBe("GET");
    expect(call.url).toContain("/rest/v1/orders");
  });

  it("forwards query filters on Arrow requests", async () => {
    const fixture = makeArrowFixture([]);
    const fetch = new FetchMock().queue(arrowResponse(fixture));
    await makeClient(fetch).from("orders").eq("status", "open").limit(10).toArrow();

    const url = new URL(fetch.last.url);
    expect(url.searchParams.get("status")).toBe("eq.open");
    expect(url.searchParams.get("limit")).toBe("10");
  });

  it("returns an Arrow Table with the correct column values from IPC response", async () => {
    const rows = [
      { id: 10, name: "Carol" },
      { id: 11, name: "Dave" },
    ];
    const fixture = makeArrowFixture(rows);
    const fetch = new FetchMock().queue(arrowResponse(fixture));
    const table = await makeClient(fetch).from("users").toArrow();

    expect(table.numRows).toBe(2);
    // Column names from the IPC schema.
    const names = table.schema.fields.map((f) => f.name);
    expect(names).toContain("id");
    expect(names).toContain("name");
    // Values.
    const idCol = table.getChild("id");
    expect(idCol?.get(0)).toBe(10);
    expect(idCol?.get(1)).toBe(11);
  });

  it("attaches X-Basin-Next-Cursor to the table as a non-enumerable property", async () => {
    const fixture = makeArrowFixture([{ id: 1 }]);
    const fetch = new FetchMock().queue(
      arrowResponse(fixture, { "x-basin-next-cursor": "cursor-tok" }),
    );
    const table = await makeClient(fetch).from("orders").limit(1).toArrow();
    // The cursor is attached as a non-enumerable own property.
    expect((table as Record<string, unknown>)["x-basin-next-cursor"]).toBe("cursor-tok");
    // It should not appear in JSON serialisation.
    expect(JSON.stringify(Object.keys(table))).not.toContain("x-basin-next-cursor");
  });

  it("falls back to JSON→Arrow when server returns JSON", async () => {
    // Server returns plain JSON (older server / 406 fallback).
    const fetch = new FetchMock().queue(
      new Response(JSON.stringify([{ id: 5, total: 99.5 }]), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );
    const table = await makeClient(fetch).from("orders").toArrow();
    expect(table.numRows).toBe(1);
    const idCol = table.getChild("id");
    // JSON numbers come through as Float64 in the fallback path.
    expect(idCol?.get(0)).toBeDefined();
  });

  it("falls back correctly for paginated JSON response {rows, next_cursor}", async () => {
    const fetch = new FetchMock().queue(
      new Response(
        JSON.stringify({ rows: [{ id: 1 }, { id: 2 }], next_cursor: "c1" }),
        {
          status: 200,
          headers: { "content-type": "application/json" },
        },
      ),
    );
    const table = await makeClient(fetch).from("orders").toArrow();
    expect(table.numRows).toBe(2);
  });

  it("throws BasinApiError on non-2xx responses", async () => {
    const fetch = new FetchMock().queue(
      new Response(JSON.stringify({ code: "E_NOT_FOUND", message: "table not found" }), {
        status: 404,
        headers: { "content-type": "application/json" },
      }),
    );
    await expect(makeClient(fetch).from("missing").toArrow()).rejects.toMatchObject({
      code: "E_NOT_FOUND",
      status: 404,
    });
  });

  it("attaches auth header", async () => {
    const fixture = makeArrowFixture([]);
    const fetch = new FetchMock().queue(arrowResponse(fixture));
    await makeClient(fetch).from("orders").toArrow();
    expect(fetch.last.headers["authorization"]).toBe("Bearer key");
  });

  it("does not attach cursor property when header is absent", async () => {
    const fixture = makeArrowFixture([{ id: 1 }]);
    // No X-Basin-Next-Cursor header.
    const fetch = new FetchMock().queue(arrowResponse(fixture));
    const table = await makeClient(fetch).from("orders").toArrow();
    expect(Object.getOwnPropertyDescriptor(table, "x-basin-next-cursor")).toBeUndefined();
  });

  it("empty IPC stream returns empty table", async () => {
    // Build an IPC stream from empty rows.
    const fixture = makeArrowFixture([]);
    const fetch = new FetchMock().queue(arrowResponse(fixture));
    const table = await makeClient(fetch).from("orders").toArrow();
    // Empty fixture — may have 0 rows.
    expect(table.numRows).toBe(0);
  });
});
