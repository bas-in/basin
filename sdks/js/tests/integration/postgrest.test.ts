/**
 * Integration: PostgREST builder — live basin-engine.
 *
 * Skip-gated. See `tests/integration/README.md` for the required env vars
 * and the one-time `basin_js_integration` table the tests read/write.
 */

import { afterEach, beforeAll, describe, expect, it } from "vitest";
import { createClient, type BasinClient } from "../../src/index.js";

const PROJECT_REF = process.env.BASIN_TEST_PROJECT_REF;
const ENGINE_URL = process.env.BASIN_TEST_ENGINE_URL;
const ANON_KEY = process.env.BASIN_TEST_ANON_KEY;

const gated = !(PROJECT_REF && ENGINE_URL && ANON_KEY);

type IntegrationRow = { id: string; value: string };

describe.skipIf(gated)("integration · postgrest (live basin-engine)", () => {
  let basin: BasinClient;
  const tag = `basin-js-it-${Date.now()}`;

  beforeAll(() => {
    basin = createClient(ENGINE_URL!, ANON_KEY!);
  });

  afterEach(async () => {
    // Best-effort cleanup. Errors here aren't fatal — the next run's
    // beforeEach overwrites the same `id` keys deterministically.
    await basin
      .from<IntegrationRow>("basin_js_integration")
      .delete()
      .like("id", `${tag}-%`);
  });

  it("insert + select roundtrips a row", async () => {
    const insert = await basin
      .from<IntegrationRow>("basin_js_integration")
      .insert({ id: `${tag}-1`, value: "hello" });
    expect(insert.error).toBeNull();

    const read = await basin
      .from<IntegrationRow>("basin_js_integration")
      .select("id, value")
      .eq("id", `${tag}-1`)
      .single();
    expect(read.error).toBeNull();
    expect(read.data).toEqual({ id: `${tag}-1`, value: "hello" });
  });

  it("update mutates in place", async () => {
    await basin
      .from<IntegrationRow>("basin_js_integration")
      .insert({ id: `${tag}-2`, value: "before" });

    const upd = await basin
      .from<IntegrationRow>("basin_js_integration")
      .update({ value: "after" })
      .eq("id", `${tag}-2`);
    expect(upd.error).toBeNull();

    const read = await basin
      .from<IntegrationRow>("basin_js_integration")
      .select("value")
      .eq("id", `${tag}-2`)
      .single();
    expect(read.data?.value).toBe("after");
  });

  it("delete removes rows", async () => {
    await basin
      .from<IntegrationRow>("basin_js_integration")
      .insert({ id: `${tag}-3`, value: "doomed" });

    const del = await basin
      .from<IntegrationRow>("basin_js_integration")
      .delete()
      .eq("id", `${tag}-3`);
    expect(del.error).toBeNull();

    const read = await basin
      .from<IntegrationRow>("basin_js_integration")
      .select("id")
      .eq("id", `${tag}-3`)
      .maybeSingle();
    expect(read.data).toBeNull();
  });

  it("limit + order shape returns an array even on a single row", async () => {
    await basin
      .from<IntegrationRow>("basin_js_integration")
      .insert([
        { id: `${tag}-a`, value: "1" },
        { id: `${tag}-b`, value: "2" },
      ]);

    const { data, error } = await basin
      .from<IntegrationRow>("basin_js_integration")
      .select("id, value")
      .like("id", `${tag}-%`)
      .order("id", { ascending: true })
      .limit(2);

    expect(error).toBeNull();
    expect(Array.isArray(data)).toBe(true);
    expect(data?.length).toBe(2);
    expect(data?.[0]?.id).toBe(`${tag}-a`);
  });

  it("vectorSearch is not_implemented until engine grows the operator", async () => {
    const { data, error } = await basin
      .from<IntegrationRow>("basin_js_integration")
      .vectorSearch("embedding", [0.1, 0.2, 0.3], { limit: 5 });
    expect(data).toBeNull();
    expect(error?.code).toBe("not_implemented");
  });
});
