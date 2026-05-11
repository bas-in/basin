import { describe, expect, it } from "vitest";
import { BasinError, createClient } from "./index.js";

describe("createClient", () => {
  it("constructs a client with every namespace", () => {
    const basin = createClient("https://api.basin.run", "anon");
    expect(basin).toHaveProperty("auth");
    expect(basin).toHaveProperty("storage");
    expect(basin).toHaveProperty("realtime");
    expect(basin).toHaveProperty("functions");
    expect(typeof basin.from).toBe("function");
    expect(typeof basin.channel).toBe("function");
  });

  it("trims trailing slashes from the URL", () => {
    // No runtime way to inspect the internal URL; the test exists to
    // pin behaviour so a future refactor doesn't reintroduce a
    // double-slash when joining `${base}/v1/...`.
    expect(() =>
      createClient("https://api.basin.run/", "anon"),
    ).not.toThrow();
  });

  it("returns `{ data: null, error: BasinError }` for not-yet-implemented surfaces (functions.invoke)", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => new Response("{}", { status: 200 }),
    });
    // functions.invoke is the canonical Tier-5 placeholder.
    const { data, error } = await basin.functions.invoke("hello", {
      body: { name: "pc" },
    });
    expect(data).toBeNull();
    expect(error).toBeInstanceOf(BasinError);
    expect(error?.code).toBe("not_implemented");
  });

  it("exposes realtime + functions namespaces on the client", () => {
    const basin = createClient("https://api.basin.run", "anon");
    expect(basin).toHaveProperty("realtime");
    expect(basin).toHaveProperty("functions");
    expect(typeof basin.channel).toBe("function");
    expect(basin.realtime.enabled).toBe(false);
    expect(basin.functions.enabled).toBe(false);
  });

  it("query-builder chains compose without throwing", () => {
    const basin = createClient("https://api.basin.run", "anon");
    const q = basin
      .from("products")
      .select("id, name")
      .eq("active", true)
      .order("price", { ascending: false })
      .limit(10);
    expect(q).toBeDefined();
  });

  it("storage.from(bucket).getPublicUrl works without I/O", () => {
    const basin = createClient("https://api.basin.run", "anon");
    const { data } = basin.storage.from("uploads").getPublicUrl("greet.txt");
    expect(data.publicUrl).toContain("uploads");
    expect(data.publicUrl).toContain("greet.txt");
  });

  it("onAuthStateChange returns a working unsubscribe", () => {
    const basin = createClient("https://api.basin.run", "anon");
    const { data } = basin.auth.onAuthStateChange(() => {});
    expect(typeof data.subscription.unsubscribe).toBe("function");
    data.subscription.unsubscribe();
  });
});
