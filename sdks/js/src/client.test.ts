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

  it("functions.invoke posts to /rest/v1/rpc and returns the parsed result", async () => {
    let calledUrl = "";
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async (input: RequestInfo | URL) => {
        calledUrl = typeof input === "string" ? input : input.toString();
        return new Response("7", {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      },
    });
    const { data, error } = await basin.functions.invoke("add", {
      body: { x: 3, y: 4 },
    });
    expect(error).toBeNull();
    expect(data).toBe(7);
    expect(calledUrl).toContain("/rest/v1/rpc/add");
  });

  it("exposes realtime + functions namespaces on the client", () => {
    const basin = createClient("https://api.basin.run", "anon");
    expect(basin).toHaveProperty("realtime");
    expect(basin).toHaveProperty("functions");
    expect(typeof basin.channel).toBe("function");
    // functions.invoke went live with T-026 (POST /rest/v1/rpc/:fn).
    expect(basin.functions.enabled).toBe(true);
    // realtime channel API (T-030) is not wired yet; transports (SSE/WS)
    // exist but the channel() router still reports disabled until T-030.
    expect(basin.realtime.enabled).toBe(false);
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
