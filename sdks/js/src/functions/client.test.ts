import { describe, expect, it } from "vitest";
import { createClient } from "../client.js";

describe("functions placeholder", () => {
  it("exposes `functions.enabled === false` on a fresh client", () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => new Response("{}", { status: 200 }),
    });
    expect(basin.functions.enabled).toBe(false);
  });

  it("invoke('slug', {body}) returns a typed not_implemented error today", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => {
        throw new Error("fetch should not be called");
      },
    });
    const { data, error } = await basin.functions.invoke("hello", {
      body: { name: "pc" },
    });
    expect(data).toBeNull();
    expect(error?.code).toBe("not_implemented");
  });

  it("invoke('') returns invalid_request", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => new Response("{}", { status: 200 }),
    });
    const { error } = await basin.functions.invoke("");
    expect(error?.code).toBe("invalid_request");
  });

  it("accepts projectRef on createClient and on the per-invoke options", async () => {
    // Both shapes compile cleanly. Runtime path stays not_implemented.
    const a = createClient("https://api.basin.run", "anon", {
      projectRef: "p_01H",
      fetch: async () => new Response("{}", { status: 200 }),
    });
    const r = await a.functions.invoke("hello", { projectRef: "p_01H_override" });
    expect(r.error?.code).toBe("not_implemented");
  });
});
