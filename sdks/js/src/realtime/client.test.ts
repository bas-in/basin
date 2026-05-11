import { describe, expect, it } from "vitest";
import { createClient } from "../client.js";
import { BasinError } from "../errors.js";
import { RealtimeChannel } from "./client.js";

describe("realtime placeholder", () => {
  it("exposes `realtime.enabled === false` on a fresh client", () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => new Response("{}", { status: 200 }),
    });
    expect(basin.realtime.enabled).toBe(false);
  });

  it("compiles the full channel chain — on().subscribe() — and throws not_implemented on subscribe", () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => new Response("{}", { status: 200 }),
    });
    const ch = basin
      .channel("room1")
      .on(
        "postgres_changes",
        { event: "INSERT", schema: "public", table: "messages" },
        (payload) => {
          // payload typed — ensures generics resolve.
          void payload.new;
        },
      );
    expect(ch).toBeInstanceOf(RealtimeChannel);
    expect(() => ch.subscribe()).toThrow(BasinError);
    try {
      ch.subscribe();
    } catch (e) {
      expect((e as BasinError).code).toBe("not_implemented");
    }
  });

  it("realtime.channel(topic) and client.channel(topic) return equivalent builders", () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => new Response("{}", { status: 200 }),
    });
    const a = basin.realtime.channel("room1");
    const b = basin.channel("room1");
    expect(a.topic).toBe("room1");
    expect(b.topic).toBe("room1");
  });

  it("rejects empty topic with invalid_request", () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => new Response("{}", { status: 200 }),
    });
    expect(() => basin.channel("")).toThrow(BasinError);
  });
});
