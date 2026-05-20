import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createClient } from "../client.js";
import { BasinError } from "../errors.js";
import { RealtimeChannel } from "./client.js";

// ---------------------------------------------------------------------------
// Mock WebSocket + SSE fetch helpers
// ---------------------------------------------------------------------------

interface MockWebSocketInstance {
  readyState: number;
  send: ReturnType<typeof vi.fn>;
  close: ReturnType<typeof vi.fn>;
  onopen: ((evt: Event) => void) | null;
  onmessage: ((evt: MessageEvent) => void) | null;
  onclose: ((evt: CloseEvent) => void) | null;
  onerror: ((evt: Event) => void) | null;
  simulateOpen(): void;
  simulateMessage(data: unknown): void;
  simulateClose(): void;
}

function makeMockWS(): MockWebSocketInstance {
  const instance: MockWebSocketInstance = {
    readyState: 0,
    send: vi.fn(),
    close: vi.fn(),
    onopen: null,
    onmessage: null,
    onclose: null,
    onerror: null,
    simulateOpen() {
      this.readyState = 1;
      this.onopen?.(new Event("open"));
    },
    simulateMessage(data) {
      this.onmessage?.(
        new MessageEvent("message", { data: JSON.stringify(data) }),
      );
    },
    simulateClose() {
      this.readyState = 3;
      this.onclose?.(new CloseEvent("close"));
    },
  };
  return instance;
}

function encodeChunks(chunks: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      controller.close();
    },
  });
}

function neverStream(): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({ start() {} });
}

// ---------------------------------------------------------------------------
// Suite: transport routing
// ---------------------------------------------------------------------------

describe("RealtimeChannel — transport routing", () => {
  let wsInstances: MockWebSocketInstance[] = [];
  let MockWS: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    wsInstances = [];
    MockWS = vi.fn().mockImplementation(() => {
      const inst = makeMockWS();
      wsInstances.push(inst);
      return inst;
    });
    vi.stubGlobal("WebSocket", MockWS);
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("one-table changes-only channel → SSE transport selected", () => {
    const fetchCalls: string[] = [];
    const mockFetch = vi.fn().mockImplementation((url: string) => {
      fetchCalls.push(url);
      return Promise.resolve(
        new Response(neverStream(), {
          status: 200,
          headers: { "Content-Type": "text/event-stream" },
        }),
      );
    }) as unknown as typeof globalThis.fetch;

    const basin = createClient("https://api.basin.run", "anon", {
      fetch: mockFetch,
    });

    const ch = basin
      .channel("orders")
      .on("postgres_changes", { event: "INSERT", table: "orders" }, () => {});

    ch.subscribe();

    // SSE path hits the fetch mock (not WebSocket)
    expect(MockWS).not.toHaveBeenCalled();
    expect(fetchCalls.length).toBeGreaterThanOrEqual(1);
    expect(fetchCalls[0]).toContain("/realtime/v1/sse/");
    expect(fetchCalls[0]).toContain("orders");

    ch.unsubscribe();
  });

  it("channel with presence binding → WS transport selected", () => {
    const mockFetch = vi.fn() as unknown as typeof globalThis.fetch;

    const basin = createClient("https://api.basin.run", "anon", {
      fetch: mockFetch,
    });

    const ch = basin
      .channel("room:1")
      .on("presence", { event: "sync" }, () => {});

    ch.subscribe();

    expect(MockWS).toHaveBeenCalledOnce();
    expect(mockFetch).not.toHaveBeenCalled();

    ch.unsubscribe();
  });

  it("channel with two table bindings → WS transport selected", () => {
    const mockFetch = vi.fn() as unknown as typeof globalThis.fetch;

    const basin = createClient("https://api.basin.run", "anon", {
      fetch: mockFetch,
    });

    const ch = basin
      .channel("multi")
      .on("postgres_changes", { event: "*", table: "orders" }, () => {})
      .on("postgres_changes", { event: "*", table: "users" }, () => {});

    ch.subscribe();

    expect(MockWS).toHaveBeenCalledOnce();
    expect(mockFetch).not.toHaveBeenCalled();

    ch.unsubscribe();
  });

  it("channel with a per-binding filter → WS transport selected", () => {
    const mockFetch = vi.fn() as unknown as typeof globalThis.fetch;

    const basin = createClient("https://api.basin.run", "anon", {
      fetch: mockFetch,
    });

    const ch = basin.channel("filtered").on(
      "postgres_changes",
      { event: "UPDATE", table: "orders", filter: "NEW.status = 'paid'" },
      () => {},
    );

    ch.subscribe();

    expect(MockWS).toHaveBeenCalledOnce();

    ch.unsubscribe();
  });
});

// ---------------------------------------------------------------------------
// Suite: end-to-end SSE — event fires callback with {op, new}
// ---------------------------------------------------------------------------

describe("RealtimeChannel — SSE end-to-end", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("subscribe → SSE event → binding callback fires with {op, new}", async () => {
    const sseEvent = {
      project: "api",
      table: "orders",
      op: "INSERT",
      after: { id: 42, status: "new" },
      seq: 1,
    };

    const mockFetch = vi.fn()
      .mockResolvedValueOnce(
        new Response(
          encodeChunks([`data: ${JSON.stringify(sseEvent)}\n\n`]),
          { status: 200, headers: { "Content-Type": "text/event-stream" } },
        ),
      )
      .mockReturnValue(new Promise<Response>(() => {})) as unknown as typeof globalThis.fetch;

    const payloads: unknown[] = [];
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: mockFetch,
    });

    const ch = basin
      .channel("orders")
      .on(
        "postgres_changes",
        { event: "INSERT", table: "orders" },
        (payload) => payloads.push(payload),
      );

    ch.subscribe();

    await vi.runAllTimersAsync();
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    expect(payloads).toHaveLength(1);
    const p = payloads[0] as { eventType: string; new: { id: number } };
    expect(p.eventType).toBe("INSERT");
    expect(p.new).toEqual({ id: 42, status: "new" });

    ch.unsubscribe();
  });

  it("event filter mismatch — callback does not fire", async () => {
    const sseEvent = {
      project: "api",
      table: "orders",
      op: "DELETE",
      after: { id: 1 },
      seq: 1,
    };

    const mockFetch = vi.fn()
      .mockResolvedValueOnce(
        new Response(
          encodeChunks([`data: ${JSON.stringify(sseEvent)}\n\n`]),
          { status: 200, headers: { "Content-Type": "text/event-stream" } },
        ),
      )
      .mockReturnValue(new Promise<Response>(() => {})) as unknown as typeof globalThis.fetch;

    const payloads: unknown[] = [];
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: mockFetch,
    });

    const ch = basin
      .channel("orders")
      .on(
        "postgres_changes",
        { event: "INSERT", table: "orders" },
        (payload) => payloads.push(payload),
      );

    ch.subscribe();

    await vi.runAllTimersAsync();
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    expect(payloads).toHaveLength(0);

    ch.unsubscribe();
  });
});

// ---------------------------------------------------------------------------
// Suite: end-to-end WS — event fires callback with {op, new}
// ---------------------------------------------------------------------------

describe("RealtimeChannel — WS end-to-end", () => {
  let wsInstances: MockWebSocketInstance[] = [];
  let MockWS: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    wsInstances = [];
    MockWS = vi.fn().mockImplementation(() => {
      const inst = makeMockWS();
      wsInstances.push(inst);
      return inst;
    });
    vi.stubGlobal("WebSocket", MockWS);
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("subscribe → WS event → binding callback fires", async () => {
    const mockFetch = vi.fn() as unknown as typeof globalThis.fetch;
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: mockFetch,
    });

    const payloads: unknown[] = [];
    const ch = basin
      .channel("orders")
      .on(
        "postgres_changes",
        { event: "*", table: "orders" },
        (p) => payloads.push(p),
      )
      .on("presence", { event: "sync" }, () => {});

    ch.subscribe();

    const ws = wsInstances[0]!;
    ws.simulateOpen();

    // Simulate server event
    ws.simulateMessage({
      type: "event",
      table: "orders",
      op: "UPDATE",
      after: { id: 7, status: "shipped" },
      seq: 3,
    });

    expect(payloads).toHaveLength(1);
    const p = payloads[0] as { eventType: string; new: { id: number } };
    expect(p.eventType).toBe("UPDATE");
    expect(p.new).toEqual({ id: 7, status: "shipped" });

    ch.unsubscribe();
  });
});

// ---------------------------------------------------------------------------
// Suite: misc — topic + error guards
// ---------------------------------------------------------------------------

describe("realtime channel API", () => {
  it("exposes `realtime.enabled === true` on a fresh client", () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => new Response("{}", { status: 200 }),
    });
    expect(basin.realtime.enabled).toBe(true);
  });

  it("realtime.channel(topic) and client.channel(topic) return equivalent builders", () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => new Response("{}", { status: 200 }),
    });
    const a = basin.realtime.channel("room1");
    const b = basin.channel("room1");
    expect(a.topic).toBe("room1");
    expect(b.topic).toBe("room1");
    expect(a).toBeInstanceOf(RealtimeChannel);
    expect(b).toBeInstanceOf(RealtimeChannel);
  });

  it("rejects empty topic with invalid_request", () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => new Response("{}", { status: 200 }),
    });
    expect(() => basin.channel("")).toThrow(BasinError);
    try {
      basin.channel("");
    } catch (e) {
      expect((e as BasinError).code).toBe("invalid_request");
    }
  });

  it("unsubscribe is idempotent and returns the channel", () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => new Response("{}", { status: 200 }),
    });
    const ch = basin
      .channel("room")
      .on("postgres_changes", { event: "*", table: "t" }, () => {});
    const ret = ch.unsubscribe();
    expect(ret).toBe(ch);
    expect(() => ch.unsubscribe()).not.toThrow();
  });

  it("subscribe returns the channel for chaining", () => {
    const mockFetch = vi.fn().mockReturnValue(
      new Promise<Response>(() => {}),
    ) as unknown as typeof globalThis.fetch;
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: mockFetch,
    });
    const ch = basin
      .channel("room")
      .on("postgres_changes", { event: "*", table: "t" }, () => {});
    const ret = ch.subscribe();
    expect(ret).toBe(ch);
    ch.unsubscribe();
  });
});
