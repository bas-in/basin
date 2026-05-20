import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { WsConnection } from "./ws.js";

// Minimal mock WebSocket
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
    readyState: 0, // CONNECTING
    send: vi.fn(),
    close: vi.fn(),
    onopen: null,
    onmessage: null,
    onclose: null,
    onerror: null,
    simulateOpen() {
      this.readyState = 1; // OPEN
      this.onopen?.(new Event("open"));
    },
    simulateMessage(data) {
      this.onmessage?.(new MessageEvent("message", { data: JSON.stringify(data) }));
    },
    simulateClose() {
      this.readyState = 3; // CLOSED
      this.onclose?.(new CloseEvent("close"));
    },
  };
  return instance;
}

describe("WsConnection", () => {
  let instances: MockWebSocketInstance[] = [];
  let MockWS: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    instances = [];
    MockWS = vi.fn().mockImplementation(() => {
      const inst = makeMockWS();
      instances.push(inst);
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

  it("two-table subscribe — both acks route correctly", async () => {
    const conn = new WsConnection("proj1", { url: "wss://api.basin.run" });
    conn.connect();

    const ws = instances[0]!;
    ws.simulateOpen();

    const ordersEvents: unknown[] = [];
    const usersEvents: unknown[] = [];

    const p1 = conn.subscribe("orders", {}, (e) => ordersEvents.push(e));
    const p2 = conn.subscribe("users", {}, (e) => usersEvents.push(e));

    ws.simulateMessage({ type: "subscribed", table: "orders" });
    ws.simulateMessage({ type: "subscribed", table: "users" });

    await p1;
    await p2;

    ws.simulateMessage({ type: "event", table: "orders", op: "INSERT", after: { id: 1 }, seq: 1 });
    ws.simulateMessage({ type: "event", table: "users", op: "UPDATE", after: { id: 2 }, seq: 1 });
    ws.simulateMessage({ type: "event", table: "orders", op: "INSERT", after: { id: 3 }, seq: 2 });

    expect(ordersEvents).toHaveLength(2);
    expect(usersEvents).toHaveLength(1);

    const [e1, e2] = ordersEvents as Array<{ table: string; op: string; after: { id: number }; seq: number }>;
    expect(e1!.table).toBe("orders");
    expect(e1!.after.id).toBe(1);
    expect(e2!.seq).toBe(2);

    const [ue1] = usersEvents as Array<{ table: string; op: string }>;
    expect(ue1!.table).toBe("users");
    expect(ue1!.op).toBe("UPDATE");

    conn.close();
  });

  it("unsubscribe one mid-stream — its events stop, other continues, socket stays open", async () => {
    const conn = new WsConnection("proj1", { url: "wss://api.basin.run" });
    conn.connect();

    const ws = instances[0]!;
    ws.simulateOpen();

    const ordersEvents: unknown[] = [];
    const itemsEvents: unknown[] = [];

    const p1 = conn.subscribe("orders", {}, (e) => ordersEvents.push(e));
    const p2 = conn.subscribe("items", {}, (e) => itemsEvents.push(e));

    ws.simulateMessage({ type: "subscribed", table: "orders" });
    ws.simulateMessage({ type: "subscribed", table: "items" });

    await p1;
    await p2;

    ws.simulateMessage({ type: "event", table: "orders", op: "INSERT", after: { id: 1 }, seq: 1 });
    ws.simulateMessage({ type: "event", table: "items", op: "INSERT", after: { id: 10 }, seq: 1 });

    expect(ordersEvents).toHaveLength(1);
    expect(itemsEvents).toHaveLength(1);

    const unsubP = conn.unsubscribe("orders");
    ws.simulateMessage({ type: "unsubscribed", table: "orders" });
    await unsubP;

    // Events for unsubscribed table are dropped
    ws.simulateMessage({ type: "event", table: "orders", op: "INSERT", after: { id: 2 }, seq: 2 });
    ws.simulateMessage({ type: "event", table: "items", op: "UPDATE", after: { id: 11 }, seq: 2 });

    expect(ordersEvents).toHaveLength(1);
    expect(itemsEvents).toHaveLength(2);

    // Socket is still open (close() not called on ws)
    expect(ws.close).not.toHaveBeenCalled();

    conn.close();
  });

  it("disconnect → reconnect → both subscriptions re-established", async () => {
    const conn = new WsConnection("proj1", { url: "wss://api.basin.run" });
    conn.connect();

    const ws1 = instances[0]!;
    ws1.simulateOpen();

    const events: unknown[] = [];

    const p1 = conn.subscribe("orders", {}, (e) => events.push(e));
    const p2 = conn.subscribe("users", {}, (e) => events.push(e));

    ws1.simulateMessage({ type: "subscribed", table: "orders" });
    ws1.simulateMessage({ type: "subscribed", table: "users" });

    await p1;
    await p2;

    // Simulate disconnect — triggers reconnect after backoff
    ws1.simulateClose();

    // Advance past backoff (1s for attempt 0)
    await vi.advanceTimersByTimeAsync(1001);

    // A second WS instance should have been created
    expect(instances).toHaveLength(2);

    const ws2 = instances[1]!;
    ws2.simulateOpen();

    // Both subscribe frames should be re-sent on reconnect
    const sent = ws2.send.mock.calls.map((c) => JSON.parse(c[0] as string) as { type: string; table: string });
    const tables = sent.filter((m) => m.type === "subscribe").map((m) => m.table).sort();
    expect(tables).toEqual(["orders", "users"]);

    conn.close();
  });

  it("subscribe with filter — filter param is sent", async () => {
    const conn = new WsConnection("proj1", { url: "wss://api.basin.run" });
    conn.connect();

    const ws = instances[0]!;
    ws.simulateOpen();

    conn.subscribe("orders", { filter: "NEW.status = 'paid'" }, () => {}).catch(() => {});

    const sent = JSON.parse(ws.send.mock.calls[0]![0] as string) as { type: string; table: string; filter: string };
    expect(sent.type).toBe("subscribe");
    expect(sent.table).toBe("orders");
    expect(sent.filter).toBe("NEW.status = 'paid'");

    conn.close();
  });

  it("lag error frame — onLag callback fires", async () => {
    const conn = new WsConnection("proj1", { url: "wss://api.basin.run" });
    conn.connect();

    const ws = instances[0]!;
    ws.simulateOpen();

    const lagEvents: unknown[] = [];

    const p = conn.subscribe("orders", { onLag: (l) => lagEvents.push(l) }, () => {});
    ws.simulateMessage({ type: "subscribed", table: "orders" });
    await p;

    ws.simulateMessage({ type: "error", code: "lag", table: "orders", missed: 7 });

    expect(lagEvents).toHaveLength(1);
    expect((lagEvents[0] as { table: string; missed: number }).missed).toBe(7);

    conn.close();
  });

  it("seq gap — onLag fires when events arrive out of sequence", async () => {
    const conn = new WsConnection("proj1", { url: "wss://api.basin.run" });
    conn.connect();

    const ws = instances[0]!;
    ws.simulateOpen();

    const lagEvents: unknown[] = [];

    const p = conn.subscribe("orders", { onLag: (l) => lagEvents.push(l) }, () => {});
    ws.simulateMessage({ type: "subscribed", table: "orders" });
    await p;

    // seq 1 then seq 5 (gap of 3)
    ws.simulateMessage({ type: "event", table: "orders", op: "INSERT", after: {}, seq: 1 });
    ws.simulateMessage({ type: "event", table: "orders", op: "INSERT", after: {}, seq: 5 });

    expect(lagEvents).toHaveLength(1);
    expect((lagEvents[0] as { missed: number }).missed).toBe(3);

    conn.close();
  });
});
