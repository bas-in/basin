import { beforeEach, describe, expect, it } from "vitest";

import { createClient } from "../src/index.js";
import type { RealtimeEvent, RealtimeGapFrame } from "../src/types.js";
import { FakeWebSocket, FetchMock, makeJwt, tick } from "./helpers.js";

const URL_BASE = "http://basin.test";
const PROJECT = "01JTESTPROJECT0000000000";

function makeRealtimeClient() {
  return createClient(URL_BASE, makeJwt(PROJECT), {
    fetch: new FetchMock().impl,
    WebSocket: FakeWebSocket as never,
  });
}

describe("realtime", () => {
  beforeEach(() => FakeWebSocket.reset());

  it("connects to /realtime/v1/ws/:project with the basin-v1 subprotocol", async () => {
    const client = makeRealtimeClient();
    await client.realtime.connect();
    const ws = FakeWebSocket.last;
    expect(ws.url).toBe(`ws://basin.test/realtime/v1/ws/${PROJECT}`);
    expect(ws.protocols[0]).toBe("basin-v1");
    expect(ws.protocols).toHaveLength(2);
    // The second subprotocol entry is the bearer token (a JWT here).
    expect(ws.protocols[1]!.split(".")).toHaveLength(3);
  });

  it("derives the project id from the JWT when no option is given", async () => {
    const client = createClient(URL_BASE, makeJwt("01JFROMJWT00000000000000"), {
      fetch: new FetchMock().impl,
      WebSocket: FakeWebSocket as never,
    });
    await client.realtime.connect();
    expect(FakeWebSocket.last.url).toContain("/realtime/v1/ws/01JFROMJWT00000000000000");
  });

  it("subscribe sends the envelope and resolves on the subscribed ack", async () => {
    const client = makeRealtimeClient();
    await client.realtime.connect();
    const ws = FakeWebSocket.last;

    const events: RealtimeEvent[] = [];
    const pending = client.realtime.subscribe(
      "orders",
      { onEvent: (e) => events.push(e) },
      { filter: "NEW.status = 'paid'", lastEventId: 42 },
    );
    await tick();
    expect(ws.sentJson[0]).toEqual({
      type: "subscribe",
      table: "orders",
      filter: "NEW.status = 'paid'",
      last_event_id: 42,
    });

    ws.serverSend({ type: "subscribed", table: "orders" });
    const channel = await pending;
    expect(channel.table).toBe("orders");

    // Event envelope {type,table,payload-ish fields} routes to the handler.
    ws.serverSend({
      type: "event",
      project: PROJECT,
      table: "orders",
      op: "INSERT",
      after: { id: 1, status: "paid" },
      seq: 43,
    });
    expect(events).toHaveLength(1);
    expect(events[0]!.op).toBe("INSERT");
    expect(events[0]!.after).toEqual({ id: 1, status: "paid" });
    expect(events[0]!.seq).toBe(43);
  });

  it("unsubscribe sends the envelope, resolves on ack, and stops routing", async () => {
    const client = makeRealtimeClient();
    await client.realtime.connect();
    const ws = FakeWebSocket.last;

    const events: RealtimeEvent[] = [];
    const pending = client.realtime.subscribe("orders", {
      onEvent: (e) => events.push(e),
    });
    await tick();
    ws.serverSend({ type: "subscribed", table: "orders" });
    const channel = await pending;

    const unsub = channel.unsubscribe();
    await tick();
    expect(ws.sentJson[1]).toEqual({ type: "unsubscribe", table: "orders" });
    ws.serverSend({ type: "unsubscribed", table: "orders" });
    await unsub;

    ws.serverSend({
      type: "event",
      project: PROJECT,
      table: "orders",
      op: "INSERT",
      after: {},
      seq: 1,
    });
    expect(events).toHaveLength(0);
  });

  it("routes lag errors and gap frames to the channel handlers", async () => {
    const client = makeRealtimeClient();
    await client.realtime.connect();
    const ws = FakeWebSocket.last;

    const errors: unknown[] = [];
    const gaps: RealtimeGapFrame[] = [];
    const pending = client.realtime.subscribe("orders", {
      onError: (e) => errors.push(e),
      onGap: (g) => gaps.push(g),
    });
    await tick();
    ws.serverSend({ type: "subscribed", table: "orders" });
    await pending;

    ws.serverSend({ type: "error", code: "lag", table: "orders", missed: 5 });
    ws.serverSend({
      type: "gap",
      table: "orders",
      last_event_id: 10,
      oldest_in_ring: 20,
      newest_in_ring: 30,
    });
    expect(errors).toEqual([{ type: "error", code: "lag", table: "orders", missed: 5 }]);
    expect(gaps[0]!.oldest_in_ring).toBe(20);
  });

  it("presence track/untrack/heartbeat send the documented frames", async () => {
    const client = makeRealtimeClient();
    await client.realtime.connect();
    const ws = FakeWebSocket.last;

    const presenceFrames: unknown[] = [];
    client.realtime.onPresence((f) => presenceFrames.push(f));

    client.realtime.presenceTrack("room:1", "c1", { name: "Alice" });
    client.realtime.presenceUntrack("room:1", "c1");
    client.realtime.presenceHeartbeat("room:1", "c1");
    expect(ws.sentJson).toEqual([
      { type: "presence_track", channel: "room:1", client_id: "c1", metadata: { name: "Alice" } },
      { type: "presence_untrack", channel: "room:1", client_id: "c1" },
      { type: "heartbeat", channel: "room:1", client_id: "c1" },
    ]);

    ws.serverSend({ type: "presence_state", channel: "room:1", presences: [] });
    expect(presenceFrames).toHaveLength(1);
  });

  it("connect rejects without a token or project id", async () => {
    const noKey = createClient(URL_BASE, undefined, {
      projectId: PROJECT,
      fetch: new FetchMock().impl,
      WebSocket: FakeWebSocket as never,
    });
    await expect(noKey.realtime.connect()).rejects.toMatchObject({
      code: "E_UNAUTHENTICATED",
    });

    const noProject = createClient(URL_BASE, "raw-api-key-not-a-jwt", {
      fetch: new FetchMock().impl,
      WebSocket: FakeWebSocket as never,
    });
    await expect(noProject.realtime.connect()).rejects.toMatchObject({
      code: "E_INVALID_REQUEST",
    });
  });
});
