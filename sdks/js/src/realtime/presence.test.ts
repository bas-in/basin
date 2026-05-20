import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { PresenceChannel } from "./presence.js";
import type { PresenceMember } from "./presence.js";

describe("PresenceChannel", () => {
  let sent: unknown[];
  let channel: PresenceChannel;

  beforeEach(() => {
    sent = [];
    vi.useFakeTimers();
    channel = new PresenceChannel({
      channel: "room:1",
      clientId: "c1",
      send: (frame) => sent.push(frame),
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("track() sends presence_track frame", () => {
    channel.track({ name: "Alice" });
    expect(sent).toHaveLength(1);
    expect(sent[0]).toEqual({
      type: "presence_track",
      channel: "room:1",
      client_id: "c1",
      metadata: { name: "Alice" },
    });
  });

  it("presence_state → presenceState() reflects members and sync callback fires", () => {
    channel.track({ name: "Alice" });
    sent = [];

    const syncMembers: PresenceMember[][] = [];
    channel.on("presence", { event: "sync" }, (m) => syncMembers.push(m));

    channel.handleMessage({
      type: "presence_state",
      channel: "room:1",
      presences: [
        { client_id: "c1", metadata: { name: "Alice" } },
        { client_id: "c2", metadata: { name: "Bob" } },
      ],
    });

    const state = channel.presenceState();
    expect(state).toHaveLength(2);
    expect(state.map((m) => m.client_id).sort()).toEqual(["c1", "c2"]);

    expect(syncMembers).toHaveLength(1);
    expect(syncMembers[0]).toHaveLength(2);
  });

  it("presence_diff with join → join callback fires and map updated", () => {
    channel.handleMessage({
      type: "presence_state",
      channel: "room:1",
      presences: [{ client_id: "c1", metadata: { name: "Alice" } }],
    });

    const joinedMembers: PresenceMember[][] = [];
    channel.on("presence", { event: "join" }, (m) => joinedMembers.push(m));

    channel.handleMessage({
      type: "presence_diff",
      channel: "room:1",
      joins: [{ client_id: "c2", metadata: { name: "Bob" } }],
      leaves: [],
    });

    expect(joinedMembers).toHaveLength(1);
    expect(joinedMembers[0]![0]!.client_id).toBe("c2");

    const state = channel.presenceState();
    expect(state.map((m) => m.client_id).sort()).toEqual(["c1", "c2"]);
  });

  it("presence_diff with leave → leave callback fires and map updated", () => {
    channel.handleMessage({
      type: "presence_state",
      channel: "room:1",
      presences: [
        { client_id: "c1", metadata: {} },
        { client_id: "c2", metadata: {} },
      ],
    });

    const leftMembers: PresenceMember[][] = [];
    channel.on("presence", { event: "leave" }, (m) => leftMembers.push(m));

    channel.handleMessage({
      type: "presence_diff",
      channel: "room:1",
      joins: [],
      leaves: [{ client_id: "c2", metadata: {} }],
    });

    expect(leftMembers).toHaveLength(1);
    expect(leftMembers[0]![0]!.client_id).toBe("c2");

    const state = channel.presenceState();
    expect(state).toHaveLength(1);
    expect(state[0]!.client_id).toBe("c1");
  });

  it("heartbeat sent at 30s intervals while tracked", () => {
    channel.track({ name: "Alice" });
    sent = [];

    vi.advanceTimersByTime(30_000);
    expect(sent).toHaveLength(1);
    expect((sent[0] as Record<string, unknown>)["type"]).toBe("heartbeat");
    expect((sent[0] as Record<string, unknown>)["channel"]).toBe("room:1");
    expect((sent[0] as Record<string, unknown>)["client_id"]).toBe("c1");

    vi.advanceTimersByTime(30_000);
    expect(sent).toHaveLength(2);
  });

  it("untrack() stops heartbeat and sends presence_untrack", () => {
    channel.track({ name: "Alice" });
    sent = [];

    channel.untrack();

    const untrackFrames = (sent as Array<Record<string, unknown>>).filter(
      (f) => f["type"] === "presence_untrack",
    );
    expect(untrackFrames).toHaveLength(1);

    vi.advanceTimersByTime(60_000);
    const heartbeats = (sent as Array<Record<string, unknown>>).filter(
      (f) => f["type"] === "heartbeat",
    );
    expect(heartbeats).toHaveLength(0);
  });

  it("messages for a different channel are ignored", () => {
    const syncCbs: unknown[] = [];
    channel.on("presence", { event: "sync" }, (m) => syncCbs.push(m));

    channel.handleMessage({
      type: "presence_state",
      channel: "room:99",
      presences: [{ client_id: "other", metadata: {} }],
    });

    expect(syncCbs).toHaveLength(0);
    expect(channel.presenceState()).toHaveLength(0);
  });

  it("close() stops heartbeat and untracks", () => {
    channel.track({ name: "Alice" });
    sent = [];

    channel.close();

    const untrackFrames = (sent as Array<Record<string, unknown>>).filter(
      (f) => f["type"] === "presence_untrack",
    );
    expect(untrackFrames).toHaveLength(1);

    vi.advanceTimersByTime(60_000);
    const heartbeats = (sent as Array<Record<string, unknown>>).filter(
      (f) => f["type"] === "heartbeat",
    );
    expect(heartbeats).toHaveLength(0);
  });
});
