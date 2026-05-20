export interface PresenceMember {
  client_id: string;
  metadata: unknown;
}

export type PresenceEvent = "sync" | "join" | "leave";

export interface PresenceBindingFilter {
  event: PresenceEvent;
}

export type PresenceCallback = (members: PresenceMember[]) => void;

interface PresenceBinding {
  event: PresenceEvent;
  callback: PresenceCallback;
}

export interface PresenceChannelDeps {
  channel: string;
  clientId: string;
  send: (frame: unknown) => void;
}

export class PresenceChannel {
  #channel: string;
  #clientId: string;
  #send: (frame: unknown) => void;

  #presences: Map<string, PresenceMember> = new Map();
  #bindings: PresenceBinding[] = [];
  #heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  #tracked = false;

  constructor(deps: PresenceChannelDeps) {
    this.#channel = deps.channel;
    this.#clientId = deps.clientId;
    this.#send = deps.send;
  }

  track(metadata: unknown): void {
    this.#tracked = true;
    this.#send({
      type: "presence_track",
      channel: this.#channel,
      client_id: this.#clientId,
      metadata,
    });
    this.#startHeartbeat();
  }

  untrack(): void {
    this.#tracked = false;
    this.#stopHeartbeat();
    this.#send({
      type: "presence_untrack",
      channel: this.#channel,
      client_id: this.#clientId,
    });
  }

  presenceState(): PresenceMember[] {
    return Array.from(this.#presences.values());
  }

  on(type: "presence", filter: PresenceBindingFilter, callback: PresenceCallback): this {
    this.#bindings.push({ event: filter.event, callback });
    return this;
  }

  handleMessage(msg: unknown): void {
    const m = msg as Record<string, unknown>;
    if (m["channel"] !== this.#channel) return;

    if (m["type"] === "presence_state") {
      const presences = m["presences"] as PresenceMember[];
      this.#presences.clear();
      for (const p of presences) {
        this.#presences.set(p.client_id, p);
      }
      this.#emit("sync", presences);
    } else if (m["type"] === "presence_diff") {
      const joins = (m["joins"] ?? []) as PresenceMember[];
      const leaves = (m["leaves"] ?? []) as PresenceMember[];
      for (const p of joins) {
        this.#presences.set(p.client_id, p);
      }
      for (const p of leaves) {
        this.#presences.delete(p.client_id);
      }
      if (joins.length > 0) this.#emit("join", joins);
      if (leaves.length > 0) this.#emit("leave", leaves);
    }
  }

  close(): void {
    this.#stopHeartbeat();
    if (this.#tracked) {
      this.#tracked = false;
      this.#send({
        type: "presence_untrack",
        channel: this.#channel,
        client_id: this.#clientId,
      });
    }
  }

  #emit(event: PresenceEvent, members: PresenceMember[]): void {
    for (const b of this.#bindings) {
      if (b.event === event) b.callback(members);
    }
  }

  #startHeartbeat(): void {
    this.#stopHeartbeat();
    this.#heartbeatTimer = setInterval(() => {
      this.#send({
        type: "heartbeat",
        channel: this.#channel,
        client_id: this.#clientId,
      });
    }, 30_000);
  }

  #stopHeartbeat(): void {
    if (this.#heartbeatTimer !== null) {
      clearInterval(this.#heartbeatTimer);
      this.#heartbeatTimer = null;
    }
  }
}
