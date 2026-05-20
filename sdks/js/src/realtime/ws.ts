import { backoff } from "./backoff.js";

export interface PresenceMessageHandler {
  handleMessage(msg: unknown): void;
}

export interface WsEvent {
  table: string;
  op: "INSERT" | "UPDATE" | "DELETE";
  after: unknown;
  seq: number;
}

export interface WsLagEvent {
  table: string;
  missed: number;
}

export type WsEventCallback = (event: WsEvent) => void;
export type WsLagCallback = (lag: WsLagEvent) => void;

interface Subscription {
  filter?: string | undefined;
  onEvent: WsEventCallback;
  onLag?: WsLagCallback | undefined;
  lastSeq: number;
}

interface SubscribeOptions {
  filter?: string;
  onLag?: WsLagCallback;
}

type ServerMsg =
  | { type: "subscribed"; table: string }
  | { type: "unsubscribed"; table: string }
  | { type: "event"; table: string; op: "INSERT" | "UPDATE" | "DELETE"; after: unknown; seq: number }
  | { type: "error"; code: string; table?: string; missed?: number }
  | { type: "presence_state"; channel: string; presences: unknown[] }
  | { type: "presence_diff"; channel: string; joins: unknown[]; leaves: unknown[] };

export class WsConnection {
  #url: string;
  #project: string;
  #headers: Record<string, string>;

  #ws: WebSocket | null = null;
  #subs: Map<string, Subscription> = new Map();
  #pending: Map<string, { resolve: () => void; reject: (e: Error) => void }> = new Map();
  #presenceHandlers: Map<string, PresenceMessageHandler> = new Map();
  #closed = false;
  #reconnectAttempt = 0;
  #reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(project: string, opts: { url: string; headers?: Record<string, string> }) {
    this.#project = project;
    this.#url = `${opts.url}/realtime/v1/ws/${project}`;
    this.#headers = opts.headers ?? {};
  }

  #buildWsUrl(): string {
    const u = new URL(this.#url);
    const jwt = this.#headers["Authorization"];
    if (jwt) {
      const token = jwt.startsWith("Bearer ") ? jwt.slice(7) : jwt;
      u.searchParams.set("apikey", token);
    }
    return u.toString();
  }

  #connect(): void {
    const ws = new globalThis.WebSocket(this.#buildWsUrl());
    this.#ws = ws;

    ws.onopen = () => {
      this.#reconnectAttempt = 0;
      for (const [table, sub] of this.#subs) {
        const frame: Record<string, string> = { type: "subscribe", table };
        if (sub.filter) frame["filter"] = sub.filter;
        ws.send(JSON.stringify(frame));
      }
    };

    ws.onmessage = (evt: MessageEvent) => {
      let msg: ServerMsg;
      try {
        msg = JSON.parse(evt.data as string) as ServerMsg;
      } catch {
        return;
      }
      this.#dispatch(msg);
    };

    ws.onclose = () => {
      if (this.#closed) return;
      this.#scheduleReconnect();
    };

    ws.onerror = () => {
      ws.close();
    };
  }

  #dispatch(msg: ServerMsg): void {
    if (msg.type === "subscribed") {
      const pend = this.#pending.get(msg.table);
      if (pend) {
        pend.resolve();
        this.#pending.delete(msg.table);
      }
    } else if (msg.type === "unsubscribed") {
      const pend = this.#pending.get(`unsub:${msg.table}`);
      if (pend) {
        pend.resolve();
        this.#pending.delete(`unsub:${msg.table}`);
      }
    } else if (msg.type === "event") {
      const sub = this.#subs.get(msg.table);
      if (!sub) return;
      if (sub.lastSeq !== 0 && msg.seq > sub.lastSeq + 1 && sub.onLag) {
        sub.onLag({ table: msg.table, missed: msg.seq - sub.lastSeq - 1 });
      }
      sub.lastSeq = msg.seq;
      sub.onEvent({ table: msg.table, op: msg.op, after: msg.after, seq: msg.seq });
    } else if (msg.type === "error") {
      if (msg.code === "lag" && msg.table) {
        const sub = this.#subs.get(msg.table);
        if (sub?.onLag) {
          sub.onLag({ table: msg.table, missed: msg.missed ?? 0 });
        }
      }
    } else if (msg.type === "presence_state" || msg.type === "presence_diff") {
      const channel = (msg as unknown as Record<string, unknown>)["channel"] as string | undefined;
      if (channel) {
        this.#presenceHandlers.get(channel)?.handleMessage(msg);
      }
    }
  }

  #scheduleReconnect(): void {
    const delay = backoff(this.#reconnectAttempt++);
    this.#reconnectTimer = setTimeout(() => {
      this.#reconnectTimer = null;
      this.#connect();
    }, delay);
  }

  connect(): void {
    this.#closed = false;
    this.#connect();
  }

  subscribe(table: string, opts: SubscribeOptions, onEvent: WsEventCallback): Promise<void> {
    const sub: Subscription = {
      ...(opts.filter !== undefined ? { filter: opts.filter } : {}),
      onEvent,
      ...(opts.onLag !== undefined ? { onLag: opts.onLag } : {}),
      lastSeq: 0,
    };
    this.#subs.set(table, sub);

    return new Promise<void>((resolve, reject) => {
      this.#pending.set(table, { resolve, reject });
      if (this.#ws && this.#ws.readyState === 1 /* OPEN */) {
        const frame: Record<string, string> = { type: "subscribe", table };
        if (sub.filter) frame["filter"] = sub.filter;
        this.#ws.send(JSON.stringify(frame));
      }
    });
  }

  unsubscribe(table: string): Promise<void> {
    this.#subs.delete(table);
    return new Promise<void>((resolve, reject) => {
      if (this.#ws && this.#ws.readyState === 1 /* OPEN */) {
        this.#pending.set(`unsub:${table}`, { resolve, reject });
        this.#ws.send(JSON.stringify({ type: "unsubscribe", table }));
      } else {
        resolve();
      }
    });
  }

  registerPresence(channel: string, handler: PresenceMessageHandler): void {
    this.#presenceHandlers.set(channel, handler);
  }

  unregisterPresence(channel: string): void {
    this.#presenceHandlers.delete(channel);
  }

  send(frame: unknown): void {
    if (this.#ws && this.#ws.readyState === 1 /* OPEN */) {
      this.#ws.send(JSON.stringify(frame));
    }
  }

  close(): void {
    this.#closed = true;
    if (this.#reconnectTimer !== null) {
      clearTimeout(this.#reconnectTimer);
      this.#reconnectTimer = null;
    }
    this.#pending.forEach((p) => p.reject(new Error("connection closed")));
    this.#pending.clear();
    this.#ws?.close();
    this.#ws = null;
  }

  get project(): string {
    return this.#project;
  }
}
