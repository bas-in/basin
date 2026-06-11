/**
 * `.realtime` — WebSocket client for `GET /realtime/v1/ws/:project`.
 *
 * Route source (verified): crates/basin-realtime/src/ws.rs:191
 * (`router_with_state` mounts `/realtime/v1/ws/:project`); merged into the
 * REST app in crates/basin-rest/src/server.rs:222-232,433-445.
 *
 * Protocol (ws.rs module docs + ClientMsg/ServerMsg):
 * - client → server: `{"type":"subscribe","table":…,"filter"?,"last_event_id"?}`,
 *   `{"type":"unsubscribe","table":…}`, presence `presence_track` /
 *   `presence_untrack` / `heartbeat`.
 * - server → client: `{"type":"event",project,table,op,before?,after?,seq}`,
 *   `subscribed`, `unsubscribed`, `error` (e.g. `lag`, `invalid_filter`),
 *   `gap`, `presence_state`, `presence_diff`.
 *
 * Auth (ws.rs `extract_token`): `Authorization: Bearer <token>` header OR the
 * `Sec-WebSocket-Protocol: basin-v1,<token>` subprotocol. Browsers can't set
 * headers on WS upgrades, so this client always uses the subprotocol form.
 */

import { BasinApiError } from "./errors.js";
import type {
  RealtimeErrorFrame,
  RealtimeEvent,
  RealtimeGapFrame,
  RealtimeServerFrame,
} from "./types.js";

/** Standard-WebSocket constructor shape (injectable for tests / Node). */
export type WebSocketCtor = new (
  url: string,
  protocols?: string | string[],
) => WebSocketLike;

export interface WebSocketLike {
  readonly readyState: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(
    type: "open" | "message" | "close" | "error",
    listener: (event: { data?: unknown; code?: number; reason?: string }) => void,
  ): void;
}

export interface SubscribeOptions {
  /** Server-side SQL predicate, e.g. `NEW.status = 'paid'` (ws.rs Filter). */
  filter?: string;
  /** Reconnect cursor: last `seq` processed; server replays missed events. */
  lastEventId?: number;
}

export interface ChannelHandlers {
  onEvent?: (event: RealtimeEvent) => void;
  /** `lag` / `invalid_filter` error frames for this table. */
  onError?: (error: RealtimeErrorFrame) => void;
  /** Replay-ring gap: client must cold re-sync. */
  onGap?: (gap: RealtimeGapFrame) => void;
}

export interface RealtimeChannel {
  readonly table: string;
  /** Send `{"type":"unsubscribe","table"}`; resolves on the server ack. */
  unsubscribe(): Promise<void>;
}

interface RealtimeContext {
  baseUrl: string;
  getToken(): Promise<string | undefined>;
  projectId(): string | undefined;
  webSocketImpl: WebSocketCtor;
}

const WS_OPEN = 1;

export class RealtimeClient {
  #ctx: RealtimeContext;
  #ws: WebSocketLike | null = null;
  #channels = new Map<string, ChannelHandlers>();
  #pendingSubscribed = new Map<string, () => void>();
  #pendingUnsubscribed = new Map<string, () => void>();
  /** Presence frame listeners (channel-name keyed delivery is up to callers). */
  #presenceListeners = new Set<(frame: RealtimeServerFrame) => void>();

  constructor(ctx: RealtimeContext) {
    this.#ctx = ctx;
  }

  get isConnected(): boolean {
    return this.#ws !== null && this.#ws.readyState === WS_OPEN;
  }

  /**
   * Open the WebSocket to `/realtime/v1/ws/:project`, authenticating via the
   * `basin-v1,<token>` subprotocol. Resolves once the socket is open.
   */
  async connect(): Promise<void> {
    if (this.isConnected) return;
    const project = this.#ctx.projectId();
    if (project === undefined) {
      throw new BasinApiError(
        "E_INVALID_REQUEST",
        "realtime requires a project id (createClient options.projectId or a JWT key/session)",
        0,
      );
    }
    const token = await this.#ctx.getToken();
    if (token === undefined) {
      throw new BasinApiError(
        "E_UNAUTHENTICATED",
        "realtime requires a JWT (key or signed-in session)",
        0,
      );
    }
    const wsBase = this.#ctx.baseUrl.replace(/^http/, "ws");
    const url = `${wsBase}/realtime/v1/ws/${project}`;
    const ws = new this.#ctx.webSocketImpl(url, ["basin-v1", token]);
    this.#ws = ws;

    ws.addEventListener("message", (ev) => {
      if (typeof ev.data !== "string") return;
      let frame: RealtimeServerFrame;
      try {
        frame = JSON.parse(ev.data) as RealtimeServerFrame;
      } catch {
        return;
      }
      this.#dispatch(frame);
    });

    await new Promise<void>((resolve, reject) => {
      ws.addEventListener("open", () => resolve());
      ws.addEventListener("error", () =>
        reject(
          new BasinApiError("E_UNKNOWN", "realtime websocket failed to open", 0),
        ),
      );
    });
  }

  /** Close the socket and drop all channel state. */
  disconnect(): void {
    this.#ws?.close(1000, "client disconnect");
    this.#ws = null;
    this.#channels.clear();
    this.#pendingSubscribed.clear();
    this.#pendingUnsubscribed.clear();
  }

  #send(msg: Record<string, unknown>): void {
    if (!this.#ws || this.#ws.readyState !== WS_OPEN) {
      throw new BasinApiError("E_UNKNOWN", "realtime socket is not open", 0);
    }
    this.#ws.send(JSON.stringify(msg));
  }

  #dispatch(frame: RealtimeServerFrame): void {
    switch (frame.type) {
      case "event":
        this.#channels.get(frame.table)?.onEvent?.(frame);
        break;
      case "error":
        this.#channels.get(frame.table)?.onError?.(frame);
        break;
      case "gap":
        this.#channels.get(frame.table)?.onGap?.(frame);
        break;
      case "subscribed":
        this.#pendingSubscribed.get(frame.table)?.();
        this.#pendingSubscribed.delete(frame.table);
        break;
      case "unsubscribed":
        this.#pendingUnsubscribed.get(frame.table)?.();
        this.#pendingUnsubscribed.delete(frame.table);
        this.#channels.delete(frame.table);
        break;
      case "presence_state":
      case "presence_diff":
      case "presenceerror":
        for (const listener of this.#presenceListeners) listener(frame);
        break;
    }
  }

  /**
   * Subscribe to change events for a table. Sends the subscribe envelope and
   * resolves with a channel handle once the server acks with `subscribed`.
   */
  async subscribe(
    table: string,
    handlers: ChannelHandlers,
    opts: SubscribeOptions = {},
  ): Promise<RealtimeChannel> {
    await this.connect();
    this.#channels.set(table, handlers);
    const acked = new Promise<void>((resolve) => {
      this.#pendingSubscribed.set(table, resolve);
    });
    const msg: Record<string, unknown> = { type: "subscribe", table };
    if (opts.filter !== undefined) msg["filter"] = opts.filter;
    if (opts.lastEventId !== undefined) msg["last_event_id"] = opts.lastEventId;
    this.#send(msg);
    await acked;

    return {
      table,
      unsubscribe: async () => {
        const unsubAcked = new Promise<void>((resolve) => {
          this.#pendingUnsubscribed.set(table, resolve);
        });
        this.#send({ type: "unsubscribe", table });
        await unsubAcked;
      },
    };
  }

  // --- Presence (ws.rs ClientMsg::PresenceTrack / Untrack / Heartbeat) -----

  /** Listen to `presence_state` / `presence_diff` / `presenceerror` frames. */
  onPresence(listener: (frame: RealtimeServerFrame) => void): () => void {
    this.#presenceListeners.add(listener);
    return () => this.#presenceListeners.delete(listener);
  }

  /**
   * `{"type":"presence_track",…}`. Note: the server binds `clientId` to the
   * authenticated JWT identity — a mismatched id is rejected (6.SEC.P1).
   */
  presenceTrack(
    channel: string,
    clientId: string,
    metadata: unknown = {},
  ): void {
    this.#send({ type: "presence_track", channel, client_id: clientId, metadata });
  }

  /** `{"type":"presence_untrack",…}`. */
  presenceUntrack(channel: string, clientId: string): void {
    this.#send({ type: "presence_untrack", channel, client_id: clientId });
  }

  /** `{"type":"heartbeat",…}` — refresh the presence TTL. */
  presenceHeartbeat(channel: string, clientId: string): void {
    this.#send({ type: "heartbeat", channel, client_id: clientId });
  }
}
