/**
 * RealtimeClient + RealtimeChannel — T-030 live implementation.
 *
 * Transport routing:
 *   SSE  — channel has exactly one `postgres_changes` binding, no presence,
 *           no per-binding filter.
 *   WS   — channel has presence bindings, multiple table bindings, or any
 *           binding that carries a filter string.
 *
 * Surface mirrors `@supabase/realtime-js` so consumers porting from
 * Supabase keep their channel-wiring code unchanged.
 */

import { BasinError } from "../errors.js";
import { SseSubscription } from "./sse.js";
import type { SseEvent } from "./sse.js";
import { WsConnection } from "./ws.js";
import { PresenceChannel } from "./presence.js";
import type { PresenceEvent, PresenceMember } from "./presence.js";

export type RealtimeEvent = "INSERT" | "UPDATE" | "DELETE" | "*";

export interface PostgresChangesPayload<T = unknown> {
  schema: string;
  table: string;
  commit_timestamp: string;
  eventType: Exclude<RealtimeEvent, "*">;
  new: T;
  old: Partial<T>;
}

export interface PostgresChangesFilter {
  /** `INSERT` | `UPDATE` | `DELETE` | `*` */
  event: RealtimeEvent;
  /** PostgreSQL schema name. Default `public`. */
  schema?: string;
  /** Table name. */
  table: string;
  /** Server-side row filter, e.g. `"id=eq.42"` (PostgREST shape). */
  filter?: string;
}

export type RealtimeListener<T = unknown> = (
  payload: PostgresChangesPayload<T>,
) => void;

export type PresenceBindingFilter = { event: PresenceEvent };
export type PresenceCallback = (members: PresenceMember[]) => void;

interface PostgresBinding {
  kind: "postgres_changes";
  filter: PostgresChangesFilter;
  callback: RealtimeListener;
}

interface PresenceBinding {
  kind: "presence";
  filter: PresenceBindingFilter;
  callback: PresenceCallback;
}

type Binding = PostgresBinding | PresenceBinding;

export interface RealtimeChannelDeps {
  topic: string;
  url: string;
  project: string;
  headers: Record<string, string>;
  fetchFn?: typeof globalThis.fetch;
}

/**
 * Chainable channel builder. `.on()` registers bindings; `.subscribe()`
 * picks the cheapest transport (SSE or WS) and connects.
 *
 * @example
 * basin
 *   .channel('orders')
 *   .on('postgres_changes', { event: 'INSERT', table: 'orders' }, cb)
 *   .subscribe();
 */
export class RealtimeChannel {
  #topic: string;
  #url: string;
  #project: string;
  #headers: Record<string, string>;
  #fetchFn: typeof globalThis.fetch | undefined;

  #bindings: Binding[] = [];
  #sseSubscription: SseSubscription | null = null;
  #wsConnection: WsConnection | null = null;
  #presenceChannel: PresenceChannel | null = null;
  #subscribed = false;

  constructor(deps: RealtimeChannelDeps) {
    this.#topic = deps.topic;
    this.#url = deps.url;
    this.#project = deps.project;
    this.#headers = deps.headers;
    this.#fetchFn = deps.fetchFn;
  }

  get topic(): string {
    return this.#topic;
  }

  /**
   * Register a postgres_changes listener.
   */
  on<T = unknown>(
    type: "postgres_changes",
    filter: PostgresChangesFilter,
    callback: RealtimeListener<T>,
  ): this;

  /**
   * Register a presence listener.
   */
  on(
    type: "presence",
    filter: PresenceBindingFilter,
    callback: PresenceCallback,
  ): this;

  on(
    type: "postgres_changes" | "presence",
    filter: PostgresChangesFilter | PresenceBindingFilter,
    callback: RealtimeListener | PresenceCallback,
  ): this {
    if (type === "postgres_changes") {
      this.#bindings.push({
        kind: "postgres_changes",
        filter: filter as PostgresChangesFilter,
        callback: callback as RealtimeListener,
      });
    } else {
      this.#bindings.push({
        kind: "presence",
        filter: filter as PresenceBindingFilter,
        callback: callback as PresenceCallback,
      });
    }
    return this;
  }

  /**
   * Connect the channel. Picks SSE or WS based on the registered bindings:
   * - SSE: exactly one postgres_changes binding, no presence, no filter.
   * - WS: otherwise.
   */
  subscribe(): this {
    if (this.#subscribed) return this;
    this.#subscribed = true;

    const pgBindings = this.#bindings.filter(
      (b): b is PostgresBinding => b.kind === "postgres_changes",
    );
    const presenceBindings = this.#bindings.filter(
      (b): b is PresenceBinding => b.kind === "presence",
    );

    const useSSE =
      pgBindings.length === 1 &&
      presenceBindings.length === 0 &&
      !pgBindings[0]!.filter.filter;

    if (useSSE) {
      this.#startSse(pgBindings[0]!);
    } else {
      this.#startWs(pgBindings, presenceBindings);
    }

    return this;
  }

  /**
   * Disconnect and clean up all transport resources.
   */
  unsubscribe(): this {
    this.#sseSubscription?.stop();
    this.#sseSubscription = null;
    this.#presenceChannel?.close();
    this.#presenceChannel = null;
    this.#wsConnection?.close();
    this.#wsConnection = null;
    this.#subscribed = false;
    return this;
  }

  #startSse(binding: PostgresBinding): void {
    const jwt = this.#extractJwt();
    const table = binding.filter.table;

    this.#sseSubscription = new SseSubscription(
      this.#url,
      this.#project,
      table,
      { jwt },
      (event: SseEvent) => {
        this.#dispatchSseEvent(event, binding);
      },
      this.#fetchFn,
    );
    this.#sseSubscription.start();
  }

  #dispatchSseEvent(event: SseEvent, binding: PostgresBinding): void {
    const filterEvent = binding.filter.event;
    const op = event.op as Exclude<RealtimeEvent, "*">;

    if (filterEvent !== "*" && filterEvent !== op) return;

    const schema = binding.filter.schema ?? "public";
    const payload: PostgresChangesPayload = {
      schema,
      table: event.table,
      commit_timestamp: new Date().toISOString(),
      eventType: op,
      new: event.after,
      old: {},
    };
    binding.callback(payload);
  }

  #startWs(
    pgBindings: PostgresBinding[],
    presenceBindings: PresenceBinding[],
  ): void {
    const conn = new WsConnection(this.#project, {
      url: this.#url,
      headers: this.#headers,
    });
    this.#wsConnection = conn;
    conn.connect();

    for (const binding of pgBindings) {
      const opts: { filter?: string } = {};
      if (binding.filter.filter) {
        opts.filter = binding.filter.filter;
      }
      conn.subscribe(binding.filter.table, opts, (wsEvent) => {
        const filterEvent = binding.filter.event;
        const op = wsEvent.op as Exclude<RealtimeEvent, "*">;
        if (filterEvent !== "*" && filterEvent !== op) return;

        const schema = binding.filter.schema ?? "public";
        const payload: PostgresChangesPayload = {
          schema,
          table: wsEvent.table,
          commit_timestamp: new Date().toISOString(),
          eventType: op,
          new: wsEvent.after as Record<string, unknown>,
          old: {},
        };
        binding.callback(payload);
      }).catch(() => {
        // Subscription may be rejected when the channel is closed before
        // the server sends "subscribed". Swallow to prevent unhandled rejection.
      });
    }

    if (presenceBindings.length > 0) {
      const presenceCh = new PresenceChannel({
        channel: this.#topic,
        clientId: crypto.randomUUID(),
        send: (frame) => conn.send(frame),
      });
      this.#presenceChannel = presenceCh;
      conn.registerPresence(this.#topic, presenceCh);

      for (const binding of presenceBindings) {
        presenceCh.on("presence", binding.filter, binding.callback);
      }
    }
  }

  #extractJwt(): string {
    const auth = this.#headers["Authorization"] ?? "";
    return auth.startsWith("Bearer ") ? auth.slice(7) : auth;
  }

  /** Exposed for testing: which transport was chosen. */
  get _transport(): "sse" | "ws" | "none" {
    if (this.#sseSubscription) return "sse";
    if (this.#wsConnection) return "ws";
    return "none";
  }
}

export interface RealtimeClientDeps {
  url: string;
  headers: Record<string, string>;
  fetchFn?: typeof globalThis.fetch;
}

/**
 * Top-level realtime entry point. Reachable as `basin.realtime`.
 *
 *  - `basin.realtime.enabled` — `true` (T-030 live).
 *  - `basin.realtime.channel(topic)` — construct a channel builder.
 *
 * The shorter `basin.channel(topic)` shim on the client object
 * delegates here so consumers can write the Supabase-shaped
 * `basin.channel('room1').on(...)` chain unchanged.
 */
export class RealtimeClient {
  readonly enabled = true;
  #url: string;
  #headers: Record<string, string>;
  #fetchFn: typeof globalThis.fetch | undefined;

  constructor(deps: RealtimeClientDeps) {
    this.#url = deps.url;
    this.#headers = deps.headers;
    this.#fetchFn = deps.fetchFn;
  }

  /**
   * Construct a channel builder.
   *
   * @example
   * basin.realtime.channel('room1')
   *   .on('postgres_changes', { event: '*', schema: 'public', table: 'messages' }, cb)
   *   .subscribe();
   */
  channel(topic: string): RealtimeChannel {
    if (!topic) {
      throw new BasinError("invalid_request", "realtime.channel requires a topic");
    }
    // Extract project from URL: https://<project>.basin.run or use 'default'
    const project = this.#extractProject();
    const chanDeps: RealtimeChannelDeps = {
      topic,
      url: this.#url,
      project,
      headers: this.#headers,
    };
    if (this.#fetchFn !== undefined) {
      chanDeps.fetchFn = this.#fetchFn;
    }
    return new RealtimeChannel(chanDeps);
  }

  /** Connect the underlying WS transport (no-op; connections are per-channel). */
  connect(): void {
    // Per-channel connections are established on subscribe().
  }

  /** Close all channels. No-op at the client level; call channel.unsubscribe(). */
  disconnect(): void {
    // No-op: channels manage their own lifecycle.
  }

  #extractProject(): string {
    try {
      const host = new URL(this.#url).hostname;
      // e.g. "acme.basin.run" → "acme"; or plain host → use as-is
      const parts = host.split(".");
      return parts[0] ?? "default";
    } catch {
      return "default";
    }
  }
}
