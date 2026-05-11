/**
 * RealtimeClient + RealtimeChannel — Tier 4 placeholder.
 *
 * Channels land when basin engine v0.2 ships logical-replication-
 * driven broadcast. The full builder chain compiles today —
 * `basin.channel('room').on('postgres_changes', {...}, cb).subscribe()`
 * — but `subscribe()` throws `not_implemented` synchronously so apps
 * written against the surface fail loudly rather than appear to work.
 *
 * Surface mirrors `@supabase/realtime-js` so consumers porting from
 * Supabase keep their channel-wiring code; only the runtime swap is
 * needed when the cloud's broadcast lands.
 */

import { BasinError } from "../errors.js";

export type RealtimeEvent =
  | "INSERT"
  | "UPDATE"
  | "DELETE"
  | "*";

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

export type RealtimeListener<T = unknown> = (payload: PostgresChangesPayload<T>) => void;

export interface RealtimeChannelDeps {
  topic: string;
  enabled: boolean;
}

/**
 * Chainable channel builder. Methods return `this` so callers compose
 * fluently. `subscribe()` throws `not_implemented` until basin v0.2.
 *
 * @example
 * const ch = basin
 *   .channel('orders')
 *   .on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'orders' }, (payload) => {
 *     console.log('new order', payload.new);
 *   });
 * try { ch.subscribe(); } catch (e) {
 *   // BasinError(code='not_implemented') until basin v0.2.
 * }
 */
export class RealtimeChannel {
  #topic: string;
  #enabled: boolean;
  #bindings: Array<{
    type: "postgres_changes";
    filter: PostgresChangesFilter;
    callback: RealtimeListener;
  }> = [];

  constructor(deps: RealtimeChannelDeps) {
    this.#topic = deps.topic;
    this.#enabled = deps.enabled;
  }

  get topic(): string {
    return this.#topic;
  }

  /**
   * Register a listener. Today only `postgres_changes` is defined —
   * matches Supabase's first-class binding type.
   */
  on<T = unknown>(
    type: "postgres_changes",
    filter: PostgresChangesFilter,
    callback: RealtimeListener<T>,
  ): this {
    this.#bindings.push({
      type,
      filter,
      callback: callback as RealtimeListener,
    });
    return this;
  }

  /**
   * Connect the channel. Throws `not_implemented` until basin engine
   * v0.2 ships channels.
   */
  subscribe(): this {
    if (!this.#enabled) {
      throw new BasinError(
        "not_implemented",
        "Realtime channels land in basin engine v0.2 — " +
          "basin.realtime.enabled is false in this build. " +
          "Write app code against this builder today; the runtime swap is in v0.2.",
      );
    }
    // v0.2: open the WebSocket, send the join frame for #topic, dispatch
    // server frames to #bindings. Today: unreachable.
    /* c8 ignore next */
    return this;
  }

  /**
   * Disconnect. No-op today; ships with the v0.2 WebSocket impl.
   */
  unsubscribe(): this {
    return this;
  }
}

export interface RealtimeClientDeps {
  url: string;
  headers: Record<string, string>;
}

/**
 * Top-level realtime entry point. Reachable as `basin.realtime`.
 *
 *  - `basin.realtime.enabled` — `false` until basin v0.2.
 *  - `basin.realtime.channel(topic)` — construct a channel builder.
 *
 * The shorter `basin.channel(topic)` shim on the client object
 * delegates here so consumers can write the Supabase-shaped
 * `basin.channel('room1').on(...)` chain unchanged.
 */
export class RealtimeClient {
  readonly enabled = false;
  #url: string;
  #headers: Record<string, string>;

  constructor(deps: RealtimeClientDeps) {
    this.#url = deps.url;
    this.#headers = deps.headers;
  }

  /**
   * Construct a channel builder. Subscribing throws `not_implemented`
   * synchronously today.
   *
   * @example
   * basin.realtime.channel('room1')
   *   .on('postgres_changes', { event: '*', schema: 'public', table: 'messages' }, cb)
   *   .subscribe(); // throws BasinError('not_implemented') today.
   */
  channel(topic: string): RealtimeChannel {
    if (!topic) {
      throw new BasinError("invalid_request", "realtime.channel requires a topic");
    }
    return new RealtimeChannel({ topic, enabled: this.enabled });
  }

  /** v0.2 — open WebSocket. No-op today. */
  connect(): void {
    /* c8 ignore next */
    void this.#url;
    /* c8 ignore next */
    void this.#headers;
  }

  /** v0.2 — close WebSocket. No-op today. */
  disconnect(): void {
    /* c8 ignore next */
  }
}
