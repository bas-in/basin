/**
 * Runtime stub for @basin/basin-js.
 *
 * This module is aliased to "@basin/basin-js" in vite.config.ts when the real
 * package is not installed.  It implements the same interface as the real SDK
 * using plain fetch calls against the basin-auth and basin-rest HTTP APIs.
 *
 * Once @basin/basin-js is published and installed, remove the alias from
 * vite.config.ts and delete this file.
 *
 * API surface (per docs/basin-js-design.md):
 *   createClient(url, anonKey) → { auth, from, rpc, channel }
 *   basin.auth.signUp({ email, password })
 *   basin.auth.signInWithPassword({ email, password })
 *   basin.auth.signOut()
 *   basin.auth.session()
 *   basin.from(table).select(cols).eq(col, val) → { data, error }
 *   basin.from(table).insert(row) → { data, error }
 *   basin.from(table).update(row).eq(col, val) → { data, error }
 *   basin.from(table).delete().eq(col, val) → { data, error }
 */

const SESSION_KEY = "basin-session";

interface StoredSession {
  access_token: string;
  refresh_token: string;
  expires_in: number;
  user?: { id: string; email?: string };
}

export interface Session {
  accessToken: string;
  refreshToken: string;
  expiresIn: number;
  user?: { id: string; email?: string };
}

export interface User {
  id: string;
  email?: string;
}

function loadSession(): StoredSession | null {
  try {
    const raw =
      typeof localStorage !== "undefined"
        ? localStorage.getItem(SESSION_KEY)
        : null;
    return raw ? (JSON.parse(raw) as StoredSession) : null;
  } catch {
    return null;
  }
}

function saveSession(s: StoredSession | null) {
  try {
    if (typeof localStorage === "undefined") return;
    if (s) localStorage.setItem(SESSION_KEY, JSON.stringify(s));
    else localStorage.removeItem(SESSION_KEY);
  } catch {
    // ignore
  }
}

class AuthClientImpl {
  private _session: StoredSession | null;

  constructor(private baseUrl: string) {
    this._session = loadSession();
  }

  session(): Session | null {
    if (!this._session) return null;
    return {
      accessToken: this._session.access_token,
      refreshToken: this._session.refresh_token,
      expiresIn: this._session.expires_in,
      user: this._session.user,
    };
  }

  async signUp(opts: { email: string; password: string }) {
    try {
      const res = await fetch(`${this.baseUrl}/auth/v1/signup`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email: opts.email, password: opts.password }),
      });
      const json = (await res.json()) as StoredSession & { error?: string };
      if (!res.ok || json.error) {
        return { data: null, error: new Error(json.error ?? `HTTP ${res.status}`) };
      }
      this._session = json;
      saveSession(json);
      return {
        data: {
          user: json.user ?? { id: "", email: opts.email },
          session: this.session()!,
        },
        error: null,
      };
    } catch (err) {
      return { data: null, error: err instanceof Error ? err : new Error(String(err)) };
    }
  }

  async signInWithPassword(opts: { email: string; password: string }) {
    try {
      const res = await fetch(`${this.baseUrl}/auth/v1/signin`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email: opts.email, password: opts.password }),
      });
      const json = (await res.json()) as StoredSession & { error?: string };
      if (!res.ok || json.error) {
        return { data: null, error: new Error(json.error ?? `HTTP ${res.status}`) };
      }
      this._session = json;
      saveSession(json);
      return {
        data: {
          user: json.user ?? { id: "", email: opts.email },
          session: this.session()!,
        },
        error: null,
      };
    } catch (err) {
      return { data: null, error: err instanceof Error ? err : new Error(String(err)) };
    }
  }

  async signOut(): Promise<void> {
    if (!this._session) return;
    try {
      await fetch(`${this.baseUrl}/auth/v1/signout`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${this._session.access_token}`,
        },
        body: JSON.stringify({ refresh_token: this._session.refresh_token }),
      });
    } finally {
      this._session = null;
      saveSession(null);
    }
  }
}

type FilterOp = "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "in" | "is";

class QueryBuilderImpl<T> {
  private _select = "*";
  private _filters: Array<[FilterOp, string, unknown]> = [];
  private _limit: number | null = null;
  private _order: string | null = null;
  private _method: "GET" | "POST" | "PATCH" | "DELETE" = "GET";
  private _body: unknown = null;

  constructor(
    private baseUrl: string,
    private table: string,
    private token: string | null
  ) {}

  select(cols: string): this {
    this._select = cols;
    return this;
  }

  eq(col: string, val: unknown): this {
    this._filters.push(["eq", col, val]);
    return this;
  }

  neq(col: string, val: unknown): this {
    this._filters.push(["neq", col, val]);
    return this;
  }

  gt(col: string, val: unknown): this {
    this._filters.push(["gt", col, val]);
    return this;
  }

  limit(n: number): this {
    this._limit = n;
    return this;
  }

  private buildUrl(method: "GET" | "DELETE"): string {
    const params = new URLSearchParams();
    if (method === "GET") params.set("select", this._select);
    for (const [op, col, val] of this._filters) {
      params.append(col, `${op}.${val}`);
    }
    if (this._limit !== null) params.set("limit", String(this._limit));
    if (this._order) params.set("order", this._order);
    return `${this.baseUrl}/rest/v1/${this.table}?${params}`;
  }

  private headers(): Record<string, string> {
    const h: Record<string, string> = {
      "Content-Type": "application/json",
      Prefer: "return=representation",
    };
    if (this.token) h["Authorization"] = `Bearer ${this.token}`;
    return h;
  }

  async then<U>(resolve: (result: { data: T[] | null; error: Error | null }) => U): Promise<U> {
    try {
      const res = await fetch(this.buildUrl("GET"), { headers: this.headers() });
      if (!res.ok) return resolve({ data: null, error: new Error(`HTTP ${res.status}`) });
      const data = (await res.json()) as T[];
      return resolve({ data, error: null });
    } catch (err) {
      return resolve({ data: null, error: err instanceof Error ? err : new Error(String(err)) });
    }
  }

  async insert(row: Record<string, unknown> | Record<string, unknown>[]): Promise<{ data: T[] | null; error: Error | null }> {
    try {
      const res = await fetch(`${this.baseUrl}/rest/v1/${this.table}`, {
        method: "POST",
        headers: this.headers(),
        body: JSON.stringify(row),
      });
      if (!res.ok) {
        const text = await res.text();
        return { data: null, error: new Error(`HTTP ${res.status}: ${text}`) };
      }
      const data = (await res.json()) as T[];
      return { data, error: null };
    } catch (err) {
      return { data: null, error: err instanceof Error ? err : new Error(String(err)) };
    }
  }

  update(row: Record<string, unknown>): UpdateBuilder<T> {
    return new UpdateBuilder<T>(this.baseUrl, this.table, this.token, row);
  }

  delete(): DeleteBuilder<T> {
    return new DeleteBuilder<T>(this.baseUrl, this.table, this.token);
  }
}

class UpdateBuilder<T> {
  private _filters: Array<[string, unknown]> = [];

  constructor(
    private baseUrl: string,
    private table: string,
    private token: string | null,
    private row: Record<string, unknown>
  ) {}

  eq(col: string, val: unknown): this {
    this._filters.push([col, val]);
    return this;
  }

  async then<U>(resolve: (result: { data: T[] | null; error: Error | null }) => U): Promise<U> {
    const params = new URLSearchParams();
    for (const [col, val] of this._filters) params.append(col, `eq.${val}`);
    const h: Record<string, string> = {
      "Content-Type": "application/json",
      Prefer: "return=representation",
    };
    if (this.token) h["Authorization"] = `Bearer ${this.token}`;
    try {
      const res = await fetch(`${this.baseUrl}/rest/v1/${this.table}?${params}`, {
        method: "PATCH",
        headers: h,
        body: JSON.stringify(this.row),
      });
      if (!res.ok) return resolve({ data: null, error: new Error(`HTTP ${res.status}`) });
      const data = (await res.json()) as T[];
      return resolve({ data, error: null });
    } catch (err) {
      return resolve({ data: null, error: err instanceof Error ? err : new Error(String(err)) });
    }
  }
}

class DeleteBuilder<T> {
  private _filters: Array<[string, unknown]> = [];

  constructor(
    private baseUrl: string,
    private table: string,
    private token: string | null
  ) {}

  eq(col: string, val: unknown): Promise<{ data: T[] | null; error: Error | null }> {
    this._filters.push([col, val]);
    const params = new URLSearchParams();
    for (const [c, v] of this._filters) params.append(c, `eq.${v}`);
    const h: Record<string, string> = {
      "Content-Type": "application/json",
      Prefer: "return=representation",
    };
    if (this.token) h["Authorization"] = `Bearer ${this.token}`;
    return fetch(`${this.baseUrl}/rest/v1/${this.table}?${params}`, {
      method: "DELETE",
      headers: h,
    }).then(async (res) => {
      if (!res.ok) {
        const text = await res.text();
        return { data: null, error: new Error(`HTTP ${res.status}: ${text}`) };
      }
      const data = (await res.json()) as T[];
      return { data, error: null };
    }).catch((err) => ({
      data: null,
      error: err instanceof Error ? err : new Error(String(err)),
    }));
  }
}

class BasinClientImpl {
  readonly auth: AuthClientImpl;

  constructor(private baseUrl: string, _anonKey: string) {
    this.auth = new AuthClientImpl(baseUrl);
  }

  from<T = Record<string, unknown>>(table: string): QueryBuilderImpl<T> {
    const token = this.auth.session()?.accessToken ?? null;
    return new QueryBuilderImpl<T>(this.baseUrl, table, token);
  }

  async rpc(fn: string, args?: Record<string, unknown>) {
    const h: Record<string, string> = { "Content-Type": "application/json" };
    const token = this.auth.session()?.accessToken;
    if (token) h["Authorization"] = `Bearer ${token}`;
    try {
      const res = await fetch(`${this.baseUrl}/rest/v1/rpc/${fn}`, {
        method: "POST",
        headers: h,
        body: JSON.stringify(args ?? {}),
      });
      if (!res.ok) return { data: null, error: new Error(`HTTP ${res.status}`) };
      const data = (await res.json()) as unknown[];
      return { data, error: null };
    } catch (err) {
      return { data: null, error: err instanceof Error ? err : new Error(String(err)) };
    }
  }

  channel(_name: string) {
    // WebSocket subscriptions — Tier 2 scope (docs/websocket-subscription-design.md)
    // The channel API shape is reserved; events are not yet delivered server-side.
    return {
      on(_event: string, _filter: Record<string, unknown>, _cb: (p: unknown) => void) {
        return { subscribe() {}, unsubscribe() {} };
      },
    };
  }
}

export function createClient(
  url: string,
  anonKey: string,
  _opts?: Record<string, unknown>
): BasinClientImpl {
  return new BasinClientImpl(url, anonKey);
}
