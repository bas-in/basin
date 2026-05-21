/**
 * Minimal fetch-based Basin client shim.
 *
 * This file implements just enough of the @basin/basin-js surface to make the
 * integration tests (tests/rls-isolation.test.ts) run against a real Basin
 * instance WITHOUT requiring the @basin/basin-js npm package.
 *
 * The shim uses the raw basin-auth HTTP endpoints and basin-rest query
 * parameters documented in docs/basin-js-design.md and docs/tutorial.md.
 *
 * Interface faithfully mirrors the design spec:
 *   - basin.auth.signUp({ email, password }) → { user, session, error }
 *   - basin.auth.signInWithPassword({ email, password }) → { user, session, error }
 *   - basin.auth.signOut() → void
 *   - basin.from(table).select(cols).eq(col, val) → { data, error }
 *   - basin.from(table).insert(row) → { data, error }
 *   - basin.from(table).delete().eq(col, val) → { data, error }
 *
 * NOTE: This shim is for test/dev use only.  Production apps should use
 * the real @basin/basin-js package once it is published.
 */

export interface BasinUser {
  id: string;
  email: string;
}

export interface BasinSession {
  accessToken: string;
  refreshToken: string;
  expiresIn: number;
}

export interface BasinResult<T> {
  data: T | null;
  error: Error | null;
}

interface AuthResponse {
  access_token?: string;
  refresh_token?: string;
  expires_in?: number;
  user?: { id: string; email: string };
  error?: string;
}

class AuthClient {
  private baseUrl: string;
  private _session: BasinSession | null = null;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl;
  }

  session(): BasinSession | null {
    return this._session;
  }

  async signUp(opts: {
    email: string;
    password: string;
  }): Promise<BasinResult<{ user: BasinUser; session: BasinSession }>> {
    try {
      const res = await fetch(`${this.baseUrl}/auth/v1/signup`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email: opts.email, password: opts.password }),
      });
      const json = (await res.json()) as AuthResponse;
      if (!res.ok || json.error) {
        return { data: null, error: new Error(json.error ?? `HTTP ${res.status}`) };
      }
      const session: BasinSession = {
        accessToken: json.access_token!,
        refreshToken: json.refresh_token!,
        expiresIn: json.expires_in ?? 3600,
      };
      const user: BasinUser = {
        id: json.user?.id ?? "",
        email: json.user?.email ?? opts.email,
      };
      this._session = session;
      return { data: { user, session }, error: null };
    } catch (err) {
      return { data: null, error: err instanceof Error ? err : new Error(String(err)) };
    }
  }

  async signInWithPassword(opts: {
    email: string;
    password: string;
  }): Promise<BasinResult<{ user: BasinUser; session: BasinSession }>> {
    try {
      const res = await fetch(`${this.baseUrl}/auth/v1/signin`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email: opts.email, password: opts.password }),
      });
      const json = (await res.json()) as AuthResponse;
      if (!res.ok || json.error) {
        return { data: null, error: new Error(json.error ?? `HTTP ${res.status}`) };
      }
      const session: BasinSession = {
        accessToken: json.access_token!,
        refreshToken: json.refresh_token!,
        expiresIn: json.expires_in ?? 3600,
      };
      const user: BasinUser = {
        id: json.user?.id ?? "",
        email: json.user?.email ?? opts.email,
      };
      this._session = session;
      return { data: { user, session }, error: null };
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
          Authorization: `Bearer ${this._session.accessToken}`,
        },
        body: JSON.stringify({ refresh_token: this._session.refreshToken }),
      });
    } finally {
      this._session = null;
    }
  }
}

class QueryBuilder<T = Record<string, unknown>> {
  private baseUrl: string;
  private table: string;
  private token: string | null;
  private _select = "*";
  private _filters: string[] = [];
  private _limit: number | null = null;

  constructor(baseUrl: string, table: string, token: string | null) {
    this.baseUrl = baseUrl;
    this.table = table;
    this.token = token;
  }

  select(cols: string): this {
    this._select = cols;
    return this;
  }

  eq(col: string, val: unknown): this {
    this._filters.push(`${col}=eq.${val}`);
    return this;
  }

  limit(n: number): this {
    this._limit = n;
    return this;
  }

  private buildUrl(): string {
    const params = new URLSearchParams();
    params.set("select", this._select);
    for (const f of this._filters) {
      const [k, v] = f.split("=");
      params.append(k, v);
    }
    if (this._limit !== null) params.set("limit", String(this._limit));
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

  async then(resolve: (result: BasinResult<T[]>) => void): Promise<void> {
    try {
      const res = await fetch(this.buildUrl(), { headers: this.headers() });
      if (!res.ok) {
        resolve({ data: null, error: new Error(`HTTP ${res.status}`) });
        return;
      }
      const data = (await res.json()) as T[];
      resolve({ data, error: null });
    } catch (err) {
      resolve({
        data: null,
        error: err instanceof Error ? err : new Error(String(err)),
      });
    }
  }

  async insert(row: Record<string, unknown>): Promise<BasinResult<T[]>> {
    try {
      const res = await fetch(
        `${this.baseUrl}/rest/v1/${this.table}`,
        {
          method: "POST",
          headers: this.headers(),
          body: JSON.stringify(row),
        }
      );
      if (!res.ok) {
        const text = await res.text();
        return { data: null, error: new Error(`HTTP ${res.status}: ${text}`) };
      }
      const data = (await res.json()) as T[];
      return { data, error: null };
    } catch (err) {
      return {
        data: null,
        error: err instanceof Error ? err : new Error(String(err)),
      };
    }
  }

  async delete(): Promise<QueryBuilder<T> & { eq: (col: string, val: unknown) => Promise<BasinResult<T[]>> }> {
    const self = this;
    return {
      ...this,
      async eq(col: string, val: unknown): Promise<BasinResult<T[]>> {
        self._filters.push(`${col}=eq.${val}`);
        const params = new URLSearchParams();
        for (const f of self._filters) {
          const [k, v] = f.split("=");
          params.append(k, v);
        }
        try {
          const res = await fetch(
            `${self.baseUrl}/rest/v1/${self.table}?${params}`,
            {
              method: "DELETE",
              headers: self.headers(),
            }
          );
          if (!res.ok) {
            const text = await res.text();
            return { data: null, error: new Error(`HTTP ${res.status}: ${text}`) };
          }
          const data = (await res.json()) as T[];
          return { data, error: null };
        } catch (err) {
          return {
            data: null,
            error: err instanceof Error ? err : new Error(String(err)),
          };
        }
      },
    } as unknown as QueryBuilder<T> & { eq: (col: string, val: unknown) => Promise<BasinResult<T[]>> };
  }
}

class BasinCompat {
  private baseUrl: string;
  readonly auth: AuthClient;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl;
    this.auth = new AuthClient(baseUrl);
  }

  from<T = Record<string, unknown>>(table: string): QueryBuilder<T> {
    const token = this.auth.session()?.accessToken ?? null;
    return new QueryBuilder<T>(this.baseUrl, table, token);
  }
}

export function createCompatClient(basinUrl: string): BasinCompat {
  return new BasinCompat(basinUrl);
}
