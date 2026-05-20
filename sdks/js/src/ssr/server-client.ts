import { AuthClient } from "../auth/client.js";
import { BasinError } from "../errors.js";
import type { AuthSession } from "../auth/types.js";

export interface CookieAdapter {
  get(name: string): string | null | undefined;
  set(name: string, value: string, options?: CookieSetOptions): void;
  remove(name: string, options?: CookieSetOptions): void;
}

export interface CookieSetOptions {
  path?: string;
  maxAge?: number;
  httpOnly?: boolean;
  secure?: boolean;
  sameSite?: "strict" | "lax" | "none";
}

export interface ServerClientOptions {
  cookies: CookieAdapter;
  fetch?: typeof globalThis.fetch;
  headers?: Record<string, string>;
}

const SESSION_COOKIE = "basin.auth.session";

function cookieStorage(adapter: CookieAdapter) {
  return {
    getItem(key: string): string | null {
      return adapter.get(key) ?? null;
    },
    setItem(key: string, value: string): void {
      adapter.set(key, value, {
        path: "/",
        httpOnly: true,
        secure: true,
        sameSite: "lax",
        maxAge: 60 * 60 * 24 * 365,
      });
    },
    removeItem(key: string): void {
      adapter.remove(key, { path: "/" });
    },
  };
}

export class ServerAuthClient {
  readonly #inner: AuthClient;
  readonly #cookies: CookieAdapter;

  constructor(inner: AuthClient, cookies: CookieAdapter) {
    this.#inner = inner;
    this.#cookies = cookies;
  }

  getSession(): AuthSession | null {
    const raw = this.#cookies.get(SESSION_COOKIE) ?? null;
    if (!raw) return null;
    try {
      return JSON.parse(raw) as AuthSession;
    } catch {
      return null;
    }
  }

  getUser() {
    return this.getSession()?.user ?? null;
  }

  async signInWithPassword(input: { email: string; password: string }) {
    const result = await this.#inner.signInWithPassword(input);
    if (result.data) {
      this.#cookies.set(SESSION_COOKIE, JSON.stringify(result.data), {
        path: "/",
        httpOnly: true,
        secure: true,
        sameSite: "lax",
        maxAge: 60 * 60 * 24 * 365,
      });
    }
    return result;
  }

  async signOut() {
    this.#cookies.remove(SESSION_COOKIE, { path: "/" });
    return this.#inner.signOut();
  }

  async refreshSession() {
    const session = this.getSession();
    if (!session) {
      return {
        data: null,
        error: new BasinError(
          "no_session",
          "No session cookie found; cannot refresh",
        ),
      };
    }
    const result = await this.#inner.refreshSession();
    if (result.data) {
      this.#cookies.set(SESSION_COOKIE, JSON.stringify(result.data), {
        path: "/",
        httpOnly: true,
        secure: true,
        sameSite: "lax",
        maxAge: 60 * 60 * 24 * 365,
      });
    }
    return result;
  }
}

export interface ServerClient {
  auth: ServerAuthClient;
}

export function createServerClient(
  url: string,
  key: string,
  options: ServerClientOptions,
): ServerClient {
  const baseHeaders: Record<string, string> = {
    "Content-Type": "application/json",
    apikey: key,
    Authorization: `Bearer ${key}`,
    ...options.headers,
  };

  const storage = cookieStorage(options.cookies);

  const authInner = new AuthClient({
    url,
    headers: baseHeaders,
    fetch: options.fetch ?? globalThis.fetch,
    storage,
  });

  const auth = new ServerAuthClient(authInner, options.cookies);

  return { auth };
}
