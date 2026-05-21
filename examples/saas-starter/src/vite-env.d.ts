/// <reference types="vite/client" />

/**
 * Type stub for @basin/basin-js.
 *
 * @basin/basin-js is not yet published to npm (design spec: docs/basin-js-design.md).
 * This stub provides just enough type information to make TypeScript happy until
 * the real package is published.  It mirrors the API surface in the design spec.
 *
 * REMOVE this file once @basin/basin-js is available on npm and installed.
 */
declare module "@basin/basin-js" {
  export interface User {
    id: string;
    email?: string;
  }

  export interface Session {
    accessToken: string;
    refreshToken: string;
    expiresIn: number;
    user?: User;
  }

  export interface BasinResult<T> {
    data: T | null;
    error: Error | null;
  }

  export interface QueryBuilder<T> {
    select(cols: string): QueryBuilder<T>;
    eq(col: string, val: unknown): QueryBuilder<T>;
    neq(col: string, val: unknown): QueryBuilder<T>;
    gt(col: string, val: unknown): QueryBuilder<T>;
    gte(col: string, val: unknown): QueryBuilder<T>;
    lt(col: string, val: unknown): QueryBuilder<T>;
    lte(col: string, val: unknown): QueryBuilder<T>;
    in(col: string, vals: unknown[]): QueryBuilder<T>;
    is(col: string, val: null | boolean): QueryBuilder<T>;
    limit(n: number): QueryBuilder<T>;
    order(col: string, opts?: { ascending?: boolean }): QueryBuilder<T>;
    insert(row: Record<string, unknown> | Record<string, unknown>[]): Promise<BasinResult<T[]>>;
    update(row: Record<string, unknown>): QueryBuilder<T>;
    delete(): QueryBuilder<T>;
    then<U>(resolve: (result: BasinResult<T[]>) => U): Promise<U>;
  }

  export interface AuthClient {
    signUp(opts: { email: string; password: string }): Promise<BasinResult<{ user: User; session: Session }>>;
    signInWithPassword(opts: { email: string; password: string }): Promise<BasinResult<{ user: User; session: Session }>>;
    signInWithMagicLink(opts: { email: string }): Promise<BasinResult<null>>;
    /** OAuth: throws E_NOT_IMPLEMENTED in v0.1; available in basin-auth v2 (ADR 0020). */
    signInWithOAuth(opts: { provider: string }): Promise<BasinResult<null>>;
    signOut(): Promise<void>;
    session(): Session | null;
  }

  export interface BasinClient {
    auth: AuthClient;
    from<T = Record<string, unknown>>(table: string): QueryBuilder<T>;
    rpc(fn: string, args?: Record<string, unknown>): Promise<BasinResult<unknown[]>>;
    channel(name: string): {
      on(event: string, filter: Record<string, unknown>, cb: (payload: unknown) => void): { subscribe(): void; unsubscribe(): void };
    };
  }

  export function createClient(url: string, anonKey: string, opts?: Record<string, unknown>): BasinClient;
}
