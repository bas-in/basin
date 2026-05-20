/**
 * AuthClient — owns the session lifecycle. Talks DIRECTLY to
 * basin-engine's `/auth/v1/*` route surface — basin-cloud is the
 * control plane (orgs / projects / billing / dashboard), basin-engine
 * is the data plane, and the SDK only ever speaks to the engine.
 *
 * Architecture (as of 2026-05-11): the `/auth/v1/*` surface is served by
 * basin-auth (open-source Rust). basin-auth's catalog — users, tenants,
 * sessions, MFA factors, magic-link nonces — now lives on basin engine
 * itself via loopback pgwire (`postgres://basin_auth@127.0.0.1:5433/basin`),
 * not on basin-cloud and not on an external Postgres. The SDK's HTTP
 * shape is unchanged; self-hosters need only run basin.
 *
 * The engine routes (verified against basin/crates/basin-rest/src/server.rs):
 *  - POST /auth/v1/signup
 *  - POST /auth/v1/signin                  (was `/login` on the cloud)
 *  - POST /auth/v1/refresh
 *  - POST /auth/v1/magic-link              (hyphenated; was `/magiclink`)
 *  - POST /auth/v1/magic-link/consume
 *  - POST /auth/v1/verify-email
 *  - POST /auth/v1/reset-password
 *  - POST /auth/v1/request-password-reset
 *
 * Public surface mirrors @supabase/auth-js so existing Supabase-shaped
 * app code ports over with a `createClient` swap. `signInWithOAuth` and
 * `auth.mfa.*` are fully wired (T-020/T-021). Methods that the engine
 * doesn't (yet) expose — the `basin.storage` namespace — return
 * `BasinError("not_implemented")` with a JSDoc note. `signOut()` has no
 * server-side counterpart on the engine; sign-out is purely local
 * (the session's refresh_token expires naturally per its TTL).
 */

import { BasinError } from "../errors.js";
import { AuthMfaClient } from "./mfa.js";
import type {
  AuthChangeEvent,
  AuthSession,
  AuthUser,
  ConsumeMagicLinkInput,
  RequestPasswordResetInput,
  ResetPasswordInput,
  SignInWithMagicLinkInput,
  SignInWithOAuthInput,
  SignInWithPasswordInput,
  SignUpInput,
  VerifyEmailInput,
} from "./types.js";

interface AuthClientDeps {
  url: string;
  headers: Record<string, string>;
  fetch: typeof fetch;
  storage:
    | {
        getItem: (key: string) => string | null | Promise<string | null>;
        setItem: (key: string, value: string) => void | Promise<void>;
        removeItem: (key: string) => void | Promise<void>;
      }
    | undefined;
}

const SESSION_STORAGE_KEY = "basin.auth.session";

/**
 * How early (in seconds) before access-token expiry the auto-refresh
 * timer should fire. 60s buffer covers clock skew + a slow refresh
 * round-trip without letting the access token go stale in flight.
 */
const REFRESH_BUFFER_SECONDS = 60;

export class AuthClient {
  #url: string;
  #headers: Record<string, string>;
  #fetch: typeof fetch;
  #storage: AuthClientDeps["storage"];
  #session: AuthSession | null = null;
  #listeners = new Set<(event: AuthChangeEvent, session: AuthSession | null) => void>();
  #refreshTimer: ReturnType<typeof setTimeout> | null = null;
  readonly mfa: AuthMfaClient;

  constructor(deps: AuthClientDeps) {
    this.#url = `${deps.url}/auth/v1`;
    this.#headers = deps.headers;
    this.#fetch = deps.fetch;
    this.#storage = deps.storage;
    this.mfa = new AuthMfaClient({
      url: this.#url,
      headers: this.#headers,
      fetch: this.#fetch,
      getSession: () => this.#session,
      adoptSession: async (session) => {
        await this.#persistSession(session);
        // verify({factor:"totp_challenge"}) completing a mid-login flow
        // is the canonical case for MFA_CHALLENGE_VERIFIED.
        this.#emit("MFA_CHALLENGE_VERIFIED", session);
      },
    });
    // Best-effort session hydration from storage. For sync adapters
    // (browser localStorage), `getSession()` returns the hydrated
    // session on the very next call. For async adapters (Deno KV,
    // AsyncStorage on RN), hydration completes on a later microtask;
    // consumers should subscribe to `onAuthStateChange` for the
    // `INITIAL_SESSION` event to be told when it lands.
    this.#hydrateFromStorage();
  }

  // ─── Public API surface (bodies stubbed; see TASKS.md Tier 1) ──────

  /**
   * Sign up with email + password against the cloud's
   * `POST /auth/v1/signup`. On success, persists the session to local
   * storage, fires the `SIGNED_IN` event, and returns
   * `{ data: session, error: null }`. The cloud auto-creates a personal
   * org for the new user; that's reachable via `basin.from('organizations')`
   * after sign-up, not surfaced on the returned session.
   *
   * `input.data` (user_metadata) and `input.emailRedirectTo` are accepted
   * on the SDK surface for forward-compat with `@supabase/auth-js`
   * shape, but ignored today — the cloud's `SignupRequest` only carries
   * `{email, password, name?}`. When the cloud grows metadata / email-
   * confirm-redirect support these fields will start serialising into
   * the request body without an SDK breaking change.
   *
   * Email-confirmation flow (cloud emits `email_verified=false` on
   * first signup) is observable through `data.user.email_confirmed_at
   * === null`; callers can route the user to a "check your inbox"
   * screen on that signal.
   *
   * @example
   * const { data, error } = await basin.auth.signUp({
   *   email: 'pc@example.com',
   *   password: 'correct-horse-battery-staple',
   * });
   * if (error) return surfaceError(error);
   * if (!data?.user.email_confirmed_at) router.push('/check-inbox');
   */
  async signUp(
    input: SignUpInput,
  ): Promise<{ data: AuthSession | null; error: BasinError | null }> {
    if (!input?.email || !input?.password) {
      return {
        data: null,
        error: new BasinError(
          "invalid_request",
          "auth.signUp requires email and password",
        ),
      };
    }
    let res: Response;
    try {
      res = await this.#fetch(`${this.#url}/signup`, {
        method: "POST",
        headers: this.#headers,
        body: JSON.stringify({
          email: input.email,
          password: input.password,
        }),
      });
    } catch (e) {
      return {
        data: null,
        error: new BasinError("network", networkErrorMessage(e)),
      };
    }
    const parsed = await unwrapAuthBody(res);
    if (parsed.kind === "error") return { data: null, error: parsed.error };
    if (parsed.kind === "mfa") {
      // signup itself never triggers MFA — MFA is enrolled later. Treat
      // an unexpected MFA envelope as a malformed cloud response.
      return {
        data: null,
        error: new BasinError(
          "invalid_response",
          "Signup returned an MFA envelope; this is a cloud bug",
          res.status,
        ),
      };
    }
    const session = mapLoginToSession(parsed.body);
    if (!session) {
      return {
        data: null,
        error: new BasinError(
          "invalid_response",
          "Signup succeeded but response is missing user or session fields",
          res.status,
        ),
      };
    }
    await this.#persistSession(session);
    return { data: session, error: null };
  }

  /**
   * Sign in with email + password against the engine's `POST /auth/v1/signin`.
   * On success, persists the session to local storage, fires the
   * `SIGNED_IN` event for any `onAuthStateChange` subscribers, and
   * returns `{ data: session, error: null }`.
   *
   * The engine's signin response shape is `{user, session, org?}`
   * (wrapped in the engine's `{data, error}` envelope). We unwrap the
   * envelope and map onto the SDK's flatter `AuthSession` shape
   * (which collapses `session_id` and the org out, both reachable via
   * `getUser()` / `basin.from('organizations')` respectively).
   *
   * TOTP-enrolled users get a special `{requires_totp, partial_token}`
   * envelope from the engine, surfaced as a typed `mfa_required` error.
   * Callers complete the flow by calling
   * `auth.mfa.verify({factor:'totp_challenge', code, partial_token})`
   * with the user's 6-digit code; on success the engine completes the
   * login and the SDK adopts the new session.
   *
   * @example
   * const { data, error } = await basin.auth.signInWithPassword({
   *   email: 'pc@example.com', password: 'hunter22',
   * });
   * if (error?.code === 'mfa_required') {
   *   const code = await prompt('Enter 6-digit code');
   *   await basin.auth.mfa.verify({
   *     factor: 'totp_challenge', code,
   *     partial_token: (error.details as { partial_token: string }).partial_token,
   *   });
   * }
   */
  async signInWithPassword(
    input: SignInWithPasswordInput,
  ): Promise<{ data: AuthSession | null; error: BasinError | null }> {
    if (!input?.email || !input?.password) {
      return {
        data: null,
        error: new BasinError(
          "invalid_request",
          "auth.signInWithPassword requires email and password",
        ),
      };
    }
    let res: Response;
    try {
      res = await this.#fetch(`${this.#url}/signin`, {
        method: "POST",
        headers: this.#headers,
        body: JSON.stringify({
          email: input.email,
          password: input.password,
        }),
      });
    } catch (e) {
      return {
        data: null,
        error: new BasinError("network", networkErrorMessage(e)),
      };
    }
    const parsed = await unwrapAuthBody(res);
    if (parsed.kind === "error") return { data: null, error: parsed.error };
    if (parsed.kind === "mfa") {
      return {
        data: null,
        error: new BasinError(
          "mfa_required",
          "Two-factor verification required; complete via auth.mfa.verify()",
          res.status,
          { partial_token: parsed.partialToken },
        ),
      };
    }
    const session = mapLoginToSession(parsed.body);
    if (!session) {
      return {
        data: null,
        error: new BasinError(
          "invalid_response",
          "Sign-in succeeded but response is missing user or session fields",
          res.status,
        ),
      };
    }
    await this.#persistSession(session);
    return { data: session, error: null };
  }

  /**
   * Sign in with an OAuth provider.
   *
   * Builds the engine's `GET /auth/v1/authorize?provider=<name>&redirect_to=…`
   * URL. The engine handles PKCE + signed `state` server-side and completes
   * the exchange via `GET /auth/v1/callback`. In a browser environment this
   * method sets `window.location.href` to redirect automatically; in all
   * other runtimes it returns the URL for the caller to act on.
   *
   * Returns `{data: {url, provider}, error: null}` — the URL is always
   * returned so callers can inspect it before the redirect.
   *
   * Provider list: Google, GitHub, Microsoft, GitLab, Slack, Discord, Apple,
   * X (Twitter), Bitbucket, Notion, Spotify, Twitch, LinkedIn, Figma, + generic
   * OIDC per ADR 0020.
   *
   * @example
   * const { data, error } = await basin.auth.signInWithOAuth({ provider: 'github' });
   * if (error) return handleError(error);
   * // In non-browser (SSR/Node): redirect manually.
   * window.location.href = data.url; // handled automatically in browser
   */
  async signInWithOAuth(
    input: SignInWithOAuthInput,
  ): Promise<{
    data: { url: string; provider: import("./types.js").OAuthProvider } | null;
    error: BasinError | null;
  }> {
    if (!input?.provider) {
      return {
        data: null,
        error: new BasinError(
          "invalid_request",
          "auth.signInWithOAuth requires a provider",
        ),
      };
    }
    const params = new URLSearchParams({ provider: input.provider });
    if (input.redirectTo) params.set("redirect_to", input.redirectTo);
    if (input.scopes) params.set("scopes", input.scopes);
    if (input.queryParams) {
      for (const [k, v] of Object.entries(input.queryParams)) {
        params.set(k, v);
      }
    }
    const url = `${this.#url}/authorize?${params.toString()}`;
    // In browser environments, redirect automatically. Return the URL
    // first so SSR callers can issue a 302 without a window reference.
    if (typeof window !== "undefined" && typeof window.location !== "undefined") {
      window.location.href = url;
    }
    return { data: { url, provider: input.provider }, error: null };
  }

  /**
   * Send a sign-in magic link to the supplied email address. The
   * engine's `POST /auth/v1/magic-link` always returns `202 Accepted`
   * with `{sent: true}` regardless of whether the email is registered
   * — that's a deliberate non-enumeration design choice. Callers can
   * therefore treat a non-error response as "we asked the engine to
   * dispatch a mail; we don't know if one will actually send".
   *
   * The clicked link lands the user back in the app with an attached
   * session token; the SPA finishes the flow by calling
   * `POST /auth/v1/magic-link/consume`. This SDK method does NOT fire
   * `SIGNED_IN` — there's no session at this point. The caller's
   * `onAuthStateChange` will fire later when the user lands back in
   * the app with a populated session URL hash.
   *
   * `shouldCreateUser` and `emailRedirectTo` are accepted for
   * `@supabase/auth-js` parity but the engine's `MagicLinkRequest`
   * only carries `{email}` today, so they're silently dropped.
   *
   * @example
   * await basin.auth.signInWithMagicLink({ email: 'pc@example.com' });
   * // UI now reads "Check your inbox" regardless of whether the
   * // address exists (non-enumeration).
   */
  async signInWithMagicLink(
    input: SignInWithMagicLinkInput,
  ): Promise<{ data: null; error: BasinError | null }> {
    if (!input?.email) {
      return {
        data: null,
        error: new BasinError(
          "invalid_request",
          "auth.signInWithMagicLink requires email",
        ),
      };
    }
    let res: Response;
    try {
      res = await this.#fetch(`${this.#url}/magic-link`, {
        method: "POST",
        headers: this.#headers,
        body: JSON.stringify({ email: input.email }),
      });
    } catch (e) {
      return {
        data: null,
        error: new BasinError("network", networkErrorMessage(e)),
      };
    }
    const parsed = await unwrapAuthBody(res);
    if (parsed.kind === "error") return { data: null, error: parsed.error };
    // MFA envelope from magic-link is impossible by design; if the
    // engine somehow emits one, treat as a successful no-op rather than
    // surface as a bug — the user-visible state ("we sent a mail") is
    // unchanged.
    return { data: null, error: null };
  }

  /**
   * Consume a magic-link token and establish a session.
   *
   * The magic-link email contains a URL with a `token` query parameter.
   * The SPA reads that token and calls this method to exchange it for a
   * full session. On success, persists the session and fires `SIGNED_IN`.
   *
   * POSTs `POST /auth/v1/magic-link/consume` with `{token}` in the body.
   *
   * @example
   * // In the SPA's magic-link callback route:
   * const token = new URL(window.location.href).searchParams.get('token')!;
   * const { data, error } = await basin.auth.consumeMagicLink({ token });
   * if (error) return showError(error);
   * router.push('/dashboard');
   */
  async consumeMagicLink(
    input: ConsumeMagicLinkInput,
  ): Promise<{ data: AuthSession | null; error: BasinError | null }> {
    if (!input?.token) {
      return {
        data: null,
        error: new BasinError(
          "invalid_request",
          "auth.consumeMagicLink requires a token",
        ),
      };
    }
    let res: Response;
    try {
      res = await this.#fetch(`${this.#url}/magic-link/consume`, {
        method: "POST",
        headers: this.#headers,
        body: JSON.stringify({ token: input.token }),
      });
    } catch (e) {
      return {
        data: null,
        error: new BasinError("network", networkErrorMessage(e)),
      };
    }
    const parsed = await unwrapAuthBody(res);
    if (parsed.kind === "error") return { data: null, error: parsed.error };
    if (parsed.kind === "mfa") {
      return {
        data: null,
        error: new BasinError(
          "mfa_required",
          "Two-factor verification required; complete via auth.mfa.verify()",
          res.status,
          { partial_token: parsed.partialToken },
        ),
      };
    }
    const session = mapLoginToSession(parsed.body);
    if (!session) {
      return {
        data: null,
        error: new BasinError(
          "invalid_response",
          "Magic-link consume succeeded but response is missing user or session fields",
          res.status,
        ),
      };
    }
    await this.#persistSession(session);
    return { data: session, error: null };
  }

  /**
   * Verify a user's email address using a one-time token.
   *
   * The verification email contains a URL with a `token` query parameter.
   * POSTs `POST /auth/v1/verify-email` with `{token}` in the body.
   * On success the engine marks the user's email as confirmed.
   *
   * Note: this does NOT automatically sign the user in. If you want a
   * session after email verification, call `signInWithPassword` afterwards.
   *
   * @example
   * const token = new URL(window.location.href).searchParams.get('token')!;
   * const { error } = await basin.auth.verifyEmail({ token });
   * if (!error) router.push('/signin?verified=true');
   */
  async verifyEmail(
    input: VerifyEmailInput,
  ): Promise<{ data: null; error: BasinError | null }> {
    if (!input?.token) {
      return {
        data: null,
        error: new BasinError(
          "invalid_request",
          "auth.verifyEmail requires a token",
        ),
      };
    }
    let res: Response;
    try {
      res = await this.#fetch(`${this.#url}/verify-email`, {
        method: "POST",
        headers: this.#headers,
        body: JSON.stringify({ token: input.token }),
      });
    } catch (e) {
      return {
        data: null,
        error: new BasinError("network", networkErrorMessage(e)),
      };
    }
    const parsed = await unwrapAuthBody(res);
    if (parsed.kind === "error") return { data: null, error: parsed.error };
    return { data: null, error: null };
  }

  /**
   * Send a password-reset email to the supplied address.
   *
   * POSTs `POST /auth/v1/request-password-reset` with `{email}`. The
   * engine always returns 202 Accepted regardless of whether the email
   * is registered (non-enumeration design choice).
   *
   * @example
   * await basin.auth.requestPasswordReset({ email: 'user@example.com' });
   * // UI reads "If that address is registered, you'll get an email."
   */
  async requestPasswordReset(
    input: RequestPasswordResetInput,
  ): Promise<{ data: null; error: BasinError | null }> {
    if (!input?.email) {
      return {
        data: null,
        error: new BasinError(
          "invalid_request",
          "auth.requestPasswordReset requires email",
        ),
      };
    }
    let res: Response;
    try {
      res = await this.#fetch(`${this.#url}/request-password-reset`, {
        method: "POST",
        headers: this.#headers,
        body: JSON.stringify({ email: input.email }),
      });
    } catch (e) {
      return {
        data: null,
        error: new BasinError("network", networkErrorMessage(e)),
      };
    }
    const parsed = await unwrapAuthBody(res);
    if (parsed.kind === "error") return { data: null, error: parsed.error };
    return { data: null, error: null };
  }

  /**
   * Complete a password reset using the token from the reset email.
   *
   * POSTs `POST /auth/v1/reset-password` with `{token, new_password}`.
   * On success the engine updates the user's password. The user must then
   * call `signInWithPassword` to obtain a new session.
   *
   * @example
   * const token = new URL(window.location.href).searchParams.get('token')!;
   * const { error } = await basin.auth.resetPassword({
   *   token,
   *   newPassword: 'correct-horse-battery-staple',
   * });
   * if (!error) router.push('/signin?password_reset=true');
   */
  async resetPassword(
    input: ResetPasswordInput,
  ): Promise<{ data: null; error: BasinError | null }> {
    if (!input?.token || !input?.newPassword) {
      return {
        data: null,
        error: new BasinError(
          "invalid_request",
          "auth.resetPassword requires token and newPassword",
        ),
      };
    }
    let res: Response;
    try {
      res = await this.#fetch(`${this.#url}/reset-password`, {
        method: "POST",
        headers: this.#headers,
        body: JSON.stringify({ token: input.token, new_password: input.newPassword }),
      });
    } catch (e) {
      return {
        data: null,
        error: new BasinError("network", networkErrorMessage(e)),
      };
    }
    const parsed = await unwrapAuthBody(res);
    if (parsed.kind === "error") return { data: null, error: parsed.error };
    return { data: null, error: null };
  }

  /**
   * Sign out — local-only.
   *
   * basin-engine has no server-side logout endpoint — sign-out is
   * local-only. The session's refresh_token will expire naturally per
   * its TTL; if a caller needs server-side revocation (a stolen token,
   * a "sign out everywhere" UX), that lands when basin-engine grows a
   * `/auth/v1/logout` or token-revocation surface in v0.2+.
   *
   * This method clears the in-memory session + storage adapter and
   * fires `SIGNED_OUT` on subscribers. No network call is made.
   * `SIGNED_OUT` always fires, matching `@supabase/auth-js` so app
   * code that hides authenticated UI on the event doesn't need a
   * "but was-it-already-signed-out" guard.
   *
   * @example
   * const { error } = await basin.auth.signOut(); // error is always null
   * router.push('/signin');
   */
  async signOut(): Promise<{ error: BasinError | null }> {
    this.#session = null;
    this.#clearAutoRefresh();
    if (this.#storage) {
      try {
        await this.#storage.removeItem(SESSION_STORAGE_KEY);
      } catch {
        // Storage errors are non-fatal — in-memory clear already happened.
      }
    }
    this.#emit("SIGNED_OUT", null);
    return { error: null };
  }

  /**
   * Read the current session synchronously. Returns `null` when no
   * user is signed in (or hydration from async storage hasn't yet
   * resolved — subscribe to `onAuthStateChange` and wait for
   * `INITIAL_SESSION` if you need to wait it out).
   *
   * The session's `access_token` is a JWT automatically attached by the
   * query builder as `Authorization: Bearer <at>` on every
   * `basin.from(...)` call. This enables Row Level Security (RLS)
   * policies that reference `auth.uid()` — the engine evaluates the JWT
   * and makes the following SQL session functions available:
   *
   *   - `auth.uid()`   returns the UUID of the signed-in user
   *   - `auth.role()`  returns `'authenticated'` (or `'anon'` without a session)
   *   - `auth.jwt()`   returns the full JWT claims as JSONB
   *
   * For direct pgwire connections (advanced), pass `access_token` as the
   * pgwire username — no password needed. API keys can be used instead:
   * use `{tenant_id}_{hex}` as the username and the full API key as the
   * password.
   *
   * @example
   * const session = basin.auth.getSession();
   * if (!session) router.push('/login');
   *
   * @example
   * // After sign-in, auth.uid() works in SQL queries via basin.from():
   * // (RLS policy on the table must already reference auth.uid())
   * const { data } = await basin.from('items').select('*');
   * // Returns only rows where owner_id = auth.uid() if RLS is enabled.
   */
  getSession(): AuthSession | null {
    return this.#session;
  }

  /**
   * Read the current user synchronously. Equivalent to
   * `getSession()?.user ?? null`.
   *
   * The returned `user.id` matches what `auth.uid()` returns in SQL
   * queries — useful for client-side filtering that mirrors your RLS
   * policies.
   *
   * @example
   * const user = basin.auth.getUser();
   * if (user) greet(user.email);
   */
  getUser(): AuthUser | null {
    return this.#session?.user ?? null;
  }

  /**
   * Subscribe to session changes. Returns an unsubscribe fn.
   *
   * ```ts
   * const { data: { subscription } } = basin.auth.onAuthStateChange((event, session) => {
   *   if (event === 'SIGNED_OUT') router.push('/login');
   * });
   * // …later
   * subscription.unsubscribe();
   * ```
   */
  onAuthStateChange(callback: (event: AuthChangeEvent, session: AuthSession | null) => void): {
    data: { subscription: { unsubscribe: () => void } };
  } {
    this.#listeners.add(callback);
    return {
      data: {
        subscription: {
          unsubscribe: () => {
            this.#listeners.delete(callback);
          },
        },
      },
    };
  }

  /**
   * Refresh the access token using the stored refresh token.
   *
   * POSTs `/auth/v1/refresh` with `{refresh_token}` in the JSON body
   * (the cloud also accepts the same token via the HttpOnly
   * `__basin_refresh` cookie for same-origin SPA callers, but the SDK
   * uses the body path so it works cross-origin without `credentials:
   * "include"` + a `SameSite=None` cookie). On success, the cloud
   * returns `{access_token, refresh_token, expires_at, session_id}` —
   * a partial shape relative to login (no user) — so we splice the
   * new tokens + expiry into the existing session and persist.
   *
   * Side effects: fires `TOKEN_REFRESHED` on subscribers; persists
   * the updated session to storage; reschedules the auto-refresh
   * timer for the new expiry. On 401 (refresh token unknown,
   * revoked, or replayed), clears local state + fires `SIGNED_OUT`
   * so callers route to a sign-in page without a separate check.
   *
   * The auto-refresh timer normally calls this for you 60s before
   * the access token expires; manual invocation is reserved for
   * "I want a fresh token right now" callers (e.g. before a long
   * upload).
   *
   * @example
   * const { error } = await basin.auth.refreshSession();
   * if (error?.code === 'refresh_failed') router.push('/login');
   */
  async refreshSession(): Promise<{
    data: AuthSession | null;
    error: BasinError | null;
  }> {
    const current = this.#session;
    if (!current?.refresh_token) {
      return {
        data: null,
        error: new BasinError(
          "no_session",
          "auth.refreshSession requires an active session with a refresh_token",
        ),
      };
    }
    let res: Response;
    try {
      res = await this.#fetch(`${this.#url}/refresh`, {
        method: "POST",
        headers: this.#headers,
        body: JSON.stringify({ refresh_token: current.refresh_token }),
      });
    } catch (e) {
      return {
        data: null,
        error: new BasinError("network", networkErrorMessage(e)),
      };
    }
    if (res.status === 401) {
      // Refresh failed — the token is unknown / revoked / replayed.
      // Tear down local state so the caller's onAuthStateChange
      // subscriber routes the user to sign-in.
      this.#session = null;
      this.#clearAutoRefresh();
      if (this.#storage) {
        try {
          await this.#storage.removeItem(SESSION_STORAGE_KEY);
        } catch {
          // ignore
        }
      }
      this.#emit("SIGNED_OUT", null);
      return {
        data: null,
        error: new BasinError(
          "refresh_failed",
          "Refresh token was rejected; signed out locally",
          401,
        ),
      };
    }
    const parsed = await unwrapAuthBody(res);
    if (parsed.kind === "error") return { data: null, error: parsed.error };
    if (parsed.kind === "mfa") {
      return {
        data: null,
        error: new BasinError(
          "invalid_response",
          "Refresh returned an MFA envelope; this is a cloud bug",
          res.status,
        ),
      };
    }
    // Partial-shape merge: keep the existing user; splice in the new
    // tokens + expiry from the cloud's response.
    const body = parsed.body as {
      access_token?: string;
      refresh_token?: string;
      expires_at?: string | number;
    };
    if (!body.access_token || !body.refresh_token || body.expires_at == null) {
      return {
        data: null,
        error: new BasinError(
          "invalid_response",
          "Refresh response is missing access_token, refresh_token, or expires_at",
          res.status,
        ),
      };
    }
    const expiresAt =
      typeof body.expires_at === "string"
        ? Math.floor(new Date(body.expires_at).getTime() / 1000)
        : body.expires_at;
    const next: AuthSession = {
      access_token: body.access_token,
      refresh_token: body.refresh_token,
      token_type: "bearer",
      expires_at: expiresAt,
      user: current.user,
    };
    this.#session = next;
    if (this.#storage) {
      try {
        await this.#storage.setItem(SESSION_STORAGE_KEY, JSON.stringify(next));
      } catch {
        // ignore storage errors
      }
    }
    this.#scheduleAutoRefresh(next);
    this.#emit("TOKEN_REFRESHED", next);
    return { data: next, error: null };
  }

  /** Internal — fired by the upcoming refresh/sign-in implementations. */
  _emit(event: AuthChangeEvent, session: AuthSession | null): void {
    this.#emit(event, session);
  }

  // ─── Private helpers ───────────────────────────────────────────────

  #emit(event: AuthChangeEvent, session: AuthSession | null): void {
    for (const cb of this.#listeners) cb(event, session);
  }

  #hydrateFromStorage(): void {
    if (!this.#storage) return;
    let result: string | null | Promise<string | null>;
    try {
      result = this.#storage.getItem(SESSION_STORAGE_KEY);
    } catch {
      return; // adapter threw — treat as no session.
    }
    if (result instanceof Promise) {
      void result.then(
        (raw) => this.#applyHydratedSession(raw),
        () => {
          // adapter rejected — treat as no session.
        },
      );
    } else {
      this.#applyHydratedSession(result);
    }
  }

  #applyHydratedSession(raw: string | null): void {
    if (!raw) {
      this.#emit("INITIAL_SESSION", null);
      return;
    }
    let parsed: AuthSession;
    try {
      parsed = JSON.parse(raw) as AuthSession;
    } catch {
      // Corrupt cache. Best-effort cleanup so the next load is clean.
      try {
        void this.#storage?.removeItem(SESSION_STORAGE_KEY);
      } catch {
        // ignore
      }
      this.#emit("INITIAL_SESSION", null);
      return;
    }
    // Defensive shape-check + expiry guard. expires_at is seconds-since-
    // epoch; auto-refresh (future firing) will rotate the token before
    // it expires, but on cold-start we ignore an already-expired one.
    if (
      !parsed?.access_token ||
      !parsed?.refresh_token ||
      typeof parsed?.expires_at !== "number" ||
      !parsed?.user?.id
    ) {
      this.#emit("INITIAL_SESSION", null);
      return;
    }
    if (parsed.expires_at * 1000 <= Date.now()) {
      // Expired — leave it in storage for refreshSession to consume
      // (refresh tokens often outlive the access token by weeks). For
      // now treat the session as absent until refreshSession lands.
      this.#emit("INITIAL_SESSION", null);
      return;
    }
    this.#session = parsed;
    this.#scheduleAutoRefresh(parsed);
    this.#emit("INITIAL_SESSION", parsed);
  }

  async #persistSession(session: AuthSession): Promise<void> {
    this.#session = session;
    if (this.#storage) {
      try {
        await this.#storage.setItem(SESSION_STORAGE_KEY, JSON.stringify(session));
      } catch {
        // Storage write failures are non-fatal — the in-memory session
        // remains valid for the lifetime of the page / process.
      }
    }
    this.#scheduleAutoRefresh(session);
    this.#emit("SIGNED_IN", session);
  }

  /**
   * Schedule a single setTimeout that fires `refreshSession()` 60s
   * before the access-token expires. Cancels + replaces any
   * previously-armed timer — every session update reschedules.
   *
   * No-op when:
   *  - `setTimeout` isn't available (some sandboxed Workers strip it)
   *  - the session already expired (delta would be ≤ 0)
   *  - the buffered delta lands past the 32-bit setTimeout ceiling
   *    (~24.8 days); the access token's lifetime is on the order of
   *    minutes-to-an-hour in basin-cloud today, so this only matters
   *    if the cloud ever issues multi-week ATs.
   */
  #scheduleAutoRefresh(session: AuthSession): void {
    this.#clearAutoRefresh();
    if (typeof setTimeout !== "function") return;
    const nowSec = Math.floor(Date.now() / 1000);
    const deltaSec = session.expires_at - nowSec - REFRESH_BUFFER_SECONDS;
    if (deltaSec <= 0) {
      // Already inside the refresh window. Fire on next tick rather
      // than synchronously — callers shouldn't see a refresh storm
      // mid-construction.
      this.#refreshTimer = setTimeout(() => {
        this.#refreshTimer = null;
        void this.refreshSession();
      }, 0);
      return;
    }
    const ms = deltaSec * 1000;
    // setTimeout's max delay is INT32_MAX (~24.8 days). Beyond that
    // the timer fires immediately; skip scheduling in that case —
    // not a real-world basin-cloud configuration.
    if (ms > 0x7fffffff) return;
    this.#refreshTimer = setTimeout(() => {
      this.#refreshTimer = null;
      void this.refreshSession();
    }, ms);
  }

  #clearAutoRefresh(): void {
    if (this.#refreshTimer !== null) {
      clearTimeout(this.#refreshTimer);
      this.#refreshTimer = null;
    }
  }
}

// ── module-level helpers (pure, testable) ──────────────────────────

/**
 * The shape the cloud emits on a successful login (inside the
 * `{data, error}` envelope): {user, session, org?}. Mirror of
 * basin-cloud's `handlers.loginResponse`.
 */
interface CloudLoginPayload {
  user?: {
    id: string;
    email: string | null;
    email_verified?: boolean;
    name?: string | null;
    created_at?: string;
    updated_at?: string;
    [k: string]: unknown;
  };
  session?: {
    access_token: string;
    refresh_token: string;
    expires_at: string; // RFC3339
    session_id?: string;
  };
  org?: unknown;
  requires_totp?: boolean;
  partial_token?: string;
}

type ParsedAuthResp =
  | { kind: "ok"; body: CloudLoginPayload }
  | { kind: "mfa"; partialToken: string }
  | { kind: "error"; error: BasinError };

/**
 * Reads + unwraps the cloud's `{data, error}` envelope from an auth
 * response. Returns one of three shapes — caller picks the branch.
 *
 *  - `ok`    : envelope `data` decoded into a CloudLoginPayload
 *  - `mfa`   : login completed step 1 but needs `auth.mfa.verify()`
 *  - `error` : cloud or transport surfaced an error
 */
async function unwrapAuthBody(res: Response): Promise<ParsedAuthResp> {
  let raw: unknown;
  try {
    raw = await res.json();
  } catch {
    return {
      kind: "error",
      error: new BasinError(
        "invalid_response",
        `auth response was not JSON (HTTP ${res.status})`,
        res.status,
      ),
    };
  }

  // Envelope unwrap — cloud wraps every body as {data, error}.
  // Tolerant: if the body isn't wrapped, treat the body itself as
  // data (matches the basin-cli unwrap heuristic).
  const body = (raw ?? {}) as Record<string, unknown>;
  const wrapped = "data" in body || "error" in body;
  const payload = (wrapped ? body.data : body) as CloudLoginPayload | null;
  const envelopeError = wrapped ? (body.error as { code?: string; message?: string } | null | undefined) : null;

  if (!res.ok) {
    return {
      kind: "error",
      error: new BasinError(
        envelopeError?.code ?? errorCodeForStatus(res.status),
        envelopeError?.message ?? `auth failed (HTTP ${res.status})`,
        res.status,
        envelopeError,
      ),
    };
  }

  if (payload?.requires_totp) {
    return {
      kind: "mfa",
      partialToken: payload.partial_token ?? "",
    };
  }
  return { kind: "ok", body: (payload ?? {}) as CloudLoginPayload };
}

/**
 * Map basin-cloud's `{user, session}` payload onto the SDK's flatter
 * AuthSession shape. Returns null when required fields are missing.
 * Side-effect-free; called from signInWithPassword + signUp +
 * refreshSession (future firings).
 */
function mapLoginToSession(payload: CloudLoginPayload): AuthSession | null {
  if (!payload?.user || !payload?.session) return null;
  const u = payload.user;
  const s = payload.session;
  if (!s.access_token || !s.refresh_token || !s.expires_at) return null;
  if (!u.id) return null;

  return {
    access_token: s.access_token,
    refresh_token: s.refresh_token,
    token_type: "bearer",
    expires_at: Math.floor(new Date(s.expires_at).getTime() / 1000),
    user: {
      id: u.id,
      email: u.email ?? null,
      email_confirmed_at: u.email_verified ? (u.updated_at ?? null) : null,
      phone: null,
      created_at: u.created_at ?? "",
      updated_at: u.updated_at ?? "",
      app_metadata: {},
      user_metadata: {},
    },
  };
}

/**
 * Map an HTTP status to a default code when the cloud's typed envelope
 * is absent. Only used as a fallback — real errors carry the cloud's
 * own `code` (e.g. `invalid_credentials`, `rate_limited`).
 */
function errorCodeForStatus(status: number): string {
  if (status === 401) return "unauthorized";
  if (status === 403) return "forbidden";
  if (status === 404) return "not_found";
  if (status === 409) return "conflict";
  if (status === 429) return "rate_limited";
  if (status >= 500) return "internal";
  return "invalid_request";
}

/** Render a non-Error thrown value into a short message. */
function networkErrorMessage(e: unknown): string {
  if (e instanceof Error && e.message) return e.message;
  return "network error reaching auth endpoint";
}
