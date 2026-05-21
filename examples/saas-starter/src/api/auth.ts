/**
 * Auth helpers wrapping @basin/basin-js auth methods.
 *
 * basin-auth endpoints (HTTP, per docs/tutorial.md):
 *   POST /auth/v1/signup  — { email, password }
 *   POST /auth/v1/signin  — { email, password }
 *   POST /auth/v1/signout — { refresh_token }
 *
 * The basin-js client wraps these into:
 *   basin.auth.signUp({ email, password })
 *   basin.auth.signInWithPassword({ email, password })
 *   basin.auth.signOut()
 *
 * After sign-in the JWT is stored in localStorage ("basin-session") and
 * forwarded automatically on every REST request.  The JWT sub claim
 * equals the user's id in the `users` table.
 *
 * OAuth (GitHub/Google) is not yet implemented in basin-auth v1 — the
 * basin-js stub throws E_NOT_IMPLEMENTED.  ADR 0020 plans OAuth for v2.
 * This module provides a placeholder signInWithOAuth that informs the UI.
 */

import { basin } from "../lib/basin";

export interface AuthUser {
  id: string;
  email: string;
}

export interface AuthSession {
  accessToken: string;
  refreshToken?: string;
}

// ---------------------------------------------------------------------------
// Sign up
// ---------------------------------------------------------------------------

export async function signUp(
  email: string,
  password: string
): Promise<{ user: AuthUser | null; error: string | null }> {
  const { data, error } = await basin.auth.signUp({ email, password });
  if (error || !data) {
    return { user: null, error: error?.message ?? "Sign-up failed" };
  }
  return {
    user: { id: data.user.id, email: data.user.email ?? email },
    error: null,
  };
}

// ---------------------------------------------------------------------------
// Sign in
// ---------------------------------------------------------------------------

export async function signIn(
  email: string,
  password: string
): Promise<{ user: AuthUser | null; error: string | null }> {
  const { data, error } = await basin.auth.signInWithPassword({
    email,
    password,
  });
  if (error || !data) {
    return { user: null, error: error?.message ?? "Sign-in failed" };
  }

  // Upsert a profile row in the `users` table so the rest of the app can
  // JOIN against it.  We do this via the REST API (which enforces RLS, so
  // the user can only upsert their own row).
  await basin
    .from("users")
    .insert({ id: data.user.id, email: data.user.email ?? email });

  return {
    user: { id: data.user.id, email: data.user.email ?? email },
    error: null,
  };
}

// ---------------------------------------------------------------------------
// Sign out
// ---------------------------------------------------------------------------

export async function signOut(): Promise<void> {
  await basin.auth.signOut();
}

// ---------------------------------------------------------------------------
// OAuth placeholder (not yet implemented in basin-auth v1)
// ---------------------------------------------------------------------------

/**
 * signInWithOAuth is a placeholder.
 *
 * basin-js v0.1 stubs basin.auth.signInWithOAuth() with E_NOT_IMPLEMENTED
 * (per basin-js-design.md § 10 "What's deferred to v0.2").
 * OAuth providers land in basin-auth v2 (ADR 0020).
 */
export function signInWithOAuth(_provider: "github" | "google"): never {
  throw new Error(
    "OAuth sign-in is not yet implemented in basin-auth v1. " +
      "See ADR 0020 — it ships in basin-auth v2."
  );
}
