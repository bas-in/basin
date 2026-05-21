/**
 * Basin client singleton.
 *
 * @basin/basin-js mirrors the Supabase JS client API exactly:
 *   createClient(url, anonKey)
 *
 * The anon key is a JWT that grants "anon" role access.  In development you
 * can leave it empty if basin-auth is not configured — the REST layer will
 * accept unauthenticated requests for tables that have no RLS policies.
 *
 * Once a user signs in, basin-js stores the JWT in localStorage and attaches
 * it to every subsequent request automatically.
 *
 * Package status: @basin/basin-js is the npm registry package name per the
 * basin-js design spec (docs/basin-js-design.md).  The JSR registry name is
 * @bas-in/basin-js.  Both resolve to the same code.
 *
 * NOTE: @basin/basin-js is not yet published to npm as of May 2026.
 * The import below will resolve once the package ships.  Until then you can
 * substitute a compatible client by implementing the same interface — see
 * src/lib/basin-compat.ts for a minimal fetch-based shim used in tests.
 */
import { createClient } from "@basin/basin-js";

const BASIN_URL =
  (typeof __BASIN_URL__ !== "undefined" ? __BASIN_URL__ : null) ??
  import.meta.env?.VITE_BASIN_URL ??
  "http://localhost:5432";

const BASIN_ANON_KEY =
  (typeof __BASIN_ANON_KEY__ !== "undefined" ? __BASIN_ANON_KEY__ : null) ??
  import.meta.env?.VITE_BASIN_ANON_KEY ??
  "";

export const basin = createClient(BASIN_URL, BASIN_ANON_KEY);

// Re-export the type helper so callers can do:
//   import { basin } from '@/lib/basin'
export type { Session, User } from "@basin/basin-js";

// Declare build-time constants injected by Vite (vite.config.ts defines them)
declare const __BASIN_URL__: string | undefined;
declare const __BASIN_ANON_KEY__: string | undefined;
