/**
 * Avatar upload/download via basin-blob (Phase 5.17).
 *
 * basin-blob HTTP API (per ADR 0021 and docs/basin-js-design.md):
 *   POST   /storage/v1/object/:bucket/:path  — upload
 *   GET    /storage/v1/object/:bucket/:path  — download (RLS-gated)
 *   POST   /storage/v1/object/sign/:bucket/:path — mint signed URL
 *   DELETE /storage/v1/object/:bucket/:path  — delete
 *
 * NOTE: The basin-js @basin/basin-js package stubs storage.from(bucket)
 * methods.  Until the SDK publishes, this module calls the HTTP API directly
 * using fetch + the current session JWT.
 *
 * basin-blob shipped in Phase 5.17 (2026-05-20).  If your basin-server build
 * pre-dates Phase 5.17 these calls will return 404 — the functions below
 * handle that gracefully and return null.
 */

import { basin } from "../lib/basin";

const BASIN_URL =
  typeof __BASIN_URL__ !== "undefined"
    ? __BASIN_URL__
    : import.meta.env?.VITE_BASIN_URL ?? "http://localhost:5432";

declare const __BASIN_URL__: string | undefined;

function authHeader(): Record<string, string> {
  const session = basin.auth.session();
  if (!session) return {};
  // basin-js session object shape per basin-js-design.md § 2 "Auth"
  const token =
    "accessToken" in session
      ? (session as { accessToken: string }).accessToken
      : (session as unknown as { access_token: string }).access_token;
  return token ? { Authorization: `Bearer ${token}` } : {};
}

// ---------------------------------------------------------------------------
// Upload an avatar (org or user)
// ---------------------------------------------------------------------------

/**
 * Upload a file to the "avatars" bucket.
 *
 * @param path  Storage path within the bucket, e.g. "org-avatars/<orgId>/avatar.png"
 * @param file  The File or Blob to upload
 * @returns     The storage path on success, null on failure
 */
export async function uploadAvatar(
  path: string,
  file: File | Blob
): Promise<{ storagePath: string | null; error: string | null }> {
  try {
    const res = await fetch(`${BASIN_URL}/storage/v1/object/avatars/${path}`, {
      method: "POST",
      headers: {
        ...authHeader(),
        "Content-Type": file.type || "application/octet-stream",
      },
      body: file,
    });

    if (res.ok) {
      return { storagePath: `avatars/${path}`, error: null };
    }
    if (res.status === 404) {
      return {
        storagePath: null,
        error:
          "Storage API not available on this Basin build " +
          "(basin-blob requires Phase 5.17 or later).",
      };
    }
    const text = await res.text();
    return { storagePath: null, error: `Upload failed (${res.status}): ${text}` };
  } catch (err) {
    return {
      storagePath: null,
      error: err instanceof Error ? err.message : String(err),
    };
  }
}

// ---------------------------------------------------------------------------
// Get a time-limited signed URL for an avatar
// ---------------------------------------------------------------------------

/**
 * Mint a signed URL for a private avatar (valid for 1 hour).
 * Signed URLs grant access without a JWT — safe to put in an <img> src.
 *
 * POST /storage/v1/object/sign/avatars/<path>
 */
export async function getAvatarUrl(
  storagePath: string
): Promise<string | null> {
  // storagePath is e.g. "avatars/org-avatars/<id>/avatar.png"
  const pathWithinBucket = storagePath.replace(/^avatars\//, "");

  try {
    const res = await fetch(
      `${BASIN_URL}/storage/v1/object/sign/avatars/${pathWithinBucket}`,
      {
        method: "POST",
        headers: {
          ...authHeader(),
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ expiresIn: 3600 }),
      }
    );
    if (!res.ok) return null;
    const json = (await res.json()) as { signedUrl?: string };
    return json.signedUrl ?? null;
  } catch {
    return null;
  }
}
