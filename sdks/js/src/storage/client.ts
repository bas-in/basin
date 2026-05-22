/**
 * StorageClient — `basin.storage.from(bucket)` returns a `StorageBucket`
 * with `.upload`, `.download`, `.list`, `.remove`, `.createSignedUrl`,
 * `.getPublicUrl`.
 *
 * Engine routes (final — basin-engine Phase 5.17.D, ADR 0021, fixes #55):
 *   POST   /storage/v1/object/:bucket/*path                        → upload
 *   GET    /storage/v1/object/:bucket/*path                        → download
 *   POST   /storage/v1/object/list/:bucket                         → list
 *   DELETE /storage/v1/object/:bucket                              → remove (bulk)
 *   POST   /storage/v1/object/sign/upload/:bucket/*path            → createSignedUrl
 *   GET    /storage/v1/object/public/:project_id/:bucket/*path     → getPublicUrl
 *
 * `deps.url` is resolved as `${engineBase}/storage/v1` by
 * `createClient`, so every method appends `/object/...` to it.
 */

import { BasinError } from "../errors.js";
import type { AuthClient } from "../auth/client.js";

interface StorageDeps {
  url: string;
  headers: Record<string, string>;
  fetch: typeof fetch;
  auth: AuthClient;
}

/** File metadata returned by `list()`. Shape mirrors Supabase storage-js. */
export interface ObjectInfo {
  name: string;
  size: number;
  contentType: string;
  created_at: string;
  updated_at: string;
  metadata?: Record<string, unknown>;
}

/** Sort options for `list()`. */
export interface ListSortBy {
  column: "name" | "created_at" | "updated_at";
  order: "asc" | "desc";
}

/** Result shape for `createSignedUrl`. */
export interface SignedUrlResult {
  signedUrl: string;
  expiresAt?: string;
}

/** Threshold above which `.upload()` should delegate to multipart. */
const MULTIPART_THRESHOLD = 5 * 1024 * 1024;

export class StorageClient {
  #deps: StorageDeps;

  constructor(deps: StorageDeps) {
    this.#deps = deps;
  }

  from(bucket: string): StorageBucket {
    return new StorageBucket(bucket, this.#deps);
  }
}

export class StorageBucket {
  readonly #bucket: string;
  readonly #deps: StorageDeps;

  constructor(bucket: string, deps: StorageDeps) {
    this.#bucket = bucket;
    this.#deps = deps;
  }

  // ─── Internal helpers ──────────────────────────────────────────────

  /** Build request headers, injecting `Authorization` from session if present. */
  #headers(extra?: Record<string, string>): Record<string, string> {
    const session = this.#deps.auth.getSession();
    const auth = session?.access_token
      ? { Authorization: `Bearer ${session.access_token}` }
      : {};
    return { ...this.#deps.headers, ...auth, ...extra };
  }

  /** Map an HTTP response to a typed BasinError. */
  #httpError(status: number, details?: unknown): BasinError {
    if (status === 401 || status === 403) {
      return new BasinError("unauthorized", "unauthorized", status, details);
    }
    if (status === 404) {
      return new BasinError("not_found", "object not found", status, details);
    }
    return new BasinError(
      "internal",
      `storage request failed (HTTP ${status})`,
      status,
      details,
    );
  }

  // ─── Public methods ────────────────────────────────────────────────

  /**
   * Upload a file to `{bucket}/{path}`.
   *
   * POST `/storage/v1/object/${bucket}/${path}`
   * Body: raw bytes / Blob / string.
   * Content-Type: from `opts.contentType`, or sniffed from `Blob.type`.
   */
  async upload(
    path: string,
    file: Blob | ArrayBuffer | Uint8Array | string,
    opts?: { contentType?: string; upsert?: boolean },
  ): Promise<{ data: { path: string } | null; error: BasinError | null }> {
    const url = `${this.#deps.url}/object/${encodeURIComponent(this.#bucket)}/${encodePathSegments(path)}`;

    let contentType = opts?.contentType;
    if (!contentType && file instanceof Blob && file.type) {
      contentType = file.type;
    }
    contentType = contentType ?? "application/octet-stream";

    const headers = this.#headers({ "Content-Type": contentType });
    if (opts?.upsert) {
      headers["x-upsert"] = "true";
    }

    let res: Response;
    try {
      res = await this.#deps.fetch(url, {
        method: "POST",
        headers,
        body: file as BodyInit,
      });
    } catch (e) {
      return {
        data: null,
        error: new BasinError(
          "network",
          e instanceof Error ? e.message : "network error during upload",
        ),
      };
    }

    if (!res.ok) {
      let details: unknown;
      try {
        details = await res.json();
      } catch {
        /* ignore */
      }
      return { data: null, error: this.#httpError(res.status, details) };
    }

    return { data: { path }, error: null };
  }

  /**
   * Download `{bucket}/{path}` as a Blob.
   *
   * GET `/storage/v1/object/${bucket}/${path}`
   */
  async download(
    path: string,
  ): Promise<{ data: Blob | null; error: BasinError | null }> {
    const url = `${this.#deps.url}/object/${encodeURIComponent(this.#bucket)}/${encodePathSegments(path)}`;

    let res: Response;
    try {
      res = await this.#deps.fetch(url, {
        method: "GET",
        headers: this.#headers(),
      });
    } catch (e) {
      return {
        data: null,
        error: new BasinError(
          "network",
          e instanceof Error ? e.message : "network error during download",
        ),
      };
    }

    if (!res.ok) {
      let details: unknown;
      try {
        details = await res.json();
      } catch {
        /* ignore */
      }
      return { data: null, error: this.#httpError(res.status, details) };
    }

    const blob = await res.blob();
    return { data: blob, error: null };
  }

  /**
   * List objects in the bucket, optionally filtered by `prefix`.
   *
   * POST `/storage/v1/object/list/${bucket}` with `{prefix, limit, offset, sortBy}`.
   * Returns `ObjectInfo[]`; empty result returns `[]` not `null`.
   */
  async list(
    prefix?: string,
    opts?: { limit?: number; offset?: number; sortBy?: ListSortBy },
  ): Promise<{ data: ObjectInfo[] | null; error: BasinError | null }> {
    const url = `${this.#deps.url}/object/list/${encodeURIComponent(this.#bucket)}`;

    const body: Record<string, unknown> = { prefix: prefix ?? "" };
    if (opts?.limit !== undefined) body.limit = opts.limit;
    if (opts?.offset !== undefined) body.offset = opts.offset;
    if (opts?.sortBy !== undefined) body.sortBy = opts.sortBy;

    let res: Response;
    try {
      res = await this.#deps.fetch(url, {
        method: "POST",
        headers: this.#headers({ "Content-Type": "application/json" }),
        body: JSON.stringify(body),
      });
    } catch (e) {
      return {
        data: null,
        error: new BasinError(
          "network",
          e instanceof Error ? e.message : "network error during list",
        ),
      };
    }

    if (!res.ok) {
      let details: unknown;
      try {
        details = await res.json();
      } catch {
        /* ignore */
      }
      return { data: null, error: this.#httpError(res.status, details) };
    }

    let items: unknown;
    try {
      items = await res.json();
    } catch {
      return {
        data: null,
        error: new BasinError(
          "internal",
          "storage.list response was not JSON",
          res.status,
        ),
      };
    }

    return { data: (Array.isArray(items) ? items : []) as ObjectInfo[], error: null };
  }

  /**
   * Remove objects in bulk.
   *
   * DELETE `/storage/v1/object/${bucket}` with `{prefixes: paths}`.
   */
  async remove(
    paths: string[],
  ): Promise<{ data: { paths: string[] } | null; error: BasinError | null }> {
    const url = `${this.#deps.url}/object/${encodeURIComponent(this.#bucket)}`;

    let res: Response;
    try {
      res = await this.#deps.fetch(url, {
        method: "DELETE",
        headers: this.#headers({ "Content-Type": "application/json" }),
        body: JSON.stringify({ prefixes: paths }),
      });
    } catch (e) {
      return {
        data: null,
        error: new BasinError(
          "network",
          e instanceof Error ? e.message : "network error during remove",
        ),
      };
    }

    if (!res.ok) {
      let details: unknown;
      try {
        details = await res.json();
      } catch {
        /* ignore */
      }
      return { data: null, error: this.#httpError(res.status, details) };
    }

    return { data: { paths }, error: null };
  }

  /**
   * Mint a short-lived signed URL for `{bucket}/{path}`.
   *
   * POST `/storage/v1/object/sign/upload/${bucket}/${path}` with `{expiresIn}`.
   * Returns `{ signedUrl: string, expiresAt?: string }`.
   *
   * NOTE: The `upload` literal segment is required to disambiguate this route
   * from the signed-download path in axum's router (Phase 5.17.D, fixes #55).
   * The old path `/object/sign/:bucket/*path` is no longer valid.
   *
   * Returns `invalid_request` immediately for negative `expiresIn`
   * (before any fetch).
   */
  async createSignedUrl(
    path: string,
    expiresIn: number,
  ): Promise<{ data: { signedUrl: string; expiresAt?: string } | null; error: BasinError | null }> {
    if (expiresIn < 0) {
      return {
        data: null,
        error: new BasinError(
          "invalid_request",
          "expiresIn must be a non-negative number of seconds",
        ),
      };
    }

    const url = `${this.#deps.url}/object/sign/upload/${encodeURIComponent(this.#bucket)}/${encodePathSegments(path)}`;

    let res: Response;
    try {
      res = await this.#deps.fetch(url, {
        method: "POST",
        headers: this.#headers({ "Content-Type": "application/json" }),
        body: JSON.stringify({ expiresIn }),
      });
    } catch (e) {
      return {
        data: null,
        error: new BasinError(
          "network",
          e instanceof Error ? e.message : "network error during createSignedUrl",
        ),
      };
    }

    if (!res.ok) {
      let details: unknown;
      try {
        details = await res.json();
      } catch {
        /* ignore */
      }
      return { data: null, error: this.#httpError(res.status, details) };
    }

    let body: unknown;
    try {
      body = await res.json();
    } catch {
      return {
        data: null,
        error: new BasinError(
          "internal",
          "storage.createSignedUrl response was not JSON",
          res.status,
        ),
      };
    }

    const result = body as { signedUrl?: string; signedURL?: string; expiresAt?: string };
    // Resolve relative signedUrl against storageUrl
    const rawUrl = result.signedUrl ?? result.signedURL ?? "";
    let signedUrl = rawUrl;
    if (rawUrl && !rawUrl.startsWith("http")) {
      const base = this.#deps.url.replace(/\/storage\/v1$/, "");
      signedUrl = `${base}${rawUrl.startsWith("/") ? "" : "/"}${rawUrl}`;
    }

    return {
      data: { signedUrl, ...(result.expiresAt ? { expiresAt: result.expiresAt } : {}) },
      error: null,
    };
  }

  /**
   * Construct a public URL for `{bucket}/{path}` synchronously. Only
   * valid for public buckets — for private buckets the URL will 401.
   *
   * No network call — pure URL composition so it stays callable
   * inside render-time React/Vue templates.
   *
   * Engine route: GET `/storage/v1/object/public/:project_id/:bucket/*path`
   * Pass `projectId` to target the correct project. Omitting it falls back
   * to the literal `"public"` segment (for testing / legacy compat only).
   */
  getPublicUrl(
    path: string,
    opts?: { projectId?: string },
  ): { data: { publicUrl: string } } {
    const projectSegment = opts?.projectId
      ? encodeURIComponent(opts.projectId)
      : "public";
    return {
      data: {
        publicUrl: `${this.#deps.url}/object/public/${projectSegment}/${encodeURIComponent(this.#bucket)}/${encodePathSegments(path)}`,
      },
    };
  }

  /**
   * Multipart upload for files > 5 MB.
   *
   * Returns `BasinError("not_implemented")` — basin-engine has
   * no presigned-multipart surface. Lands in basin v0.3+.
   */
  async uploadMultipart(
    _path: string,
    _file: Blob | ArrayBuffer | string,
    _opts?: {
      contentType?: string;
      upsert?: boolean;
      partSizeBytes?: number;
      onProgress?: (loaded: number, total: number) => void;
    },
  ): Promise<{ data: { path: string } | null; error: BasinError }> {
    return {
      data: null,
      error: new BasinError(
        "not_implemented",
        "storage.uploadMultipart (Storage) ships when the engine route lands — tracked in ROADMAP 0.3",
      ),
    };
  }

  /**
   * Resumable upload via TUS (tus.io).
   *
   * Returns `BasinError("not_implemented")` — basin-engine has
   * no TUS proxy surface. Lands in basin v0.3+.
   */
  async uploadResumable(
    _path: string,
    _file: Blob | ArrayBuffer | string,
    _opts?: {
      contentType?: string;
      upsert?: boolean;
      chunkSizeBytes?: number;
      onProgress?: (loaded: number, total: number) => void;
    },
  ): Promise<{ data: { path: string } | null; error: BasinError }> {
    return {
      data: null,
      error: new BasinError(
        "not_implemented",
        "storage.uploadResumable (Storage) ships when the engine route lands — tracked in ROADMAP 0.3",
      ),
    };
  }

  // ─── Static ────────────────────────────────────────────────────────

  /**
   * `MULTIPART_THRESHOLD` is exposed as a static constant for consumer
   * code that wants to branch on `.upload` vs `.uploadMultipart`
   * without hard-coding 5_242_880.
   */
  static MULTIPART_THRESHOLD = MULTIPART_THRESHOLD;
}

// ── module-level helpers (pure) ────────────────────────────────────

/**
 * Encode each path segment but preserve `/` separators so consumers
 * can pass `folder/sub/file.png` without manual encoding.
 */
function encodePathSegments(path: string): string {
  return path
    .split("/")
    .map((seg) => encodeURIComponent(seg))
    .join("/");
}
