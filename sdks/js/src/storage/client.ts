/**
 * StorageClient — `basin.storage.from(bucket)` returns a `StorageBucket`
 * with `.upload`, `.download`, `.list`, `.remove`, `.createSignedUrl`,
 * `.getPublicUrl`.
 *
 * Returns `BasinError("not_implemented")` from every async method
 * today — basin-engine v0.1 has no `/object/*` surface (verified
 * against `basin/crates/basin-rest/src/server.rs`). Lands when
 * basin-engine grows the storage object surface in v0.2+. The
 * synchronous `getPublicUrl` keeps its URL-construction body so
 * render-time templates compile; the URL won't resolve until the
 * engine ships the storage surface.
 *
 * The shape is preserved exactly — every signature matches what the
 * implemented version will return — so app code can be written today
 * against the stable shape and flip on with no breaking change.
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

/** Threshold above which `.upload()` should delegate to multipart. */
const MULTIPART_THRESHOLD = 5 * 1024 * 1024;

const STORAGE_NOT_IMPLEMENTED_MESSAGE =
  "Storage (network methods) ships when the engine route lands — tracked in ROADMAP 0.3";

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
  // Retained for the v0.2 swap — the bucket name + deps both feed the
  // URL builder in `getPublicUrl` and will feed the network methods
  // when they land.
  readonly #bucket: string;
  readonly #deps: StorageDeps;

  constructor(bucket: string, deps: StorageDeps) {
    this.#bucket = bucket;
    this.#deps = deps;
  }

  /**
   * Upload a file to `{bucket}/{path}`.
   *
   * Returns `BasinError("not_implemented")` today — basin-engine has
   * no `/object/*` routes. Lands when basin-engine grows the storage
   * surface in v0.2+.
   */
  async upload(
    _path: string,
    _file: Blob | ArrayBuffer | string,
    _opts?: { contentType?: string; upsert?: boolean },
  ): Promise<{ data: { path: string } | null; error: BasinError | null }> {
    return {
      data: null,
      error: new BasinError("not_implemented", STORAGE_NOT_IMPLEMENTED_MESSAGE),
    };
  }

  /**
   * Download `{bucket}/{path}` as a Blob.
   *
   * Returns `BasinError("not_implemented")` today — basin-engine has
   * no `/object/*` routes. Lands in basin v0.2+.
   */
  async download(
    _path: string,
  ): Promise<{ data: Blob | null; error: BasinError | null }> {
    return {
      data: null,
      error: new BasinError("not_implemented", STORAGE_NOT_IMPLEMENTED_MESSAGE),
    };
  }

  /**
   * List objects in the bucket, optionally filtered by `prefix`.
   *
   * Returns `BasinError("not_implemented")` today — basin-engine has
   * no `/object/list/*` route. Lands in basin v0.2+.
   */
  async list(
    _prefix?: string,
    _opts?: { limit?: number; offset?: number; sortBy?: ListSortBy },
  ): Promise<{ data: ObjectInfo[] | null; error: BasinError | null }> {
    return {
      data: null,
      error: new BasinError("not_implemented", STORAGE_NOT_IMPLEMENTED_MESSAGE),
    };
  }

  /**
   * Remove objects in bulk.
   *
   * Returns `BasinError("not_implemented")` today — basin-engine has
   * no `/object/remove` route. Lands in basin v0.2+.
   */
  async remove(
    _paths: string[],
  ): Promise<{ data: { paths: string[] } | null; error: BasinError | null }> {
    return {
      data: null,
      error: new BasinError("not_implemented", STORAGE_NOT_IMPLEMENTED_MESSAGE),
    };
  }

  /**
   * Mint a short-lived signed URL for `{bucket}/{path}`.
   *
   * Returns `BasinError("not_implemented")` today — basin-engine has
   * no `/object/sign/*` route. Lands in basin v0.2+.
   */
  async createSignedUrl(
    _path: string,
    _expiresIn: number,
  ): Promise<{ data: { signedUrl: string } | null; error: BasinError | null }> {
    return {
      data: null,
      error: new BasinError("not_implemented", STORAGE_NOT_IMPLEMENTED_MESSAGE),
    };
  }

  /**
   * Construct a public URL for `{bucket}/{path}` synchronously. Only
   * valid for public buckets — for private buckets the URL will 401.
   *
   * No network call — pure URL composition so it stays callable
   * inside render-time React/Vue templates. The URL won't actually
   * resolve until basin-engine ships the storage surface in v0.2+;
   * the construction is kept so consumer code that interpolates it
   * into `<img src>` etc. compiles today.
   */
  getPublicUrl(path: string): { data: { publicUrl: string } } {
    return {
      data: {
        publicUrl: `${this.#deps.url}/object/public/${encodeURIComponent(this.#bucket)}/${encodePathSegments(path)}`,
      },
    };
  }

  /**
   * Multipart upload for files > 5 MB.
   *
   * Returns `BasinError("not_implemented")` today — basin-engine has
   * no presigned-multipart surface. Lands in basin v0.2+.
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
   * Returns `BasinError("not_implemented")` today — basin-engine has
   * no TUS proxy surface. Lands in basin v0.2+.
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

  // ─── Internal ──────────────────────────────────────────────────────

  /**
   * `MULTIPART_THRESHOLD` is exposed as a static-ish constant for
   * consumer code that wants to branch on `.upload` vs
   * `.uploadMultipart` without hard-coding 5_242_880.
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
