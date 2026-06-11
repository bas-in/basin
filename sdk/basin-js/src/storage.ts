/**
 * `.storage` — bindings for `/storage/v1/*` (ADR 0021 surface).
 *
 * Route sources (verified, crates/basin-rest/src/server.rs:373-424; handlers
 * in crates/basin-rest/src/routes/storage.rs and storage_sign.rs):
 *
 * - `POST   /storage/v1/bucket`                                  create bucket
 * - `GET    /storage/v1/bucket/:name`                            bucket metadata
 * - `DELETE /storage/v1/bucket/:name`                            delete + purge
 * - `POST   /storage/v1/object/:bucket/*path`                    upload (JWT)
 * - `GET    /storage/v1/object/:bucket/*path`                    download (JWT)
 * - `DELETE /storage/v1/object/:bucket/*path`                    delete (JWT)
 * - `POST   /storage/v1/object/list/:bucket`                     list (body: prefix/limit/offset)
 * - `DELETE /storage/v1/object/:bucket`                          bulk delete (body: prefixes[])
 * - `GET    /storage/v1/object/public/:project_id/:bucket/*path` public download (no JWT)
 * - `POST   /storage/v1/object/sign/upload/:bucket/*path`        mint signed URL (JWT)
 * - `GET    /storage/v1/object/sign/:project/:bucket/*path`      signed download (no JWT)
 */

import { BasinApiError } from "./errors.js";
import type { ClientContext } from "./http.js";
import { encodeObjectPath, requestJson, requestRaw } from "./http.js";
import type { Bucket, SignedUrl, StorageObject } from "./types.js";

export interface CreateBucketOptions {
  public?: boolean;
  fileSizeLimit?: number;
  allowedMimeTypes?: string[];
}

export interface DownloadResult {
  data: ArrayBuffer;
  contentType: string | null;
  etag: string | null;
}

export class StorageClient {
  #ctx: ClientContext;

  constructor(ctx: ClientContext) {
    this.#ctx = ctx;
  }

  /** `POST /storage/v1/bucket` (201). */
  async createBucket(
    name: string,
    opts: CreateBucketOptions = {},
  ): Promise<Bucket> {
    return requestJson(this.#ctx, "POST", "/storage/v1/bucket", {
      body: {
        name,
        public: opts.public ?? false,
        file_size_limit: opts.fileSizeLimit ?? null,
        allowed_mime_types: opts.allowedMimeTypes ?? [],
      },
    });
  }

  /** `GET /storage/v1/bucket/:name`. */
  async getBucket(name: string): Promise<Bucket> {
    return requestJson(
      this.#ctx,
      "GET",
      `/storage/v1/bucket/${encodeURIComponent(name)}`,
    );
  }

  /** `DELETE /storage/v1/bucket/:name` — also purges object bytes. */
  async deleteBucket(
    name: string,
  ): Promise<{ message: string; objects_purged: number }> {
    return requestJson(
      this.#ctx,
      "DELETE",
      `/storage/v1/bucket/${encodeURIComponent(name)}`,
    );
  }

  /** Object operations scoped to one bucket. */
  from(bucket: string): StorageBucketClient {
    return new StorageBucketClient(this.#ctx, bucket);
  }
}

export class StorageBucketClient {
  #ctx: ClientContext;
  #bucket: string;

  constructor(ctx: ClientContext, bucket: string) {
    this.#ctx = ctx;
    this.#bucket = bucket;
  }

  #objectPath(path: string): string {
    return `/storage/v1/object/${encodeURIComponent(this.#bucket)}/${encodeObjectPath(path)}`;
  }

  /**
   * `POST /storage/v1/object/:bucket/*path` — upload. MIME is taken from the
   * `Content-Type` header (extensions are never trusted server-side).
   */
  async upload(
    path: string,
    body: BodyInit,
    opts: { contentType?: string } = {},
  ): Promise<StorageObject> {
    const headers: Record<string, string> = {};
    if (opts.contentType !== undefined) headers["content-type"] = opts.contentType;
    return requestJson(this.#ctx, "POST", this.#objectPath(path), {
      rawBody: body,
      headers,
    });
  }

  /** `GET /storage/v1/object/:bucket/*path` — authenticated download. */
  async download(path: string): Promise<DownloadResult> {
    const res = await requestRaw(this.#ctx, "GET", this.#objectPath(path));
    return {
      data: await res.arrayBuffer(),
      contentType: res.headers.get("content-type"),
      etag: res.headers.get("etag"),
    };
  }

  /** `DELETE /storage/v1/object/:bucket/*path` — single delete. */
  async remove(path: string): Promise<{ message: string }> {
    return requestJson(this.#ctx, "DELETE", this.#objectPath(path));
  }

  /** `POST /storage/v1/object/list/:bucket` — list with optional prefix. */
  async list(
    opts: { prefix?: string; limit?: number; offset?: number } = {},
  ): Promise<StorageObject[]> {
    return requestJson(
      this.#ctx,
      "POST",
      `/storage/v1/object/list/${encodeURIComponent(this.#bucket)}`,
      {
        body: {
          prefix: opts.prefix ?? null,
          limit: opts.limit ?? null,
          offset: opts.offset ?? null,
        },
      },
    );
  }

  /**
   * `DELETE /storage/v1/object/:bucket` with `{ prefixes: [...] }` —
   * bulk delete every object matching any prefix. RLS-denied objects are
   * skipped server-side (not counted in `deleted`).
   */
  async removeByPrefixes(prefixes: string[]): Promise<{ deleted: number }> {
    return requestJson(
      this.#ctx,
      "DELETE",
      `/storage/v1/object/${encodeURIComponent(this.#bucket)}`,
      { body: { prefixes } },
    );
  }

  /**
   * Build the no-JWT public URL:
   * `GET /storage/v1/object/public/:project_id/:bucket/*path`.
   * Requires a known project id; only works for buckets with `public: true`.
   */
  getPublicUrl(path: string): string {
    const project = this.#ctx.projectId();
    if (project === undefined) {
      throw new BasinApiError(
        "E_INVALID_REQUEST",
        "public URLs require a project id (createClient options.projectId or a JWT key/session)",
        0,
      );
    }
    return `${this.#ctx.baseUrl}/storage/v1/object/public/${project}/${encodeURIComponent(this.#bucket)}/${encodeObjectPath(path)}`;
  }

  /**
   * `POST /storage/v1/object/sign/upload/:bucket/*path` — mint a time-boxed
   * signed *download* URL (despite the `upload` literal, which only
   * disambiguates the axum route — see storage_sign.rs). TTL is capped at 7
   * days server-side; default 3600s.
   */
  async createSignedUrl(
    path: string,
    opts: { expiresIn?: number } = {},
  ): Promise<SignedUrl & { absoluteUrl: string }> {
    const signed = await requestJson<SignedUrl>(
      this.#ctx,
      "POST",
      `/storage/v1/object/sign/upload/${encodeURIComponent(this.#bucket)}/${encodeObjectPath(path)}`,
      { body: { expires_in: opts.expiresIn ?? 3600 } },
    );
    return { ...signed, absoluteUrl: this.#ctx.baseUrl + signed.signedUrl };
  }
}
