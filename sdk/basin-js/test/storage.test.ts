import { describe, expect, it } from "vitest";

import { createClient } from "../src/index.js";
import { FetchMock } from "./helpers.js";

const URL_BASE = "http://basin.test";
const PROJECT = "01JTESTPROJECT0000000000";

function client(fetch: FetchMock) {
  return createClient(URL_BASE, "key", { projectId: PROJECT, fetch: fetch.impl });
}

describe("storage", () => {
  it("createBucket POSTs /storage/v1/bucket with the documented body", async () => {
    const fetch = new FetchMock().queueJson(
      {
        id: "b1", name: "avatars", public: true, file_size_limit: null,
        allowed_mime_types: ["image/png"], created_at: "t", updated_at: "t",
      },
      201,
    );
    const bucket = await client(fetch).storage.createBucket("avatars", {
      public: true,
      allowedMimeTypes: ["image/png"],
    });
    expect(bucket.name).toBe("avatars");
    expect(fetch.last.url).toBe(`${URL_BASE}/storage/v1/bucket`);
    expect(JSON.parse(fetch.last.body!)).toEqual({
      name: "avatars",
      public: true,
      file_size_limit: null,
      allowed_mime_types: ["image/png"],
    });
  });

  it("getBucket / deleteBucket bind /storage/v1/bucket/:name", async () => {
    const fetch = new FetchMock()
      .queueJson({ id: "b1", name: "avatars", public: false, file_size_limit: null, allowed_mime_types: [], created_at: "t", updated_at: "t" })
      .queueJson({ message: "bucket deleted", objects_purged: 3 });
    const c = client(fetch);
    await c.storage.getBucket("avatars");
    expect(fetch.calls[0]!.method).toBe("GET");
    expect(fetch.calls[0]!.url).toBe(`${URL_BASE}/storage/v1/bucket/avatars`);

    const res = await c.storage.deleteBucket("avatars");
    expect(res.objects_purged).toBe(3);
    expect(fetch.calls[1]!.method).toBe("DELETE");
  });

  it("upload POSTs raw bytes with content-type to /storage/v1/object/:bucket/*path", async () => {
    const fetch = new FetchMock().queueJson({
      id: "o1", bucket_id: "b1", path: "a/b.png", size: 3, mime_type: "image/png",
      metadata: null, owner: null, etag: "abc", created_at: "t", updated_at: "t",
    });
    const obj = await client(fetch)
      .storage.from("avatars")
      .upload("a/b.png", new Uint8Array([1, 2, 3]), { contentType: "image/png" });
    expect(obj.etag).toBe("abc");
    expect(fetch.last.url).toBe(`${URL_BASE}/storage/v1/object/avatars/a/b.png`);
    expect(fetch.last.method).toBe("POST");
    expect(fetch.last.headers["content-type"]).toBe("image/png");
  });

  it("download GETs the object and returns bytes + headers", async () => {
    const fetch = new FetchMock().queue(
      new Response(new Uint8Array([9, 9]), {
        status: 200,
        headers: { "content-type": "image/png", etag: "abc" },
      }),
    );
    const res = await client(fetch).storage.from("avatars").download("a/b.png");
    expect(fetch.last.method).toBe("GET");
    expect(new Uint8Array(res.data)).toEqual(new Uint8Array([9, 9]));
    expect(res.contentType).toBe("image/png");
    expect(res.etag).toBe("abc");
  });

  it("list POSTs prefix/limit/offset to /storage/v1/object/list/:bucket", async () => {
    const fetch = new FetchMock().queueJson([]);
    await client(fetch).storage.from("avatars").list({ prefix: "a/", limit: 10 });
    expect(fetch.last.url).toBe(`${URL_BASE}/storage/v1/object/list/avatars`);
    expect(fetch.last.method).toBe("POST");
    expect(JSON.parse(fetch.last.body!)).toEqual({ prefix: "a/", limit: 10, offset: null });
  });

  it("removeByPrefixes sends DELETE with a prefixes[] body", async () => {
    const fetch = new FetchMock().queueJson({ deleted: 2 });
    const res = await client(fetch).storage.from("avatars").removeByPrefixes(["a/"]);
    expect(res.deleted).toBe(2);
    expect(fetch.last.url).toBe(`${URL_BASE}/storage/v1/object/avatars`);
    expect(fetch.last.method).toBe("DELETE");
    expect(JSON.parse(fetch.last.body!)).toEqual({ prefixes: ["a/"] });
  });

  it("getPublicUrl embeds the project id (no-JWT route shape)", () => {
    const url = client(new FetchMock()).storage.from("avatars").getPublicUrl("a/b.png");
    expect(url).toBe(
      `${URL_BASE}/storage/v1/object/public/${PROJECT}/avatars/a/b.png`,
    );
  });

  it("createSignedUrl POSTs sign/upload and returns the camelCase body", async () => {
    const fetch = new FetchMock().queueJson({
      signedUrl: `/storage/v1/object/sign/${PROJECT}/avatars/a/b.png?token=aa&expires=99`,
      expiresAt: "2026-01-01T00:00:00Z",
    });
    const signed = await client(fetch)
      .storage.from("avatars")
      .createSignedUrl("a/b.png", { expiresIn: 600 });
    expect(fetch.last.url).toBe(
      `${URL_BASE}/storage/v1/object/sign/upload/avatars/a/b.png`,
    );
    expect(JSON.parse(fetch.last.body!)).toEqual({ expires_in: 600 });
    expect(signed.expiresAt).toBe("2026-01-01T00:00:00Z");
    expect(signed.absoluteUrl).toBe(
      `${URL_BASE}/storage/v1/object/sign/${PROJECT}/avatars/a/b.png?token=aa&expires=99`,
    );
  });
});
