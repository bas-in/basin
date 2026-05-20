import { describe, expect, it } from "vitest";
import { createClient } from "../client.js";
import { BasinError } from "../errors.js";

// ── helpers ─────────────────────────────────────────────────────────

function mockFetch(
  status: number,
  body: unknown,
  headers?: Record<string, string>,
): typeof fetch {
  return async () =>
    new Response(
      body instanceof Blob
        ? body
        : typeof body === "string"
          ? body
          : JSON.stringify(body),
      {
        status,
        headers: { "Content-Type": "application/json", ...headers },
      },
    );
}

function noFetch(): typeof fetch {
  return async () => {
    throw new Error("unexpected network call");
  };
}

function networkErrorFetch(): typeof fetch {
  return async () => {
    throw new TypeError("Failed to fetch");
  };
}

function basinWithFetch(fetcher: typeof fetch) {
  return createClient("https://api.basin.run", "anon", { fetch: fetcher, retry: false });
}

// ── upload ──────────────────────────────────────────────────────────

describe("storage.upload", () => {
  it("happy path with Blob — POST to correct URL, returns path", async () => {
    let capturedUrl = "";
    let capturedMethod = "";
    let capturedContentType = "";

    const basin = createClient("https://api.basin.run", "anon", {
      retry: false,
      fetch: async (input, init) => {
        capturedUrl = input.toString();
        capturedMethod = init?.method ?? "";
        capturedContentType = (init?.headers as Record<string, string>)?.["Content-Type"] ?? "";
        return new Response(null, { status: 200 });
      },
    });

    const file = new Blob(["png-bytes"], { type: "image/png" });
    const { data, error } = await basin.storage
      .from("avatars")
      .upload("u1/avatar.png", file);

    expect(error).toBeNull();
    expect(data).toEqual({ path: "u1/avatar.png" });
    expect(capturedMethod).toBe("POST");
    expect(capturedUrl).toContain("/object/avatars/u1/avatar.png");
    expect(capturedContentType).toBe("image/png");
  });

  it("happy path with ArrayBuffer — uses application/octet-stream by default", async () => {
    let capturedContentType = "";
    const basin = createClient("https://api.basin.run", "anon", {
      retry: false,
      fetch: async (_input, init) => {
        capturedContentType = (init?.headers as Record<string, string>)?.["Content-Type"] ?? "";
        return new Response(null, { status: 200 });
      },
    });

    const buf = new ArrayBuffer(8);
    const { data, error } = await basin.storage.from("docs").upload("file.bin", buf);

    expect(error).toBeNull();
    expect(data).toEqual({ path: "file.bin" });
    expect(capturedContentType).toBe("application/octet-stream");
  });

  it("happy path with string body", async () => {
    const basin = basinWithFetch(mockFetch(200, null));
    const { data, error } = await basin.storage.from("text").upload("readme.txt", "hello");
    expect(error).toBeNull();
    expect(data).toEqual({ path: "readme.txt" });
  });

  it("contentType opt overrides Blob.type", async () => {
    let capturedContentType = "";
    const basin = createClient("https://api.basin.run", "anon", {
      retry: false,
      fetch: async (_input, init) => {
        capturedContentType = (init?.headers as Record<string, string>)?.["Content-Type"] ?? "";
        return new Response(null, { status: 200 });
      },
    });

    const file = new Blob(["data"], { type: "image/png" });
    await basin.storage.from("bucket").upload("f.bin", file, { contentType: "application/pdf" });
    expect(capturedContentType).toBe("application/pdf");
  });

  it("upsert opt sets x-upsert header", async () => {
    let capturedUpsert = "";
    const basin = createClient("https://api.basin.run", "anon", {
      retry: false,
      fetch: async (_input, init) => {
        capturedUpsert = (init?.headers as Record<string, string>)?.["x-upsert"] ?? "";
        return new Response(null, { status: 200 });
      },
    });

    await basin.storage.from("bucket").upload("f.png", "data", { upsert: true });
    expect(capturedUpsert).toBe("true");
  });

  it("401 response → unauthorized error", async () => {
    const basin = basinWithFetch(mockFetch(401, { error: "Unauthorized" }));
    const { data, error } = await basin.storage.from("avatars").upload("x.png", "data");
    expect(data).toBeNull();
    expect(error).toBeInstanceOf(BasinError);
    expect(error?.code).toBe("unauthorized");
    expect(error?.status).toBe(401);
  });

  it("500 server error → internal error", async () => {
    const basin = basinWithFetch(mockFetch(500, { error: "Internal Server Error" }));
    const { data, error } = await basin.storage.from("avatars").upload("x.png", "data");
    expect(data).toBeNull();
    expect(error?.code).toBe("internal");
    expect(error?.status).toBe(500);
  });

  it("network failure → network error", async () => {
    const basin = basinWithFetch(networkErrorFetch());
    const { data, error } = await basin.storage.from("avatars").upload("x.png", "data");
    expect(data).toBeNull();
    expect(error?.code).toBe("network");
  });
});

// ── download ─────────────────────────────────────────────────────────

describe("storage.download", () => {
  it("happy path — GET to correct URL, returns Blob", async () => {
    let capturedUrl = "";
    let capturedMethod = "";

    const basin = createClient("https://api.basin.run", "anon", {
      retry: false,
      fetch: async (input, init) => {
        capturedUrl = input.toString();
        capturedMethod = init?.method ?? "";
        return new Response(new Blob(["png-bytes"]), { status: 200 });
      },
    });

    const { data, error } = await basin.storage.from("avatars").download("u1/avatar.png");

    expect(error).toBeNull();
    expect(data).toBeInstanceOf(Blob);
    expect(capturedMethod).toBe("GET");
    expect(capturedUrl).toContain("/object/avatars/u1/avatar.png");
  });

  it("404 → not_found error", async () => {
    const basin = basinWithFetch(mockFetch(404, { error: "Not Found" }));
    const { data, error } = await basin.storage.from("avatars").download("missing.png");
    expect(data).toBeNull();
    expect(error?.code).toBe("not_found");
    expect(error?.status).toBe(404);
  });

  it("401 private bucket without JWT → unauthorized error", async () => {
    const basin = basinWithFetch(mockFetch(401, { error: "Unauthorized" }));
    const { data, error } = await basin.storage.from("private").download("secret.pdf");
    expect(data).toBeNull();
    expect(error?.code).toBe("unauthorized");
  });

  it("500 server error → internal error", async () => {
    const basin = basinWithFetch(mockFetch(500, { error: "Internal Server Error" }));
    const { data, error } = await basin.storage.from("avatars").download("x.png");
    expect(data).toBeNull();
    expect(error?.code).toBe("internal");
    expect(error?.status).toBe(500);
  });

  it("network failure → network error", async () => {
    const basin = basinWithFetch(networkErrorFetch());
    const { data, error } = await basin.storage.from("avatars").download("x.png");
    expect(data).toBeNull();
    expect(error?.code).toBe("network");
  });
});

// ── list ─────────────────────────────────────────────────────────────

describe("storage.list", () => {
  const sampleObjects = [
    {
      name: "avatar.png",
      size: 1024,
      contentType: "image/png",
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-01T00:00:00Z",
    },
  ];

  it("happy path — POST to correct URL, returns ObjectInfo[]", async () => {
    let capturedUrl = "";
    let capturedMethod = "";
    let capturedBody: unknown;

    const basin = createClient("https://api.basin.run", "anon", {
      retry: false,
      fetch: async (input, init) => {
        capturedUrl = input.toString();
        capturedMethod = init?.method ?? "";
        capturedBody = JSON.parse(init?.body as string);
        return new Response(JSON.stringify(sampleObjects), { status: 200 });
      },
    });

    const { data, error } = await basin.storage
      .from("avatars")
      .list("u1/", { limit: 50, offset: 0 });

    expect(error).toBeNull();
    expect(data).toEqual(sampleObjects);
    expect(capturedMethod).toBe("POST");
    expect(capturedUrl).toContain("/object/list/avatars");
    expect(capturedBody).toMatchObject({ prefix: "u1/", limit: 50, offset: 0 });
  });

  it("empty result returns [] not null", async () => {
    const basin = basinWithFetch(mockFetch(200, []));
    const { data, error } = await basin.storage.from("avatars").list();
    expect(error).toBeNull();
    expect(data).toEqual([]);
  });

  it("no prefix defaults to empty string in body", async () => {
    let capturedBody: unknown;
    const basin = createClient("https://api.basin.run", "anon", {
      retry: false,
      fetch: async (_input, init) => {
        capturedBody = JSON.parse(init?.body as string);
        return new Response("[]", { status: 200 });
      },
    });

    await basin.storage.from("avatars").list();
    expect((capturedBody as Record<string, unknown>).prefix).toBe("");
  });

  it("sortBy is forwarded in body", async () => {
    let capturedBody: unknown;
    const basin = createClient("https://api.basin.run", "anon", {
      retry: false,
      fetch: async (_input, init) => {
        capturedBody = JSON.parse(init?.body as string);
        return new Response("[]", { status: 200 });
      },
    });

    await basin.storage.from("avatars").list(undefined, {
      sortBy: { column: "created_at", order: "desc" },
    });
    expect((capturedBody as Record<string, unknown>).sortBy).toEqual({
      column: "created_at",
      order: "desc",
    });
  });

  it("401 unauthorized → unauthorized error", async () => {
    const basin = basinWithFetch(mockFetch(401, { error: "Unauthorized" }));
    const { data, error } = await basin.storage.from("avatars").list("u1/");
    expect(data).toBeNull();
    expect(error?.code).toBe("unauthorized");
  });

  it("500 server error → internal error", async () => {
    const basin = basinWithFetch(mockFetch(500, { error: "Server Error" }));
    const { data, error } = await basin.storage.from("avatars").list();
    expect(data).toBeNull();
    expect(error?.code).toBe("internal");
  });

  it("network failure → network error", async () => {
    const basin = basinWithFetch(networkErrorFetch());
    const { data, error } = await basin.storage.from("avatars").list();
    expect(data).toBeNull();
    expect(error?.code).toBe("network");
  });
});

// ── remove ────────────────────────────────────────────────────────────

describe("storage.remove", () => {
  it("happy path — DELETE to correct URL with {prefixes} body", async () => {
    let capturedUrl = "";
    let capturedMethod = "";
    let capturedBody: unknown;

    const basin = createClient("https://api.basin.run", "anon", {
      retry: false,
      fetch: async (input, init) => {
        capturedUrl = input.toString();
        capturedMethod = init?.method ?? "";
        capturedBody = JSON.parse(init?.body as string);
        return new Response(null, { status: 200 });
      },
    });

    const { data, error } = await basin.storage.from("avatars").remove(["a.png", "b.png"]);

    expect(error).toBeNull();
    expect(data).toEqual({ paths: ["a.png", "b.png"] });
    expect(capturedMethod).toBe("DELETE");
    expect(capturedUrl).toContain("/object/avatars");
    expect(capturedBody).toEqual({ prefixes: ["a.png", "b.png"] });
  });

  it("single path remove works", async () => {
    let capturedBody: unknown;
    const basin = createClient("https://api.basin.run", "anon", {
      retry: false,
      fetch: async (_input, init) => {
        capturedBody = JSON.parse(init?.body as string);
        return new Response(null, { status: 200 });
      },
    });

    await basin.storage.from("avatars").remove(["single.png"]);
    expect(capturedBody).toEqual({ prefixes: ["single.png"] });
  });

  it("401 unauthorized → unauthorized error", async () => {
    const basin = basinWithFetch(mockFetch(401, { error: "Unauthorized" }));
    const { data, error } = await basin.storage.from("avatars").remove(["a.png"]);
    expect(data).toBeNull();
    expect(error?.code).toBe("unauthorized");
  });

  it("500 server error → internal error", async () => {
    const basin = basinWithFetch(mockFetch(500, { error: "Server Error" }));
    const { data, error } = await basin.storage.from("avatars").remove(["a.png"]);
    expect(data).toBeNull();
    expect(error?.code).toBe("internal");
  });

  it("network failure → network error", async () => {
    const basin = basinWithFetch(networkErrorFetch());
    const { data, error } = await basin.storage.from("avatars").remove(["a.png"]);
    expect(data).toBeNull();
    expect(error?.code).toBe("network");
  });
});

// ── createSignedUrl ───────────────────────────────────────────────────

describe("storage.createSignedUrl", () => {
  it("happy path — POST to correct URL, returns signedUrl", async () => {
    let capturedUrl = "";
    let capturedMethod = "";
    let capturedBody: unknown;

    const basin = createClient("https://api.basin.run", "anon", {
      retry: false,
      fetch: async (input, init) => {
        capturedUrl = input.toString();
        capturedMethod = init?.method ?? "";
        capturedBody = JSON.parse(init?.body as string);
        return new Response(
          JSON.stringify({ signedUrl: "https://api.basin.run/storage/v1/render/image/sign/avatars/u1/avatar.png?token=abc" }),
          { status: 200 },
        );
      },
    });

    const { data, error } = await basin.storage
      .from("avatars")
      .createSignedUrl("u1/avatar.png", 300);

    expect(error).toBeNull();
    expect(data?.signedUrl).toContain("token=abc");
    expect(capturedMethod).toBe("POST");
    expect(capturedUrl).toContain("/object/sign/avatars/u1/avatar.png");
    expect(capturedBody).toEqual({ expiresIn: 300 });
  });

  it("relative signedUrl is resolved against storageUrl base", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      retry: false,
      fetch: async () =>
        new Response(JSON.stringify({ signedUrl: "/storage/v1/render/image/sign/bucket/file.png?token=xyz" }), {
          status: 200,
        }),
    });

    const { data, error } = await basin.storage.from("bucket").createSignedUrl("file.png", 60);
    expect(error).toBeNull();
    expect(data?.signedUrl).toContain("https://api.basin.run");
    expect(data?.signedUrl).toContain("token=xyz");
  });

  it("expiresAt is forwarded when present", async () => {
    const basin = basinWithFetch(
      mockFetch(200, {
        signedUrl: "https://example.com/signed?token=abc",
        expiresAt: "2026-06-01T00:00:00Z",
      }),
    );

    const { data, error } = await basin.storage.from("avatars").createSignedUrl("f.png", 3600);
    expect(error).toBeNull();
    expect(data?.expiresAt).toBe("2026-06-01T00:00:00Z");
  });

  it("negative expiresIn → invalid_request before any fetch", async () => {
    const basin = basinWithFetch(noFetch());
    const { data, error } = await basin.storage
      .from("avatars")
      .createSignedUrl("u1/avatar.png", -1);
    expect(data).toBeNull();
    expect(error?.code).toBe("invalid_request");
  });

  it("404 unknown path → not_found error", async () => {
    const basin = basinWithFetch(mockFetch(404, { error: "Not Found" }));
    const { data, error } = await basin.storage.from("avatars").createSignedUrl("missing.png", 60);
    expect(data).toBeNull();
    expect(error?.code).toBe("not_found");
  });

  it("401 unauthorized → unauthorized error", async () => {
    const basin = basinWithFetch(mockFetch(401, { error: "Unauthorized" }));
    const { data, error } = await basin.storage.from("private").createSignedUrl("file.png", 60);
    expect(data).toBeNull();
    expect(error?.code).toBe("unauthorized");
  });

  it("500 server error → internal error", async () => {
    const basin = basinWithFetch(mockFetch(500, { error: "Server Error" }));
    const { data, error } = await basin.storage.from("avatars").createSignedUrl("file.png", 60);
    expect(data).toBeNull();
    expect(error?.code).toBe("internal");
  });

  it("network failure → network error", async () => {
    const basin = basinWithFetch(networkErrorFetch());
    const { data, error } = await basin.storage.from("avatars").createSignedUrl("file.png", 60);
    expect(data).toBeNull();
    expect(error?.code).toBe("network");
  });
});

// ── getPublicUrl (unchanged) ─────────────────────────────────────────

describe("storage.getPublicUrl (sync URL construction)", () => {
  it("constructs the public URL synchronously without a network call", () => {
    let called = false;
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: async () => {
        called = true;
        throw new Error("should not fetch");
      },
    });
    const { data } = basin.storage.from("avatars").getPublicUrl("u1/avatar.png");
    expect(data.publicUrl).toContain("avatars");
    expect(data.publicUrl).toContain("u1/avatar.png");
    expect(data.publicUrl).toContain("/object/public/");
    expect(called).toBe(false);
  });

  it("encodes path segments but preserves / separators", () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: noFetch(),
    });
    const { data } = basin.storage
      .from("docs")
      .getPublicUrl("folder one/file name.png");
    expect(data.publicUrl).toContain("folder%20one/file%20name.png");
  });

  it("encodes the bucket name", () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: noFetch(),
    });
    const { data } = basin.storage
      .from("public assets")
      .getPublicUrl("logo.png");
    expect(data.publicUrl).toContain("public%20assets");
  });
});

// ── multipart + TUS (still not_implemented) ──────────────────────────

describe("storage multipart + TUS (not_implemented)", () => {
  it("uploadMultipart returns not_implemented", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: noFetch(),
    });
    const { data, error } = await basin.storage
      .from("avatars")
      .uploadMultipart("big.bin", new Blob(["x"]));
    expect(data).toBeNull();
    expect(error.code).toBe("not_implemented");
    expect(error.message).toContain("ROADMAP 0.3");
  });

  it("uploadResumable returns not_implemented", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: noFetch(),
    });
    const { data, error } = await basin.storage
      .from("avatars")
      .uploadResumable("big.bin", new Blob(["x"]));
    expect(data).toBeNull();
    expect(error.code).toBe("not_implemented");
    expect(error.message).toContain("ROADMAP 0.3");
  });
});
