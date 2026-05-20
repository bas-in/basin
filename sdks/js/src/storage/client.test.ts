import { describe, expect, it } from "vitest";
import { createClient } from "../client.js";
import { BasinError } from "../errors.js";

// basin-engine v0.1 has no /object/* surface, so every async storage
// method returns BasinError("not_implemented") without making any
// network call. These tests pin that contract — the day the engine
// grows storage routes the bodies + tests get swapped together.

function noFetch(): typeof fetch {
  return async () => {
    throw new Error("storage methods must not touch the network until v0.2");
  };
}

describe("storage.upload (not_implemented)", () => {
  it("returns BasinError('not_implemented') without any network call", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: noFetch(),
    });
    const { data, error } = await basin.storage
      .from("avatars")
      .upload("u1/avatar.png", new Blob(["png-bytes"], { type: "image/png" }));
    expect(data).toBeNull();
    expect(error).toBeInstanceOf(BasinError);
    expect(error?.code).toBe("not_implemented");
    expect(error?.message).toContain("Storage");
    expect(error?.message).toContain("ROADMAP 0.3");
  });
});

describe("storage.download (not_implemented)", () => {
  it("returns BasinError('not_implemented') without any network call", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: noFetch(),
    });
    const { data, error } = await basin.storage
      .from("avatars")
      .download("u1/avatar.png");
    expect(data).toBeNull();
    expect(error?.code).toBe("not_implemented");
    expect(error?.message).toContain("ROADMAP 0.3");
  });
});

describe("storage.list (not_implemented)", () => {
  it("returns BasinError('not_implemented') without any network call", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: noFetch(),
    });
    const { data, error } = await basin.storage
      .from("avatars")
      .list("u1/", { limit: 50, offset: 0 });
    expect(data).toBeNull();
    expect(error?.code).toBe("not_implemented");
    expect(error?.message).toContain("ROADMAP 0.3");
  });
});

describe("storage.remove (not_implemented)", () => {
  it("returns BasinError('not_implemented') without any network call", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: noFetch(),
    });
    const { data, error } = await basin.storage
      .from("avatars")
      .remove(["a.png", "b.png"]);
    expect(data).toBeNull();
    expect(error?.code).toBe("not_implemented");
    expect(error?.message).toContain("ROADMAP 0.3");
  });
});

describe("storage.createSignedUrl (not_implemented)", () => {
  it("returns BasinError('not_implemented') without any network call", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: noFetch(),
    });
    const { data, error } = await basin.storage
      .from("avatars")
      .createSignedUrl("u1/avatar.png", 300);
    expect(data).toBeNull();
    expect(error?.code).toBe("not_implemented");
    expect(error?.message).toContain("ROADMAP 0.3");
  });
});

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
    // The URL won't resolve until the engine ships storage in v0.2+,
    // but the construction is preserved so render-time templates
    // compile against the stable shape.
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
