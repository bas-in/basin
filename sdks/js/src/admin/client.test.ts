import { describe, expect, it } from "vitest";
import { createClient } from "../client.js";
import { BasinError } from "../errors.js";

function noFetch(): typeof fetch {
  return async () => {
    throw new Error("admin methods must not touch the network until T-008/T-009/T-010");
  };
}

describe("admin.projects.provision (not_implemented)", () => {
  it("returns BasinError('not_implemented') without any network call", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: noFetch(),
    });
    const { data, error } = await basin.admin.projects.provision({
      projectId: "proj_x",
    });
    expect(data).toBeNull();
    expect(error).toBeInstanceOf(BasinError);
    expect(error?.code).toBe("not_implemented");
    expect(error?.message).toContain("T-008/T-009/T-010");
  });
});

describe("admin.projects.rotateCredentials (not_implemented)", () => {
  it("returns BasinError('not_implemented') without any network call", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: noFetch(),
    });
    const { data, error } = await basin.admin.projects.rotateCredentials("user_x");
    expect(data).toBeNull();
    expect(error).toBeInstanceOf(BasinError);
    expect(error?.code).toBe("not_implemented");
    expect(error?.message).toContain("T-008/T-009/T-010");
  });
});

describe("admin.projects.listCredentials (not_implemented)", () => {
  it("returns BasinError('not_implemented') without any network call", async () => {
    const basin = createClient("https://api.basin.run", "anon", {
      fetch: noFetch(),
    });
    const { data, error } = await basin.admin.projects.listCredentials("proj_x");
    expect(data).toBeNull();
    expect(error).toBeInstanceOf(BasinError);
    expect(error?.code).toBe("not_implemented");
    expect(error?.message).toContain("T-008/T-009/T-010");
  });
});
