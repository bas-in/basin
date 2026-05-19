import { BasinError } from "../errors.js";
import type { AuthClient } from "../auth/client.js";
import type { Credential, ProvisionResult } from "./types.js";

interface AdminDeps {
  url: string;
  headers: Record<string, string>;
  fetch: typeof fetch;
  auth: AuthClient;
}

const NOT_IMPLEMENTED_MSG =
  "Admin projects lands when wired in T-008/T-009/T-010";

export class AdminProjectsClient {
  readonly #deps: AdminDeps;

  constructor(deps: AdminDeps) {
    this.#deps = deps;
  }

  async provision(_input: {
    projectId: string;
  }): Promise<{ data: ProvisionResult | null; error: BasinError | null }> {
    return {
      data: null,
      error: new BasinError("not_implemented", NOT_IMPLEMENTED_MSG),
    };
  }

  async rotateCredentials(
    _pgwireUser: string,
  ): Promise<{ data: ProvisionResult | null; error: BasinError | null }> {
    return {
      data: null,
      error: new BasinError("not_implemented", NOT_IMPLEMENTED_MSG),
    };
  }

  async listCredentials(
    _projectId: string,
  ): Promise<{ data: Credential[] | null; error: BasinError | null }> {
    return {
      data: null,
      error: new BasinError("not_implemented", NOT_IMPLEMENTED_MSG),
    };
  }
}

export class AdminClient {
  readonly projects: AdminProjectsClient;

  constructor(deps: AdminDeps) {
    this.projects = new AdminProjectsClient(deps);
  }
}
