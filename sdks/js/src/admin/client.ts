import { BasinError } from "../errors.js";
import type { AuthClient } from "../auth/client.js";
import type { Credential, ProvisionResult } from "./types.js";

interface AdminDeps {
  url: string;
  headers: Record<string, string>;
  fetch: typeof fetch;
  auth: AuthClient;
}

export class AdminProjectsClient {
  readonly #deps: AdminDeps;

  constructor(deps: AdminDeps) {
    this.#deps = deps;
  }

  async provision(input: {
    projectId: string;
  }): Promise<{ data: ProvisionResult | null; error: BasinError | null }> {
    const session = this.#deps.auth.getSession();
    const headers: Record<string, string> = {
      ...this.#deps.headers,
      "Content-Type": "application/json",
      ...(session ? { Authorization: `Bearer ${session.access_token}` } : {}),
    };

    let res: Response;
    try {
      res = await this.#deps.fetch(`${this.#deps.url}/admin/v1/projects`, {
        method: "POST",
        headers,
        body: JSON.stringify({ project_id: input.projectId }),
      });
    } catch (e) {
      return {
        data: null,
        error: new BasinError("network", networkErrorMessage(e), undefined, { cause: e }),
      };
    }

    if (res.status === 401 || res.status === 403) {
      return {
        data: null,
        error: new BasinError(
          "unauthorized",
          "Admin endpoints require is_admin claims",
          res.status,
        ),
      };
    }

    let body: unknown;
    try {
      body = await res.json();
    } catch {
      return {
        data: null,
        error: new BasinError(
          "invalid_response",
          `Admin provision response was not JSON (HTTP ${res.status})`,
          res.status,
        ),
      };
    }

    const connectionString = Object.keys(body as Record<string, unknown>)[0] ?? "";
    return { data: { connectionString }, error: null };
  }

  async rotateCredentials(
    pgwireUser: string,
  ): Promise<{ data: ProvisionResult | null; error: BasinError | null }> {
    const session = this.#deps.auth.getSession();
    const headers: Record<string, string> = {
      ...this.#deps.headers,
      ...(session ? { Authorization: `Bearer ${session.access_token}` } : {}),
    };

    let res: Response;
    try {
      res = await this.#deps.fetch(
        `${this.#deps.url}/admin/v1/projects/${encodeURIComponent(pgwireUser)}/rotate`,
        { method: "POST", headers },
      );
    } catch (e) {
      return {
        data: null,
        error: new BasinError("network", networkErrorMessage(e), undefined, { cause: e }),
      };
    }

    if (res.status === 401 || res.status === 403) {
      return {
        data: null,
        error: new BasinError(
          "unauthorized",
          "Admin endpoints require is_admin claims",
          res.status,
        ),
      };
    }

    if (res.status === 404) {
      return {
        data: null,
        error: new BasinError("not_found", `Unknown pgwire user: ${pgwireUser}`, 404),
      };
    }

    let body: unknown;
    try {
      body = await res.json();
    } catch {
      return {
        data: null,
        error: new BasinError(
          "invalid_response",
          `Admin rotateCredentials response was not JSON (HTTP ${res.status})`,
          res.status,
        ),
      };
    }

    const connectionString = Object.keys(body as Record<string, unknown>)[0] ?? "";
    return { data: { connectionString }, error: null };
  }

  async listCredentials(
    projectId: string,
  ): Promise<{ data: Credential[] | null; error: BasinError | null }> {
    const session = this.#deps.auth.getSession();
    const headers: Record<string, string> = {
      ...this.#deps.headers,
      ...(session ? { Authorization: `Bearer ${session.access_token}` } : {}),
    };

    let res: Response;
    try {
      res = await this.#deps.fetch(
        `${this.#deps.url}/admin/v1/projects/${encodeURIComponent(projectId)}/credentials`,
        { method: "GET", headers },
      );
    } catch (e) {
      return {
        data: null,
        error: new BasinError("network", networkErrorMessage(e), undefined, { cause: e }),
      };
    }

    if (res.status === 401 || res.status === 403) {
      return {
        data: null,
        error: new BasinError(
          "unauthorized",
          "Admin endpoints require is_admin claims",
          res.status,
        ),
      };
    }

    let body: unknown;
    try {
      body = await res.json();
    } catch {
      return {
        data: null,
        error: new BasinError(
          "invalid_response",
          `Admin listCredentials response was not JSON (HTTP ${res.status})`,
          res.status,
        ),
      };
    }

    return { data: body as Credential[], error: null };
  }
}

export class AdminClient {
  readonly projects: AdminProjectsClient;

  constructor(deps: AdminDeps) {
    this.projects = new AdminProjectsClient(deps);
  }
}

function networkErrorMessage(e: unknown): string {
  if (e instanceof Error && e.message) return e.message;
  return "network error reaching admin endpoint";
}
