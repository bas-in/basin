/** Shared test doubles: recording fetch mock + fake standard WebSocket. */

export function jsonResponse(
  body: unknown,
  status = 200,
  headers: Record<string, string> = {},
): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

export interface RecordedCall {
  url: string;
  method: string;
  headers: Record<string, string>;
  body: string | undefined;
}

/** FIFO fetch mock that records every call. */
export class FetchMock {
  calls: RecordedCall[] = [];
  #queue: Response[] = [];

  queue(res: Response): this {
    this.#queue.push(res);
    return this;
  }

  queueJson(body: unknown, status = 200): this {
    return this.queue(jsonResponse(body, status));
  }

  impl = async (input: string | URL, init?: RequestInit): Promise<Response> => {
    const headers: Record<string, string> = {};
    for (const [k, v] of Object.entries(
      (init?.headers as Record<string, string>) ?? {},
    )) {
      headers[k.toLowerCase()] = v;
    }
    this.calls.push({
      url: String(input),
      method: init?.method ?? "GET",
      headers,
      body: typeof init?.body === "string" ? init.body : undefined,
    });
    const res = this.#queue.shift();
    if (res === undefined) {
      throw new Error(`FetchMock: unexpected call to ${String(input)}`);
    }
    return res;
  };

  get last(): RecordedCall {
    const call = this.calls[this.calls.length - 1];
    if (call === undefined) throw new Error("FetchMock: no calls recorded");
    return call;
  }
}

type Listener = (event: {
  data?: unknown;
  code?: number;
  reason?: string;
}) => void;

/** Fake of the standard WebSocket: opens on the next microtask. */
export class FakeWebSocket {
  static instances: FakeWebSocket[] = [];
  static reset(): void {
    FakeWebSocket.instances = [];
  }
  static get last(): FakeWebSocket {
    const ws = FakeWebSocket.instances[FakeWebSocket.instances.length - 1];
    if (ws === undefined) throw new Error("FakeWebSocket: none constructed");
    return ws;
  }

  url: string;
  protocols: string[];
  readyState = 0; // CONNECTING
  sent: string[] = [];
  #listeners = new Map<string, Listener[]>();

  constructor(url: string, protocols?: string | string[]) {
    this.url = url;
    this.protocols =
      protocols === undefined
        ? []
        : Array.isArray(protocols)
          ? protocols
          : [protocols];
    FakeWebSocket.instances.push(this);
    queueMicrotask(() => {
      this.readyState = 1; // OPEN
      this.#emit("open", {});
    });
  }

  addEventListener(type: string, listener: Listener): void {
    const list = this.#listeners.get(type) ?? [];
    list.push(listener);
    this.#listeners.set(type, list);
  }

  send(data: string): void {
    this.sent.push(data);
  }

  close(code?: number, reason?: string): void {
    this.readyState = 3; // CLOSED
    this.#emit("close", { code, reason });
  }

  /** Deliver a server frame to the client. */
  serverSend(frame: unknown): void {
    this.#emit("message", { data: JSON.stringify(frame) });
  }

  #emit(type: string, event: Parameters<Listener>[0]): void {
    for (const l of this.#listeners.get(type) ?? []) l(event);
  }

  get sentJson(): unknown[] {
    return this.sent.map((s) => JSON.parse(s));
  }
}

/** Flush pending microtasks/macrotasks. */
export async function tick(times = 3): Promise<void> {
  for (let i = 0; i < times; i++) {
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

function b64url(s: string): string {
  return Buffer.from(s, "utf8")
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

/** Unsigned test JWT with the claim layout from basin-auth's WireClaims. */
export function makeJwt(projectId: string): string {
  const header = b64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const payload = b64url(
    JSON.stringify({
      project_id: projectId,
      user_id: "00000000-0000-0000-0000-000000000001",
      email: "t@example.com",
      roles: [],
      exp: Math.floor(Date.now() / 1000) + 3600,
      iat: Math.floor(Date.now() / 1000),
    }),
  );
  return `${header}.${payload}.sig`;
}
