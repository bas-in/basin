import { BasinError } from "../errors.js";
import { backoff } from "./backoff.js";

export interface SseEvent {
  project: string;
  table: string;
  op: "INSERT" | "UPDATE" | "DELETE";
  after: Record<string, unknown>;
  seq: number;
}

export type SseEventCallback = (event: SseEvent) => void;
export type SseErrorCallback = (err: BasinError) => void;

export interface SseSubscribeOptions {
  jwt: string;
  onError?: SseErrorCallback | undefined;
}

function buildUrl(baseUrl: string, project: string, table: string): string {
  return `${baseUrl}/realtime/v1/sse/${encodeURIComponent(project)}/${encodeURIComponent(table)}`;
}

export class SseSubscription {
  #baseUrl: string;
  #project: string;
  #table: string;
  #jwt: string;
  #onEvent: SseEventCallback;
  #onError: SseErrorCallback | undefined;
  #lastSeq: number | null = null;
  #stopped = false;
  #attempt = 0;
  #fetchFn: typeof globalThis.fetch;
  #abortController: AbortController | null = null;

  constructor(
    baseUrl: string,
    project: string,
    table: string,
    opts: SseSubscribeOptions,
    onEvent: SseEventCallback,
    fetchFn: typeof globalThis.fetch = globalThis.fetch,
  ) {
    this.#baseUrl = baseUrl;
    this.#project = project;
    this.#table = table;
    this.#jwt = opts.jwt;
    this.#onError = opts.onError;
    this.#onEvent = onEvent;
    this.#fetchFn = fetchFn;
  }

  start(): void {
    void this.#loop();
  }

  stop(): void {
    this.#stopped = true;
    this.#abortController?.abort();
  }

  async #loop(): Promise<void> {
    while (!this.#stopped) {
      if (this.#attempt > 0) {
        await delay(backoff(this.#attempt - 1));
      }
      if (this.#stopped) break;

      try {
        await this.#connect();
        this.#attempt = 0;
      } catch (err) {
        if (this.#stopped) break;
        if (err instanceof BasinError && this.#onError) {
          this.#onError(err);
        }
        this.#attempt++;
      }
    }
  }

  async #connect(): Promise<void> {
    const url = buildUrl(this.#baseUrl, this.#project, this.#table);
    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.#jwt}`,
      Accept: "text/event-stream",
    };
    if (this.#lastSeq !== null) {
      headers["Last-Event-Id"] = String(this.#lastSeq);
    }

    this.#abortController = new AbortController();

    if (typeof globalThis.EventSource !== "undefined") {
      await this.#connectEventSource(url);
    } else {
      await this.#connectFetch(url, headers);
    }
  }

  async #connectFetch(url: string, headers: Record<string, string>): Promise<void> {
    let response: Response;
    try {
      response = await this.#fetchFn(url, {
        headers,
        signal: this.#abortController!.signal,
      });
    } catch (err) {
      if (this.#stopped) return;
      throw new BasinError("network", String(err instanceof Error ? err.message : err));
    }

    if (!response.ok) {
      throw new BasinError("network", `SSE connect failed: ${response.status}`, response.status);
    }

    const body = response.body;
    if (!body) {
      throw new BasinError("network", "SSE response has no body");
    }

    await this.#readStream(body);
  }

  async #readStream(body: ReadableStream<Uint8Array>): Promise<void> {
    const decoder = new TextDecoder();
    const reader = body.getReader();
    let buf = "";

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        const lines = buf.split("\n");
        buf = lines.pop() ?? "";
        for (const line of lines) {
          this.#processLine(line);
        }
      }
    } catch (err) {
      if (this.#stopped) return;
      if (err instanceof BasinError) throw err;
      throw new BasinError("network", String(err instanceof Error ? err.message : err));
    } finally {
      reader.releaseLock();
    }
  }

  #processLine(line: string): void {
    if (line.startsWith(":") || line.trim() === "") return;

    if (line.startsWith("data:")) {
      const payload = line.slice(5).trimStart();
      let parsed: unknown;
      try {
        parsed = JSON.parse(payload);
      } catch {
        throw new BasinError("invalid_response", `SSE data line is not valid JSON: ${payload}`);
      }

      const ev = parsed as SseEvent;
      if (ev && typeof ev.seq === "number") {
        this.#lastSeq = ev.seq;
      }
      this.#onEvent(ev);
    }
  }

  async #connectEventSource(url: string): Promise<void> {
    return new Promise((resolve, reject) => {
      const es = new globalThis.EventSource(url);

      const cleanup = (): void => {
        es.close();
      };

      this.#abortController!.signal.addEventListener("abort", () => {
        cleanup();
        resolve();
      });

      es.onmessage = (ev: MessageEvent): void => {
        const data = typeof ev.data === "string" ? ev.data : "";
        if (!data || data.startsWith(":")) return;
        let parsed: unknown;
        try {
          parsed = JSON.parse(data);
        } catch {
          cleanup();
          reject(new BasinError("invalid_response", `SSE data is not valid JSON: ${data}`));
          return;
        }
        const event = parsed as SseEvent;
        if (event && typeof event.seq === "number") {
          this.#lastSeq = event.seq;
        }
        this.#onEvent(event);
      };

      es.onerror = (): void => {
        cleanup();
        if (!this.#stopped) {
          reject(new BasinError("network", "EventSource error"));
        } else {
          resolve();
        }
      };
    });
  }
}

export function subscribe(
  baseUrl: string,
  project: string,
  table: string,
  opts: SseSubscribeOptions,
  onEvent: SseEventCallback,
  fetchFn?: typeof globalThis.fetch,
): SseSubscription {
  const sub = new SseSubscription(baseUrl, project, table, opts, onEvent, fetchFn);
  sub.start();
  return sub;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
