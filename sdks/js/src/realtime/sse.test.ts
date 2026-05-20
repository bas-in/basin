import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { BasinError } from "../errors.js";
import { SseSubscription } from "./sse.js";

const BASE = "https://api.basin.run";
const PROJECT = "acme";
const TABLE = "orders";
const JWT = "tok.en";

function encodeChunks(chunks: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(encoder.encode(chunk));
      }
      controller.close();
    },
  });
}

function neverStream(): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({ start() {} });
}

function okResponse(chunks: string[]): Response {
  return new Response(encodeChunks(chunks), {
    status: 200,
    headers: { "Content-Type": "text/event-stream" },
  });
}

describe("backoff", () => {
  it("caps at 30s", async () => {
    const { backoff } = await import("./backoff.js");
    expect(backoff(0)).toBe(1000);
    expect(backoff(1)).toBe(2000);
    expect(backoff(4)).toBe(16000);
    expect(backoff(10)).toBe(30000);
  });
});

describe("SseSubscription — fetch transport", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("event frame fires onEvent with parsed payload", async () => {
    const event = { project: PROJECT, table: TABLE, op: "INSERT", after: { id: 1 }, seq: 1 };
    const mockFetch = vi.fn()
      .mockResolvedValueOnce(okResponse([`data: ${JSON.stringify(event)}\n\n`]))
      .mockReturnValueOnce(new Promise(() => {})) as unknown as typeof globalThis.fetch;

    const onEvent = vi.fn();
    const sub = new SseSubscription(BASE, PROJECT, TABLE, { jwt: JWT }, onEvent, mockFetch);
    sub.start();

    await vi.runAllTimersAsync();
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    expect(onEvent).toHaveBeenCalledOnce();
    expect(onEvent.mock.calls[0][0]).toMatchObject({ op: "INSERT", seq: 1 });

    sub.stop();
  });

  it("heartbeat comment frame does not fire onEvent", async () => {
    const event = { project: PROJECT, table: TABLE, op: "UPDATE", after: { id: 2 }, seq: 2 };
    const chunks = [`: heartbeat\n\n`, `data: ${JSON.stringify(event)}\n\n`];
    const mockFetch = vi.fn()
      .mockResolvedValueOnce(okResponse(chunks))
      .mockReturnValueOnce(new Promise(() => {})) as unknown as typeof globalThis.fetch;

    const onEvent = vi.fn();
    const sub = new SseSubscription(BASE, PROJECT, TABLE, { jwt: JWT }, onEvent, mockFetch);
    sub.start();

    await vi.runAllTimersAsync();
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    expect(onEvent).toHaveBeenCalledOnce();
    expect(onEvent.mock.calls[0][0]).toMatchObject({ op: "UPDATE", seq: 2 });

    sub.stop();
  });

  it("tracks lastSeq and sends Last-Event-Id on reconnect", async () => {
    const event1 = { project: PROJECT, table: TABLE, op: "INSERT", after: { id: 1 }, seq: 5 };
    const event2 = { project: PROJECT, table: TABLE, op: "DELETE", after: { id: 2 }, seq: 6 };

    const mockFetch = vi.fn()
      .mockResolvedValueOnce(okResponse([`data: ${JSON.stringify(event1)}\n\n`]))
      .mockResolvedValueOnce(okResponse([`data: ${JSON.stringify(event2)}\n\n`]))
      .mockReturnValueOnce(new Promise(() => {})) as unknown as typeof globalThis.fetch;

    const onEvent = vi.fn();
    const sub = new SseSubscription(BASE, PROJECT, TABLE, { jwt: JWT }, onEvent, mockFetch);
    sub.start();

    await vi.runAllTimersAsync();
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    expect(onEvent).toHaveBeenCalledTimes(2);

    const calls = (mockFetch as ReturnType<typeof vi.fn>).mock.calls;
    expect(calls.length).toBeGreaterThanOrEqual(2);
    const secondHeaders = (calls[1][1] as RequestInit).headers as Record<string, string>;
    expect(secondHeaders["Last-Event-Id"]).toBe("5");

    sub.stop();
  });

  it("non-200 response triggers reconnect attempt", async () => {
    const mockFetch = vi.fn()
      .mockResolvedValueOnce(new Response("forbidden", { status: 403 }))
      .mockReturnValueOnce(new Response(neverStream(), { status: 200 })) as unknown as typeof globalThis.fetch;

    const onEvent = vi.fn();
    const sub = new SseSubscription(BASE, PROJECT, TABLE, { jwt: JWT }, onEvent, mockFetch);
    sub.start();

    await vi.runAllTimersAsync();
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    expect(mockFetch).toHaveBeenCalledTimes(2);

    sub.stop();
  });

  it("invalid JSON in data line surfaces BasinError via onError", async () => {
    const mockFetch = vi.fn()
      .mockResolvedValueOnce(okResponse([`data: not-json\n\n`]))
      .mockReturnValueOnce(new Promise(() => {})) as unknown as typeof globalThis.fetch;

    const onEvent = vi.fn();
    const onError = vi.fn();
    const sub = new SseSubscription(
      BASE, PROJECT, TABLE,
      { jwt: JWT, onError },
      onEvent,
      mockFetch,
    );
    sub.start();

    await vi.runAllTimersAsync();
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    expect(onError).toHaveBeenCalledOnce();
    const err = onError.mock.calls[0][0] as BasinError;
    expect(err).toBeInstanceOf(BasinError);
    expect(err.code).toBe("invalid_response");

    sub.stop();
  });

  it("disconnect triggers reconnect and Last-Event-Id is not sent before any event", async () => {
    const mockFetch = vi.fn()
      .mockResolvedValueOnce(okResponse([]))
      .mockReturnValueOnce(new Promise(() => {})) as unknown as typeof globalThis.fetch;

    const onEvent = vi.fn();
    const sub = new SseSubscription(BASE, PROJECT, TABLE, { jwt: JWT }, onEvent, mockFetch);
    sub.start();

    await vi.runAllTimersAsync();
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    expect(mockFetch).toHaveBeenCalledTimes(2);

    const firstHeaders = (mockFetch as ReturnType<typeof vi.fn>).mock.calls[0][1] as RequestInit;
    expect((firstHeaders.headers as Record<string, string>)["Last-Event-Id"]).toBeUndefined();

    sub.stop();
  });

  it("stop() prevents further reconnects", async () => {
    const mockFetch = vi.fn()
      .mockRejectedValue(new Error("fail")) as unknown as typeof globalThis.fetch;
    const onEvent = vi.fn();

    const sub = new SseSubscription(BASE, PROJECT, TABLE, { jwt: JWT }, onEvent, mockFetch);
    sub.start();
    sub.stop();

    await vi.runAllTimersAsync();
    await Promise.resolve();

    expect(onEvent).not.toHaveBeenCalled();
  });
});
