import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { withRetry } from "./retry.js";

describe("withRetry", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("happy path — 200 on first attempt makes a single fetch call", async () => {
    const mockFetch = vi.fn().mockResolvedValue(new Response("{}", { status: 200 }));
    const retryingFetch = withRetry(mockFetch as typeof globalThis.fetch);

    const promise = retryingFetch("https://example.com/");
    await vi.runAllTimersAsync();
    const response = await promise;

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(response.status).toBe(200);
  });

  it("500 then 200 — retries once and returns the 200 response", async () => {
    const mockFetch = vi
      .fn()
      .mockResolvedValueOnce(new Response("err", { status: 500 }))
      .mockResolvedValueOnce(new Response("{}", { status: 200 }));
    const retryingFetch = withRetry(mockFetch as typeof globalThis.fetch);

    const promise = retryingFetch("https://example.com/");
    await vi.runAllTimersAsync();
    const response = await promise;

    expect(mockFetch).toHaveBeenCalledTimes(2);
    expect(response.status).toBe(200);
  });

  it("3x 500 — exhausts retries and returns the last 500 response", async () => {
    const mockFetch = vi
      .fn()
      .mockResolvedValue(new Response("err", { status: 500 }));
    const retryingFetch = withRetry(mockFetch as typeof globalThis.fetch, {
      maxAttempts: 3,
    });

    const promise = retryingFetch("https://example.com/");
    await vi.runAllTimersAsync();
    const response = await promise;

    expect(mockFetch).toHaveBeenCalledTimes(3);
    expect(response.status).toBe(500);
  });

  it("network error then 200 — retries and returns the 200 response", async () => {
    const networkError = new TypeError("Failed to fetch");
    const mockFetch = vi
      .fn()
      .mockRejectedValueOnce(networkError)
      .mockResolvedValueOnce(new Response("{}", { status: 200 }));
    const retryingFetch = withRetry(mockFetch as typeof globalThis.fetch);

    const promise = retryingFetch("https://example.com/");
    await vi.runAllTimersAsync();
    const response = await promise;

    expect(mockFetch).toHaveBeenCalledTimes(2);
    expect(response.status).toBe(200);
  });

  it("429 with Retry-After: 2 — second attempt happens after ~2000ms", async () => {
    const mockFetch = vi
      .fn()
      .mockResolvedValueOnce(
        new Response("rate limited", {
          status: 429,
          headers: { "Retry-After": "2" },
        }),
      )
      .mockResolvedValueOnce(new Response("{}", { status: 200 }));
    const retryingFetch = withRetry(mockFetch as typeof globalThis.fetch);

    const promise = retryingFetch("https://example.com/");

    expect(mockFetch).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(1999);
    expect(mockFetch).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(1);
    await Promise.resolve();
    await Promise.resolve();

    expect(mockFetch).toHaveBeenCalledTimes(2);

    const response = await promise;
    expect(response.status).toBe(200);
  });

  it("401 — not retried, returned immediately", async () => {
    const mockFetch = vi
      .fn()
      .mockResolvedValue(new Response("unauthorized", { status: 401 }));
    const retryingFetch = withRetry(mockFetch as typeof globalThis.fetch);

    const promise = retryingFetch("https://example.com/");
    await vi.runAllTimersAsync();
    const response = await promise;

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(response.status).toBe(401);
  });

  it("retry: false — original fetch used, no retries on 500", async () => {
    const mockFetch = vi
      .fn()
      .mockResolvedValue(new Response("err", { status: 500 }));

    const response = await mockFetch("https://example.com/");

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(response.status).toBe(500);
  });
});
