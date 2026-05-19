export interface RetryOptions {
  maxAttempts?: number;
  baseMs?: number;
  maxMs?: number;
  retryOn?: (
    response: Response | null,
    error: unknown,
    attempt: number,
  ) => boolean;
}

function defaultRetryOn(
  response: Response | null,
  _error: unknown,
  _attempt: number,
): boolean {
  if (response === null) return true;
  if (response.status >= 500) return true;
  if (response.status === 429) return true;
  return false;
}

function parseRetryAfterMs(header: string): number | null {
  const trimmed = header.trim();
  if (/^\d+$/.test(trimmed)) {
    return parseInt(trimmed, 10) * 1000;
  }
  const date = new Date(trimmed);
  if (!isNaN(date.getTime())) {
    return Math.max(0, date.getTime() - Date.now());
  }
  return null;
}

export function withRetry(
  fetchFn: typeof globalThis.fetch,
  opts: RetryOptions = {},
): typeof globalThis.fetch {
  const maxAttempts = opts.maxAttempts ?? 3;
  const baseMs = opts.baseMs ?? 250;
  const maxMs = opts.maxMs ?? 5000;
  const retryOn = opts.retryOn ?? defaultRetryOn;

  return async function retryingFetch(
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> {
    let attempt = 0;

    while (true) {
      attempt++;
      let response: Response | null = null;
      let fetchError: unknown = null;

      try {
        response = await fetchFn(input, init);
      } catch (err) {
        fetchError = err;
      }

      const shouldRetry =
        attempt < maxAttempts && retryOn(response, fetchError, attempt);

      if (!shouldRetry) {
        if (fetchError !== null && response === null) {
          throw fetchError;
        }
        return response!;
      }

      let delayMs: number;

      if (
        response !== null &&
        response.status === 429 &&
        opts.retryOn === undefined
      ) {
        const retryAfter = response.headers.get("Retry-After");
        if (retryAfter !== null) {
          const parsed = parseRetryAfterMs(retryAfter);
          if (parsed !== null) {
            delayMs = Math.min(parsed, maxMs);
          } else {
            delayMs = computeBackoff(attempt, baseMs, maxMs);
          }
        } else {
          delayMs = computeBackoff(attempt, baseMs, maxMs);
        }
      } else {
        delayMs = computeBackoff(attempt, baseMs, maxMs);
      }

      await delay(delayMs);
    }
  };
}

function computeBackoff(attempt: number, baseMs: number, maxMs: number): number {
  const jitter = Math.random() * 50;
  return Math.min(baseMs * Math.pow(2, attempt - 1) + jitter, maxMs);
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
