const BASE_MS = 1000;
const CAP_MS = 30_000;

export function backoff(attempt: number): number {
  return Math.min(BASE_MS * Math.pow(2, attempt), CAP_MS);
}
