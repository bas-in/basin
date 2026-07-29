/**
 * BasinError — every API call resolves to `{ data, error }` where
 * `error` is either `null` or a `BasinError` instance. The shape
 * mirrors the cloud's HTTP envelope: a stable `code`, a human-readable
 * `message`, and an optional `details` blob carrying field-level
 * validation errors or other context the cloud chose to surface.
 */

export type BasinErrorCode =
  | "network"
  | "unauthorized"
  | "forbidden"
  | "not_found"
  | "invalid_request"
  | "conflict"
  | "rate_limited"
  | "internal"
  | "unsupported"
  | "token_expired"
  | "not_implemented"
  | string;

export class BasinError extends Error {
  override readonly name = "BasinError";

  constructor(
    readonly code: BasinErrorCode,
    message: string,
    readonly status?: number,
    readonly details?: unknown,
  ) {
    super(message);
    // Restore prototype chain — required when extending Error in TS targets
    // earlier than ES2022 (kept for safety against downstream bundlers).
    Object.setPrototypeOf(this, BasinError.prototype);
  }
}
