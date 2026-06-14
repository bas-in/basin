/**
 * Typed errors mirroring basin-rest's error envelope.
 *
 * Every failed request returns `{ "code": "...", "message": "..." }` with an
 * appropriate HTTP status (crates/basin-rest/src/errors.rs). `code` is the
 * stable contract; `message` is human-readable and not promised to be stable.
 */

/** Stable error codes emitted by `basin-rest` (errors.rs `ErrorCode::as_str`). */
export const ERROR_CODES = [
  "E_UNAUTHENTICATED",
  "E_FORBIDDEN",
  "E_NOT_FOUND",
  "E_INVALID_REQUEST",
  "E_RATE_LIMITED",
  "E_ENGINE_UNSUPPORTED",
  "E_INTERNAL",
  "E_EMAIL_DISABLED",
  "E_REVOKED_TOKEN",
] as const;

export type KnownErrorCode = (typeof ERROR_CODES)[number];

/**
 * An error code from the server. Known codes are typed; unknown codes pass
 * through as plain strings so a newer server doesn't break an older SDK.
 */
export type BasinErrorCode = KnownErrorCode | (string & {});

/** Error thrown for any non-2xx response from a Basin server. */
export class BasinApiError extends Error {
  /** Stable error code from the response body (`E_*`). */
  readonly code: BasinErrorCode;
  /** HTTP status of the response. */
  readonly status: number;
  /**
   * 5-character Postgres SQLSTATE code (e.g. `"23505"` for a unique
   * violation), present for SQL-layer errors; `undefined` otherwise.
   */
  readonly sqlstate?: string;

  constructor(
    code: BasinErrorCode,
    message: string,
    status: number,
    sqlstate?: string,
  ) {
    super(message);
    this.name = "BasinApiError";
    this.code = code;
    this.status = status;
    this.sqlstate = sqlstate;
  }
}

/**
 * Parse a failed `Response` into a `BasinApiError`.
 *
 * Falls back to `E_UNKNOWN` when the body isn't the standard envelope (e.g.
 * a proxy error page or the realtime upgrade handler's plain-text 401s).
 */
export async function errorFromResponse(res: Response): Promise<BasinApiError> {
  let code: BasinErrorCode = "E_UNKNOWN";
  let message = `HTTP ${res.status}`;
  let sqlstate: string | undefined;
  try {
    const text = await res.text();
    if (text.length > 0) {
      try {
        const body = JSON.parse(text) as {
          code?: unknown;
          message?: unknown;
          sqlstate?: unknown;
        };
        if (typeof body.code === "string") code = body.code;
        if (typeof body.message === "string") message = body.message;
        else message = text;
        if (typeof body.sqlstate === "string") sqlstate = body.sqlstate;
      } catch {
        message = text;
      }
    }
  } catch {
    // Body unreadable; keep the defaults.
  }
  return new BasinApiError(code, message, res.status, sqlstate);
}

/** True when `body` looks like the basin-rest error envelope. */
export function isErrorEnvelope(
  body: unknown,
): body is { code: string; message: string } {
  return (
    typeof body === "object" &&
    body !== null &&
    typeof (body as { code?: unknown }).code === "string" &&
    (body as { code: string }).code.startsWith("E_") &&
    typeof (body as { message?: unknown }).message === "string"
  );
}
