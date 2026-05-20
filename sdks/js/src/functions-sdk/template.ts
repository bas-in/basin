/**
 * @basin/functions — example handler template.
 *
 * This file shows how to author a Basin function handler that:
 *   1. Reads a `user_id` from the request body.
 *   2. Queries the database for that user's orders.
 *   3. Returns the result as JSON.
 *
 * To deploy (once W3 ships):
 *   basin functions deploy ./template.ts
 *
 * To test locally, see `harness.test.ts` for a runWithMockHost example.
 *
 * IMPORTANT: this file is importable but is primarily documentation / a
 * copy-paste starting point.  It is not wired into the main barrel index
 * by default so it doesn't add dead code to consumer bundles.
 */

import { log, query, secret } from "./host.js";
import type { BasinHandler, BasinRequest, BasinResponse } from "./types.js";

/**
 * Example handler: given a JSON body `{ "user_id": number }`, returns the
 * caller's recent orders as a JSON array.
 *
 * Host imports used:
 *   - `secret('INTERNAL_API_KEY')` — to fetch a project secret
 *   - `query(sql, params)` — to run a parameterised SQL query
 *   - `log('info', …)` — to emit a log line
 */
const handler: BasinHandler = async (req: BasinRequest): Promise<BasinResponse> => {
  // 1. Parse and validate the request body.
  let userId: number;
  try {
    const parsed = JSON.parse(req.body) as Record<string, unknown>;
    if (typeof parsed["user_id"] !== "number") {
      return {
        status: 400,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ error: "user_id must be a number" }),
      };
    }
    userId = parsed["user_id"];
  } catch {
    return {
      status: 400,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "request body must be valid JSON" }),
    };
  }

  // 2. Optionally read a project secret.
  const _apiKey = await secret("INTERNAL_API_KEY");

  // 3. Run a parameterised SQL query (RLS applies automatically).
  log("info", `Fetching orders for user ${userId}`);
  const { rows } = await query(
    `SELECT id, total_cents, status, created_at
       FROM orders
      WHERE user_id = $1
      ORDER BY created_at DESC
      LIMIT 50`,
    [userId],
  );

  // 4. Return a JSON response.
  return {
    status: 200,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ orders: rows }),
  };
};

export default handler;
