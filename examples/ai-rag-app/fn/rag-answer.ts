/**
 * Basin Wasm function: rag-answer
 *
 * Deployed via:
 *   basin functions deploy ./fn/rag-answer.ts --name rag-answer
 *
 * The CLI compiles this TypeScript to a WebAssembly component (via
 * ComponentizeJS / Javy) and issues:
 *   CREATE OR REPLACE FUNCTION rag_answer() RETURNS bytea
 *   LANGUAGE javascript AS '<base64-encoded-wasm>';
 *
 * The function is then reachable at:
 *   POST /fn/v1/rag-answer
 *   Authorization: Bearer <jwt>
 *   Content-Type: application/json
 *   Body: { "question": "...", "context": "..." }
 *
 * Host imports used (WIT: crates/basin-fn/wit/basin-fn.wit):
 *   basin:functions/http  — outbound HTTP to the inference API
 *   basin:functions/secret — read OPENAI_API_KEY / ANTHROPIC_API_KEY
 *   basin:functions/log   — structured logging
 *
 * The @bas-in/functions SDK is a forward-spec (basin-js-design.md).
 * The import shape below matches the docs/functions.md authoring guide.
 *
 * NOTE: @bas-in/functions is not yet published to npm (Basin v0.1 pre-alpha).
 * The import below is the canonical form from docs/functions.md and
 * docs/tutorial.md.  Until the package ships, this file documents the
 * correct authoring pattern without being runnable via `npm install`.
 */

// @ts-expect-error — @bas-in/functions is forward-spec; not yet on npm.
import { handle, http, secret, log } from "@bas-in/functions";

interface RequestBody {
  question: string;
  context: string;
}

interface OpenAiMessage {
  role: "system" | "user" | "assistant";
  content: string;
}

interface OpenAiRequest {
  model: string;
  messages: OpenAiMessage[];
  max_tokens: number;
  stream: boolean;
}

/**
 * Handler entrypoint.
 *
 * Shape (WIT basin:functions/handler, world basin-functions-handler):
 *   export handle: func(req: request) -> result<response, string>
 *
 * The host validates the JWT before calling this function; auth.uid()
 * is available inside any SQL query.exec call.
 */
export default handle(async (req: {
  method: string;
  path: string;
  headers: [string, string][];
  body: Uint8Array;
}) => {
  log.info(`rag-answer called: ${req.method} ${req.path}`);

  // Only POST is accepted.
  if (req.method !== "POST") {
    return {
      status: 405,
      headers: [["content-type", "text/plain"]],
      body: new TextEncoder().encode("Method Not Allowed"),
    };
  }

  // Parse request body.
  let parsed: RequestBody;
  try {
    parsed = JSON.parse(new TextDecoder().decode(req.body)) as RequestBody;
  } catch {
    return {
      status: 400,
      headers: [["content-type", "text/plain"]],
      body: new TextEncoder().encode("Invalid JSON body"),
    };
  }

  const { question, context } = parsed;
  if (!question || !context) {
    return {
      status: 400,
      headers: [["content-type", "text/plain"]],
      body: new TextEncoder().encode("Missing 'question' or 'context' fields"),
    };
  }

  // Read the API key from Basin's secret store.
  // Set via: basin secrets set OPENAI_API_KEY sk-...
  // Accessed via basin:functions/secret host import.
  let apiKey: string;
  try {
    apiKey = secret.get("OPENAI_API_KEY");
  } catch {
    return {
      status: 500,
      headers: [["content-type", "text/plain"]],
      body: new TextEncoder().encode(
        "OPENAI_API_KEY secret not found — run: basin secrets set OPENAI_API_KEY <key>"
      ),
    };
  }

  // Build the inference request.
  const payload: OpenAiRequest = {
    model: "gpt-4o-mini",
    messages: [
      {
        role: "system",
        content:
          "You are a helpful assistant. Answer the question using ONLY the provided context. " +
          "Be concise and factual.",
      },
      {
        role: "user",
        content: `Context:\n${context}\n\nQuestion: ${question}`,
      },
    ],
    max_tokens: 512,
    stream: false,
  };

  // Call OpenAI via the basin:functions/http host import.
  // The host enforces the project's allowlist (SSRF protection), per-second
  // rate limit, body-size cap, and request timeout.
  const apiResp = http.fetch({
    url: "https://api.openai.com/v1/chat/completions",
    method: "POST",
    headers: [
      ["content-type", "application/json"],
      ["authorization", `Bearer ${apiKey}`],
    ],
    body: new TextEncoder().encode(JSON.stringify(payload)),
  });

  if (apiResp.status !== 200) {
    log.warn(`OpenAI API error: ${apiResp.status}`);
    return {
      status: 502,
      headers: [["content-type", "text/plain"]],
      body: new TextEncoder().encode(
        `Inference API error: ${apiResp.status}`
      ),
    };
  }

  // Parse and return the answer.
  interface OpenAiResponse {
    choices: Array<{ message: { content: string } }>;
  }
  const completion = JSON.parse(
    new TextDecoder().decode(new Uint8Array(apiResp.body))
  ) as OpenAiResponse;
  const answer = completion.choices[0]?.message?.content ?? "";

  log.info(`rag-answer: answered "${question.slice(0, 60)}…"`);

  return {
    status: 200,
    headers: [["content-type", "application/json"]],
    body: new TextEncoder().encode(JSON.stringify({ answer })),
  };
});
