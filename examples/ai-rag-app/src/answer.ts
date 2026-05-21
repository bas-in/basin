/**
 * Answer generation — two paths:
 *
 * 1. Via basin-fn Wasm function (/fn/v1/rag-answer):
 *    When BASIN_URL and BASIN_JWT are set, the question + context are POSTed
 *    to the deployed Wasm function.  The function calls the inference API
 *    using the basin:functions/http host import (outbound HTTP).
 *
 * 2. Direct OpenAI/Anthropic call (local fallback):
 *    When basin-fn is not reachable or not yet deployed, the Node process
 *    calls the API directly.  This path is useful during development before
 *    the function is compiled and deployed.
 *
 * Both paths read API keys from environment variables — never hardcoded.
 */

import OpenAI from "openai";

const BASIN_FN_URL = process.env.BASIN_URL
  ? `${process.env.BASIN_URL.replace(/\/$/, "")}/fn/v1/rag-answer`
  : null;
const BASIN_JWT = process.env.BASIN_JWT ?? null;

/**
 * Generate an answer for `question` given retrieved `context`.
 * Tries the basin-fn path first; falls back to a direct API call.
 */
export async function answerViaFn(
  question: string,
  context: string
): Promise<string> {
  // Path 1: basin-fn Wasm function
  if (BASIN_FN_URL && BASIN_JWT) {
    try {
      return await callBasinFn(question, context);
    } catch (err) {
      console.warn("basin-fn unavailable, falling back to direct API:", err);
    }
  }

  // Path 2: direct API call
  return await directApiAnswer(question, context);
}

/**
 * Call the deployed basin-fn Wasm function at /fn/v1/rag-answer.
 *
 * The function body lives at fn/rag-answer.ts and is compiled+deployed via:
 *   basin functions deploy ./fn/rag-answer.ts --name rag-answer
 *
 * The endpoint is JWT-gated — Basin validates the token before the
 * function is even resolved (a missing/invalid JWT → 401 before fn load).
 */
async function callBasinFn(question: string, context: string): Promise<string> {
  const resp = await fetch(BASIN_FN_URL!, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${BASIN_JWT}`,
    },
    body: JSON.stringify({ question, context }),
  });

  if (!resp.ok) {
    throw new Error(`basin-fn returned ${resp.status}: ${await resp.text()}`);
  }

  const body = (await resp.json()) as { answer: string };
  return body.answer;
}

/**
 * Direct call to the inference API — no Wasm involved.
 * Tries OpenAI first (OPENAI_API_KEY), then Anthropic (ANTHROPIC_API_KEY).
 * Returns a canned message when neither key is available.
 */
async function directApiAnswer(
  question: string,
  context: string
): Promise<string> {
  const systemPrompt =
    "You are a helpful assistant. Answer the question using ONLY the provided context. " +
    "If the context doesn't contain enough information, say so. " +
    "Be concise.";

  const userPrompt = `Context:\n${context}\n\nQuestion: ${question}`;

  if (process.env.OPENAI_API_KEY) {
    return await openAiAnswer(systemPrompt, userPrompt);
  }

  if (process.env.ANTHROPIC_API_KEY) {
    return await anthropicAnswer(systemPrompt, userPrompt);
  }

  // No API key — return a stub so the demo still runs.
  return (
    "[No API key set — set OPENAI_API_KEY or ANTHROPIC_API_KEY to get real answers]\n\n" +
    `Retrieved context for "${question}":\n\n${context}`
  );
}

async function openAiAnswer(
  system: string,
  user: string
): Promise<string> {
  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  const completion = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [
      { role: "system", content: system },
      { role: "user", content: user },
    ],
    max_tokens: 512,
  });
  return completion.choices[0].message.content ?? "";
}

async function anthropicAnswer(
  system: string,
  user: string
): Promise<string> {
  // Anthropic SDK is not in devDependencies to keep the example lean.
  // We call the API directly via fetch (same approach as the basin-fn Wasm
  // function uses the http host import).
  const resp = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "x-api-key": process.env.ANTHROPIC_API_KEY!,
      "anthropic-version": "2023-06-01",
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: "claude-haiku-4-5",
      max_tokens: 512,
      system,
      messages: [{ role: "user", content: user }],
    }),
  });

  if (!resp.ok) {
    throw new Error(`Anthropic API error ${resp.status}: ${await resp.text()}`);
  }

  const body = (await resp.json()) as {
    content: Array<{ type: string; text: string }>;
  };
  return body.content
    .filter((b) => b.type === "text")
    .map((b) => b.text)
    .join("");
}
