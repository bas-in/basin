/**
 * Embedding helpers.
 *
 * Calls the OpenAI embeddings API (text-embedding-3-small, 1536 dims) by
 * default, or falls back to a deterministic mock when OPENAI_API_KEY is not
 * set.  The mock is only for CI / offline testing — embeddings from the mock
 * are not semantically meaningful.
 *
 * We also accept Anthropic API keys (ANTHROPIC_API_KEY) for completions (see
 * fn/rag-answer.ts), but embeddings today require OpenAI because Anthropic does
 * not yet expose a public embeddings endpoint.
 */

import OpenAI from "openai";

/** Dimensionality of the OpenAI text-embedding-3-small model. */
export const EMBED_DIM = 1536;

/** Dimensionality used by the mock (must match table DDL). */
export const MOCK_EMBED_DIM = 1536;

let _openai: OpenAI | null = null;

function openaiClient(): OpenAI | null {
  if (!process.env.OPENAI_API_KEY) return null;
  if (!_openai) {
    _openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  }
  return _openai;
}

/**
 * Returns a unit-length float32 embedding for `text`.
 *
 * When OPENAI_API_KEY is absent, returns a deterministic mock vector that
 * preserves relative ordering for the seeded test corpus (same seed → same
 * vector → stable similarity scores).
 */
export async function embed(text: string): Promise<number[]> {
  const client = openaiClient();
  if (client) {
    const resp = await client.embeddings.create({
      model: "text-embedding-3-small",
      input: text,
    });
    return resp.data[0].embedding;
  }
  return mockEmbed(text);
}

/**
 * Batch embed — reduces API round-trips for the ingest pipeline.
 * Falls back to sequential mock calls when no API key is present.
 */
export async function embedBatch(texts: string[]): Promise<number[][]> {
  const client = openaiClient();
  if (client) {
    const resp = await client.embeddings.create({
      model: "text-embedding-3-small",
      input: texts,
    });
    // OpenAI returns embeddings in the same order as the input.
    return resp.data.map((d) => d.embedding);
  }
  return texts.map(mockEmbed);
}

/**
 * Deterministic mock embedding.
 *
 * Uses a simple hash of the text to seed a pseudo-random vector, then
 * normalises it to unit length.  Documents with the same text get the same
 * vector; distinct texts get distinct (but not semantically related) vectors.
 * This is sufficient for the retrieval unit test (which seeds a known corpus
 * and checks that the seeded chunk is returned for its own text).
 */
export function mockEmbed(text: string): number[] {
  const seed = hashCode(text);
  const vec = new Array<number>(MOCK_EMBED_DIM);
  let state = seed >>> 0;
  for (let i = 0; i < MOCK_EMBED_DIM; i++) {
    // SplitMix32-inspired step.
    state = (state + 0x9e3779b9) >>> 0;
    let z = state;
    z = ((z ^ (z >>> 16)) * 0x85ebca6b) >>> 0;
    z = ((z ^ (z >>> 13)) * 0xc2b2ae35) >>> 0;
    z = (z ^ (z >>> 16)) >>> 0;
    // Map to [-0.5, 0.5).
    vec[i] = (z / 0x100000000) - 0.5;
  }
  return normalise(vec);
}

function hashCode(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = (h * 0x01000193) >>> 0;
  }
  return h;
}

function normalise(v: number[]): number[] {
  const norm = Math.sqrt(v.reduce((acc, x) => acc + x * x, 0));
  if (norm === 0) return v;
  return v.map((x) => x / norm);
}

/**
 * Format a float32 vector as a Basin vector literal.
 *
 * Basin uses pgvector-style bracket notation: '[0.1, 0.2, ...]'
 * (ADR 0003; same syntax as pg_vector for ergonomics).
 */
export function toVectorLiteral(v: number[]): string {
  return "[" + v.map((x) => x.toFixed(8)).join(", ") + "]";
}
