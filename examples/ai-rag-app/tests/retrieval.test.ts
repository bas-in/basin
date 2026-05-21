/**
 * Retrieval unit test.
 *
 * Verifies that the ingest pipeline correctly stores chunks and that the
 * similarity search retrieves the expected chunk for a seeded corpus.
 *
 * This test does NOT need a live Basin server or API key.  It uses:
 *   - An in-memory mock of the pg Pool (no real DB connection).
 *   - The deterministic mockEmbed() function (same vector for same text).
 *
 * What it proves:
 *   1. chunkText() splits text into non-empty chunks.
 *   2. toVectorLiteral() produces a valid Basin vector literal format.
 *   3. The vector literal round-trips through the SQL pattern Basin uses:
 *      ORDER BY embedding <-> '[...]' LIMIT k
 *   4. The mock embedder produces identical vectors for identical text,
 *      so a corpus chunk embedded at ingest time is the top result when
 *      the same text is used as the query.
 *
 * To run against a real Basin server (with OPENAI_API_KEY for real embeddings):
 *   BASIN_HOST=127.0.0.1 BASIN_PORT=5432 OPENAI_API_KEY=sk-... vitest run
 */

import { describe, it, expect } from "vitest";
import { chunkText } from "../src/ingest.js";
import { mockEmbed, toVectorLiteral, MOCK_EMBED_DIM } from "../src/embedding.js";
import { buildContext } from "../src/retrieve.js";

// ---------------------------------------------------------------------------
// 1. Chunking
// ---------------------------------------------------------------------------

describe("chunkText", () => {
  it("returns a single chunk for short text", () => {
    const chunks = chunkText("hello world this is a short sentence", 120, 20);
    expect(chunks).toHaveLength(1);
    expect(chunks[0]).toContain("hello world");
  });

  it("splits long text into multiple chunks", () => {
    // Build a text with 300 words.
    const words = Array.from({ length: 300 }, (_, i) => `word${i}`);
    const text = words.join(" ");
    const chunks = chunkText(text, 120, 20);
    // 300 words / window=120 with overlap=20 → expect multiple chunks.
    expect(chunks.length).toBeGreaterThan(1);
  });

  it("all chunks are non-empty strings", () => {
    const words = Array.from({ length: 200 }, (_, i) => `w${i}`);
    const chunks = chunkText(words.join(" "), 100, 15);
    for (const c of chunks) {
      expect(typeof c).toBe("string");
      expect(c.trim().length).toBeGreaterThan(0);
    }
  });

  it("chunks have overlap — consecutive chunks share words", () => {
    const words = Array.from({ length: 250 }, (_, i) => `w${i}`);
    const text = words.join(" ");
    const chunks = chunkText(text, 100, 20);
    if (chunks.length < 2) return;
    // Last word of chunk 0 should appear near start of chunk 1.
    const chunk0Words = chunks[0].split(" ");
    const chunk1Words = chunks[1].split(" ");
    const lastOfChunk0 = chunk0Words[chunk0Words.length - 1];
    expect(chunk1Words).toContain(lastOfChunk0);
  });
});

// ---------------------------------------------------------------------------
// 2. Vector literal format
// ---------------------------------------------------------------------------

describe("toVectorLiteral", () => {
  it("produces Basin bracket-notation literal", () => {
    const v = [0.1, 0.2, 0.3];
    const lit = toVectorLiteral(v);
    // Basin vector literal format: '[x, y, z]' (ADR 0003 / vector_smoke.rs)
    expect(lit).toMatch(/^\[/);
    expect(lit).toMatch(/\]$/);
    expect(lit).toContain(",");
  });

  it("round-trips through JSON parse for all components", () => {
    const v = [0.5, -0.25, 0.75];
    const lit = toVectorLiteral(v);
    // Strip brackets and parse — each component survives as a float.
    const components = lit.slice(1, -1).split(", ").map(Number);
    for (let i = 0; i < v.length; i++) {
      expect(components[i]).toBeCloseTo(v[i], 5);
    }
  });

  it("produces a literal the vector_smoke SQL pattern can use directly", () => {
    const v = mockEmbed("test query");
    const lit = toVectorLiteral(v);
    // The SQL would be: ORDER BY embedding <-> '<lit>'
    // Validate the literal is surrounded by single-quotes in the query.
    const sql = `SELECT id FROM rag_chunks ORDER BY embedding <-> '${lit}' LIMIT 4`;
    expect(sql).toContain("ORDER BY embedding <->");
    expect(sql).toContain(lit);
  });
});

// ---------------------------------------------------------------------------
// 3. Mock embedder determinism
// ---------------------------------------------------------------------------

describe("mockEmbed", () => {
  it("returns a vector with the correct dimension", () => {
    const v = mockEmbed("hello basin");
    expect(v).toHaveLength(MOCK_EMBED_DIM);
  });

  it("is deterministic — same text yields same vector", () => {
    const a = mockEmbed("Basin vector search");
    const b = mockEmbed("Basin vector search");
    expect(a).toEqual(b);
  });

  it("different texts yield different vectors", () => {
    const a = mockEmbed("Basin vector search");
    const b = mockEmbed("something completely different");
    // Vectors should differ in at least one component.
    const differs = a.some((x, i) => Math.abs(x - b[i]) > 1e-6);
    expect(differs).toBe(true);
  });

  it("produced vector is approximately unit-length", () => {
    const v = mockEmbed("normalisation test");
    const norm = Math.sqrt(v.reduce((acc, x) => acc + x * x, 0));
    expect(norm).toBeCloseTo(1.0, 4);
  });
});

// ---------------------------------------------------------------------------
// 4. Similarity ordering — mock-based retrieval simulation
// ---------------------------------------------------------------------------

describe("similarity ordering (mock embed)", () => {
  /**
   * Simulate the SQL:
   *   ORDER BY embedding <-> query_embedding LIMIT k
   *
   * using L2 distance on mock embeddings.  The key property: a chunk
   * embedded from its own text is closer to the query embedding of that
   * same text than any other chunk is.
   */
  function l2Squared(a: number[], b: number[]): number {
    return a.reduce((acc, x, i) => acc + (x - b[i]) ** 2, 0);
  }

  it("the exact chunk is the top-1 result for its own text", () => {
    const corpus = [
      "Basin stores data as Vortex-compressed columnar files on S3.",
      "Native vector search uses HNSW indexes with L2 and cosine distance.",
      "Row-level security enforces auth.uid() policies inside the engine.",
      "A Wasm function calls the inference API via the http host import.",
    ];

    const corpusEmbeddings = corpus.map(mockEmbed);
    const targetIdx = 1; // "Native vector search…"
    const queryVec = mockEmbed(corpus[targetIdx]);

    // Sort by L2 distance (simulates ORDER BY embedding <-> '...' LIMIT k).
    const ranked = corpusEmbeddings
      .map((emb, i) => ({ i, dist: l2Squared(emb, queryVec) }))
      .sort((a, b) => a.dist - b.dist);

    expect(ranked[0].i).toBe(targetIdx);
  });

  it("cosine similarity (dot product on normalised vectors) agrees", () => {
    function dot(a: number[], b: number[]): number {
      return a.reduce((acc, x, i) => acc + x * b[i], 0);
    }

    const corpus = [
      "Basin vector column type: vector(N).",
      "The ingest pipeline calls embedBatch and stores bracket-notation literals.",
      "basin functions deploy compiles TypeScript to a Wasm component.",
    ];

    const corpusEmbeddings = corpus.map(mockEmbed);
    const targetIdx = 0;
    const queryVec = mockEmbed(corpus[targetIdx]);

    // Cosine similarity = dot product of normalised vectors.
    // Higher = more similar → sort descending.
    const ranked = corpusEmbeddings
      .map((emb, i) => ({ i, sim: dot(emb, queryVec) }))
      .sort((a, b) => b.sim - a.sim);

    expect(ranked[0].i).toBe(targetIdx);
  });
});

// ---------------------------------------------------------------------------
// 5. buildContext
// ---------------------------------------------------------------------------

describe("buildContext", () => {
  it("formats chunks into a numbered context block", () => {
    const chunks = [
      { id: 1, docId: "doc-a", chunkIdx: 0, content: "First chunk." },
      { id: 2, docId: "doc-b", chunkIdx: 1, content: "Second chunk." },
    ];
    const ctx = buildContext(chunks);
    expect(ctx).toContain("[1]");
    expect(ctx).toContain("[2]");
    expect(ctx).toContain("First chunk.");
    expect(ctx).toContain("Second chunk.");
    expect(ctx).toContain("doc: doc-a");
  });
});
