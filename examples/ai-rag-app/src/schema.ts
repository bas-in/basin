/**
 * Schema management for the RAG app.
 *
 * Creates the `documents` and `chunks` tables if they don't exist.
 *
 * SQL shapes used (verified against Basin integration tests and ADR 0003):
 *
 *   CREATE TABLE chunks (
 *     id         BIGSERIAL PRIMARY KEY,
 *     doc_id     TEXT        NOT NULL,
 *     chunk_idx  INT         NOT NULL,
 *     content    TEXT        NOT NULL,
 *     embedding  vector(1536)         -- Basin-native vector type; no pg_vector needed
 *   );
 *
 * The `embedding <-> '[...]'` ORDER BY pattern (L2 / Euclidean distance) is
 * what the engine recognises and routes through the HNSW index when one
 * exists (ADR 0003, vector_smoke integration test).  The cosine operator
 * `<=>` and negative dot-product `<#>` are also available.
 */

import type { BasinPool } from "./basin-client.js";
import { EMBED_DIM } from "./embedding.js";

export async function ensureSchema(pool: BasinPool): Promise<void> {
  const client = await pool.connect();
  try {
    // Source documents (top-level metadata).
    await client.query(`
      CREATE TABLE IF NOT EXISTS rag_documents (
        id         TEXT PRIMARY KEY,
        title      TEXT NOT NULL,
        source_url TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )
    `);

    // Chunks with embeddings.
    // Basin uses vector(N) as the column type backed by Arrow FixedSizeList<Float32>.
    // Insert with a single-quoted bracket literal: '[0.1, 0.2, ...]'
    await client.query(`
      CREATE TABLE IF NOT EXISTS rag_chunks (
        id         BIGSERIAL PRIMARY KEY,
        doc_id     TEXT        NOT NULL REFERENCES rag_documents(id),
        chunk_idx  INT         NOT NULL,
        content    TEXT        NOT NULL,
        embedding  vector(${EMBED_DIM})
      )
    `);
  } finally {
    client.release();
  }
}
