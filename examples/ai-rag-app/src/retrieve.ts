/**
 * Retrieval: embed query → vector similarity search → top-K chunks.
 *
 * SQL pattern used (verified against vector_smoke.rs and ADR 0003):
 *
 *   SELECT id, doc_id, chunk_idx, content
 *   FROM rag_chunks
 *   ORDER BY embedding <-> '[...]'
 *   LIMIT $k
 *
 * The `<->` operator is L2/Euclidean distance (pgvector syntax, Basin-native
 * implementation — no pg_vector extension required).  For cosine similarity
 * use `<=>`, for negative dot-product use `<#>`.
 *
 * The engine routes this pattern through the HNSW index when one is present
 * (per-file HNSW sidecars built by basin-storage at write time).  Without an
 * index, the engine falls back to brute-force — both paths return the same
 * top-K for the same query vector.
 */

import type { BasinPool } from "./basin-client.js";
import { embed, toVectorLiteral } from "./embedding.js";

export interface RetrievedChunk {
  id: number;
  docId: string;
  chunkIdx: number;
  content: string;
}

/**
 * Retrieve the top-K most similar chunks for `query`.
 *
 * @param query  - The user's natural-language question.
 * @param pool   - Basin connection pool.
 * @param topK   - Number of chunks to return (default 4).
 * @param metric - Distance metric: "l2" | "cosine" | "dot" (default "cosine").
 */
export async function retrieveChunks(
  query: string,
  pool: BasinPool,
  topK = 4,
  metric: "l2" | "cosine" | "dot" = "cosine"
): Promise<RetrievedChunk[]> {
  const embedding = await embed(query);
  const lit = toVectorLiteral(embedding);

  // Operator mapping (ADR 0003 / README.md):
  //   <->  L2 / Euclidean distance
  //   <=>  cosine distance
  //   <#>  negative inner product (dot product)
  const op = metric === "l2" ? "<->" : metric === "cosine" ? "<=>" : "<#>";

  const client = await pool.connect();
  try {
    const result = await client.query<{
      id: number;
      doc_id: string;
      chunk_idx: number;
      content: string;
    }>(
      `SELECT id, doc_id, chunk_idx, content
       FROM rag_chunks
       ORDER BY embedding ${op} '${lit}'
       LIMIT $1`,
      [topK]
    );
    return result.rows.map((r) => ({
      id: Number(r.id),
      docId: r.doc_id,
      chunkIdx: r.chunk_idx,
      content: r.content,
    }));
  } finally {
    client.release();
  }
}

/**
 * Format retrieved chunks into a context block for the LLM prompt.
 */
export function buildContext(chunks: RetrievedChunk[]): string {
  return chunks
    .map(
      (c, i) =>
        `[${i + 1}] (doc: ${c.docId}, chunk ${c.chunkIdx})\n${c.content}`
    )
    .join("\n\n");
}
