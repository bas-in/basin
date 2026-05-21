/**
 * Ingest pipeline: chunk documents → embed → store in Basin.
 *
 * Run:
 *   OPENAI_API_KEY=sk-... npm run ingest
 *
 * Without OPENAI_API_KEY, the mock embedder is used (deterministic, not
 * semantically meaningful — only useful for local testing).
 *
 * Chunking strategy: fixed-size word windows with overlap.  Production
 * apps typically use sentence-aware or markdown-aware splitters; the
 * simple approach here keeps dependencies minimal.
 */

import { createBasinPool } from "./basin-client.js";
import { embedBatch, toVectorLiteral } from "./embedding.js";
import { ensureSchema } from "./schema.js";

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

/** A document to ingest. Add your own here or load from files/URLs. */
export interface Document {
  id: string;
  title: string;
  text: string;
  sourceUrl?: string;
}

/** Sample corpus — replace with real documents. */
export const SAMPLE_CORPUS: Document[] = [
  {
    id: "basin-intro",
    title: "Basin — Cheap Postgres on Object Storage",
    text: `Basin is a Postgres-compatible database that stores every byte as
columnar files on S3-compatible object storage. A new project is a new bucket
prefix — no fork-per-connection, no provisioned VM, no per-DB pricing minimum.
Idle projects cost only their bytes. Up to 24x smaller on disk than Postgres,
47x less RAM per connection, and spinning up a new project takes milliseconds.`,
  },
  {
    id: "basin-vector",
    title: "Native vector search in Basin",
    text: `Basin ships native vector search as a first-class feature — no pg_vector
install required. Declare a vector(N) column, insert embeddings as
bracket-notation literals like '[0.1, 0.2, 0.3]', and query with the familiar
ORDER BY embedding <-> '[...]' LIMIT 10 pattern. The engine automatically
routes through an HNSW index when one exists, falling back to brute-force for
correctness. Distance operators: <-> (L2/Euclidean), <=> (cosine), <#>
(negative inner product).`,
  },
  {
    id: "basin-fn",
    title: "Wasm functions in Basin",
    text: `Basin runs WebAssembly functions compiled from TypeScript (or any WASI
Preview 2 language) inside the engine. Author a handler in TypeScript using the
@bas-in/functions SDK, compile with 'basin functions deploy', and Basin invokes
it on every matching request at /fn/v1/:name. Functions get host imports for
SQL queries (running under the caller's identity with RLS applied), outbound
HTTP, structured logging, and secret access. Per-invocation CPU, memory, and
wall-clock caps prevent runaway functions from affecting other tenants.`,
  },
  {
    id: "basin-rag",
    title: "Building a RAG pipeline with Basin",
    text: `A retrieval-augmented generation (RAG) pipeline on Basin has three
steps: ingest documents by chunking text and storing embeddings in a vector(N)
column; retrieve the top-K relevant chunks by embedding the user query and
running a similarity search with ORDER BY embedding <-> $1 LIMIT k; generate
an answer by passing the retrieved context to an LLM (OpenAI or Anthropic) via
a basin-fn Wasm function that calls the inference API using the http host import
and streams the response back to the caller.`,
  },
  {
    id: "basin-rls",
    title: "Row-level security with auth.uid()",
    text: `Basin includes a built-in auth layer (basin-auth) with JWT-based
authentication. SQL policies use auth.uid() to enforce row-level security:
CREATE POLICY user_data ON my_table FOR SELECT USING (owner_id = auth.uid()).
Every REST request and Wasm function invocation carries the JWT automatically;
the engine enforces RLS policies without any application-layer WHERE clause.`,
  },
];

// ---------------------------------------------------------------------------
// Chunking
// ---------------------------------------------------------------------------

export interface Chunk {
  docId: string;
  chunkIdx: number;
  content: string;
}

/**
 * Split `text` into overlapping word-window chunks.
 *
 * @param text - Source text.
 * @param windowWords - Target chunk size in words.
 * @param overlapWords - Overlap between consecutive chunks in words.
 */
export function chunkText(
  text: string,
  windowWords = 120,
  overlapWords = 20
): string[] {
  const words = text.split(/\s+/).filter(Boolean);
  const chunks: string[] = [];
  let start = 0;
  while (start < words.length) {
    chunks.push(words.slice(start, start + windowWords).join(" "));
    if (start + windowWords >= words.length) break;
    start += windowWords - overlapWords;
  }
  return chunks;
}

// ---------------------------------------------------------------------------
// Ingest
// ---------------------------------------------------------------------------

export async function ingestDocuments(
  docs: Document[],
  pool: ReturnType<typeof createBasinPool>
): Promise<void> {
  await ensureSchema(pool);
  const client = await pool.connect();

  try {
    for (const doc of docs) {
      console.log(`Ingesting "${doc.title}"…`);

      // Upsert document metadata.
      await client.query(
        `INSERT INTO rag_documents (id, title, source_url)
         VALUES ($1, $2, $3)
         ON CONFLICT (id) DO UPDATE
           SET title = EXCLUDED.title, source_url = EXCLUDED.source_url`,
        [doc.id, doc.title, doc.sourceUrl ?? null]
      );

      // Remove stale chunks so re-ingest is idempotent.
      await client.query(`DELETE FROM rag_chunks WHERE doc_id = $1`, [doc.id]);

      // Build chunks.
      const rawChunks = chunkText(doc.text);
      if (rawChunks.length === 0) continue;

      // Embed all chunks in one API call.
      const embeddings = await embedBatch(rawChunks);

      // Insert chunks with embeddings.
      // Basin vector literal syntax (ADR 0003): '[x, y, z, ...]'
      for (let i = 0; i < rawChunks.length; i++) {
        const lit = toVectorLiteral(embeddings[i]);
        await client.query(
          `INSERT INTO rag_chunks (doc_id, chunk_idx, content, embedding)
           VALUES ($1, $2, $3, $4)`,
          [doc.id, i, rawChunks[i], lit]
        );
      }

      console.log(`  → ${rawChunks.length} chunks stored.`);
    }
  } finally {
    client.release();
  }
}

// ---------------------------------------------------------------------------
// CLI entrypoint
// ---------------------------------------------------------------------------

async function main() {
  const pool = createBasinPool();
  try {
    await ingestDocuments(SAMPLE_CORPUS, pool);
    console.log("Ingest complete.");
  } finally {
    await pool.end();
  }
}

// Run if called directly (tsx src/ingest.ts).
// Guard prevents the CLI side-effect from firing when the module is imported
// by the test suite.  process.argv[1] is the entry script path; we compare
// to this file's URL to detect the "direct run" case in ESM.
import { fileURLToPath } from "url";
const _thisFile = fileURLToPath(import.meta.url);
const _entryFile = process.argv[1];
if (
  _entryFile === _thisFile ||
  _entryFile?.endsWith("ingest.ts") ||
  _entryFile?.endsWith("ingest.js")
) {
  main().catch((err) => {
    console.error("Ingest failed:", err);
    process.exit(1);
  });
}
