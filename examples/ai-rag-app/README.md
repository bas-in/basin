# Basin AI/RAG Reference App

A minimal but complete retrieval-augmented generation (RAG) pipeline built on
Basin's native vector search and Wasm function surface.  Demonstrates:

- **Document chunking + embedding** — fixed-size word-window chunking, batched
  OpenAI embeddings, stored in a `vector(1536)` Basin column.
- **Vector similarity retrieval** — `ORDER BY embedding <=> '...' LIMIT k` via
  Basin's native `<=>` (cosine) and `<->` (L2) operators; HNSW index used
  automatically when present (ADR 0003).
- **basin-fn Wasm function** — `fn/rag-answer.ts` receives the question,
  retrieves context via `query.exec`, calls the inference API via the
  `basin:functions/http` host import, and returns a JSON answer.
- **React/Vite UI** — minimal chat interface that calls `/fn/v1/rag-answer`.
- **CLI query loop** — `npm run query` for terminal use without the UI.

---

## What this demonstrates vs what is pending

| Feature | Status | Notes |
|---|---|---|
| `vector(N)` column DDL | Verified | Confirmed in `vector_smoke.rs`, ADR 0003 |
| `ORDER BY embedding <-> '...'` (L2) | Verified | Used in integration tests |
| `ORDER BY embedding <=> '...'` (cosine) | Verified | ADR 0003 / README |
| HNSW index auto-routing | Verified | `vector_planner.rs` routes when sidecar exists |
| basin-fn `@bas-in/functions` SDK import | **Forward-spec** | Package not yet on npm (Basin v0.1 pre-alpha); `fn/rag-answer.ts` is the correct authoring shape from `docs/functions.md` but cannot be `npm install`-ed yet |
| `basin functions deploy` CLI | **Forward-spec** | `basin-cli` not yet released; see `docs/basin-cli-design.md` |
| `@bas-in/basin-js` JS client | **Forward-spec** | Not yet on npm; we use `pg` (node-postgres) directly over pgwire |
| Streaming LLM responses | Not implemented | `http.fetch` in basin-fn is non-streaming; streaming would require SSE support in the fn runtime |

---

## Prerequisites

- **Node.js 20+** and **npm 10+**
- A running Basin server (see below)
- An OpenAI API key (`OPENAI_API_KEY`) _or_ Anthropic API key
  (`ANTHROPIC_API_KEY`) for real answers.  Without either key a stub is
  returned and the ingest uses deterministic mock embeddings.

---

## Start Basin

### Quickest path — single Docker container

```sh
docker run --rm \
  -p 5432:5432 \
  -v basin-data:/var/basin \
  --name basin \
  ghcr.io/bas-in/basin-server:latest
```

Or build from the repo root (see `docs/quickstart-docker.md`):

```sh
docker build -t basin-server .
docker run --rm -p 5432:5432 -v basin-data:/var/basin basin-server
```

Basin is ready when you see:
```
INFO basin_server: pgwire listener is accept-ready bind=0.0.0.0:5432
```

### Dev stack (Postgres catalog + MinIO)

```sh
bash dev/scripts/up.sh
# pgwire on localhost:5533, user alice
```

Set `BASIN_PORT=5533 BASIN_USER=alice` when using the dev stack.

---

## Environment variables

| Variable | Default | Required | Description |
|---|---|---|---|
| `OPENAI_API_KEY` | — | For real embeddings | OpenAI API key for `text-embedding-3-small` + `gpt-4o-mini` |
| `ANTHROPIC_API_KEY` | — | Alternative to OpenAI for generation only | Calls `claude-haiku-4-5` directly |
| `BASIN_HOST` | `127.0.0.1` | No | Basin server hostname |
| `BASIN_PORT` | `5432` | No | pgwire port (`5433` for the dev `cargo run` binary) |
| `BASIN_USER` | `basin` | No | Project user (`alice` on dev stack) |
| `BASIN_PASSWORD` | — | No | Password (none for default dev config) |
| `BASIN_DATABASE` | `postgres` | No | Database name |
| `BASIN_URL` | — | To use basin-fn UI path | `http://127.0.0.1:5432` — enables the `/fn/v1/rag-answer` route |
| `BASIN_JWT` | — | With `BASIN_URL` | JWT from `POST /auth/v1/signin` |
| `VITE_BASIN_FN_URL` | `/fn/v1/rag-answer` | No | Override for the UI's function endpoint |
| `VITE_BASIN_JWT` | — | No | JWT for the UI |

Never hardcode API keys.  Use a `.env` file (gitignored) or shell exports.

---

## Run

```sh
cd examples/ai-rag-app
npm install

# 1. Create schema + ingest the sample corpus.
OPENAI_API_KEY=sk-... npm run ingest

# 2a. CLI query loop (no browser needed).
OPENAI_API_KEY=sk-... npm run query

# 2b. OR start the Vite dev server for the chat UI.
OPENAI_API_KEY=sk-... npm run dev
# Open http://localhost:5174
```

---

## Deploy the basin-fn Wasm function

Once `basin-cli` is available (see `docs/basin-cli-design.md`):

```sh
# Set the OpenAI key as a project secret (basin:functions/secret host import).
basin secrets set OPENAI_API_KEY sk-...

# Compile fn/rag-answer.ts → Wasm component and upload to the engine catalog.
basin functions deploy ./fn/rag-answer.ts --name rag-answer

# Invoke it directly.
curl -X POST http://127.0.0.1:5432/fn/v1/rag-answer \
  -H "Authorization: Bearer $JWT" \
  -H "Content-Type: application/json" \
  -d '{"question":"What is Basin?","context":""}'
```

The function is mounted at `ANY /fn/v1/rag-answer` (JWT-gated; 401 on missing
or invalid token).  See `docs/functions.md` for the full ABI reference.

---

## Run tests

```sh
npm test
```

The test suite does **not** require a live Basin server or API key.  It uses:

- A deterministic mock embedder (`mockEmbed()` — same text → same unit-length
  vector; not semantically meaningful).
- In-memory similarity comparisons that simulate the SQL
  `ORDER BY embedding <-> '...' LIMIT k` pattern.

Key assertions:
- `chunkText` splits and overlaps correctly.
- `toVectorLiteral` produces valid Basin bracket-notation literals.
- The mock embedder is deterministic and produces unit-length vectors.
- A chunk's own text is the top-1 cosine/L2 result for its own query.

---

## Architecture

```
User question
      │
      ▼
 embed(question)           ← OpenAI text-embedding-3-small (or mock)
      │
      ▼
 SELECT id, content        ← Basin pgwire (pg driver → port 5432)
 FROM rag_chunks
 ORDER BY embedding <=> $vec
 LIMIT 4                   ← HNSW index when sidecar exists; brute-force fallback
      │
      ▼
 context = top-4 chunks
      │
      ▼
 POST /fn/v1/rag-answer    ← basin-fn Wasm function (JWT-gated)
   { question, context }
      │  basin:functions/http host import
      ▼
 OpenAI /v1/chat/completions
      │
      ▼
 { answer: "..." }
      │
      ▼
 UI / CLI output
```

---

## File layout

```
examples/ai-rag-app/
  src/
    basin-client.ts   — pg Pool connecting to Basin pgwire
    embedding.ts      — OpenAI embed() / deterministic mockEmbed() fallback
    schema.ts         — CREATE TABLE rag_documents / rag_chunks (vector(1536))
    ingest.ts         — chunk → embed → INSERT pipeline + CLI entrypoint
    retrieve.ts       — ORDER BY embedding <=> '...' LIMIT k retrieval
    answer.ts         — basin-fn call or direct API fallback
    query-cli.ts      — interactive CLI query loop
    App.tsx           — minimal React chat UI
    main.tsx          — Vite entry point
    index.css
  fn/
    rag-answer.ts     — basin-fn Wasm function (TypeScript, @bas-in/functions)
  tests/
    retrieval.test.ts — unit tests (no Basin/API key needed)
  index.html          — Vite HTML entry
  package.json
  tsconfig.json
  vite.config.ts
  README.md
```

---

## SQL shapes used

All SQL was verified against Basin's integration tests and ADR 0003.

```sql
-- Schema (src/schema.ts)
CREATE TABLE IF NOT EXISTS rag_documents (
  id TEXT PRIMARY KEY, title TEXT NOT NULL, source_url TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS rag_chunks (
  id BIGSERIAL PRIMARY KEY,
  doc_id TEXT NOT NULL REFERENCES rag_documents(id),
  chunk_idx INT NOT NULL,
  content TEXT NOT NULL,
  embedding vector(1536)          -- Basin native type; no pg_vector needed
);

-- Ingest (src/ingest.ts)
INSERT INTO rag_chunks (doc_id, chunk_idx, content, embedding)
VALUES ($1, $2, $3, '[0.01234, -0.05678, ...]');   -- bracket-notation literal

-- Retrieval (src/retrieve.ts)
SELECT id, doc_id, chunk_idx, content
FROM rag_chunks
ORDER BY embedding <=> '[0.01234, -0.05678, ...]'  -- cosine distance
LIMIT 4;
```

Distance operators (ADR 0003):
- `<->` — Euclidean (L2) distance
- `<=>` — cosine distance
- `<#>` — negative inner product (dot product)
