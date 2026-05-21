/**
 * Phase 5.27.C — Basin `@vercel/postgres` e2e load harness.
 *
 * DRIVER SUBSTITUTION NOTE
 * ──────────────────────────────────────────────────────────────────────────────
 * The original Phase 5.27.A spec called for `@vercel/postgres`.  That package
 * is deprecated (see npm registry notice) and, crucially, wraps
 * `@neondatabase/serverless` which routes every connection through Neon's
 * WebSocket proxy — it cannot speak to a raw pgwire endpoint like Basin.
 *
 * Substitution: `pg` (node-postgres) v8.  This is the underlying Postgres
 * driver that production Vercel serverless functions use when they talk to
 * standard Postgres databases (including via `@neondatabase/serverless`'s
 * TCP path).  The function _shape_ — stateless handler, no persistent globals,
 * short-lived Pool lifecycle — matches a Vercel Edge/Serverless function.
 *
 * FUNCTION SHAPE
 * ──────────────────────────────────────────────────────────────────────────────
 * Each HTTP request simulates one Vercel serverless function _invocation_.
 * Like a real serverless function the handler:
 *   - Creates its own Pool (or borrows from a warm module-level pool).
 *   - Runs its queries, returns the response, and releases the client.
 *   - Does NOT rely on any per-invocation session state persisting to the next.
 *
 * The /bench endpoint fires 50 concurrent invocations × 10 queries each
 * (= 500 total queries) and returns:
 *   { "ok": true, "failures": 0, "p99_ms": <number> }
 *
 * Basin is expected to be reachable at POSTGRES_URL (default:
 *   postgres://basin@basin:5432/basin?pool_mode=transaction
 * where "basin" resolves to the basin-server service on the compose network).
 *
 * ENDPOINTS
 * ──────────────────────────────────────────────────────────────────────────────
 *   GET /health  — liveness probe; returns 200 {"status":"ok"}
 *   GET /bench   — runs 50×10 benchmark, returns results JSON
 */

'use strict';

const http = require('http');
const { Pool } = require('pg');

// ── Configuration ─────────────────────────────────────────────────────────────

const POSTGRES_URL =
  process.env.POSTGRES_URL ||
  'postgres://basin@basin:5432/basin?pool_mode=transaction';

// Strip ?pool_mode=transaction — pg driver doesn't understand it; it is parsed
// by basin-router/basin-pool on the server side.  We keep it in the DSN for
// clarity but must remove it before handing it to `pg`.
const pgUrl = new URL(POSTGRES_URL);
pgUrl.searchParams.delete('pool_mode');
const PG_CONNECTION_STRING = pgUrl.toString();

const PORT = parseInt(process.env.PORT || '3000', 10);

const CONCURRENCY   = 50;   // simulated serverless invocations
const QUERIES_EACH  = 10;   // queries per invocation
const P99_BUDGET_MS = 100;  // acceptance threshold from Phase 5.27.A

// ── Shared Pool ───────────────────────────────────────────────────────────────
// In a real Vercel deployment each cold-start creates its own pool; warm
// invocations reuse the module-level pool.  We mirror that here: one pool
// for the process lifetime, sized to allow all 50 concurrent invocations.
const pool = new Pool({
  connectionString: PG_CONNECTION_STRING,
  max: CONCURRENCY + 5,  // headroom so none of the 50 invocations starve
  idleTimeoutMillis: 10_000,
  connectionTimeoutMillis: 5_000,
});

pool.on('error', (err) => {
  console.error('[pool] background error', err.message);
});

// ── One simulated serverless invocation ──────────────────────────────────────

/**
 * Runs QUERIES_EACH queries, each in its own transaction, and returns an array
 * of per-query latencies in milliseconds.
 *
 * Uses a single pool.connect() checkout for the full invocation (cheapest
 * path; the session-state isolation guarantee comes from Basin's transaction-
 * mode pool on the server side, not from client-side reconnection).
 */
async function runInvocation(invocationId) {
  const latencies = [];
  let failure = null;

  const client = await pool.connect();
  try {
    for (let q = 0; q < QUERIES_EACH; q++) {
      const t0 = Date.now();
      try {
        // Each query is wrapped in a BEGIN/COMMIT so Basin's transaction-mode
        // pool can reclaim the physical connection after each one.
        await client.query('BEGIN');
        const result = await client.query(
          'SELECT $1::int AS invocation, $2::int AS query_idx, now() AS ts',
          [invocationId, q]
        );
        await client.query('COMMIT');

        // Verify no session-state bleed: the returned row must echo our params.
        const row = result.rows[0];
        if (row.invocation !== invocationId || row.query_idx !== q) {
          throw new Error(
            `bleed detected: expected invocation=${invocationId} query_idx=${q}, ` +
            `got invocation=${row.invocation} query_idx=${row.query_idx}`
          );
        }

        latencies.push(Date.now() - t0);
      } catch (err) {
        // Best-effort rollback on error.
        try { await client.query('ROLLBACK'); } catch (_) {}
        failure = err;
        latencies.push(Date.now() - t0);
      }
    }
  } finally {
    client.release();
  }

  return { latencies, failure };
}

// ── /bench handler ────────────────────────────────────────────────────────────

async function handleBench(res) {
  const allLatencies = [];
  let failures = 0;

  // Launch all 50 invocations concurrently.
  const results = await Promise.allSettled(
    Array.from({ length: CONCURRENCY }, (_, i) => runInvocation(i))
  );

  for (const r of results) {
    if (r.status === 'fulfilled') {
      allLatencies.push(...r.value.latencies);
      if (r.value.failure) {
        failures++;
        console.error('[bench] invocation failure:', r.value.failure.message);
      }
    } else {
      // The whole invocation threw (e.g. connection timeout).
      failures++;
      console.error('[bench] invocation rejected:', r.reason?.message ?? r.reason);
      // Fill in dummy latencies so p99 is still computed.
      for (let i = 0; i < QUERIES_EACH; i++) allLatencies.push(P99_BUDGET_MS * 10);
    }
  }

  // Compute p99.
  allLatencies.sort((a, b) => a - b);
  const p99Index = Math.ceil(allLatencies.length * 0.99) - 1;
  const p99Ms = allLatencies[Math.max(0, p99Index)] ?? 0;

  const ok = failures === 0 && p99Ms <= P99_BUDGET_MS;

  const body = JSON.stringify({
    ok,
    failures,
    p99_ms: p99Ms,
    total_queries: allLatencies.length,
    concurrency: CONCURRENCY,
    queries_each: QUERIES_EACH,
    p99_budget_ms: P99_BUDGET_MS,
  });

  res.writeHead(ok ? 200 : 500, { 'Content-Type': 'application/json' });
  res.end(body);
}

// ── HTTP server ───────────────────────────────────────────────────────────────

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);

  if (req.method === 'GET' && url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok' }));
    return;
  }

  if (req.method === 'GET' && url.pathname === '/bench') {
    try {
      await handleBench(res);
    } catch (err) {
      console.error('[server] unhandled error in /bench:', err);
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: err.message }));
    }
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not found. Available: GET /health, GET /bench\n');
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[app] listening on port ${PORT}`);
  console.log(`[app] basin DSN: ${POSTGRES_URL}`);
  console.log(`[app] concurrency=${CONCURRENCY} queries_each=${QUERIES_EACH} p99_budget=${P99_BUDGET_MS}ms`);
});

// Graceful shutdown.
process.on('SIGTERM', () => {
  console.log('[app] SIGTERM — draining pool and exiting');
  pool.end(() => {
    server.close(() => process.exit(0));
  });
});
