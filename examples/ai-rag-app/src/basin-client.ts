/**
 * Thin pgwire client wrapper for the RAG app (server-side / ingest scripts).
 *
 * Basin speaks the full Postgres wire protocol, so any pg-compatible driver
 * works.  We use the `pg` (node-postgres) package here — the same approach
 * described in the basin-js design spec's `@bas-in/basin-js/pgwire` subpackage
 * (forward-spec; not yet published to npm as of Basin v0.1).
 *
 * Connection defaults match the Basin Docker quickstart
 * (docs/quickstart-docker.md): port 5432, user "basin", no password.
 * Override via environment variables.
 */

import pg from "pg";

const { Pool } = pg;

export function createBasinPool(): pg.Pool {
  return new Pool({
    host: process.env.BASIN_HOST ?? "127.0.0.1",
    port: parseInt(process.env.BASIN_PORT ?? "5432", 10),
    user: process.env.BASIN_USER ?? "basin",
    password: process.env.BASIN_PASSWORD ?? undefined,
    database: process.env.BASIN_DATABASE ?? "postgres",
    // Basin supports up to 1,000 concurrent connections with 47x less RAM
    // than Postgres — we keep the pool small for a dev example.
    max: 5,
  });
}

export type BasinPool = pg.Pool;
