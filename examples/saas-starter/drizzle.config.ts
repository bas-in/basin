import { defineConfig } from "drizzle-kit";

/**
 * Drizzle Kit configuration.
 *
 * Basin speaks the Postgres wire protocol (pgwire v3) on port 5432.
 * The `postgres` driver (postgres.js) connects over TCP just like it would
 * to a real Postgres instance — Basin is transparent at this layer.
 *
 * Connection string format: postgres://<user>@<host>:<port>/<dbname>
 * For the default dev Docker setup the user is "basin" and no password is
 * needed (no basin-auth in the simple quickstart config).
 */
export default defineConfig({
  schema: "./src/lib/schema.ts",
  out: "./drizzle",
  dialect: "postgresql",
  dbCredentials: {
    url:
      process.env.DATABASE_URL ??
      "postgres://basin@localhost:5432/postgres",
  },
});
