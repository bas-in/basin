/**
 * Apply pending SQL migrations against a running Basin instance.
 *
 * Basin speaks pgwire v3 so `postgres` (postgres.js) connects directly.
 * Run: npm run db:migrate
 *
 * Environment variables:
 *   DATABASE_URL — defaults to postgres://basin@localhost:5432/postgres
 */
import postgres from "postgres";
import { readFileSync } from "fs";
import { join } from "path";

const DATABASE_URL =
  process.env.DATABASE_URL ?? "postgres://basin@localhost:5432/postgres";

async function main() {
  console.log(`Connecting to Basin: ${DATABASE_URL}`);
  const sql = postgres(DATABASE_URL, { max: 1 });

  const migrations = ["0001_initial.sql"];
  const migrationsDir = join(import.meta.dirname ?? __dirname, "../drizzle");

  for (const file of migrations) {
    const filePath = join(migrationsDir, file);
    console.log(`Applying migration: ${file}`);
    const content = readFileSync(filePath, "utf8");
    await sql.unsafe(content);
    console.log(`  done: ${file}`);
  }

  await sql.end();
  console.log("Migrations complete.");
}

main().catch((err) => {
  console.error("Migration failed:", err);
  process.exit(1);
});
