/**
 * Apply RLS policies + create the avatars storage bucket.
 *
 * Run AFTER the initial migration (npm run db:migrate).
 * Run: npm run db:setup
 *
 * This script connects over pgwire to enable RLS on each table and install
 * policies. It is NOT a privileged RLS bypass — Basin has none. An
 * unauthenticated pgwire session has `auth.uid() IS NULL`, so once the policies
 * below are in force that same session reads zero rows from the tables it just
 * configured (see tests/integration/tests/auth_rls_uid.rs). Run this AFTER the
 * migration and BEFORE seeding, or seed from a session the policies admit.
 * In production you would scope this to a service role that has ALTER TABLE +
 * CREATE POLICY rights but not end-user read/write.
 *
 * Environment variables:
 *   DATABASE_URL — defaults to postgres://basin@localhost:5432/postgres
 *   BASIN_URL    — defaults to http://localhost:5432 (for storage bucket creation)
 */
import postgres from "postgres";
import { readFileSync } from "fs";
import { join } from "path";

const DATABASE_URL =
  process.env.DATABASE_URL ?? "postgres://basin@localhost:5432/postgres";

const BASIN_URL = process.env.BASIN_URL ?? "http://localhost:5432";

async function createAvatarsBucket() {
  /**
   * Create the "avatars" storage bucket via the basin-blob HTTP API.
   *
   * POST /storage/v1/bucket
   *
   * NOTE: basin-blob (Phase 5.17) ships in basin-server v0.1.  If your local
   * build pre-dates Phase 5.17, this call will return 404.  The rest of the
   * app still works; avatar uploads will be skipped gracefully.
   */
  try {
    const res = await fetch(`${BASIN_URL}/storage/v1/bucket`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        id: "avatars",
        name: "avatars",
        public: false,
        fileSizeLimit: 5 * 1024 * 1024, // 5 MiB
        allowedMimeTypes: ["image/jpeg", "image/png", "image/webp", "image/gif"],
      }),
    });

    if (res.ok) {
      console.log("  created storage bucket: avatars");
    } else if (res.status === 409) {
      console.log("  storage bucket 'avatars' already exists — skipping");
    } else if (res.status === 404) {
      console.warn(
        "  WARNING: /storage/v1/bucket returned 404 — " +
          "basin-blob (Phase 5.17) may not be enabled on this build. " +
          "Avatar uploads will be unavailable."
      );
    } else {
      const body = await res.text();
      console.warn(`  WARNING: bucket creation failed (${res.status}): ${body}`);
    }
  } catch (err) {
    console.warn(
      "  WARNING: could not reach storage API — " +
        "check that basin-server is running and BASIN_URL is correct.",
      err
    );
  }
}

async function main() {
  console.log(`Connecting to Basin: ${DATABASE_URL}`);
  const sql = postgres(DATABASE_URL, { max: 1 });

  const rlsFile = join(
    import.meta.dirname ?? __dirname,
    "../drizzle/0002_rls.sql"
  );
  console.log("Applying RLS policies…");

  const content = readFileSync(rlsFile, "utf8");

  // Apply each statement individually so we get helpful per-statement errors.
  // We strip comments and split on semicolons.
  const stmts = content
    .split("\n")
    .filter((l) => !l.trim().startsWith("--"))
    .join("\n")
    .split(";")
    .map((s) => s.trim())
    .filter(Boolean);

  for (const stmt of stmts) {
    try {
      await sql.unsafe(stmt);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      // "already exists" is fine — policies are idempotent on re-run
      if (msg.includes("already exists")) {
        console.log(`  skipping (already exists): ${stmt.slice(0, 60)}…`);
      } else {
        console.error(`  ERROR applying: ${stmt.slice(0, 80)}`);
        console.error("  ", msg);
      }
    }
  }

  await sql.end();

  console.log("Creating storage buckets…");
  await createAvatarsBucket();

  console.log("RLS setup complete.");
}

main().catch((err) => {
  console.error("Setup failed:", err);
  process.exit(1);
});
