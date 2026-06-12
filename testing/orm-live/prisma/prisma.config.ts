// Prisma 7 config — connection URLs live here now (datasource.url was removed
// from schema files in Prisma 7). The CLI loads this itself; the runtime
// client gets its connection through the @prisma/adapter-pg driver adapter in
// suite.mjs.
import { defineConfig } from "prisma/config";

export default defineConfig({
  schema: "schema.prisma",
  migrations: { path: "migrations" },
  datasource: {
    // Placeholder keeps offline commands (validate/generate) working.
    url: process.env.BASIN_DSN ?? "postgres://placeholder.invalid:5432/none",
  },
});
