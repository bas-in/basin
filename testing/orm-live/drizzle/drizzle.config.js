import { defineConfig } from "drizzle-kit";

export default defineConfig({
  dialect: "postgresql",
  schema: "./schema.js",
  out: "./migrations",
  dbCredentials: {
    url: process.env.BASIN_DSN ?? "postgres://invalid.invalid:5432/none",
  },
});
