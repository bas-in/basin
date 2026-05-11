import { defineConfig } from "tsup";

// Dual CJS + ESM build. Every namespace gets its own entry so consumers
// can tree-shake (e.g. import only `@basin/basin-js/auth` in a pure
// auth bundle). The barrel `src/index.ts` re-exports everything for
// the default import.
export default defineConfig({
  entry: [
    "src/index.ts",
    "src/auth/index.ts",
    "src/postgrest/index.ts",
    "src/storage/index.ts",
    "src/realtime/index.ts",
    "src/functions/index.ts",
  ],
  format: ["esm", "cjs"],
  target: "es2022",
  dts: true,
  sourcemap: true,
  clean: true,
  splitting: false,
  treeshake: true,
  minify: false,
});
