import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { resolve } from "path";

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      /**
       * @basin/basin-js is not yet published to npm.
       * We alias the import to our local fetch-based stub so the build
       * succeeds.  When the real package is published and installed, remove
       * this alias and the stub file (src/lib/basin-js-stub.ts).
       */
      "@basin/basin-js": resolve(
        __dirname,
        "src/lib/basin-js-stub.ts"
      ),
    },
  },
  server: {
    port: 5173,
    proxy: {
      // Proxy Basin auth + REST + storage endpoints so the browser app
      // can talk to a local basin-server without CORS headaches in dev.
      "/auth": "http://localhost:5432",
      "/rest": "http://localhost:5432",
      "/storage": "http://localhost:5432",
    },
  },
  define: {
    // Vite replaces these at build time; override in .env.local for prod.
    __BASIN_URL__: JSON.stringify(
      process.env.VITE_BASIN_URL ?? "http://localhost:5432"
    ),
    __BASIN_ANON_KEY__: JSON.stringify(process.env.VITE_BASIN_ANON_KEY ?? ""),
  },
});
