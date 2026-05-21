import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5174,
    proxy: {
      // Proxy basin function calls to avoid CORS in dev
      "/fn": {
        target: process.env.BASIN_URL ?? "http://127.0.0.1:5433",
        changeOrigin: true,
      },
      "/rest": {
        target: process.env.BASIN_URL ?? "http://127.0.0.1:5433",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "dist",
  },
  test: {
    environment: "node",
  },
});
