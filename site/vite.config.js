import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// DuckDB-WASM loads its worker + wasm from the jsDelivr CDN at runtime, so it must be
// excluded from Vite's dependency pre-bundling (otherwise Vite tries to bundle the
// worker and the wasm resolution breaks).
export default defineConfig({
  plugins: [react()],
  optimizeDeps: { exclude: ["@duckdb/duckdb-wasm"] },
});
