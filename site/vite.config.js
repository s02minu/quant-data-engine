import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv } from "vite";

// DuckDB-WASM loads its worker + wasm from the jsDelivr CDN at runtime, so it must be
// excluded from Vite's dependency pre-bundling (otherwise Vite tries to bundle the
// worker and the wasm resolution breaks).
export default defineConfig(({ mode }) => {
  // A production build without the bucket configured is a build that LOOKS fine and
  // silently serves the bundled snapshot instead of the lake. That is not theoretical:
  // it shipped, and the site served an 11-day-old catalogue of 12 sources while the
  // bucket held 15, with the console querying a 190 KB sample instead of the real mart.
  // Nothing failed, nothing alerted, and the only signal was a small notice on the page.
  //
  // So the fallback stays available for local work and becomes an error for a deployed
  // build. `VITE_ALLOW_SAMPLE_BUILD=1` is the deliberate escape hatch -- explicit, so a
  // sample build is a choice someone made rather than a variable someone forgot.
  if (mode === "production" && !process.env.VITE_ALLOW_SAMPLE_BUILD) {
    const base = loadEnv(mode, process.cwd(), "VITE_").VITE_PUBLIC_BASE_URL || "";
    if (!base || /REPLACE-ME/.test(base)) {
      throw new Error(
        "VITE_PUBLIC_BASE_URL is not set for a production build, so the site would " +
          "quietly serve the bundled sample instead of the public bucket. Set it in " +
          "site/.env.production (committed, it is a public R2 origin), or pass " +
          "VITE_ALLOW_SAMPLE_BUILD=1 to build the sample-only site on purpose.",
      );
    }
  }

  return {
    plugins: [react()],
    optimizeDeps: { exclude: ["@duckdb/duckdb-wasm"] },
  };
});
