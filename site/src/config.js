// Runtime configuration for the showcase site.
//
// The public bucket's HTTPS origin. Set VITE_PUBLIC_BASE_URL at build time once the
// public R2 bucket is live (e.g. https://data.yourdomain or an r2.dev URL). Until
// then everything falls back to the bundled sample, so the site is fully functional
// before any bucket exists.
export const PUBLIC_BASE_URL = (import.meta.env.VITE_PUBLIC_BASE_URL || "").replace(/\/$/, "");

// The bundled sample mart (same-origin, always works, needs no CORS). Absolute URL,
// because DuckDB-WASM's httpfs needs a scheme+host to fetch, not a root-relative path.
const ORIGIN = typeof window !== "undefined" ? window.location.origin : "";
export const SAMPLE_URL = `${ORIGIN}/sample/fct_bars_daily.parquet`;

// What the live console reads by default: the live public mart if configured, else
// the bundled sample.
export const BARS_URL = PUBLIC_BASE_URL
  ? `${PUBLIC_BASE_URL}/gold/group=bars/mart=fct_bars_daily/data.parquet`
  : SAMPLE_URL;

// Catalogue: live from the public bucket if configured, else the bundled snapshot.
export const CATALOGUE_URL = PUBLIC_BASE_URL ? `${PUBLIC_BASE_URL}/catalogue.json` : "/catalogue.json";

// Data-quality history, for the status page. These are the CONSOLIDATED files, not
// the date partitions: plain HTTP has no directory listing, so a `**/*.parquet` glob
// cannot be expanded from a browser — DuckDB fails with "Globs for generic HTTP file
// are not supported". One file at a stable path is the only thing a client can read.
export const DQ_RUNS_URL = PUBLIC_BASE_URL ? `${PUBLIC_BASE_URL}/quality/dq_runs.parquet` : null;
export const DQ_VIOLATIONS_URL = PUBLIC_BASE_URL
  ? `${PUBLIC_BASE_URL}/quality/dq_violations.parquet`
  : null;

export const GITHUB_URL = "https://github.com/s02minu/quant-data-engine";
export const STREAMLIT_URL = import.meta.env.VITE_STREAMLIT_URL || "";

export const USING_SAMPLE = !PUBLIC_BASE_URL;
