# Orchestration (Dagster) — Phase 7

Registry-driven Dagster assets over the batch pipeline. **Local/dev-only by design**
(ROADMAP §7): the 3.7 GB VPS keeps its lightweight `scripts/maintain.sh` cron as the
*production* runner, while this is the orchestration/observability layer — the asset
graph, lineage, per-asset retries + freshness, and partial re-runs.

## Run it

From the repo root, with the orchestration extra installed (`pip install -e ".[transform,orchestration]"`):

```bash
export DAGSTER_HOME="$PWD/.tmp_dagster_home"   # any dir; gitignored
mkdir -p "$DAGSTER_HOME"
dagster dev -f orchestration/definitions.py
```

Then open the UI (default <http://localhost:3000>) to see the graph and materialize assets.

Materialize headless instead:

```bash
dagster asset materialize -f orchestration/definitions.py --select "*"
```

## The graph

```
bronze_<source> ×12  ──►  dbt_build  ──►  publish_to_r2
```

- **`bronze_<source>`** — one asset **generated from each `SourceSpec`** in the registry
  (bars / series / events; microstructure is excluded — it's a streaming collector, not
  a scheduled asset). A new source appears here with no orchestration code. Each carries a
  `RetryPolicy` and a `FreshnessPolicy` read from its spec's `freshness_sla_minutes`.
- **`dbt_build`** — runs the dbt project (silver views + gold marts) over the freshly
  updated bronze. Shells out to `dbt build` rather than using `dagster-dbt` (whose
  transitive `dbt-extractor` needs a Rust toolchain to build on Windows).
- **`publish_to_r2`** — mirrors gold to R2. A clean no-op without write credentials, so
  the graph runs end-to-end locally (dev has only the read-only token).

A `nightly_refresh` job on a `30 0 * * *` UTC schedule materializes the whole graph in
dependency order — mirroring the cron, but with Dagster's retries and observability.
