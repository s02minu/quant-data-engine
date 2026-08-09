"""Dagster orchestration (Phase 7) — registry-driven assets over the batch pipeline.

**Local/dev-only by design** (ROADMAP §7 + the 3.7 GB VPS). Run it from the repo
root:

    dagster dev -f orchestration/definitions.py

The lightweight VPS cron (`scripts/maintain.sh`) stays the *production* runner; this
project is the orchestration/observability layer — the asset graph, lineage, per-
asset retries and freshness, and partial re-runs (re-materialize just the failed
FRED source instead of the whole nightly). A two-way door: the same assets could
later drive prod if the box grows.

**The graph is generated from the registry.** One bronze asset per non-
microstructure `SourceSpec` — so a new source appears in the Dagster UI with *no*
orchestration code, the little-book payoff (ROADMAP §3.1). Those feed a single dbt
asset (bronze→silver→gold), which feeds a publish asset (gold→R2):

    bronze_<source> ×N  ──►  dbt_build  ──►  publish_to_r2

Retry policy and freshness SLA are read from each source's `SourceSpec`, the same
definition that configures the ingestors and the DQ checks — one definition, many
consumers. Microstructure is deliberately absent: it is a 24/7 streaming collector,
not a scheduled asset.
"""

import os
import shutil
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    Definitions,
    FreshnessPolicy,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from qde.loaders import NoNewData
from qde.registry import all_specs
from qde.storage import (
    list_bars_series,
    list_series,
    update_events,
    update_ohlcv,
    update_series,
)

# dagster dev is launched from the repo root, so the lake is ./data (matching
# qde.daily_update / qde.compact / qde.sync); the dbt project sits alongside.
BASE_DIR = os.getenv("QDE_BASE_DIR", "data")
REPO_ROOT = Path(__file__).resolve().parents[1]
TRANSFORM_DIR = REPO_ROOT / "transform"

# The batch groups Dagster orchestrates. Microstructure is excluded on purpose (a
# streaming collector, not a scheduled asset).
_BATCH_GROUPS = {"bars", "series", "events"}
# Full-history floor for the events full-refresh, matching qde.daily_update.
_EVENTS_REFRESH_START = "2000-01-01"


# --- per-group update routines (thin wrappers over the existing storage layer) ---
#
# Each brings *one source* current, mirroring qde.daily_update's loops but scoped to
# a single source so Dagster can retry / re-materialize it in isolation. Returns the
# number of series advanced, surfaced as asset metadata.


def _update_bars_source(source: str, base_dir: str) -> int:
    df = list_bars_series(base_dir)
    n = 0
    for row in df[df["source"] == source].itertuples(index=False):
        update_ohlcv(row.symbol, source=source, interval=row.interval, base_dir=base_dir)
        n += 1
    return n


def _update_series_source(source: str, base_dir: str) -> int:
    df = list_series(base_dir)
    n = 0
    for row in df[df["source"] == source].itertuples(index=False):
        update_series(row.series_id, source=source, base_dir=base_dir)
        n += 1
    return n


def _update_events_source(spec, base_dir: str) -> int:
    # Events full-refresh (a revision is a new row for an old period), like the
    # nightly. NoNewData is the benign already-current case.
    calendar = spec.calendar or spec.name
    n = 0
    for series_id in spec.canonical_symbols:
        try:
            update_events(series_id, spec.name, calendar, _EVENTS_REFRESH_START, base_dir)
            n += 1
        except NoNewData:
            continue
    return n


def _make_bronze_asset(spec):
    """Build one bronze asset for a source spec (the registry-driven factory)."""
    group = spec.group

    @asset(
        name=f"bronze_{spec.name}",
        group_name=group,
        # Batch APIs blip; a couple of retries turns a transient failure into a
        # non-event instead of a red asset.
        retry_policy=RetryPolicy(max_retries=2, delay=10),
        # Freshness SLA straight from the SourceSpec (the same field the DQ checks
        # read) -- Dagster marks the asset stale if it hasn't materialized within it.
        freshness_policy=FreshnessPolicy.time_window(
            fail_window=timedelta(minutes=spec.freshness_sla_minutes)
        ),
        description=(
            f"Bronze {group} for source '{spec.name}': brings its seeded series "
            f"current in the lake. Generated from the registry SourceSpec."
        ),
    )
    def _bronze(context: AssetExecutionContext) -> MaterializeResult:
        if group == "bars":
            n = _update_bars_source(spec.name, BASE_DIR)
        elif group == "series":
            n = _update_series_source(spec.name, BASE_DIR)
        else:
            n = _update_events_source(spec, BASE_DIR)
        context.log.info(f"{spec.name}: advanced {n} series")
        return MaterializeResult(metadata={"series_updated": MetadataValue.int(n)})

    return _bronze


_bronze_specs = [s for s in all_specs() if s.group in _BATCH_GROUPS]
bronze_assets = [_make_bronze_asset(s) for s in _bronze_specs]
_bronze_keys = [AssetKey(f"bronze_{s.name}") for s in _bronze_specs]


# --- transform: the dbt project as one asset (bronze -> silver -> gold) ---
#
# Shells out to `dbt build` rather than using dagster-dbt (whose transitive
# dbt-extractor has no Windows wheel and needs a Rust toolchain -- not worth it for
# a dev-only tool). Depends on every bronze asset, so the UI shows bronze feeding
# the transform, and a nightly run rebuilds gold only after bronze is current.

_GOLD_DIRS = [
    "gold/group=bars/mart=fct_bars_daily",
    "gold/group=series/mart=fct_series_features",
    "gold/group=events/mart=fct_events_revisions",
    "gold/dim_sources",
]


@asset(
    deps=_bronze_keys,
    group_name="transform",
    description="Runs `dbt build` over the freshly-updated bronze: silver views + gold marts.",
)
def dbt_build(context: AssetExecutionContext) -> MaterializeResult:
    # DuckDB's COPY will not create the gold dirs; ensure they exist first (mirrors
    # scripts/maintain.sh).
    for rel in _GOLD_DIRS:
        (Path(BASE_DIR) / rel).mkdir(parents=True, exist_ok=True)

    # Resolve dbt explicitly: on Windows the subprocess won't find the venv's
    # dbt.exe by bare name. Prefer the executable next to the running interpreter
    # (the same venv dbt is installed in), else fall back to PATH.
    _cand = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
    dbt_exe = str(_cand) if _cand.exists() else (shutil.which("dbt") or "dbt")

    env = {**os.environ, "DBT_PROFILES_DIR": "."}
    lake_root = os.path.relpath(Path(BASE_DIR).resolve(), TRANSFORM_DIR).replace("\\", "/")
    result = subprocess.run(
        [dbt_exe, "build", "--vars", f"lake_root: {lake_root}"],
        cwd=TRANSFORM_DIR,
        env=env,
        capture_output=True,
        text=True,
    )
    context.log.info(result.stdout[-4000:] or result.stderr[-4000:])
    if result.returncode != 0:
        raise RuntimeError(f"dbt build failed (exit {result.returncode}); see logs above")
    return MaterializeResult(metadata={"dbt": MetadataValue.text("build passed")})


# --- publish: gold -> R2 (guarded; a no-op without write creds, as in dev) ---


@asset(
    deps=[dbt_build],
    group_name="publish",
    description="Mirrors gold marts to R2. Skipped (no-op) without write credentials.",
)
def publish_to_r2(context: AssetExecutionContext) -> MaterializeResult:
    if not os.getenv("QDE_R2_BUCKET"):
        # Dev has only the read-only token; publishing is the VPS's job. Skip
        # cleanly rather than fail, so `dagster dev` can run the graph end to end.
        context.log.info("no R2 write credentials set; skipping publish (dev/local)")
        return MaterializeResult(metadata={"published": MetadataValue.text("skipped (no creds)")})

    from qde.sync import publish_gold, r2_client_from_env

    client = r2_client_from_env()
    summary = publish_gold(base_dir=BASE_DIR, bucket=os.environ["QDE_R2_BUCKET"], client=client)
    context.log.info(f"published gold: {summary}")
    return MaterializeResult(metadata={"published": MetadataValue.int(summary["published"])})


# --- job + schedule ---
# (Freshness is a per-asset FreshnessPolicy attached in the factory above.)

# The nightly refresh: bronze -> dbt -> publish, in dependency order.
nightly_job = define_asset_job("nightly_refresh", selection="*")
nightly_schedule = ScheduleDefinition(
    job=nightly_job,
    cron_schedule="30 0 * * *",  # 00:30 UTC, matching the VPS cron
    execution_timezone="UTC",
)

defs = Definitions(
    assets=[*bronze_assets, dbt_build, publish_to_r2],
    jobs=[nightly_job],
    schedules=[nightly_schedule],
)
