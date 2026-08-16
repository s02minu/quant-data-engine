"""Build the public data catalogue (``catalogue.json``) — the product's front door.

The catalogue is what makes the lake *discoverable* (ROADMAP §5.1 / §12): it lists
what datasets exist, their schema, size, freshness, licence, and a **copyable DuckDB
query** that runs against the public lake with no credentials. It is generated from
the registry (``dim_sources``) plus live stats read straight from the lake — one
definition, many consumers again.

A **static JSON artifact**, deliberately not a live service (the ROADMAP §12 [open]
question, resolved): cheaper, and it fits *serve files, not queries* — the React
showcase site and the Streamlit dashboard both just read this file; nothing runs
server-side. It is regenerated nightly and published beside the data.

Sample queries point at ``public_base_url`` (the public bucket's HTTPS origin, e.g.
``https://data.example.com`` or an ``r2.dev`` URL), so a reader can paste one into
their own DuckDB and it works against public Parquet over plain HTTPS — no secret.
"""

import json
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from qde.registry import all_specs, dim_sources
from qde.storage import query

# The bronze groups the catalogue can describe from the LOCAL lake, and the column
# each dates its freshness by. Microstructure is public but absent here because the
# sync prunes it from local disk, so nothing about it can be measured at this point —
# it is described statically instead (see _microstructure_dataset).
_BRONZE_GROUPS = {"bars": "date", "series": "date", "events": "observed_ts"}

# Prefix of the mirrored microstructure archive. A prefix rather than a fillable
# path template because part-file names carry a flush timestamp and cannot be
# predicted — which is also why no runnable sample query is offered for it.
_MICROSTRUCTURE_PREFIX = "bronze/group=microstructure/"

# The gold marts (dbt `external` Parquet), each a single file. `date_col` is what a
# freshness/range is computed from, or None where the mart has no natural date.
_GOLD_MARTS: dict[str, dict[str, Any]] = {
    "fct_bars_daily": {
        "path": "gold/group=bars/mart=fct_bars_daily/data.parquet", "date_col": "date"
    },
    "fct_series_features": {
        "path": "gold/group=series/mart=fct_series_features/data.parquet", "date_col": "date"
    },
    "fct_events_revisions": {
        "path": "gold/group=events/mart=fct_events_revisions/data.parquet",
        "date_col": "reference_date",
    },
}

_DEFAULT_BASE_URL = "https://REPLACE-ME.r2.dev"  # overridden by QDE_PUBLIC_BASE_URL at publish


def _schema(con: duckdb.DuckDBPyConnection, relation_sql: str) -> list[dict[str, str]]:
    """Return ``[{name, type}]`` for a FROM-able relation, via DuckDB DESCRIBE."""
    df = con.execute(f"DESCRIBE SELECT * FROM {relation_sql}").df()
    return [
        {"name": str(r.column_name), "type": str(r.column_type)}
        for r in df.itertuples(index=False)
    ]


def _age(last: Any, now: pd.Timestamp) -> dict[str, Any] | None:
    """Return ``{last, age_hours}`` for a latest-timestamp value, or None if absent."""
    if last is None or pd.isna(last):
        return None
    ts = pd.Timestamp(last)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return {"last": ts.isoformat(), "age_hours": round((now - ts).total_seconds() / 3600, 1)}


def _source_stats(base_dir: str, now: pd.Timestamp) -> dict[str, dict[str, Any]]:
    """Per-source live stats (rows, latest observation) across the bronze groups.

    Uses ``storage.query`` so the mixed-depth ``series`` view (flat + metric globs)
    is handled for us. A group with no data yet is skipped.
    """
    stats: dict[str, dict[str, Any]] = {}
    for group, date_col in _BRONZE_GROUPS.items():
        if not any((Path(base_dir) / "bronze" / f"group={group}").glob("**/*.parquet")):
            continue
        rows = query(
            f"SELECT source, count(*) AS n, max({date_col}) AS last FROM {group} GROUP BY source",
            base_dir=base_dir,
        )
        for _, r in rows.iterrows():
            stats[str(r["source"])] = {"rows": int(r["n"]), "freshness": _age(r["last"], now)}
    return stats


def _sources_section(base_dir: str, now: pd.Timestamp) -> list[dict[str, Any]]:
    """The registry catalogue (``dim_sources``) enriched with live per-source stats."""
    live = _source_stats(base_dir, now)
    out = []
    for _, row in dim_sources().iterrows():
        name = str(row["name"])
        s = {
            "name": name,
            "group": str(row["group"]),
            "n_symbols": int(row["n_symbols"]),
            "symbols": str(row["symbols"]),
            "redistributable": bool(row["redistributable"]),
            "license_note": str(row["license_note"]),
            **live.get(name, {"rows": 0, "freshness": None}),
        }
        out.append(s)
    return out


def _bronze_dataset(group: str, base_dir: str, public_base_url: str, now: pd.Timestamp) -> dict:
    date_col = _BRONZE_GROUPS[group]
    agg = query(
        f"SELECT count(*) AS n, max({date_col}) AS last FROM {group}", base_dir=base_dir
    )
    # Point at the consolidated per-group file, NOT a `**/*.parquet` glob: plain HTTP
    # has no directory listing, so DuckDB cannot expand a glob against an r2.dev URL
    # ("Globs for generic HTTP file are not supported"). The partitioned files are
    # still published for anyone who wants a specific slice, but the query we hand
    # out has to be one that actually runs.
    url = f"{public_base_url}/bronze/group={group}/all.parquet"
    return {
        "id": group,
        "layer": "bronze",
        "group": group,
        "row_count": int(agg["n"].iloc[0]),
        "freshness": _age(agg["last"].iloc[0], now),
        "schema": _bronze_schema(group, base_dir),
        "sample_query": f"SELECT *\nFROM read_parquet('{url}')\nLIMIT 100;",
    }


def _microstructure_dataset(public_base_url: str) -> dict:
    """Describe the streamed archive without measuring it.

    Every other dataset reports a row count and a freshness read from the local
    lake. This one cannot: it is mirrored bucket-to-bucket precisely *because* the
    nightly prunes it from disk. Publishing a fabricated or stale number here would
    be worse than publishing none, so the entry is explicit that the figures are
    unavailable and tells the reader how to address the archive instead.
    """
    return {
        "id": "microstructure",
        "layer": "bronze",
        "group": "microstructure",
        "row_count": None,
        "freshness": None,
        "schema": None,
        "partition_prefix": f"{public_base_url}/{_MICROSTRUCTURE_PREFIX}",
        "notes": (
            "Full tick + L2 archive, Hive-partitioned by source/kind/symbol/date. "
            "Unlike the other datasets it has no consolidated file — it is orders of "
            "magnitude larger than all of them combined — and plain HTTP offers no "
            "directory listing, so a `*` in the path will NOT expand: DuckDB rejects "
            "it outright. Reach it with a client that can list objects (the S3 API "
            "against the bucket, or duckdb's httpfs with S3 credentials), or address "
            "an exact object key. Kinds: trades, depth, book_ticker, snapshot, gaps, "
            "session (coinbase adds heartbeat). Prices and sizes are stored as "
            "strings exactly as the venue sent them; cast before comparing."
        ),
        # Deliberately absent. Every other dataset advertises a query that can be
        # copied and run; a glob over an r2.dev URL cannot, so offering one here
        # would hand out a snippet that fails on first use. Better to say plainly
        # that this one needs a different access path.
        "sample_query": None,
    }


def _bronze_schema(group: str, base_dir: str) -> list[dict[str, str]]:
    # DESCRIBE through storage.query's registered view so the mixed-depth series view
    # resolves; strip to name/type.
    df = query(f"DESCRIBE SELECT * FROM {group}", base_dir=base_dir)
    return [
        {"name": str(r.column_name), "type": str(r.column_type)}
        for r in df.itertuples(index=False)
    ]


def _gold_dataset(
    mart: str, spec: dict, base_dir: str, public_base_url: str, now: pd.Timestamp
) -> dict | None:
    path = Path(base_dir) / spec["path"]
    if not path.exists():
        return None
    con = duckdb.connect()
    rel = f"read_parquet('{path.as_posix()}')"
    n = con.execute(f"SELECT count(*) AS n FROM {rel}").df()["n"].iloc[0]
    date_col = spec["date_col"]
    fresh = None
    if date_col:
        last = con.execute(f"SELECT max({date_col}) AS last FROM {rel}").df()["last"].iloc[0]
        fresh = _age(last, now)
    url = f"{public_base_url}/{spec['path']}"
    return {
        "id": mart,
        "layer": "gold",
        "row_count": int(n),
        "freshness": fresh,
        "schema": _schema(con, rel),
        "sample_query": f"SELECT *\nFROM read_parquet('{url}')\nLIMIT 100;",
    }


def build_catalogue(base_dir: str = "data", public_base_url: str | None = None) -> dict:
    """Build the catalogue dict from the registry + live lake stats.

    Args:
        base_dir: the local lake root.
        public_base_url: HTTPS origin of the public bucket, embedded in sample
            queries (defaults to ``QDE_PUBLIC_BASE_URL`` env, else a placeholder).

    Returns:
        The catalogue as a JSON-serialisable dict.
    """
    import os

    public_base_url = (
        public_base_url or os.getenv("QDE_PUBLIC_BASE_URL") or _DEFAULT_BASE_URL
    ).rstrip("/")
    now = pd.Timestamp.now(tz="UTC")

    nonredist = sorted(s.name for s in all_specs() if not s.redistributable)

    datasets: list[dict] = []
    for group in _BRONZE_GROUPS:
        if any((Path(base_dir) / "bronze" / f"group={group}").glob("**/*.parquet")):
            datasets.append(_bronze_dataset(group, base_dir, public_base_url, now))
    # Listed unconditionally: it is mirrored from the private bucket rather than
    # published from here, so its absence on local disk says nothing about whether
    # it exists in the public lake. Gating on a local glob would hide the largest
    # dataset on the platform on every VPS run.
    datasets.append(_microstructure_dataset(public_base_url))
    for mart, spec in _GOLD_MARTS.items():
        entry = _gold_dataset(mart, spec, base_dir, public_base_url, now)
        if entry is not None:
            datasets.append(entry)

    return {
        "generated_at": now.isoformat(),
        "serving_model": "publish-files-not-queries",
        "public_base_url": public_base_url,
        "notes": {
            "how_to_query": (
                "Point your own DuckDB at the sample_query URLs — no credentials. "
                "DuckDB fetches only the columns and row-groups your query touches; "
                "your machine does the compute, R2 serves the bytes (zero egress)."
            ),
            "excluded_sources": nonredist,
            "excluded_reason": (
                "not redistributable (code-only); the ingestor is open, the data is not."
            ),
        },
        "sources": _sources_section(base_dir, now),
        "datasets": datasets,
    }


def write_catalogue(catalogue: dict, path: str) -> None:
    """Write the catalogue dict to ``path`` as pretty JSON (atomic temp→rename)."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(f".{p.name}.tmp")
    tmp.write_text(json.dumps(catalogue, indent=2, default=str), encoding="utf-8")
    tmp.replace(p)


if __name__ == "__main__":
    import os

    base_dir = os.getenv("QDE_BASE_DIR", "data")
    catalogue = build_catalogue(base_dir)
    out = str(Path(base_dir) / "catalogue.json")
    write_catalogue(catalogue, out)
    print(
        f"catalogue: {len(catalogue['sources'])} sources, "
        f"{len(catalogue['datasets'])} datasets -> {out}"
    )
