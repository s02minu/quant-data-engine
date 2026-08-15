"""Generate a tiny synthetic lake so `dbt build` can run without the real data.

CI lints, type-checks and unit-tests the Python, but until now nothing exercised the
*transform* layer — a broken model was only discovered when it reached the VPS. That
is an expensive place to find out: each round trip is push → pull → rebuild → run →
read logs. On 2026-08-15 three consecutive deploys failed that way, and two of the
three (a config var resolving to None, a missing output directory) would have been
caught in seconds by simply running dbt against a fixture.

This writes the smallest lake that satisfies every model:

- ``bars``            one source, two symbols, clean OHLC
- ``series``          BOTH partition depths — source/series_id AND
                      source/series_id/metric — because DuckDB rejects mixed hive
                      depth under one glob, and only the real lake had that shape
- ``events``          two vintages of one release, so the bitemporal mart has a
                      revision to find
- ``microstructure``  book_ticker for TWO venues, so the cross-venue basis mart has
                      an overlap to compute

It also creates the gold output directories: dbt-duckdb's ``external`` materialization
writes with COPY, and **COPY does not create directories** — the same gotcha that
bites on the VPS whenever a new mart lands.

Usage:
    python scripts/make_sample_lake.py /tmp/sample-lake
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Two days, so anything that groups or diffs by date has more than one group.
DAYS = ["2026-08-14", "2026-08-15"]
BASE_VENUE, QUOTE_VENUE = "binance", "coinbase"


def _write(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _bars(root: Path) -> None:
    """Clean daily OHLCV. Values are ordered low <= open/close <= high so the
    marts' OHLC sanity tests hold exactly rather than within a tolerance."""
    for symbol, base in (("BTCUSDT", 60000.0), ("ETHUSDT", 3000.0)):
        idx = pd.date_range("2026-07-01", periods=40, freq="D", tz="UTC")
        drift = np.linspace(0, 0.05, len(idx))
        close = base * (1 + drift)
        df = pd.DataFrame(
            {
                "date": idx,
                "open": close * 0.995,
                "high": close * 1.01,
                "low": close * 0.99,
                "close": close,
                "volume": np.linspace(100.0, 200.0, len(idx)),
            }
        )
        _write(
            df,
            root
            / "bronze"
            / "group=bars"
            / f"source={BASE_VENUE}"
            / f"symbol={symbol}"
            / "interval=1d"
            / "bars.parquet",
        )


def _series(root: Path) -> None:
    """Scalar series at BOTH partition depths — the mixed-depth shape that broke a
    real publish, so CI should carry it."""
    idx = pd.date_range("2026-06-01", periods=30, freq="D", tz="UTC")
    flat = pd.DataFrame({"date": idx, "value": np.linspace(4.0, 4.5, len(idx))})
    _write(
        flat,
        root / "bronze" / "group=series" / "source=fred" / "series_id=UNRATE" / "series.parquet",
    )
    # ...and one WITH the optional metric= level.
    nested = pd.DataFrame({"date": idx, "value": np.linspace(0.01, 0.02, len(idx))})
    _write(
        nested,
        root
        / "bronze"
        / "group=series"
        / "source=binancefut"
        / "series_id=BTCUSDT"
        / "metric=funding_rate"
        / "series.parquet",
    )


def _events(root: Path) -> None:
    """One release observed twice: an initial print and a later revision, so the
    bitemporal mart has an actual revision to surface.

    Column names follow docs/schemas/events.md exactly — notably ``event_id`` in
    the form ``<series_id>:<YYYY-MM-DD>``, which staging splits to derive the
    reference period, and ``revision_seq`` ordering the vintages.
    """
    df = pd.DataFrame(
        {
            "event_id": ["GDPC1:2026-04-01", "GDPC1:2026-04-01"],
            "series_id": ["GDPC1", "GDPC1"],
            "scheduled_ts": pd.to_datetime(["2026-05-28", "2026-05-28"], utc=True),
            "observed_ts": pd.to_datetime(["2026-05-28", "2026-06-25"], utc=True),
            "actual": [2.1, 2.4],
            "forecast": [2.0, 2.0],
            "previous": [1.8, 1.8],
            "revision_seq": [0, 1],
        }
    )
    _write(
        df,
        root
        / "bronze"
        / "group=events"
        / "source=fredcal"
        / "calendar=us_macro"
        / "events.parquet",
    )


def _microstructure(root: Path) -> None:
    """Top-of-book on two venues with a deliberate, constant spread between them,
    so the basis mart produces a known, non-zero number."""
    # All three pairs the mart declares, so CI exercises the real multi-symbol loop
    # rather than only its "symbol missing" fallback.
    for symbol, level in (("BTCUSDT", 60000.0), ("ETHUSDT", 3000.0), ("SOLUSDT", 150.0)):
        for day in DAYS:
            start = pd.Timestamp(day, tz="UTC").value // 1_000_000  # epoch ms
            n = 600
            received = start + np.arange(n) * 1000  # one quote per second
            mid = level + np.sin(np.linspace(0, 6, n)) * (level * 0.001)

            for venue, factor in ((BASE_VENUE, 1.0), (QUOTE_VENUE, 0.9993)):
                # Coinbase set ~7bp below Binance: the sign and rough size of the
                # real USDT premium, so a wrong-sign regression is visible.
                m = mid * factor
                df = pd.DataFrame(
                    {
                        "symbol": symbol,
                        # Bronze stores prices as strings; the mart TRY_CASTs them,
                        # and CI should exercise that rather than hand it floats.
                        "bid_price": [f"{v:.8f}" for v in m * 0.99995],
                        "bid_qty": "1.0",
                        "ask_price": [f"{v:.8f}" for v in m * 1.00005],
                        "ask_qty": "1.0",
                        "update_id": np.arange(n),
                        "received_at": received,
                    }
                )
                _write(
                    df,
                    root
                    / "bronze"
                    / "group=microstructure"
                    / f"source={venue}"
                    / "kind=book_ticker"
                    / f"symbol={symbol}"
                    / f"date={day}"
                    / "part-0001.parquet",
                )


def _gold_dirs(root: Path) -> None:
    """dbt-duckdb writes external models with COPY, which will not create the
    directory — so every mart's folder must exist first."""
    for rel in (
        "gold/group=bars/mart=fct_bars_daily",
        "gold/group=series/mart=fct_series_features",
        "gold/group=events/mart=fct_events_revisions",
        "gold/group=microstructure/mart=fct_cross_venue_basis",
        "gold/dim_sources",
    ):
        (root / rel).mkdir(parents=True, exist_ok=True)


def build(root: str | Path) -> Path:
    root = Path(root)
    _bars(root)
    _series(root)
    _events(root)
    _microstructure(root)
    _gold_dirs(root)
    return root


if __name__ == "__main__":
    target = build(sys.argv[1] if len(sys.argv) > 1 else "sample-lake")
    files = sum(1 for _ in target.rglob("*.parquet"))
    print(f"sample lake at {target} ({files} parquet files)")
