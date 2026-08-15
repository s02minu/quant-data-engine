"""Gold: the cross-venue basis between Binance BTC/USDT and Coinbase BTC/USD.

The first mart built on the streamed microstructure lake, and the platform's wedge:
not single-venue tick data (a commodity anyone can buy) but the *relationship*
between venues. The basis is the USD/USDT stablecoin premium plus the US-vs-offshore
flow difference — a number single-venue data cannot show.

Shape: one row per (symbol, date, bucket) carrying both venue mids and the basis in
basis points. This is the raw signal; daily summaries (mean/std/extremes, share of
time one venue is richer) are a `GROUP BY` away, so there is no second mart.

**PRIVATE.** Registered in `lake._GOLD_MARTS` so the nightly syncs it to the private
bucket, and deliberately absent from `publish_public._PUBLIC_MARTS` so it is not
served on the open lake. Publishing is a one-way door; this stays closed until
there's a reason to open it.

Why a Python model rather than SQL: the heavy step (collapsing millions of
`book_ticker` rows to one mid per bucket) is already DuckDB SQL and runs in-session,
but the alignment — a gap-free bucket grid, forward-fill, restricted to the true
overlap — is far clearer in pandas, and `qde.analytics` already implements and
unit-tests it. Reusing that module keeps one definition of the basis rather than two.

Note: this model's config lives in `_microstructure.yml`, not inline. dbt parses
inline `dbt.config()` arguments with `literal_eval`, so `var('lake_root')` cannot be
interpolated there; YAML config renders Jinja normally.
"""

import pandas as pd

from qde.analytics import BASE_VENUE, QUOTE_VENUE, align, basis_bps

# One symbol to start. The basis holds across BTC/ETH/SOL, but proving the shape on
# the most liquid pair first keeps the row count and the review small; widening is
# adding to this tuple.
SYMBOLS = ("BTCUSDT",)

# 1s buckets: fine enough to see the basis move, coarse enough that a day is ~86k
# rows per venue rather than millions.
BUCKET_MS = 1000


def _mids_for_day(session, lake_root: str, symbol: str, day: str) -> pd.DataFrame:
    """One mid per (venue, bucket) for a symbol on one UTC date.

    ``arg_max(mid, received_at)`` keeps the *last* quote in each bucket — a quote
    holds until the next one — and ``TRY_CAST`` tolerates the string-typed bronze
    prices. Grouping in DuckDB means only the bucketed series is materialised, not
    the millions of underlying quotes.
    """
    glob = (
        f"{lake_root}/bronze/group=microstructure/source=*/kind=book_ticker/"
        f"symbol={symbol}/date={day}/*.parquet"
    )
    sql = f"""
        SELECT source,
               received_at // {BUCKET_MS} AS bucket,
               arg_max(
                   (TRY_CAST(bid_price AS DOUBLE) + TRY_CAST(ask_price AS DOUBLE)) / 2,
                   received_at
               ) AS mid
        FROM read_parquet('{glob}', hive_partitioning = true)
        WHERE source IN ('{BASE_VENUE}', '{QUOTE_VENUE}')
        GROUP BY source, bucket
        ORDER BY bucket
    """
    long = session.sql(sql).df()
    if long.empty:
        return pd.DataFrame()
    return long.pivot(index="bucket", columns="source", values="mid").sort_index()


def _days_available(session, lake_root: str, symbol: str) -> list[str]:
    """UTC dates with book_ticker for this symbol on BOTH venues.

    A day where only one venue captured is not a basis — the overlap would be empty
    — so those are skipped here rather than producing rows of NaN downstream.
    """
    glob = (
        f"{lake_root}/bronze/group=microstructure/source=*/kind=book_ticker/"
        f"symbol={symbol}/date=*/*.parquet"
    )
    sql = f"""
        SELECT date, count(DISTINCT source) AS venues
        FROM read_parquet('{glob}', hive_partitioning = true)
        WHERE source IN ('{BASE_VENUE}', '{QUOTE_VENUE}')
        GROUP BY date
        HAVING count(DISTINCT source) = 2
        ORDER BY date
    """
    # The hive `date` key is typed as a date/timestamp by DuckDB, so `str()` would
    # yield "2026-08-15 00:00:00" and no longer match the `date=` directory name.
    # Format explicitly back to the partition's own representation.
    return [pd.Timestamp(d).strftime("%Y-%m-%d") for d in session.sql(sql).df()["date"]]


def model(dbt, session):
    lake_root = dbt.config.get("lake_root")
    frames = []

    for symbol in SYMBOLS:
        for day in _days_available(session, lake_root, symbol):
            wide = _mids_for_day(session, lake_root, symbol, day)
            if wide.empty or BASE_VENUE not in wide or QUOTE_VENUE not in wide:
                continue

            aligned = align(wide)
            if aligned.empty:
                continue

            out = pd.DataFrame(
                {
                    "symbol": symbol,
                    "date": pd.Timestamp(day).date(),
                    "bucket": aligned.index.astype("int64"),
                    # Bucket start as a real timestamp, so consumers can join on time
                    # without knowing the bucket arithmetic.
                    "bucket_ts": pd.to_datetime(
                        aligned.index.astype("int64") * BUCKET_MS, unit="ms", utc=True
                    ),
                    "bucket_ms": BUCKET_MS,
                    "base_venue": BASE_VENUE,
                    "quote_venue": QUOTE_VENUE,
                    "base_mid": aligned[BASE_VENUE].to_numpy(),
                    "quote_mid": aligned[QUOTE_VENUE].to_numpy(),
                    "basis_bps": basis_bps(aligned).to_numpy(),
                }
            )
            frames.append(out)

    if not frames:
        # An empty mart still needs its columns, or downstream reads break on a
        # schema that does not exist.
        return pd.DataFrame(
            {
                "symbol": pd.Series(dtype="object"),
                "date": pd.Series(dtype="object"),
                "bucket": pd.Series(dtype="int64"),
                "bucket_ts": pd.Series(dtype="datetime64[ns, UTC]"),
                "bucket_ms": pd.Series(dtype="int64"),
                "base_venue": pd.Series(dtype="object"),
                "quote_venue": pd.Series(dtype="object"),
                "base_mid": pd.Series(dtype="float64"),
                "quote_mid": pd.Series(dtype="float64"),
                "basis_bps": pd.Series(dtype="float64"),
            }
        )

    return pd.concat(frames, ignore_index=True)
