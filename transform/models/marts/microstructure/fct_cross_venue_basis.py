"""Gold: the cross-venue basis between Binance BTC/USDT and Coinbase BTC/USD.

The first mart built on the streamed microstructure lake, and the platform's wedge:
not single-venue tick data (a commodity anyone can buy) but the *relationship*
between venues. The basis is the USD/USDT stablecoin premium plus the US-vs-offshore
flow difference — a number single-venue data cannot show.

Shape: one row per (symbol, date, bucket) carrying both venue mids and the basis in
basis points. This is the raw signal; daily summaries (mean/std/extremes, share of
time one venue is richer) are a `GROUP BY` away, so there is no second mart.

**A monitoring artifact, not an archive.** This is a full rebuild over whatever
`book_ticker` is still on local disk, and the nightly prunes bronze after syncing it
to R2 — so the mart is a *rolling window* (~the local retention), not an accumulating
history, and it will start dropping its oldest day once retention is reached. That is
deliberate: the platform's job is to serve current state and clean data, and the
durable full history already exists as raw bronze in R2. Anything needing years of
basis (a backtest, say) should build its own features from that archive rather than
expect this mart to be one.

Because the window's edges are partial — the newest day is however much of today has
been captured — every row carries its day's `day_buckets` and `day_coverage_pct`. A
daily `GROUP BY` over a 30%-covered day is not comparable to a full one, and crypto
basis has strong intraday structure, so a partial day is a *biased* sample rather
than merely a smaller one. Filter on coverage before aggregating.

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

# All three pairs, because the agreement BETWEEN them is the evidence. A single
# symbol shows a number; three symbols moving together show a market-wide stablecoin
# quantity (the USDT/USD premium) rather than per-symbol noise. Cross-symbol
# consistency is what validated this signal originally — BTC/ETH/SOL came in at
# -6.87 / -6.41 / -6.57 bps on the same day — so the mart should carry it.
SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")

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
    try:
        rows = session.sql(sql).df()["date"]
    except Exception:
        # A symbol with no capture at all makes the glob match nothing, which
        # DuckDB raises on. One symbol going quiet must not take the whole mart
        # down with it — the others still have a basis worth computing.
        return []
    # The hive `date` key is typed as a date/timestamp by DuckDB, so `str()` would
    # yield "2026-08-15 00:00:00" and no longer match the `date=` directory name.
    # Format explicitly back to the partition's own representation.
    return [pd.Timestamp(d).strftime("%Y-%m-%d") for d in rows]


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

            # Coverage of this day, so a consumer can tell a full day from a partial
            # one without counting rows itself. Denormalised onto every row on
            # purpose: the alternative is a join, and the whole point is that
            # filtering before aggregating should be trivial.
            n_buckets = int(len(aligned))
            expected = 86_400_000 // BUCKET_MS
            coverage = round(100.0 * n_buckets / expected, 2)

            out = pd.DataFrame(
                {
                    "symbol": symbol,
                    "date": pd.Timestamp(day).date(),
                    "day_buckets": n_buckets,
                    "day_coverage_pct": coverage,
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
                "day_buckets": pd.Series(dtype="int64"),
                "day_coverage_pct": pd.Series(dtype="float64"),
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
