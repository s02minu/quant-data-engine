"""Yahoo Finance OHLCV ingestor.

yfinance returns a whole date range in one ``download`` call, so there is no
pagination: a single page, no next cursor. The hand-written loader this replaces
lives in git history.
"""

from typing import Any

import pandas as pd
import yfinance as yf

from qde.ingest.base import BaseIngestor, RawPage


def _inclusive_end(end: str | None) -> str | None:
    """Shift an inclusive end date to the exclusive bound yfinance expects."""
    if end is None:
        return None
    return str((pd.Timestamp(end) + pd.Timedelta(days=1)).date())


class YfinanceIngestor(BaseIngestor):
    def first_cursor(self, symbol: str, start: str, end: str | None, interval: str) -> str:
        # No cursor to page from; return the start so the loop runs exactly once.
        return start

    def fetch_page(
        self, symbol: str, cursor: Any, start: str, end: str | None, interval: str
    ) -> RawPage:
        # yfinance treats `end` as EXCLUSIVE; every other source here, and this
        # platform's own contract (`qde.backfill` documents the range as
        # "[start, end]"), treats it as inclusive. Left as-is it silently drops the
        # final day of every ranged fetch — which showed up as all eight yfinance
        # symbols reporting "the source is dropping history" in the weekly
        # verification, each missing exactly the last settled date. Normalised at
        # the boundary so callers get one range convention regardless of source.
        df = yf.download(
            tickers=symbol,
            start=start,
            end=_inclusive_end(end),
            interval=interval,
            auto_adjust=True,
        )

        # An empty frame is the "no rows in range" case; yield nothing so the base
        # raises NoNewData. yfinance cannot tell an empty range from an unknown
        # ticker, but genuinely unmapped symbols are rejected upstream by the
        # registry lookup in load_ohlcv.
        if df.empty:
            return RawPage(rows=[], next_cursor=None)

        return RawPage(rows=[df], next_cursor=None)

    def normalize(self, rows: list[Any]) -> pd.DataFrame:
        df = rows[0]

        # A single-ticker download can still come back with a MultiIndex column
        # axis; drop the ticker level so columns are flat.
        if isinstance(df.columns, pd.MultiIndex):
            df = df.droplevel(1, axis="columns")

        df.columns = df.columns.str.lower()
        df = df[["open", "high", "low", "close", "volume"]]
        df.columns.name = None

        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")
        df.index.name = "date"

        return df
