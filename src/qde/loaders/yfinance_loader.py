import pandas as pd
import yfinance as yf

from qde.loaders.exceptions import NoNewData


def load_yfinance_ohlcv(symbol, start, end=None, interval="1d"):
    """Load OHLCV data for a single symbol from Yahoo Finance, returning a cleaned
    DataFrame with flat lowercase columns and a UTC-aware index.

    Args:
        symbol (str): a ticker symbol
        start (str): start date, YYYY-MM-DD format
        end (str, optional): end date, YYYY-MM-DD format. Defaults to today if omitted.
        interval (str, optional): bar size, e.g. '1d', '1h', '1m'. Default: '1d'

    Returns:
        DataFrame with columns: date, open, high, low, close, volume.
        Index by a UTC-aware DatetimeIndex named 'date'.

    Raises:
        NoNewData: If yfinance returns an empty frame -- the source has no rows
            in range (a subclass of ``ValueError``). yfinance cannot distinguish
            an empty range from an unknown ticker; genuinely unmapped symbols are
            already rejected upstream by ``load_ohlcv``'s symbol-map lookup.
    """

    df = yf.download(
        tickers=symbol,
        start=start,
        end=end,
        interval=interval,
        auto_adjust=True,
    )

    # An empty frame from a successful download is the "no new data" case, not a
    # failure -- an incremental pull past the last bar legitimately gets nothing.
    if df.empty:
        raise NoNewData(
            f"No df returned for symbol={symbol!r}, start={start!r}, "
            f"end={end!r}, interval={interval!r}"
        )

    # Guard against multiindex update
    if isinstance(df.columns, pd.MultiIndex):
        df = df.droplevel(1, axis="columns")

    # LowerCasing for standardization
    df.columns = df.columns.str.lower()

    # Reordering the columns
    df = df[["open", "high", "low", "close", "volume"]]

    # Remove the name of the index
    df.columns.name = None

    # Timezone handling
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    df.index.name = "date"

    return df
