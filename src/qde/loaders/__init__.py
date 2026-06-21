import pandas as pd

from qde.loaders.kraken_loader import load_kraken_ohlcv
from qde.loaders.symbols import SYMBOL_MAP
from qde.loaders.yfinance_loader import load_yfinance_ohlcv
from qde.loaders.binance_loader import load_binance_ohlcv
from qde.loaders.kraken_loader import load_kraken_ohlcv



def load_ohlcv(symbol: str,
               start: str,
               end: str | None = None,
               interval: str = "1d",
               source: str="yfinance"
               ) -> pd.DataFrame:

    # Translate symbol for this source
    mapped = SYMBOL_MAP.get(source, {}).get(symbol)
    if mapped is None:
        raise ValueError(
            f"Unknown symbol {symbol!r} for source {source!r}"
        )

    if source == "yfinance":
        return load_yfinance_ohlcv(mapped, start, end, interval)

    elif source == "binance":
        return load_binance_ohlcv(mapped, start, end, interval)

    elif source == "kraken":
        return load_kraken_ohlcv(mapped, start, end, interval)

    else:
        raise ValueError(f"The source {source} is not supported."
                         f"Here are the available sources:\n"
                         f"yfinance, binance")




