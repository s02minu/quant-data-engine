import pandas as pd

from qde.loaders.yfinance_loader import load_yfinance_ohlcv
from qde.loaders.binance_loader import load_binance_ohlcv



def load_ohlcv(symbol: str,
               start: str,
               end: str | None = None,
               interval: str = "1d",
               source: str="yfinance"
               ) -> pd.DataFrame:

    if source == "yfinance":
        return load_yfinance_ohlcv(symbol, start, end, interval)

    elif source == "binance":
        return load_binance_ohlcv(symbol, start, end, interval)

    else:
        raise ValueError(f"The source {source} is not supported."
                         f"Here are the available sources:\n"
                         f"yfinance, binance")




