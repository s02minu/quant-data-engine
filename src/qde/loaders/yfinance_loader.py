import pandas as pd
import yfinance as yf

def load_ohlcv(symbol, start, end=None, interval='1d'):
        data  = yf.download(
                tickers=symbol,
                start=start,
                end=end,
                interval=interval,
                auto_adjust=True,
        )

        if isinstance(data.columns, pd.MultiIndex):
                data = data.droplevel(1, axis=1)

        return data

