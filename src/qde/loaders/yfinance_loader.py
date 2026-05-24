import pandas as pd
import yfinance as yf

def load_ohlcv(symbol, start, end=None, interval='id'):
        data  = yf.download(
                tickers=symbol,
                start=start,
                end=end,
                interval=interval,
                auto_adjust=True,
        )
        return data

