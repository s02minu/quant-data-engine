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

    # Guard against multiindex update
    if isinstance(data.columns, pd.MultiIndex):
        data = data.droplevel(1, axis='columns')

    # LowerCasing for standardization
    data.columns = data.columns.str.lower()

    # Timezone handling
    if data.index.tz is None:
        data.index = data.index.tz_localize('UTC')

    else:
        data.index = data.index.tz_convert('UTC')

    data.index.name = 'date'

    return data

