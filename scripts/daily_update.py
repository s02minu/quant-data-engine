import pandas as pd

from qde.quality import build_quality_summary
from qde.storage import list_bars_series, update_ohlcv

print(f"Update started at {pd.Timestamp.now()}")

# Discover series from the lake's partition metadata, then update each.
series = list_bars_series()

for symbol, source, interval in zip(
    series["symbol"], series["source"], series["interval"], strict=True
):
    try:
        update_ohlcv(symbol, source=source, interval=interval)
        print(f"{symbol} updated")
    except Exception as e:
        print(f"{symbol} failed: {e}")


# Refresh the csv
summary = build_quality_summary()

print(f"Update complete at {pd.Timestamp.now()}")
