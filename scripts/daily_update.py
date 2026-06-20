import pandas as pd

from pathlib import Path
from qde.storage import update_ohlcv
from qde.quality import build_quality_summary



print(f"Update started at {pd.Timestamp.now()}")

# Loop through all the symbols and call the update function on each
files = list((Path("data") / "ohlcv").glob("*.parquet"))

for file in files:
    parts = file.stem.split("_")
    symbol = parts[0]
    source = parts[1]
    interval = parts[2]

    try:
        update_ohlcv(symbol, source=source, interval=interval)
        print(f"{symbol} updated")
    except Exception as e:
        print(f"{symbol} failed: {e}")


# Refresh the csv
summary = build_quality_summary()

print(f"Update complete at {pd.Timestamp.now()}")



