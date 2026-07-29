import pandas as pd

from qde.quality import build_quality_summary
from qde.storage import _bars_path


def test_quality_summary_empty(tmp_path):
    summary = build_quality_summary(base_dir=str(tmp_path))
    assert summary.empty


def test_quality_summary_reports_series(tmp_path):
    # Two consecutive daily bars, clean.
    df = pd.DataFrame(
        {
            "open": [1.0, 1.1],
            "high": [2.0, 2.1],
            "low": [0.5, 0.6],
            "close": [1.5, 1.6],
            "volume": [10.0, 11.0],
        },
        index=pd.DatetimeIndex(["2024-01-01", "2024-01-02"], tz="UTC", name="date"),
    )
    path = _bars_path("BTCUSDT", "binance", "1d", base_dir=str(tmp_path))
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine="pyarrow")

    summary = build_quality_summary(base_dir=str(tmp_path))

    assert len(summary) == 1
    assert summary.loc[0, "symbol"] == "BTCUSDT"
    assert summary.loc[0, "total_rows"] == 2
