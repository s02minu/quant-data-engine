import pandas as pd
import pytest

from qde.ingest import get_ingestor


def _yfinance():
    return get_ingestor("yfinance")


def test_returns_nonempty_dataframe(offline_yfinance):
    df = _yfinance().load_native("BTC-USD", "2024-01-01", "2024-02-01")
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_columns_are_lowercase_ohlcv(offline_yfinance):
    df = _yfinance().load_native("BTC-USD", "2024-01-01", "2024-02-01")
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]
    assert df.index.name == "date"


def test_invalid_symbol_raises(offline_yfinance):
    with pytest.raises(ValueError):
        _yfinance().load_native("NOTAREALTICKER", "2024-01-01", "2024-02-01")


def test_end_is_inclusive_like_every_other_source(monkeypatch):
    """yfinance's `end` is exclusive; this platform's range contract is not.

    Unnormalised it drops the final day of every ranged fetch — which surfaced as
    all eight yfinance symbols reporting "the source is dropping history" in the
    weekly verification, each missing exactly the last settled date.
    """
    import pandas as pd

    from qde.ingest.yfinance import YfinanceIngestor

    seen = {}

    def _fake_download(tickers=None, start=None, end=None, interval=None, auto_adjust=None):
        seen["end"] = end
        idx = pd.DatetimeIndex(pd.to_datetime(["2024-01-01"], utc=True), name="Date")
        return pd.DataFrame(
            {"Open": [1.0], "High": [2.0], "Low": [0.5], "Close": [1.5], "Volume": [10]},
            index=idx,
        )

    monkeypatch.setattr("qde.ingest.yfinance.yf.download", _fake_download)
    from qde.registry import get_spec
    YfinanceIngestor(get_spec("yfinance")).fetch_page(
        "SPY", "2024-01-01", "2024-01-01", "2024-01-31", "1d"
    )

    assert seen["end"] == "2024-02-01", "the requested end day must be included"


def test_a_missing_end_stays_missing(monkeypatch):
    import pandas as pd

    from qde.ingest.yfinance import YfinanceIngestor

    seen = {}

    def _fake_download(tickers=None, start=None, end=None, interval=None, auto_adjust=None):
        seen["end"] = end
        return pd.DataFrame()

    monkeypatch.setattr("qde.ingest.yfinance.yf.download", _fake_download)
    from qde.registry import get_spec
    YfinanceIngestor(get_spec("yfinance")).fetch_page(
        "SPY", "2024-01-01", "2024-01-01", None, "1d"
    )
    assert seen["end"] is None
