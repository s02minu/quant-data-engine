"""Shared fixtures: canned API payloads so the batch-loader tests run offline.

The batch loaders reach the network at two boundaries — ``requests.get`` for
Binance and Kraken (via ``qde.loaders.http``) and ``yfinance.download`` for
Yahoo. These fixtures monkeypatch those boundaries with realistic canned
responses so the loader tests need no internet. They exercise the parsing and
shaping contract, not the live API's — matching the fake-injection style already
used in ``test_sync`` and ``test_stream_collector``.
"""

import pandas as pd
import pytest

import qde.ingest.yfinance as yf_mod
import qde.loaders.http as http_mod


class FakeResponse:
    """Minimal stand-in for ``requests.Response``: just ``status_code``,
    ``json()``, and ``text`` — the only attributes the HTTP helper touches."""

    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload


def _binance_klines():
    """Three daily BTC klines in Binance's list-of-lists shape (12 fields each)."""
    return [
        [1704067200000, "42000.0", "43000.0", "41500.0", "42750.0", "1000.0",
         1704153599999, "42750000.0", 5000, "500.0", "21375000.0", "0"],
        [1704153600000, "42750.0", "45500.0", "42600.0", "44900.0", "1200.0",
         1704239999999, "53880000.0", 6000, "600.0", "26940000.0", "0"],
        [1704240000000, "44900.0", "45200.0", "43800.0", "44100.0", "900.0",
         1704326399999, "39690000.0", 4500, "450.0", "19845000.0", "0"],
    ]


def _kraken_result(since):
    """One page of Kraken OHLC. ``last`` echoes the request cursor so the
    loader's pagination loop terminates on the next identical page — there is no
    live cursor to advance here."""
    candles = [
        [1704067200, "42000.0", "43000.0", "41500.0", "42750.0", "42600.0", "1000.0", 5000],
        [1704153600, "42750.0", "45500.0", "42600.0", "44900.0", "44100.0", "1200.0", 6000],
        [1704240000, "44900.0", "45200.0", "43800.0", "44100.0", "44000.0", "900.0", 4500],
    ]
    return {"error": [], "result": {"XXBTZUSD": candles, "last": since}}


def _yf_frame():
    """A yfinance-style daily OHLCV frame: title-case columns, naive index."""
    index = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
    return pd.DataFrame(
        {
            "Open": [42000.0, 42750.0, 44900.0],
            "High": [43000.0, 45500.0, 45200.0],
            "Low": [41500.0, 42600.0, 43800.0],
            "Close": [42750.0, 44900.0, 44100.0],
            "Volume": [1000, 1200, 900],
        },
        index=index,
    )


@pytest.fixture
def offline_binance(monkeypatch):
    """Serve canned Binance klines; an unknown symbol returns a 400, as the API
    does, so the loader's error path is exercised without internet."""

    def fake_get(url, params):
        if params["symbol"] == "NOTAREALTICKER":
            return FakeResponse(status_code=400, text="Invalid symbol.")
        return FakeResponse(payload=_binance_klines())

    monkeypatch.setattr(http_mod.requests, "get", fake_get)


@pytest.fixture
def offline_kraken(monkeypatch):
    """Serve canned Kraken OHLC; an unknown pair returns Kraken's in-body error
    list, which the loader raises on."""

    def fake_get(url, params):
        if params["pair"] == "NOTREAL":
            return FakeResponse(payload={"error": ["EQuery:Unknown asset pair"], "result": {}})
        return FakeResponse(payload=_kraken_result(params["since"]))

    monkeypatch.setattr(http_mod.requests, "get", fake_get)


@pytest.fixture
def offline_yfinance(monkeypatch):
    """Serve a canned yfinance frame; an unknown ticker returns an empty frame,
    which the loader raises on."""

    def fake_download(tickers, start=None, end=None, interval="1d", auto_adjust=True):
        if tickers == "NOTAREALTICKER":
            return pd.DataFrame()
        return _yf_frame()

    monkeypatch.setattr(yf_mod.yf, "download", fake_download)


def _fred_observations():
    """Three FRED observations, including a missing '.' value (coerced to NaN)."""
    return {
        "observations": [
            {"realtime_start": "2024-06-01", "realtime_end": "9999-12-31",
             "date": "2024-01-01", "value": "3.1"},
            {"realtime_start": "2024-06-01", "realtime_end": "9999-12-31",
             "date": "2024-02-01", "value": "3.2"},
            {"realtime_start": "2024-06-01", "realtime_end": "9999-12-31",
             "date": "2024-03-01", "value": "."},  # missing -> NaN, row kept
        ]
    }


@pytest.fixture
def offline_fred(monkeypatch):
    """Serve a canned FRED observations payload; an unknown series id returns a
    400 as the API does. Sets a dummy FRED_API_KEY so the key guard passes."""
    monkeypatch.setenv("FRED_API_KEY", "test-key")

    def fake_get(url, params):
        if params["series_id"] == "NOTREAL":
            return FakeResponse(status_code=400, text="Bad Request")
        return FakeResponse(payload=_fred_observations())

    monkeypatch.setattr(http_mod.requests, "get", fake_get)
