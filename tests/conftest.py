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


# CBOE volatility-index CSVs, keyed by ticker. VIX carries OHLC (value is the
# last column, CLOSE, which differs from OPEN so a test can prove the right
# column is picked); VVIX/SKEW carry a single value column. Dates are MM/DD/YYYY.
_CBOE_CSVS = {
    "VIX": (
        "DATE,OPEN,HIGH,LOW,CLOSE\n"
        "01/02/2024,12.000000,13.000000,11.500000,12.500000\n"
        "01/03/2024,12.500000,14.000000,12.100000,13.750000\n"
        "01/04/2024,13.750000,15.200000,13.600000,14.900000\n"
    ),
    "VVIX": (
        "DATE,VVIX\n"
        "01/02/2024,80.000000\n"
        "01/03/2024,82.500000\n"
        "01/04/2024,85.100000\n"
    ),
    "SKEW": (
        "DATE,SKEW\n"
        "01/02/2024,120.000000\n"
        "01/03/2024,121.500000\n"
        "01/04/2024,119.900000\n"
    ),
}


@pytest.fixture
def offline_cboe(monkeypatch):
    """Serve a canned CBOE index CSV chosen by the URL's ``{SYMBOL}_History.csv``;
    an unknown index returns a 404 as the CDN does. No API key — the CSVs are
    public. The symbol is parsed from the filename so ``VIX`` is not mistaken for
    the ``VVIX`` file (``VIX_History.csv`` is a substring of ``VVIX_History.csv``)."""

    def fake_get(url, params):
        symbol = url.rsplit("/", 1)[-1].split("_")[0]
        if symbol not in _CBOE_CSVS:
            return FakeResponse(status_code=404, text="Not Found")
        return FakeResponse(text=_CBOE_CSVS[symbol])

    monkeypatch.setattr(http_mod.requests, "get", fake_get)


def _cot_rows():
    """Three weekly TFF rows in Socrata's shape (string values). The last row
    omits the leveraged-funds long field, exercising the missing-value -> NaN
    path; the position values differ per column so a test can prove the
    raw-column -> metric mapping is not scrambled."""
    base = {
        "dealer_positions_long_all": "100",
        "dealer_positions_short_all": "110",
        "asset_mgr_positions_long": "200",
        "asset_mgr_positions_short": "210",
        "lev_money_positions_long": "300",
        "lev_money_positions_short": "310",
        "other_rept_positions_long": "400",
        "other_rept_positions_short": "410",
        "nonrept_positions_long_all": "500",
        "nonrept_positions_short_all": "510",
        "open_interest_all": "9000",
    }
    r1 = {"report_date_as_yyyy_mm_dd": "2024-01-02T00:00:00.000", **base}
    r2 = {"report_date_as_yyyy_mm_dd": "2024-01-09T00:00:00.000", **base}
    r3 = {"report_date_as_yyyy_mm_dd": "2024-01-16T00:00:00.000", **base}
    del r3["lev_money_positions_long"]  # missing category -> NaN, row kept
    return [r1, r2, r3]


@pytest.fixture
def offline_cftc(monkeypatch):
    """Serve canned CFTC COT rows, honoring the SoQL ``>= 'date'`` filter so a
    caught-up incremental pull returns an empty page (-> NoNewData). No API key —
    the Socrata endpoint is public."""
    import re

    def fake_get(url, params):
        rows = _cot_rows()
        match = re.search(r">= '([0-9-]+)T", params["$where"])
        if match:
            start = match.group(1)
            rows = [r for r in rows if r["report_date_as_yyyy_mm_dd"][:10] >= start]
        return FakeResponse(payload=rows)

    monkeypatch.setattr(http_mod.requests, "get", fake_get)


def _funding_rows():
    """Three 8-hourly Binance perp funding settlements (2024-01-01 00/08/16h). The
    first omits markPrice (empty string, as the earliest real history does) -> NaN;
    the funding rates differ so a column-mapping mistake would surface."""
    def row(ms, rate, mark):
        return {"symbol": "BTCUSDT", "fundingTime": ms, "fundingRate": rate, "markPrice": mark}

    return [
        row(1704067200000, "0.0001", ""),  # 2024-01-01 00:00, markPrice missing -> NaN
        row(1704096000000, "0.0002", "42000.0"),  # 08:00
        row(1704124800000, "-0.0003", "42500.0"),  # 16:00
    ]


@pytest.fixture
def offline_binance_futures(monkeypatch):
    """Serve canned Binance perp funding, honoring startTime/endTime/limit so both
    the caught-up (-> NoNewData) and the multi-page pagination paths are exercised
    against the same fake. No API key — the fapi endpoint is public."""

    def fake_get(url, params):
        rows = [
            r
            for r in _funding_rows()
            if params["startTime"] <= r["fundingTime"] <= params["endTime"]
        ]
        return FakeResponse(payload=rows[: params["limit"]])

    monkeypatch.setattr(http_mod.requests, "get", fake_get)
