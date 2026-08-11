"""Tests for the public catalogue + redistributable-filtered public publish."""

from pathlib import Path

import duckdb
import pandas as pd

from qde.catalogue import build_catalogue
from qde.publish_public import publish_public


class FakeS3:
    """Records uploads as {(bucket, key): size} and keeps the bytes so a test can
    read an uploaded Parquet back and assert its contents."""

    def __init__(self):
        self.objects: dict[tuple[str, str], int] = {}
        self.blobs: dict[tuple[str, str], bytes] = {}

    def upload_file(self, filename, bucket, key):
        data = Path(filename).read_bytes()
        self.objects[(bucket, key)] = len(data)
        self.blobs[(bucket, key)] = data

    def head_object(self, Bucket, Key):
        return {"ContentLength": self.objects[(Bucket, Key)]}


def _write_bars(base, source, symbol):
    part = (
        Path(base) / "bronze" / "group=bars" / f"source={source}"
        / f"symbol={symbol}" / "interval=1d"
    )
    part.mkdir(parents=True, exist_ok=True)
    idx = pd.DatetimeIndex(pd.to_datetime(["2024-01-01", "2024-01-02"], utc=True), name="date")
    df = pd.DataFrame(
        {"open": [1.0, 2.0], "high": [2.0, 3.0], "low": [0.5, 1.5],
         "close": [1.5, 2.5], "volume": [10.0, 20.0]},
        index=idx,
    )
    df.to_parquet(part / "bars.parquet")


def _write_gold_bars(base, sources):
    # Minimal fct_bars_daily with a `source` column spanning multiple sources.
    part = Path(base) / "gold" / "group=bars" / "mart=fct_bars_daily"
    part.mkdir(parents=True, exist_ok=True)
    rows = [{"source": s, "symbol": "X", "date": pd.Timestamp("2024-01-01"), "close": 1.0}
            for s in sources]
    pd.DataFrame(rows).to_parquet(part / "data.parquet", index=False)


def _tiny_lake(base):
    _write_bars(base, "binance", "BTCUSDT")  # redistributable
    _write_bars(base, "yfinance", "SPY")     # NOT redistributable
    _write_gold_bars(base, ["binance", "yfinance"])


def test_catalogue_has_sources_datasets_and_flags_excluded(tmp_path):
    _tiny_lake(str(tmp_path))
    cat = build_catalogue(str(tmp_path), public_base_url="https://data.test")

    assert cat["notes"]["excluded_sources"] == ["yfinance"]
    assert cat["serving_model"] == "publish-files-not-queries"
    # every registered source appears; binance carries live rows, and its flag is set.
    binance = next(s for s in cat["sources"] if s["name"] == "binance")
    assert binance["rows"] == 2 and binance["redistributable"] is True
    yf = next(s for s in cat["sources"] if s["name"] == "yfinance")
    assert yf["redistributable"] is False
    # datasets include the bars group and the gold mart, each with a schema + query.
    ids = {d["id"] for d in cat["datasets"]}
    assert {"bars", "fct_bars_daily"} <= ids
    bars_ds = next(d for d in cat["datasets"] if d["id"] == "bars")
    assert bars_ds["schema"] and "https://data.test" in bars_ds["sample_query"]


def test_publish_skips_nonredistributable_bronze(tmp_path):
    _tiny_lake(str(tmp_path))
    client = FakeS3()

    summary = publish_public(str(tmp_path), "qde-public", client, public_base_url="https://d")

    keys = {k for (_, k) in client.objects}
    assert any("source=binance" in k for k in keys)          # redistributable shipped
    assert not any("source=yfinance" in k for k in keys)     # excluded withheld
    assert summary["bronze_skipped"] >= 1
    assert summary["excluded"] == ["yfinance"]


def test_publish_filters_nonredistributable_rows_from_gold(tmp_path):
    _tiny_lake(str(tmp_path))
    client = FakeS3()

    publish_public(str(tmp_path), "qde-public", client, public_base_url="https://d")

    key = "gold/group=bars/mart=fct_bars_daily/data.parquet"
    blob = client.blobs[("qde-public", key)]
    out = tmp_path / "roundtrip.parquet"
    out.write_bytes(blob)
    sources = set(
        duckdb.sql(f"SELECT DISTINCT source FROM read_parquet('{out.as_posix()}')")
        .df()["source"]
    )
    assert sources == {"binance"}  # yfinance rows filtered out of the public gold


def test_publish_uploads_catalogue_json(tmp_path):
    _tiny_lake(str(tmp_path))
    client = FakeS3()

    summary = publish_public(str(tmp_path), "qde-public", client, public_base_url="https://d")

    assert summary["catalogue"] is True
    assert ("qde-public", "catalogue.json") in client.objects
