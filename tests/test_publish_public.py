"""Tests for the public catalogue + redistributable-filtered public publish."""
import io
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


class DenyingS3(FakeS3):
    """An S3 stand-in that rejects every PutObject, the way an R2 token scoped to
    the wrong bucket does."""

    def upload_file(self, filename, bucket, key):
        raise PermissionError("An error occurred (AccessDenied) when calling PutObject")


def test_publish_counts_failed_uploads(tmp_path):
    # Regression: a token without write access to the public bucket made every
    # upload fail, yet the summary reported success with zero published files and
    # the nightly logged "maintenance done". Failures must be counted, not swallowed.
    _tiny_lake(tmp_path)
    summary = publish_public(str(tmp_path), "qde-public", DenyingS3(), public_base_url="https://d")

    assert summary["bronze_published"] == 0
    assert summary["gold_published"] == 0
    assert summary["catalogue"] is False
    assert summary["bronze_failed"] >= 1
    assert summary["gold_failed"] >= 1
    # The catalogue failure counts toward the total too.
    assert summary["failed"] == summary["bronze_failed"] + summary["gold_failed"] + 1


def test_publish_reports_no_failures_on_success(tmp_path):
    _tiny_lake(tmp_path)
    summary = publish_public(str(tmp_path), "qde-public", FakeS3(), public_base_url="https://d")

    assert summary["failed"] == 0
    assert summary["bronze_failed"] == 0
    assert summary["gold_failed"] == 0


def test_publish_mirrors_dq_history(tmp_path):
    # The quality history is our own operational record, not licensed data, so it
    # publishes in full — it is what a status page reads.
    from qde.checks import Violation
    from qde.dq_history import record_run

    _tiny_lake(tmp_path)
    record_run(
        [Violation("series", "fred", "UNRATE", None, "freshness", "warn", "stale")],
        str(tmp_path),
    )
    client = FakeS3()
    summary = publish_public(str(tmp_path), "qde-public", client, public_base_url="https://d")

    keys = [k for (_b, k) in client.objects]
    assert any(k.startswith("quality/dq_runs/") for k in keys)
    assert any(k.startswith("quality/dq_violations/") for k in keys)
    assert summary["quality_published"] >= 2
    assert summary["failed"] == 0


def test_clean_run_still_publishes_the_run_record(tmp_path):
    # A clean night has no violations file, but the run record must still reach the
    # bucket — otherwise a status page cannot tell "healthy" from "never ran".
    from qde.dq_history import record_run

    _tiny_lake(tmp_path)
    record_run([], str(tmp_path))
    client = FakeS3()
    summary = publish_public(str(tmp_path), "qde-public", client, public_base_url="https://d")

    keys = [k for (_b, k) in client.objects]
    assert any(k.startswith("quality/dq_runs/") for k in keys)
    assert not any(k.startswith("quality/dq_violations/") for k in keys)
    assert summary["quality_published"] >= 1


def test_publish_writes_http_readable_consolidated_quality_files(tmp_path):
    # Partitioned files cannot be globbed over plain HTTP (no directory listing),
    # so each quality table also ships as one file at a stable path.
    from qde.checks import Violation
    from qde.dq_history import record_run

    _tiny_lake(tmp_path)
    v = Violation("series", "fred", "UNRATE", None, "freshness", "warn", "x")
    record_run([v], str(tmp_path))
    client = FakeS3()
    publish_public(str(tmp_path), "qde-public", client, public_base_url="https://d")

    keys = [k for (_b, k) in client.objects]
    assert "quality/dq_runs.parquet" in keys
    assert "quality/dq_violations.parquet" in keys


def test_consolidated_quality_file_spans_every_partition(tmp_path):
    import pandas as pd

    from qde.dq_history import record_run

    _tiny_lake(tmp_path)
    record_run([], str(tmp_path), pd.Timestamp("2026-08-14T00:30:00Z"))
    record_run([], str(tmp_path), pd.Timestamp("2026-08-15T00:30:00Z"))
    client = FakeS3()
    publish_public(str(tmp_path), "qde-public", client, public_base_url="https://d")

    blob = client.blobs[("qde-public", "quality/dq_runs.parquet")]
    merged = pd.read_parquet(io.BytesIO(blob))
    # Both days present in the single file a browser will actually read.
    assert sorted(merged["run_date"].unique()) == ["2026-08-14", "2026-08-15"]


def test_bronze_consolidated_file_is_published(tmp_path):
    # The catalogue's advertised query points here; a glob cannot be expanded over
    # plain HTTP, so this single file is what actually has to exist.
    _tiny_lake(tmp_path)
    client = FakeS3()
    publish_public(str(tmp_path), "qde-public", client, public_base_url="https://d")

    keys = [k for (_b, k) in client.objects]
    assert "bronze/group=bars/all.parquet" in keys


def test_bronze_consolidated_file_excludes_nonredistributable_sources(tmp_path):
    # The whole licensing split rests on this: the merged file is built from the
    # included files only, so yfinance must not reappear in it.
    import pandas as pd

    _tiny_lake(tmp_path)
    client = FakeS3()
    publish_public(str(tmp_path), "qde-public", client, public_base_url="https://d")

    blob = client.blobs[("qde-public", "bronze/group=bars/all.parquet")]
    merged = pd.read_parquet(io.BytesIO(blob))
    if "source" in merged.columns:
        assert "yfinance" not in set(merged["source"])
    assert len(merged) > 0


def _write_series(base, source, series_id, metric=None):
    """A series partition, with or without the optional metric= level."""
    part = Path(base) / "bronze" / "group=series" / f"source={source}" / f"series_id={series_id}"
    if metric:
        part = part / f"metric={metric}"
    part.mkdir(parents=True, exist_ok=True)
    idx = pd.DatetimeIndex(pd.to_datetime(["2024-01-01", "2024-01-02"], utc=True), name="date")
    pd.DataFrame({"value": [1.0, 2.0]}, index=idx).to_parquet(part / "series.parquet")


def test_consolidates_series_across_mixed_partition_depths(tmp_path):
    # Regression: single-value sources sit at source/series_id/ while multi-metric
    # ones add a metric= level. Reading both under one hive glob raises
    # "Hive partition mismatch ... key metric not found", which broke the publish.
    _write_series(tmp_path, "cboe", "SKEW")                              # flat
    _write_series(tmp_path, "binancefut", "BTCUSDT", metric="funding_rate")  # nested
    client = FakeS3()

    summary = publish_public(str(tmp_path), "qde-public", client, public_base_url="https://d")

    assert summary["failed"] == 0
    blob = client.blobs[("qde-public", "bronze/group=series/all.parquet")]
    merged = pd.read_parquet(io.BytesIO(blob))
    # Both depths present; the flat source simply carries a null metric.
    assert set(merged["source"]) == {"cboe", "binancefut"}
    assert merged["metric"].isna().any()
