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


def test_cross_venue_basis_is_never_published():
    # The cross-venue basis is derived from the private microstructure capture and
    # is the platform's research wedge. It is synced to the PRIVATE bucket (it is in
    # lake._GOLD_MARTS) but must not appear in the public one. Publishing is a
    # one-way door, so this asserts the two lists stay separate.
    from qde.lake import _GOLD_MARTS
    from qde.publish_public import _PUBLIC_MARTS

    assert "fct_cross_venue_basis" in _GOLD_MARTS, "should sync to the private bucket"
    assert not any("cross_venue_basis" in k for k in _PUBLIC_MARTS), (
        "cross-venue basis must NOT be in the public publish list"
    )


# --- the publication gate is an allowlist ---------------------------------------
#
# Publishing is the one irreversible thing the platform does. Filtering by "not
# forbidden" served anything the registry had never heard of; filtering by
# "explicitly permitted" cannot.


def test_a_source_the_registry_does_not_know_is_never_published(tmp_path):
    _write_bars(tmp_path, "binance", "BTCUSDT")  # registered, redistributable
    # A retired spec whose files remain, a directory dropped in by hand, a source
    # seeded before it was declared — all the same shape, and all were published.
    _write_bars(tmp_path, "some_licensed_vendor", "SECRET")

    s3 = FakeS3()
    summary = publish_public(base_dir=str(tmp_path), public_bucket="pub", client=s3)

    assert not any("some_licensed_vendor" in k for _, k in s3.objects)
    assert any("source=binance" in k for _, k in s3.objects)
    assert summary["bronze_skipped"] >= 1


def test_an_unknown_source_is_kept_out_of_the_consolidated_files(tmp_path):
    # The per-file loop and the merged file are separate code paths, so the gate has
    # to hold in both — the consolidated file is what the catalogue tells people to
    # query, which makes it the copy most likely to actually be read.
    _write_bars(tmp_path, "binance", "BTCUSDT")
    _write_bars(tmp_path, "some_licensed_vendor", "SECRET")
    _write_gold_bars(tmp_path, ["binance", "some_licensed_vendor", "yfinance"])

    s3 = FakeS3()
    publish_public(base_dir=str(tmp_path), public_bucket="pub", client=s3)

    for key in ("bronze/group=bars/all.parquet",
                "gold/group=bars/mart=fct_bars_daily/data.parquet"):
        blob = s3.blobs[("pub", key)]
        got = duckdb.connect().execute(
            "SELECT DISTINCT source FROM read_parquet(?)", [_as_file(tmp_path, key, blob)]
        ).fetchall()
        assert {r[0] for r in got} == {"binance"}, key


def _as_file(tmp_path, key, blob):
    path = tmp_path / (key.replace("/", "__") + ".check")
    path.write_bytes(blob)
    return str(path)


# --- microstructure: mirrored bucket-to-bucket -----------------------------------
#
# Every other group publishes from the local lake. Microstructure cannot: the sync
# that runs first ships it to R2 and prunes it locally, so on disk there is only a
# half-written current day. The bytes exist only in R2, so the copy happens there.


class FakeR2:
    """Two buckets with server-side copy, enough to exercise the mirror."""

    def __init__(self, private: dict[str, int]):
        self.buckets = {"priv": dict(private), "pub": {}}
        self.copies: list[str] = []
        self.fail_on: set[str] = set()

    def get_paginator(self, _op):
        outer = self

        class P:
            def paginate(self, Bucket, Prefix=""):
                yield {
                    "Contents": [
                        {"Key": k, "Size": v}
                        for k, v in sorted(outer.buckets[Bucket].items())
                        if k.startswith(Prefix)
                    ]
                }

        return P()

    def head_object(self, Bucket, Key):
        if Key not in self.buckets[Bucket]:
            raise KeyError(Key)
        return {"ContentLength": self.buckets[Bucket][Key]}

    def copy_object(self, Bucket, Key, CopySource):
        if Key in self.fail_on:
            raise OSError("copy refused")
        self.buckets[Bucket][Key] = self.buckets[CopySource["Bucket"]][CopySource["Key"]]
        self.copies.append(Key)


def test_microstructure_is_mirrored_from_the_private_bucket(tmp_path):
    from qde.publish_public import _MIRRORED_PREFIX, mirror_private_prefix

    tick = "kind=trades/symbol=BTCUSDT/date=2026-08-01/p.parquet"
    r2 = FakeR2({
        f"{_MIRRORED_PREFIX}source=binance/{tick}": 10,
        f"{_MIRRORED_PREFIX}source=coinbase/{tick}": 20,
        "bronze/group=bars/source=binance/symbol=BTCUSDT/interval=1d/bars.parquet": 5,
    })
    out = mirror_private_prefix(r2, "priv", "pub", _MIRRORED_PREFIX)

    assert out["copied"] == 2
    # Only the requested prefix moves; bars publish from the local lake instead and
    # must not be duplicated through this path.
    assert not any("group=bars" in k for k in r2.buckets["pub"])


def test_mirror_skips_what_is_already_published(tmp_path):
    # Without this the nightly would re-copy the entire archive every single night.
    from qde.publish_public import _MIRRORED_PREFIX, mirror_private_prefix

    r2 = FakeR2({f"{_MIRRORED_PREFIX}a/p.parquet": 10, f"{_MIRRORED_PREFIX}b/p.parquet": 20})
    first = mirror_private_prefix(r2, "priv", "pub", _MIRRORED_PREFIX)
    second = mirror_private_prefix(r2, "priv", "pub", _MIRRORED_PREFIX)

    assert first["copied"] == 2 and first["skipped"] == 0
    assert second["copied"] == 0 and second["skipped"] == 2


def test_mirror_recopies_a_partition_that_changed_size(tmp_path):
    # Compaction rewrites a settled partition into one file; the public copy has to
    # follow, or it keeps serving the superseded version forever.
    from qde.publish_public import _MIRRORED_PREFIX, mirror_private_prefix

    r2 = FakeR2({f"{_MIRRORED_PREFIX}a/p.parquet": 10})
    mirror_private_prefix(r2, "priv", "pub", _MIRRORED_PREFIX)
    r2.buckets["priv"][f"{_MIRRORED_PREFIX}a/p.parquet"] = 99

    out = mirror_private_prefix(r2, "priv", "pub", _MIRRORED_PREFIX)
    assert out["copied"] == 1
    assert r2.buckets["pub"][f"{_MIRRORED_PREFIX}a/p.parquet"] == 99


def test_mirror_never_deletes_from_the_public_bucket():
    # The public copy is an archive. A partition pruned locally, or absent from the
    # private bucket, is expected — never a reason to withdraw published history.
    from qde.publish_public import _MIRRORED_PREFIX, mirror_private_prefix

    r2 = FakeR2({f"{_MIRRORED_PREFIX}new/p.parquet": 10})
    r2.buckets["pub"][f"{_MIRRORED_PREFIX}old/p.parquet"] = 7
    mirror_private_prefix(r2, "priv", "pub", _MIRRORED_PREFIX)

    assert f"{_MIRRORED_PREFIX}old/p.parquet" in r2.buckets["pub"]


def test_a_failed_copy_is_counted_not_swallowed():
    from qde.publish_public import _MIRRORED_PREFIX, mirror_private_prefix

    r2 = FakeR2({f"{_MIRRORED_PREFIX}a/p.parquet": 10, f"{_MIRRORED_PREFIX}b/p.parquet": 20})
    r2.fail_on = {f"{_MIRRORED_PREFIX}b/p.parquet"}
    out = mirror_private_prefix(r2, "priv", "pub", _MIRRORED_PREFIX)

    assert out["copied"] == 1 and out["failed"] == 1


def test_staging_files_cannot_pollute_the_next_merge(tmp_path):
    # The merged file used to be written beside the partitions it was built from,
    # inside a recursive glob. One crash between writing and deleting it and every
    # row would be counted twice, forever, with nothing raised.
    from qde.publish_public import _STAGING

    _write_bars(tmp_path, "binance", "BTCUSDT")
    publish_public(base_dir=str(tmp_path), public_bucket="pub", client=FakeS3())

    stray = list((tmp_path / "bronze").rglob("all.parquet"))
    assert stray == [], "a consolidated file must never be left inside the lake tree"
    assert _STAGING not in {p.name for p in (tmp_path / "bronze").rglob("*")}


# --- the catalogue must describe the file it publishes, not the lake it read -------


def test_catalogue_row_counts_exclude_withheld_sources(tmp_path):
    # Found live: catalogue.json advertised 57,362 bars rows against a public file
    # holding 40,642. Nothing leaked — the publish filter was correct — but the
    # published *description* of the product overstated it by 29%, because the count
    # was taken over the whole local lake including yfinance.
    _tiny_lake(str(tmp_path))
    cat = build_catalogue(str(tmp_path), public_base_url="https://data.test")

    bars = next(d for d in cat["datasets"] if d["id"] == "bars")
    assert bars["row_count"] == 2, "binance only; the two yfinance rows are never published"

    gold = next(d for d in cat["datasets"] if d["id"] == "fct_bars_daily")
    assert gold["row_count"] == 1, "the gold mart is row-filtered on publish too"


def test_the_advertised_count_matches_what_publish_actually_uploads(tmp_path):
    # The invariant that makes the bug impossible rather than merely fixed: whatever
    # the catalogue claims, read the bytes the publisher uploaded and count them.
    _tiny_lake(str(tmp_path))
    fake = FakeS3()
    publish_public(str(tmp_path), "pub", fake, public_base_url="https://data.test")

    blob = fake.blobs[("pub", "bronze/group=bars/all.parquet")]
    uploaded = duckdb.connect().execute(
        "SELECT count(*) FROM read_parquet(?)", [_spill(tmp_path, blob)]
    ).fetchone()[0]

    cat = build_catalogue(str(tmp_path), public_base_url="https://data.test")
    advertised = next(d for d in cat["datasets"] if d["id"] == "bars")["row_count"]
    assert advertised == uploaded, (
        f"catalogue advertises {advertised} rows, public file holds {uploaded}"
    )


def _spill(tmp_path, blob: bytes) -> str:
    path = Path(tmp_path) / "_uploaded.parquet"
    path.write_bytes(blob)
    return path.as_posix()


# --- one definition of "late", published so every surface can share it ------------


def _write_weekly_series(base, source, series_id, weeks=12):
    part = Path(base) / "bronze" / "group=series" / f"source={source}" / f"series_id={series_id}"
    part.mkdir(parents=True, exist_ok=True)
    idx = pd.DatetimeIndex(
        pd.date_range("2024-01-05", periods=weeks, freq="7D", tz="UTC"), name="date"
    )
    pd.DataFrame({"value": range(weeks)}, index=idx).to_parquet(part / "series.parquet")


def test_the_catalogue_publishes_the_threshold_the_pipeline_enforces(tmp_path):
    # The status page graded freshness with its own group constants (series = 72h),
    # so CFTC's weekly release looked overdue every week and the page reported
    # "2 of 14 sources behind schedule" on a night the pipeline logged zero
    # violations. The threshold now ships with the data, from qde.checks itself.
    from qde.checks import freshness_threshold
    from qde.registry import get_spec

    _write_weekly_series(str(tmp_path), "cftc", "ES")
    cat = build_catalogue(str(tmp_path), public_base_url="https://data.test")

    cftc = next(s for s in cat["sources"] if s["name"] == "cftc")
    published = cftc["expected_within_hours"]

    dates = pd.DatetimeIndex(pd.date_range("2024-01-05", periods=12, freq="7D", tz="UTC"))
    floor = pd.Timedelta(minutes=get_spec("cftc").freshness_sla_minutes)
    expected = freshness_threshold(dates, floor).total_seconds() / 3600

    assert published == round(expected, 1)
    assert published > 168, "a weekly release must not be judged on a daily budget"


def test_a_multi_metric_series_is_measured_at_the_metric_grain(tmp_path):
    # CFTC's eleven metrics share one series_id and identical report dates. Keyed on
    # series_id alone they pool, the gaps collapse to zero, and a weekly release
    # inherits the 25-hour SLA floor.
    base = Path(tmp_path) / "bronze" / "group=series" / "source=cftc" / "series_id=ES"
    idx = pd.DatetimeIndex(
        pd.date_range("2024-01-05", periods=12, freq="7D", tz="UTC"), name="date"
    )
    for metric in ("dealer_long", "dealer_short", "lev_long"):
        part = base / f"metric={metric}"
        part.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"value": range(12)}, index=idx).to_parquet(part / "series.parquet")

    cat = build_catalogue(str(tmp_path), public_base_url="https://data.test")
    cftc = next(s for s in cat["sources"] if s["name"] == "cftc")
    assert cftc["expected_within_hours"] > 168, "pooled metrics collapsed the cadence"


# --- publishing gates on EVERY route to the public bucket -------------------------


class FakeMirrorS3:
    """Minimal S3 double for the server-side copy path."""

    def __init__(self, keys):
        self._keys = list(keys)
        self.copied = []

    def get_paginator(self, _op):
        outer = self

        class _P:
            def paginate(self, Bucket=None, Prefix=None):
                yield {"Contents": [{"Key": k, "Size": 10} for k in outer._keys
                                    if k.startswith(Prefix or "")]}

        return _P()

    def head_object(self, Bucket, Key):
        raise RuntimeError("absent")

    def copy_object(self, Bucket, Key, CopySource):
        self.copied.append(Key)


def test_the_microstructure_mirror_withholds_unregistered_sources():
    # This path copies by prefix rather than from the local lake, so it originally
    # trusted the prefix alone — an unregistered venue, or a licensing change on an
    # existing one, would have been republished automatically on the next nightly.
    from qde.publish_public import mirror_private_prefix

    keys = [
        "bronze/group=microstructure/source=binance/kind=trades/date=2026-01-01/p.parquet",
        "bronze/group=microstructure/source=ghostvenue/kind=trades/date=2026-01-01/p.parquet",
    ]
    fake = FakeMirrorS3(keys)
    out = mirror_private_prefix(
        fake, "priv", "pub", "bronze/group=microstructure/",
        pairs=frozenset({("microstructure", "binance")}),
    )

    assert out["withheld"] == 1
    assert all("ghostvenue" not in k for k in fake.copied)
    assert any("binance" in k for k in fake.copied)


def test_a_key_with_unreadable_partitions_is_withheld_not_assumed_safe():
    from qde.publish_public import mirror_private_prefix

    fake = FakeMirrorS3(["bronze/group=microstructure/mystery-object.parquet"])
    out = mirror_private_prefix(
        fake, "priv", "pub", "bronze/group=microstructure/",
        pairs=frozenset({("microstructure", "binance")}),
    )
    assert out["withheld"] == 1 and fake.copied == []


def test_permission_in_one_group_does_not_leak_into_another():
    # The rule is a (group, source) pair precisely because a venue can be
    # redistributable in one group and not another. Filtering consolidated files on
    # the bare source name quietly widened that back out.
    from qde.publish_public import _group_in_list

    pairs = frozenset({("microstructure", "acme"), ("bars", "other")})
    assert "acme" in _group_in_list(pairs, "microstructure")
    assert "acme" not in _group_in_list(pairs, "bars"), "cleared for microstructure only"
    assert _group_in_list(pairs, "events") == "''", "no cleared source matches nothing"
