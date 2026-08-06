from qde.lake import bars_glob, bronze_glob, series_glob


def test_bronze_glob_defaults_to_all_partitions(monkeypatch):
    monkeypatch.setenv("QDE_R2_BUCKET", "qde-lake")
    assert bronze_glob() == (
        "r2://qde-lake/bronze/group=microstructure/source=binance/kind=*/symbol=*/date=*/*.parquet"
    )


def test_bronze_glob_narrows_by_partition(monkeypatch):
    monkeypatch.setenv("QDE_R2_BUCKET", "qde-lake")
    glob = bronze_glob(kind="trades", symbol="BTCUSDT", date="2026-07-25")
    assert "kind=trades/symbol=BTCUSDT/date=2026-07-25" in glob


def test_bronze_glob_bucket_override():
    assert bronze_glob(bucket="other").startswith("r2://other/bronze/")


def test_bars_glob_defaults_to_all_partitions(monkeypatch):
    monkeypatch.setenv("QDE_R2_BUCKET", "qde-lake")
    assert bars_glob() == (
        "r2://qde-lake/bronze/group=bars/source=*/symbol=*/interval=*/*.parquet"
    )


def test_bars_glob_narrows_by_partition(monkeypatch):
    monkeypatch.setenv("QDE_R2_BUCKET", "qde-lake")
    assert "source=binance/symbol=BTCUSDT/interval=1d" in bars_glob(
        source="binance", symbol="BTCUSDT", interval="1d"
    )


def test_series_glob_defaults_to_the_flat_depth(monkeypatch):
    # metric=None (default) globs the flat single-value files (FRED/CBOE).
    monkeypatch.setenv("QDE_R2_BUCKET", "qde-lake")
    assert series_glob() == (
        "r2://qde-lake/bronze/group=series/source=*/series_id=*/series.parquet"
    )


def test_series_glob_metric_depth(monkeypatch):
    # A metric value globs one level deeper (COT/perp metric= partitions).
    monkeypatch.setenv("QDE_R2_BUCKET", "qde-lake")
    assert series_glob(metric="*") == (
        "r2://qde-lake/bronze/group=series/source=*/series_id=*/metric=*/series.parquet"
    )
    assert series_glob(source="cftc", series_id="ES", metric="dealer_long").endswith(
        "source=cftc/series_id=ES/metric=dealer_long/series.parquet"
    )


def test_series_glob_narrows_by_partition(monkeypatch):
    monkeypatch.setenv("QDE_R2_BUCKET", "qde-lake")
    assert "source=fred/series_id=CPIAUCSL" in series_glob(
        source="fred", series_id="CPIAUCSL"
    )


def test_series_glob_bucket_override():
    assert series_glob(bucket="other").startswith("r2://other/bronze/group=series/")


def test_query_registers_a_series_view(tmp_path, monkeypatch):
    # Same shape as the bars-view test: point the series glob at a LOCAL parquet so
    # no R2 is needed. bars_glob points at a real bars file (its view is
    # unconditional); bronze_glob points at nothing (kinds skipped).
    import duckdb
    import pandas as pd

    import qde.lake as lake

    bars_part = (
        tmp_path / "bronze" / "group=bars" / "source=binance" / "symbol=BTCUSDT" / "interval=1d"
    )
    bars_part.mkdir(parents=True)
    pd.DataFrame(
        {"date": pd.to_datetime(["2024-01-01"], utc=True), "close": [42.0]}
    ).to_parquet(bars_part / "bars.parquet", index=False)

    series_part = tmp_path / "bronze" / "group=series" / "source=fred" / "series_id=CPIAUCSL"
    series_part.mkdir(parents=True)
    pd.DataFrame(
        {"date": pd.to_datetime(["2024-01-01"], utc=True), "value": [307.5]}
    ).to_parquet(series_part / "series.parquet", index=False)

    series_root = tmp_path / "bronze" / "group=series"

    def fake_series_glob(source="*", series_id="*", metric=None, bucket=None):
        # Mirror the real depth split against the local tree: flat vs metric=.
        depth = "*/*/series.parquet" if metric is None else "*/*/*/series.parquet"
        return (series_root / depth).as_posix()

    monkeypatch.setattr(lake, "open_lake", lambda: duckdb.connect())
    monkeypatch.setattr(
        lake,
        "bars_glob",
        lambda *a, **k: (tmp_path / "bronze" / "group=bars" / "**" / "*.parquet").as_posix(),
    )
    monkeypatch.setattr(lake, "series_glob", fake_series_glob)
    monkeypatch.setattr(
        lake, "bronze_glob", lambda *a, **k: (tmp_path / "none" / "*.parquet").as_posix()
    )

    out = lake.query("SELECT series_id, value FROM series")

    assert out.loc[0, "series_id"] == "CPIAUCSL"
    assert out.loc[0, "value"] == 307.5


def test_query_series_view_unions_flat_and_metric_depths(tmp_path, monkeypatch):
    # The mixed-depth case COT introduces: a flat FRED file (source/series_id) and
    # a metric COT file (source/series_id/metric) must both surface through one
    # `series` view, with metric NULL on the flat side. A single glob over both
    # depths raises "Hive partition mismatch"; the union must avoid it.
    import duckdb
    import pandas as pd

    import qde.lake as lake

    # A bars file too: query() registers the bars view unconditionally.
    bars_part = (
        tmp_path / "bronze" / "group=bars" / "source=binance" / "symbol=BTCUSDT" / "interval=1d"
    )
    bars_part.mkdir(parents=True)
    pd.DataFrame(
        {"date": pd.to_datetime(["2024-01-01"], utc=True), "close": [42.0]}
    ).to_parquet(bars_part / "bars.parquet", index=False)

    series_root = tmp_path / "bronze" / "group=series"
    flat = series_root / "source=fred" / "series_id=DGS10"
    flat.mkdir(parents=True)
    pd.DataFrame(
        {"date": pd.to_datetime(["2024-01-01"], utc=True), "value": [4.0]}
    ).to_parquet(flat / "series.parquet", index=False)

    metric = series_root / "source=cftc" / "series_id=ES" / "metric=dealer_long"
    metric.mkdir(parents=True)
    pd.DataFrame(
        {"date": pd.to_datetime(["2024-01-02"], utc=True), "value": [100.0]}
    ).to_parquet(metric / "series.parquet", index=False)

    def fake_series_glob(source="*", series_id="*", metric=None, bucket=None):
        depth = "*/*/series.parquet" if metric is None else "*/*/*/series.parquet"
        return (series_root / depth).as_posix()

    monkeypatch.setattr(lake, "open_lake", lambda: duckdb.connect())
    monkeypatch.setattr(lake, "series_glob", fake_series_glob)
    monkeypatch.setattr(
        lake,
        "bars_glob",
        lambda *a, **k: (tmp_path / "bronze" / "group=bars" / "**" / "*.parquet").as_posix(),
    )
    monkeypatch.setattr(
        lake, "bronze_glob", lambda *a, **k: (tmp_path / "none" / "*.parquet").as_posix()
    )

    out = lake.query("SELECT source, series_id, metric, value FROM series ORDER BY source")
    got = {(r.source, r.series_id, r.metric, r.value) for r in out.itertuples(index=False)}
    assert ("cftc", "ES", "dealer_long", 100.0) in got
    # The flat FRED row comes back with metric NULL (NaN) — filled by UNION ALL BY NAME.
    fred = next(r for r in out.itertuples(index=False) if r.source == "fred")
    assert fred.value == 4.0 and pd.isna(fred.metric)


def test_query_registers_a_bars_view(tmp_path, monkeypatch):
    # Exercise query()'s view registration + SQL execution against a LOCAL parquet
    # by pointing the globs at tmp_path, so no R2 is needed. bronze_glob points at
    # nothing, so the microstructure views are skipped (empty-kind path).
    import duckdb
    import pandas as pd

    import qde.lake as lake

    part = (
        tmp_path / "bronze" / "group=bars" / "source=binance" / "symbol=BTCUSDT" / "interval=1d"
    )
    part.mkdir(parents=True)
    pd.DataFrame(
        {"date": pd.to_datetime(["2024-01-01"], utc=True), "close": [42.0]}
    ).to_parquet(part / "bars.parquet", index=False)

    monkeypatch.setattr(lake, "open_lake", lambda: duckdb.connect())
    monkeypatch.setattr(
        lake, "bars_glob", lambda *a, **k: (tmp_path / "bronze" / "**" / "*.parquet").as_posix()
    )
    monkeypatch.setattr(
        lake, "bronze_glob", lambda *a, **k: (tmp_path / "none" / "*.parquet").as_posix()
    )

    out = lake.query("SELECT symbol, close FROM bars")

    assert out.loc[0, "symbol"] == "BTCUSDT"
    assert out.loc[0, "close"] == 42.0
