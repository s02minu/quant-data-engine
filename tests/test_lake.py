from qde.lake import bars_glob, bronze_glob


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
