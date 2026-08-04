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
