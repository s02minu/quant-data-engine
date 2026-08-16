"""Tests for host health checks (disk pressure)."""

from collections import namedtuple

from qde.host import check_disk, check_small_files

Usage = namedtuple("Usage", "total used free")


def _disk(total_gb: float, free_gb: float) -> Usage:
    total = int(total_gb * 1e9)
    free = int(free_gb * 1e9)
    return Usage(total=total, used=total - free, free=free)


def test_healthy_disk_reports_nothing():
    assert check_disk("/", usage=_disk(total_gb=38, free_gb=30)) == []


def test_warns_before_it_is_too_late_to_act():
    # The real case: the VPS sat at 79% and nothing was watching. One notch up
    # should speak, while there is still room to do something about it.
    v = check_disk("/", usage=_disk(total_gb=38, free_gb=7.0))
    assert len(v) == 1
    assert v[0].severity == "warn"
    assert v[0].group == "host" and v[0].check == "disk"


def test_errors_when_nearly_full():
    v = check_disk("/", usage=_disk(total_gb=38, free_gb=2.0))
    assert len(v) == 1
    assert v[0].severity == "error"


def test_absolute_floor_catches_a_small_disk_at_an_ok_percentage():
    # The floor exists for SMALL disks: 10 GB at 75% used passes the percentage
    # test but leaves 2.5 GB — not enough headroom for a compaction that rewrites
    # a day of files. On a large disk low free space always implies a high
    # percentage, so percentage alone would be enough there; here it is not.
    v = check_disk("/", usage=_disk(total_gb=10, free_gb=2.5))
    assert len(v) == 1
    assert v[0].severity == "warn"


def test_reports_one_violation_not_one_per_rule():
    # Both the percentage and the floor are breached; that is still a single
    # problem and should be a single alert.
    assert len(check_disk("/", usage=_disk(total_gb=38, free_gb=0.5))) == 1


def test_detail_is_human_readable():
    v = check_disk("/", usage=_disk(total_gb=38, free_gb=7.0))
    assert "% used" in v[0].detail and "GB free" in v[0].detail


def test_zero_total_does_not_divide_by_zero():
    assert check_disk("/", usage=Usage(total=0, used=0, free=0)) == []


# --- small-files watch ---------------------------------------------------------


def _part(tmp, group, source, symbol, day, n, kind="book_ticker"):
    d = (
        tmp / "bronze" / f"group={group}" / f"source={source}"
        / f"kind={kind}" / f"symbol={symbol}" / f"date={day}"
    )
    d.mkdir(parents=True, exist_ok=True)
    for i in range(n):
        (d / f"part-{i:04d}.parquet").write_bytes(b"x")


def test_compacted_partition_is_quiet(tmp_path):
    _part(tmp_path, "microstructure", "binance", "BTCUSDT", "2026-08-10", n=1)
    assert check_small_files(str(tmp_path), today="2026-08-15") == []


def test_uncompacted_settled_partition_is_flagged(tmp_path):
    # Compaction should leave one file per settled day; hundreds means it stopped
    # working, which nothing else would report — queries just get slower.
    _part(tmp_path, "microstructure", "binance", "BTCUSDT", "2026-08-10", n=120)
    v = check_small_files(str(tmp_path), today="2026-08-15")
    assert len(v) == 1
    assert v[0].check == "small_files" and v[0].severity == "warn"
    assert "120 part files" in v[0].detail


def test_todays_partition_is_never_flagged(tmp_path):
    # The live day legitimately holds many micro-batch flushes; flagging it would
    # cry wolf every single night.
    _part(tmp_path, "microstructure", "binance", "BTCUSDT", "2026-08-15", n=500)
    assert check_small_files(str(tmp_path), today="2026-08-15") == []


def test_severity_escalates_when_it_is_really_bad(tmp_path):
    _part(tmp_path, "microstructure", "binance", "BTCUSDT", "2026-08-10", n=400)
    v = check_small_files(str(tmp_path), today="2026-08-15")
    assert v[0].severity == "error"


def test_reports_partition_identity_not_just_a_count(tmp_path):
    _part(tmp_path, "microstructure", "coinbase", "ETHUSDT", "2026-08-09", n=90)
    v = check_small_files(str(tmp_path), today="2026-08-15")
    assert v[0].source == "coinbase"
    assert v[0].series_id == "ETHUSDT/2026-08-09"
    assert v[0].metric == "book_ticker"


def test_kinds_of_one_symbol_are_distinguishable(tmp_path):
    # A symbol's kinds are separate partitions, so they must not report as
    # identical-looking rows — the reader could not tell which to go and look at.
    for kind in ("book_ticker", "trades", "depth"):
        _part(tmp_path, "microstructure", "binance", "BTCUSDT", "2026-08-09", n=90, kind=kind)
    v = check_small_files(str(tmp_path), today="2026-08-15")
    assert len(v) == 3
    assert {x.metric for x in v} == {"book_ticker", "trades", "depth"}
    assert len({x.label() for x in v}) == 3


def test_many_offenders_are_capped(tmp_path):
    for i in range(20):
        _part(tmp_path, "microstructure", "binance", f"SYM{i}", "2026-08-10", n=60)
    v = check_small_files(str(tmp_path), today="2026-08-15")
    # A systemic compaction failure should report scale, not emit 20 alerts.
    assert len(v) == 10


def test_missing_lake_is_not_an_error(tmp_path):
    assert check_small_files(str(tmp_path / "nope"), today="2026-08-15") == []
