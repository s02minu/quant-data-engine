"""Tests for host health checks (disk pressure)."""

from collections import namedtuple

from qde.host import check_disk

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
