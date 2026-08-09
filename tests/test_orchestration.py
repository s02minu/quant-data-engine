"""Guard for the Dagster orchestration graph.

Dagster is a dev-only optional dependency (`.[orchestration]`), not installed in CI,
so this whole module skips cleanly where it is absent. Importing ``definitions``
also constructs the ``Definitions`` object, which is Dagster's own validation --
so a broken graph fails the import, and this test catches it.
"""

import sys
from pathlib import Path

import pytest

pytest.importorskip("dagster")  # dev-only; skipped in CI and light-client installs

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "orchestration"))


def test_registry_generates_one_bronze_asset_per_batch_source():
    import definitions as d

    from qde.registry import all_specs

    expected = {s.name for s in all_specs() if s.group in {"bars", "series", "events"}}
    got = {s.name for s in d._bronze_specs}
    assert got == expected  # the little-book payoff: sources -> assets, no per-source code
    assert len(d.bronze_assets) == len(expected)


def test_transform_and_publish_assets_present_and_defs_build():
    import definitions as d

    # Import already built Definitions() (Dagster's validation); assert the two
    # non-bronze stages exist and the schedule matches the VPS cron.
    assert d.dbt_build is not None and d.publish_to_r2 is not None
    assert d.nightly_schedule.cron_schedule == "30 0 * * *"
    assert d.defs is not None
