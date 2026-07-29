"""One-shot migration: flat data/ohlcv/*.parquet -> bronze/group=bars/...

For each legacy file it rewrites the data into the partitioned bars layout via
`_bars_path`, then verifies the copy round-trips identically (same columns,
index, and values) before touching the original. Originals are KEPT by default;
pass --prune to delete them once verified. Idempotent: re-running re-verifies
existing copies and only prunes what is confirmed safe.

    python scripts/migrate_ohlcv_to_bronze.py            # copy + verify (safe)
    python scripts/migrate_ohlcv_to_bronze.py --prune    # then remove originals
"""

import sys
from pathlib import Path

import pandas as pd

from qde.storage import _bars_path


def migrate(base_dir: str = "data", prune: bool = False) -> None:
    legacy = sorted((Path(base_dir) / "ohlcv").glob("*.parquet"))
    if not legacy:
        print("Nothing to migrate.")
        return

    for file in legacy:
        symbol, source, interval = file.stem.rsplit("_", 2)
        dest = _bars_path(symbol, source, interval, base_dir)

        old = pd.read_parquet(file, engine="pyarrow")  # type: ignore[call-overload]

        if not dest.exists():
            dest.parent.mkdir(parents=True, exist_ok=True)
            old.to_parquet(dest, engine="pyarrow")

        # Verify the copy is identical before removing anything.
        new = pd.read_parquet(dest, engine="pyarrow")  # type: ignore[call-overload]
        verified = (
            list(old.columns) == list(new.columns)
            and old.index.equals(new.index)
            and old.equals(new)
        )
        if not verified:
            print(f"VERIFY FAILED, original kept: {file.name}")
            continue

        if prune:
            file.unlink()
            print(f"migrated + pruned: {file.name} ({len(new)} rows)")
        else:
            print(f"copied + verified: {file.name} ({len(new)} rows) -- original kept")

    tail = "Done." if prune else "Done. Re-run with --prune to remove originals."
    print(tail)


if __name__ == "__main__":
    migrate(prune="--prune" in sys.argv)
