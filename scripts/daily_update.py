"""Thin wrapper so the batch refresh can still be run by file path.

The logic lives in ``qde.daily_update`` (a package module, so it ships in the
Docker image and honours ``QDE_BASE_DIR``). Prefer ``python -m qde.daily_update``.
"""

from qde.daily_update import main

if __name__ == "__main__":
    main()
