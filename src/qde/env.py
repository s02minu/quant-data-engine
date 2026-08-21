"""Loading local secrets from gitignored ``.env`` files.

A CLI convenience so commands work without sourcing a file first: ``KEY=VALUE``
lines are read into the process environment. Existing environment variables are
never overridden, so a value already exported (or set on the VPS) wins.

Tolerant of the encodings Windows PowerShell writes: ``echo "K=v" > file`` there
produces UTF-16LE (or UTF-8) *with a BOM*, which a naive ``open()`` or a bash
``source`` chokes on. The loader sniffs the BOM and decodes accordingly.
"""

import os
from pathlib import Path


def load_source_secrets(directory: str = "secrets", extra: tuple[str, ...] = ()) -> list[str]:
    """Load credentials for REGISTERED SOURCES only, never infrastructure ones.

    Two failures this sits between, and it must avoid both.

    Naming the files each entry point needs — ``fred.env`` here, ``discord.env``
    there — meant adding a source with credentials required editing three unrelated
    modules. Tiingo was added, ``qde.backfill`` still loaded only FRED's key, and all
    27 symbols failed against an empty token.

    Loading the whole directory fixed that and introduced something worse: on the VPS
    ``secrets/`` also holds ``r2.env``, so every nightly, backfill and verification run
    was handed ``QDE_R2_ACCESS_KEY_ID``, ``QDE_R2_SECRET_ACCESS_KEY`` and
    ``QDE_R2_PUBLIC_BUCKET`` — write access to the public bucket, in jobs that only
    read APIs and write local Parquet. Publishing is the one irreversible action this
    platform takes, and it was suddenly reachable from every batch process.

    So the set is derived from the **registry**: a file is loaded when it is named
    after a declared source. Adding a source already requires a registry row, so
    nothing can be forgotten — and ``r2.env`` can never be loaded, because "r2" is not
    a source. Infrastructure credentials stay where they were: passed explicitly with
    ``-e`` to the sync and publish containers by ``scripts/maintain.sh``.

    Args:
        directory: where the ``*.env`` files live.
        extra: non-source files a caller genuinely needs, e.g. ``("discord.env",)``
            for alerting. Named explicitly so each grant is visible at the call site.

    Returns:
        The files loaded, sorted.
    """
    from qde.registry import all_specs

    root = Path(directory)
    if not root.is_dir():
        return []

    wanted = {f"{spec.name}.env" for spec in all_specs()} | set(extra)
    loaded = []
    for name in sorted(wanted):
        path = root / name
        if path.exists():
            load_env_file(str(path))
            loaded.append(name)
    return loaded


def load_env_file(path: str) -> None:
    """Load ``KEY=VALUE`` lines from ``path`` into ``os.environ``.

    Missing files are ignored (the value may already be exported). Existing
    variables are kept — ``setdefault`` semantics — so an env override or a
    VPS-set value takes precedence over the file. Comment (``#``) and blank
    lines are skipped; surrounding quotes on a value are stripped.
    """
    if not os.path.exists(path):
        return

    with open(path, "rb") as handle:
        raw = handle.read()
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):  # UTF-16 LE/BE BOM (PowerShell `>`)
        text = raw.decode("utf-16")
    elif raw[:3] == b"\xef\xbb\xbf":  # UTF-8 BOM
        text = raw.decode("utf-8-sig")
    else:
        text = raw.decode("utf-8", errors="replace")

    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))
