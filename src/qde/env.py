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


def load_secrets(directory: str = "secrets") -> list[str]:
    """Load every ``*.env`` file in ``directory`` into the environment.

    Each entry point used to name the files it needed — ``fred.env`` here,
    ``discord.env`` there — which meant adding a source with credentials required
    remembering to edit three unrelated modules. Tiingo was added and
    ``qde.backfill`` still loaded only ``fred.env``, so every one of its 27 symbols
    sent an empty token and came back 403: not a missing-key error anyone could read,
    just a wall of forbidden responses.

    Loading the whole directory removes the step that can be forgotten. Same
    ``setdefault`` semantics as :func:`load_env_file`, so an already-exported value
    or one set on the VPS still wins over a file.

    Returns:
        The files loaded, sorted — so a caller can log what it picked up rather than
        assume.
    """
    root = Path(directory)
    if not root.is_dir():
        return []
    loaded = []
    for path in sorted(root.glob("*.env")):
        load_env_file(str(path))
        loaded.append(path.name)
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
