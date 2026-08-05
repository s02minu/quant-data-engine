"""Loading local secrets from gitignored ``.env`` files.

A CLI convenience so commands work without sourcing a file first: ``KEY=VALUE``
lines are read into the process environment. Existing environment variables are
never overridden, so a value already exported (or set on the VPS) wins.

Tolerant of the encodings Windows PowerShell writes: ``echo "K=v" > file`` there
produces UTF-16LE (or UTF-8) *with a BOM*, which a naive ``open()`` or a bash
``source`` chokes on. The loader sniffs the BOM and decodes accordingly.
"""

import os


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
