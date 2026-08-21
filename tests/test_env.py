"""Tests for the BOM-tolerant secrets/.env loader (`qde.env`)."""

import os

import pytest

from qde.env import load_env_file


@pytest.fixture
def clean_environ(monkeypatch):
    """Give each test a disposable copy of os.environ, restored on teardown."""
    monkeypatch.setattr(os, "environ", os.environ.copy())


def _write(path, text, encoding):
    with open(path, "w", encoding=encoding, newline="\n") as f:
        f.write(text)


def test_loads_plain_utf8(tmp_path, clean_environ):
    p = tmp_path / "x.env"
    _write(str(p), "QDE_TEST_K=hello\n", "utf-8")
    load_env_file(str(p))
    assert os.environ["QDE_TEST_K"] == "hello"


def test_loads_utf16_bom(tmp_path, clean_environ):
    # PowerShell `>` writes UTF-16LE with a BOM — the case that bit us.
    p = tmp_path / "x16.env"
    _write(str(p), "QDE_TEST_K=world\n", "utf-16")  # utf-16 emits a BOM
    load_env_file(str(p))
    assert os.environ["QDE_TEST_K"] == "world"


def test_loads_utf8_bom(tmp_path, clean_environ):
    p = tmp_path / "x8.env"
    _write(str(p), "QDE_TEST_K=v\n", "utf-8-sig")
    load_env_file(str(p))
    assert os.environ["QDE_TEST_K"] == "v"


def test_does_not_override_existing(tmp_path, clean_environ):
    os.environ["QDE_TEST_K"] = "already"
    p = tmp_path / "x.env"
    _write(str(p), "QDE_TEST_K=fromfile\n", "utf-8")
    load_env_file(str(p))
    assert os.environ["QDE_TEST_K"] == "already"  # setdefault: exported value wins


def test_missing_file_is_noop(tmp_path):
    load_env_file(str(tmp_path / "nope.env"))  # must not raise


def test_skips_comments_and_strips_quotes(tmp_path, clean_environ):
    p = tmp_path / "x.env"
    _write(str(p), '# a comment\nQDE_TEST_K="quoted"\n', "utf-8")
    load_env_file(str(p))
    assert os.environ["QDE_TEST_K"] == "quoted"


# --- credentials are scoped to what a job actually needs --------------------------


def test_only_registered_sources_have_their_secrets_loaded(tmp_path, monkeypatch):
    """The regression this exists to prevent.

    Loading every `*.env` fixed a real bug (a new source's key never reaching the
    backfill) and introduced a worse one: on the VPS `secrets/` also holds `r2.env`,
    so every nightly and backfill was handed R2 WRITE credentials for the public
    bucket — in jobs that only read APIs and write local Parquet. Publishing is the
    one irreversible action here, and it became reachable from every batch process.
    """
    from qde.env import load_source_secrets

    (tmp_path / "fred.env").write_text("FRED_API_KEY=source_key\n", encoding="utf-8")
    (tmp_path / "r2.env").write_text(
        "QDE_R2_ACCESS_KEY_ID=write_key\nQDE_R2_SECRET_ACCESS_KEY=write_secret\n",
        encoding="utf-8",
    )
    for var in ("FRED_API_KEY", "QDE_R2_ACCESS_KEY_ID", "QDE_R2_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(var, raising=False)

    loaded = load_source_secrets(str(tmp_path))

    assert "fred.env" in loaded, "a declared source's key must reach the job"
    assert "r2.env" not in loaded, "infrastructure credentials must never be loaded"
    assert os.environ.get("FRED_API_KEY") == "source_key"
    assert os.environ.get("QDE_R2_ACCESS_KEY_ID") is None, "WRITE credentials leaked"
    assert os.environ.get("QDE_R2_SECRET_ACCESS_KEY") is None, "WRITE credentials leaked"


def test_a_new_source_needs_no_entry_point_edit(tmp_path, monkeypatch):
    # The bug the wide loader was solving: a source added to the registry whose key
    # never reached the backfill. Derived from the registry, so it cannot be forgotten.
    from qde.env import load_source_secrets
    from qde.registry import all_specs

    names = {spec.name for spec in all_specs()}
    assert "tiingo" in names, "fixture assumes tiingo is registered"
    (tmp_path / "tiingo.env").write_text("TIINGO_API_KEY=abc\n", encoding="utf-8")
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)

    assert "tiingo.env" in load_source_secrets(str(tmp_path))
    assert os.environ.get("TIINGO_API_KEY") == "abc"


def test_a_non_source_grant_must_be_named_explicitly(tmp_path, monkeypatch):
    # Alerting is not a source, so discord.env is granted at the call site where the
    # grant is visible — rather than swept up by a directory glob.
    from qde.env import load_source_secrets

    (tmp_path / "discord.env").write_text("QDE_DISCORD_WEBHOOK=hook\n", encoding="utf-8")
    monkeypatch.delenv("QDE_DISCORD_WEBHOOK", raising=False)

    assert load_source_secrets(str(tmp_path)) == []
    assert load_source_secrets(str(tmp_path), extra=("discord.env",)) == ["discord.env"]


def test_an_exported_value_still_wins_over_a_file(tmp_path, monkeypatch):
    # setdefault semantics: a value set on the VPS must not be overwritten by a file.
    from qde.env import load_source_secrets

    (tmp_path / "fred.env").write_text("FRED_API_KEY=from_file\n", encoding="utf-8")
    monkeypatch.setenv("FRED_API_KEY", "from_environment")
    load_source_secrets(str(tmp_path))
    assert os.environ["FRED_API_KEY"] == "from_environment"


def test_a_missing_secrets_directory_is_not_an_error(tmp_path):
    from qde.env import load_source_secrets

    assert load_source_secrets(str(tmp_path / "nope")) == []
