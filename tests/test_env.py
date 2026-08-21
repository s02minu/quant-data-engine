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


# --- one definition of "load the credentials" ------------------------------------


def test_load_secrets_loads_every_env_file(tmp_path, monkeypatch):
    """Entry points used to name the files they needed, one by one.

    Adding Tiingo meant `qde.backfill` still loaded only fred.env, so all 27 symbols
    sent an empty token and came back 403 — a wall of "forbidden" that named nothing.
    Loading the directory removes the step that can be forgotten.
    """
    from qde.env import load_secrets

    (tmp_path / "a.env").write_text("ALPHA_KEY=one\n", encoding="utf-8")
    (tmp_path / "b.env").write_text("BETA_KEY=two\n", encoding="utf-8")
    (tmp_path / "notes.txt").write_text("IGNORED_KEY=three\n", encoding="utf-8")
    monkeypatch.delenv("ALPHA_KEY", raising=False)
    monkeypatch.delenv("BETA_KEY", raising=False)
    monkeypatch.delenv("IGNORED_KEY", raising=False)

    loaded = load_secrets(str(tmp_path))

    assert loaded == ["a.env", "b.env"]
    assert os.environ["ALPHA_KEY"] == "one"
    assert os.environ["BETA_KEY"] == "two"
    assert "IGNORED_KEY" not in os.environ, "only *.env files are secrets"


def test_an_exported_value_still_wins_over_a_file(tmp_path, monkeypatch):
    # setdefault semantics: a value set on the VPS must not be overwritten by a file.
    from qde.env import load_secrets

    (tmp_path / "x.env").write_text("SHARED_KEY=from_file\n", encoding="utf-8")
    monkeypatch.setenv("SHARED_KEY", "from_environment")
    load_secrets(str(tmp_path))
    assert os.environ["SHARED_KEY"] == "from_environment"


def test_a_missing_secrets_directory_is_not_an_error(tmp_path):
    from qde.env import load_secrets

    assert load_secrets(str(tmp_path / "nope")) == []
