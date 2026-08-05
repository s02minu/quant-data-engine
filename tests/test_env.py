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
