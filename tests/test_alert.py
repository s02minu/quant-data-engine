"""Tests for the Discord health alert (offline; the poster is injected)."""

from qde.alert import format_health, send_discord
from qde.checks import Violation


def test_no_webhook_is_a_noop(monkeypatch):
    monkeypatch.delenv("QDE_DISCORD_WEBHOOK", raising=False)
    assert send_discord("anything") is False


def test_posts_content_with_injected_client():
    seen = {}

    def client(url, payload):
        seen["url"], seen["payload"] = url, payload
        return 204  # Discord's success status

    assert send_discord("hello", webhook_url="http://hook", client=client) is True
    assert seen["url"] == "http://hook"
    assert seen["payload"] == {"content": "hello"}


def test_truncates_over_the_discord_limit():
    captured = {}

    def client(url, payload):
        captured["len"] = len(payload["content"])
        return 200

    send_discord("x" * 3000, webhook_url="http://hook", client=client)
    assert captured["len"] <= 2000


def test_http_error_returns_false():
    assert send_discord("hi", webhook_url="http://hook", client=lambda u, p: 500) is False


def test_client_exception_is_swallowed():
    def boom(url, payload):
        raise RuntimeError("network down")

    # An alerting failure must never crash the job it reports on.
    assert send_discord("hi", webhook_url="http://hook", client=boom) is False


def test_format_health_lists_failures_and_violations():
    text = format_health(
        updated=30,
        failures=[{"label": "fred/DGS10", "error": "ValueError", "detail": "bad key"}],
        violations=[Violation("series", "cboe", "VIX", None, "freshness", "warn", "stale 9d")],
    )
    assert "updated 30" in text
    assert "1 failed" in text and "fred/DGS10" in text and "bad key" in text
    assert "cboe/VIX" in text and "stale 9d" in text


def test_format_health_clean_run():
    text = format_health(updated=30, failures=[], violations=[])
    assert "all sources fresh" in text
