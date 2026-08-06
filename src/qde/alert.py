"""Health alerts to a Discord webhook (Phase 10, observability).

The nightly job runs unattended on the VPS; without a push, a broken source or a
stale series is invisible until someone happens to query the lake. This posts a
short health summary to a Discord webhook so a failure reaches you the same night.

Deliberately quiet: an alert is sent only when something needs attention (a fetch
failure or a data-quality violation), never on a clean run — an unattended job that
pings every night trains you to ignore it. The webhook URL is read from
``QDE_DISCORD_WEBHOOK`` (a gitignored ``secrets/discord.env``, delivered to the
container by the same read-only ``./secrets`` mount as the FRED key). With no
webhook configured the sender is a logged no-op, so the pipeline runs unchanged on
a box that has not opted in.
"""

import os

from qde.log import get_logger

log = get_logger(__name__)

_DISCORD_LIMIT = 2000  # Discord rejects a message body longer than this


def send_discord(text: str, webhook_url: str | None = None, client=None) -> bool:
    """Post ``text`` to a Discord webhook; return whether it was delivered.

    The URL falls back to ``QDE_DISCORD_WEBHOOK``. With none set this is a no-op
    (logged, returns False) so an un-opted-in box is unaffected. ``client`` injects
    a stand-in poster for tests; by default ``requests`` is used. Network/HTTP
    failures are swallowed (logged) — an alerting hiccup must never crash the job
    it is reporting on.
    """
    url = webhook_url or os.getenv("QDE_DISCORD_WEBHOOK")
    if not url:
        log.info("alert_skipped", reason="no QDE_DISCORD_WEBHOOK configured")
        return False

    body = text if len(text) <= _DISCORD_LIMIT else text[: _DISCORD_LIMIT - 1] + "…"
    try:
        post = client if client is not None else _requests_post
        status = post(url, {"content": body})
    except Exception as exc:  # an alert failure must not crash the nightly run
        log.warning("alert_error", error=type(exc).__name__, detail=str(exc))
        return False

    if status >= 300:
        log.warning("alert_failed", status=status)
        return False
    log.info("alert_sent", chars=len(body))
    return True


def _requests_post(url: str, payload: dict) -> int:
    """Default poster: POST JSON and return the HTTP status. Imported lazily so the
    module (and its tests) don't need ``requests`` unless an alert actually fires."""
    import requests

    return requests.post(url, json=payload, timeout=10).status_code


def format_health(
    updated: int, failures: list[dict], violations: list, base_dir: str = ""
) -> str:
    """Render a compact, Discord-ready health summary of the nightly run.

    ``failures`` are the hard fetch errors from the update loop (a bad key, an API
    outage) — the unambiguous, high-priority signal. ``violations`` are
    :class:`~qde.checks.Violation` from the data-quality pass (freshness, nulls).
    A clean run returns a one-line OK, but callers should only send when there is
    something to report.
    """
    lines = [f"**qde nightly** — updated {updated}, {len(failures)} failed, {len(violations)} DQ"]
    if base_dir:
        lines[0] += f"  `{base_dir}`"

    for f in failures[:15]:
        label, err, detail = f.get("label", "?"), f.get("error", ""), f.get("detail", "")
        lines.append(f"❌ fetch `{label}`: {err} {detail}".rstrip())

    errors = [v for v in violations if getattr(v, "severity", "") == "error"]
    warns = [v for v in violations if getattr(v, "severity", "") == "warn"]
    for v in errors[:15]:
        lines.append(f"🔴 {v.check} `{v.label()}`: {v.detail}")
    for v in warns[:15]:
        lines.append(f"🟠 {v.check} `{v.label()}`: {v.detail}")

    if not failures and not violations:
        lines.append("✅ all sources fresh and within tolerance")
    return "\n".join(lines)
