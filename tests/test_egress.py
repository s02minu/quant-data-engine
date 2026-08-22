"""Tests for the allowlisting egress proxy.

This is the only part of the sandbox that does not depend on reading the candidate's
code, so it is the part worth exercising for real rather than mocking: the tests below
speak the proxy protocol over a socket against a local server. Nothing leaves the
machine.
"""

import socket
import socketserver
import threading

import pytest

from qde.draft import _egress_stage
from qde.egress import host_allowed, serve

_ALLOW = frozenset({"acme.com", "localhost"})


@pytest.mark.parametrize(
    ("host", "expected"),
    [
        ("acme.com", True),
        ("api.acme.com", True),          # subdomains: APIs move, operators should not chase
        ("API.Acme.Com", True),          # case
        ("acme.com:443", True),          # port ignored
        ("acme.com.", True),             # trailing root dot
        ("evil-acme.com", False),        # the suffix trap: must match on a label boundary
        ("acme.com.evil.net", False),    # the other suffix trap
        ("notacme.com", False),
        ("", False),
    ],
)
def test_the_allowlist_matches_on_label_boundaries(host, expected):
    assert host_allowed(host, _ALLOW) is expected


class _Upstream(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.recv(1024)
        self.request.sendall(b"UPSTREAM-OK")


@pytest.fixture
def upstream():
    server = socketserver.TCPServer(("127.0.0.1", 0), _Upstream)
    server.allow_reuse_address = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield server.server_address[1]
    server.shutdown()
    server.server_close()


@pytest.fixture
def proxy():
    server = serve(_ALLOW, port=0, host="127.0.0.1")
    yield server.server_address[1]
    server.shutdown()
    server.server_close()


def _connect_through(proxy_port: int, target: str) -> bytes:
    sock = socket.create_connection(("127.0.0.1", proxy_port), timeout=5)
    sock.sendall(f"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n".encode())
    return sock, sock.recv(4096)


def test_an_allowlisted_host_is_tunnelled(upstream, proxy):
    """A working tunnel, end to end.

    Worth doing over a real socket: an earlier revision emitted a literal backslash-r
    instead of a CRLF in the status line, which every mock would have accepted and no
    client would.
    """
    sock, reply = _connect_through(proxy, f"localhost:{upstream}")
    try:
        assert reply.startswith(b"HTTP/1.1 200"), reply
        assert reply.endswith(b"\r\n\r\n"), "the status line must be terminated properly"
        sock.sendall(b"hello")
        assert sock.recv(1024) == b"UPSTREAM-OK"
    finally:
        sock.close()


def test_a_host_nobody_allowed_is_refused(proxy):
    sock, reply = _connect_through(proxy, "evil.example.com:443")
    try:
        assert reply.startswith(b"HTTP/1.1 403"), reply
    finally:
        sock.close()


def test_the_suffix_trap_is_refused_over_the_wire(proxy):
    # `evil-acme.com` ends with `acme.com` as a string. A naive endswith would tunnel
    # a credential straight to an attacker who registered the lookalike.
    sock, reply = _connect_through(proxy, "evil-acme.com:443")
    try:
        assert reply.startswith(b"HTTP/1.1 403"), reply
    finally:
        sock.close()


def test_origin_form_is_not_treated_as_a_proxy_request(proxy):
    sock = socket.create_connection(("127.0.0.1", proxy), timeout=5)
    try:
        sock.sendall(b"GET /secrets HTTP/1.1\r\nHost: acme.com\r\n\r\n")
        assert sock.recv(4096).startswith(b"HTTP/1.1 403")
    finally:
        sock.close()


# --- the verdict the gauntlet draws from the proxy's record -------------------------


def test_a_clean_egress_log_passes():
    stage = _egress_stage("EGRESS READY port=8888\nEGRESS ALLOW api.acme.com CONNECT\n")
    assert stage.passed and not stage.blocking
    assert "1 connection" in stage.detail


def test_any_denied_attempt_fails_the_run():
    """A blocked attempt is still the most informative thing the sandbox can see.

    The credential did not leave — but a draft that wants to talk to a host nobody
    named is not one to promote because its frame shape looked right.
    """
    stage = _egress_stage(
        "EGRESS ALLOW api.acme.com CONNECT\nEGRESS DENY paste.example.net host-not-allowed\n"
    )
    assert not stage.passed
    assert stage.blocking
    assert "paste.example.net" in stage.detail
