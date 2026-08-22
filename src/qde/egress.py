"""An allowlisting HTTP proxy, so a drafted ingestor can only reach its own API.

The gauntlet already denies a candidate almost everything: unprivileged, no
capabilities, read-only root, one credential, no host filesystem. What it could not
deny was the open internet — and with outbound access, the one credential it *is*
given can be posted anywhere. That is the whole realistic threat from a hostile API
doc, and an AST screen does not stop it: a URL assembled from string fragments walks
straight past a screen that looks for literals.

So containment moves to the network, where it does not depend on reading the code.
The candidate runs on a Docker network created ``--internal`` — no default route,
nowhere for a raw socket to go — and its only path out is this proxy, which refuses
any host the operator did not name. Every attempt is logged, allowed or not, so a
draft that tried to reach somewhere else leaves a record even though it failed.

Deliberately stdlib-only and run from the project's own image. A third-party proxy
image would mean pulling another artifact onto the box and trusting it, to guard
against untrusted code — which is a strange trade to make.

Not a general-purpose proxy. It speaks ``CONNECT`` (all real traffic here is HTTPS)
and absolute-form plain HTTP, and refuses everything else.
"""

import argparse
import contextlib
import selectors
import socket
import socketserver
import sys
import threading
from urllib.parse import urlsplit

_BUF = 65536
_HANDSHAKE_TIMEOUT = 20.0
_IDLE_TIMEOUT = 120.0


def host_allowed(host: str, allow: frozenset[str]) -> bool:
    """Whether ``host`` is in the allowlist, subdomains included.

    ``api.acme.com`` is permitted by an entry of ``acme.com``, because APIs move
    between subdomains and an operator naming the vendor should not have to enumerate
    them. ``notacme.com`` is not — the match is on a label boundary, not a suffix, or
    ``evil-acme.com`` would pass.

    Matching is case-insensitive and ignores any port.
    """
    host = host.strip().lower().rsplit(":", 1)[0].rstrip(".")
    if not host:
        return False
    return any(host == a or host.endswith("." + a) for a in allow)


def _pump(a: socket.socket, b: socket.socket) -> None:
    """Shuttle bytes both ways until either side closes or goes quiet."""
    sel = selectors.DefaultSelector()
    sel.register(a, selectors.EVENT_READ)
    sel.register(b, selectors.EVENT_READ)
    try:
        while True:
            ready = sel.select(timeout=_IDLE_TIMEOUT)
            if not ready:
                return
            for key, _ in ready:
                src = key.fileobj
                dst = b if src is a else a
                data = src.recv(_BUF)  # type: ignore[union-attr]
                if not data:
                    return
                dst.sendall(data)
    except OSError:
        return
    finally:
        sel.close()


class _Handler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        self.request.settimeout(_HANDSHAKE_TIMEOUT)
        try:
            head = b""
            while b"\r\n" not in head and len(head) < 8192:
                chunk = self.request.recv(_BUF)
                if not chunk:
                    return
                head += chunk
        except OSError:
            return

        line = head.split(b"\r\n", 1)[0].decode("latin-1", "replace")
        parts = line.split()
        if len(parts) < 2:
            self._deny("malformed", line[:60])
            return

        method, target = parts[0], parts[1]
        if method.upper() == "CONNECT":
            host, _, port_s = target.partition(":")
            port = int(port_s or 443)
        else:
            split = urlsplit(target)
            if not split.hostname:
                # Origin-form (`GET /path`) means the client thought it was talking
                # to the server directly, not to a proxy. Nothing to authorise.
                self._deny("not-proxy-form", target[:60])
                return
            host, port = split.hostname, split.port or 80

        if not host_allowed(host, self.server.allow):  # type: ignore[attr-defined]
            self._deny("host-not-allowed", host)
            return

        try:
            upstream = socket.create_connection((host, port), timeout=_HANDSHAKE_TIMEOUT)
        except OSError as exc:
            self._log("DENY", host, f"upstream-unreachable: {exc}")
            self._respond(502, "Bad Gateway")
            return

        self._log("ALLOW", host, method)
        try:
            if method.upper() == "CONNECT":
                self._respond(200, "Connection Established")
            else:
                upstream.sendall(head)  # replay the request we already read
            self.request.settimeout(None)
            upstream.settimeout(None)
            _pump(self.request, upstream)
        finally:
            upstream.close()

    def _respond(self, code: int, reason: str) -> None:
        # The client may already be gone; nothing useful to do about it.
        with contextlib.suppress(OSError):
            self.request.sendall(
                f"HTTP/1.1 {code} {reason}\r\nConnection: close\r\n\r\n".encode()
            )

    def _deny(self, why: str, what: str) -> None:
        self._log("DENY", what, why)
        self._respond(403, "Forbidden")

    def _log(self, verdict: str, host: str, detail: str) -> None:
        # stdout, unbuffered, so `docker logs` on the proxy is the egress record even
        # when the candidate is killed mid-run.
        print(f"EGRESS {verdict} {host} {detail}", flush=True)


class _Proxy(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, addr, allow: frozenset[str]):
        self.allow = allow
        super().__init__(addr, _Handler)


def serve(allow: frozenset[str], port: int, host: str = "0.0.0.0") -> _Proxy:  # noqa: S104
    """Start the proxy and return it. Binds every interface: it runs inside a
    container on an internal Docker network, which is the actual boundary."""
    server = _Proxy((host, port), allow)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"EGRESS READY port={port} allow={sorted(allow)}", flush=True)
    return server


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m qde.egress")
    parser.add_argument("--allow", action="append", default=[], metavar="HOST",
                        help="a permitted host; subdomains included. Repeatable.")
    parser.add_argument("--port", type=int, default=8888)
    args = parser.parse_args()

    allow = frozenset(h.strip().lower() for h in args.allow if h.strip())
    if not allow:
        # An empty allowlist would proxy nothing, but starting anyway invites someone
        # to read a running proxy as a working one.
        print("EGRESS FATAL no --allow hosts given", flush=True)
        sys.exit(2)

    server = serve(allow, args.port)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
