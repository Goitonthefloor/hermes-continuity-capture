"""
continuity_capture.nats_client — Minimal stdlib-only NATS core client.

Adapted from the deployed Hermes-Continuity bridge client
(``/opt/hermes-continuity/bridge/nats_client.py``); the wire-level behaviour is
kept 1:1 so the plugin speaks exactly the same protocol:

- CONNECT / PING-PONG handshake with a buffered line reader (a NATS ``MSG`` body
  arriving in the same ``recv`` as its header line is never lost).
- ``PUB`` / ``HPUB`` publish; ``HPUB`` carries a ``NATS/1.0`` header block (used
  for request/reply and the ``Nats-Msg-Id`` dedup header).
- one-shot ``request`` over a private ``_INBOX`` (the ``$JS.API.*`` subjects need
  a reply subject).

Credentials are never hard-coded and never logged. No third-party dependency.
"""

from __future__ import annotations

import socket
import time
from typing import Callable, Dict, Optional

CRLF = b"\r\n"


class NatsError(Exception):
    """Transport-level error of the minimal NATS client."""


class NatsClient:
    """Minimal synchronous NATS core client for PUBLISH (bridge egress)."""

    def __init__(self, url: str = "nats://127.0.0.1:4222", user: Optional[str] = None,
                 password: Optional[str] = None, name: str = "hermes-continuity-capture",
                 connect_timeout: float = 3.0, verbose: bool = False,
                 tls: bool = False) -> None:
        self.url = url
        self.user = user
        self.password = password
        self.name = name
        self.connect_timeout = connect_timeout
        self.verbose = verbose
        self.tls = bool(tls)
        self._sock: Optional[socket.socket] = None
        self._subs: Dict[str, str] = {}  # inbox -> sid
        self._sid = 0
        self._buf = b""  # rest buffer so a MSG body after its header line is never lost

    # ------------------------------------------------------------- connect
    def connect(self) -> None:
        if not self.url or not str(self.url).strip():
            raise NatsError("empty NATS url")
        host, port = self._parse_url(self.url)
        raw = socket.create_connection((host, port), timeout=self.connect_timeout)
        if self.tls:
            import ssl
            context = ssl.create_default_context()
            raw = context.wrap_socket(raw, server_hostname=host)
        self._sock = raw
        self._sock.settimeout(2.0)
        info = self._readline_info()
        if self.verbose:
            print("[nats] server INFO", info)
        self._send_connect()
        self._pump_until_pong()

    def _parse_url(self, url: str) -> tuple:
        rest = url
        if rest.startswith("nats://"):
            rest = rest[len("nats://"):]
        if "/" in rest:
            rest = rest.split("/", 1)[0]
        if ":" in rest:
            h, p = rest.rsplit(":", 1)
            return h, int(p)
        return rest, 4222

    def _readline_info(self) -> str:
        return self._readline()

    def _send_connect(self) -> None:
        assert self._sock
        auth: Dict[str, str] = {}
        # Only send auth when something was configured; sending no credentials to a
        # server that does not require them must not raise.
        if self.user is not None or self.password is not None:
            auth = {"user": self.user or "", "pass": self.password or ""}
        cfg = {
            "verbose": False,
            "pedantic": False,
            "tls_required": self.tls,
            "name": self.name,
            "lang": "py",
            "version": "0.1.0",
            "protocol": 1,
            "headers": True,  # needed for the HPUB reply / JetStream API
            "echo": False,
        }
        cfg.update(auth)
        import json
        line = ("CONNECT " + json.dumps(cfg) + CRLF.decode()).encode()
        self._sock.sendall(line)
        self._sock.sendall(b"PING\r\n")

    def _readline(self) -> str:
        """Read one CRLF-terminated line, buffering the rest bytes for later reads."""
        assert self._sock
        while b"\r\n" not in self._buf:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise NatsError("connection closed while reading")
            self._buf += chunk
        idx = self._buf.index(b"\r\n")
        line, self._buf = self._buf[:idx], self._buf[idx + 2:]
        return line.decode("utf-8", "replace")

    def _read_n(self, n: int) -> bytes:
        """Read exactly n bytes, draining the rest buffer first."""
        assert self._sock
        while len(self._buf) < n:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise NatsError("connection closed while reading body")
            self._buf += chunk
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def _pump_until_pong(self, timeout: float = 4.0) -> None:
        """Handshake: process server replies until our PING is answered with PONG."""
        assert self._sock
        deadline = time.time() + timeout
        self._sock.settimeout(max(0.1, min(timeout, 3.0)))
        while time.time() < deadline:
            try:
                line = self._readline()
            except NatsError:
                raise NatsError("connection closed during handshake")
            if line == "PONG" or line == "+PONG":
                return
            if line == "PING" or line == "+PING":
                self._sock.sendall(b"PONG\r\n")
                continue
            if line.startswith("-ERR"):
                raise NatsError("NATS-ERR: " + line)
            if line.startswith("+OK") or line.startswith("INFO"):
                continue
            # unknown control line: ignore
        raise NatsError("timeout waiting for PONG in handshake")

    # ------------------------------------------------------------ publish
    def publish(self, subject: str, payload: bytes = b"", reply: Optional[str] = None) -> None:
        """Publish a core NATS message (``PUB`` or ``HPUB`` when a reply is requested)."""
        assert self._sock
        if reply:
            cmd = "HPUB %s %s 0 %d\r\n" % (subject, reply, len(payload))
        else:
            cmd = "PUB %s %d\r\n" % (subject, len(payload))
        self._sock.sendall(cmd.encode())
        self._sock.sendall(payload)
        self._sock.sendall(b"\r\n")
        if self.verbose:
            print("[nats] PUB %s (%dB) reply=%s" % (subject, len(payload), reply or ""))

    def request(self, subject: str, payload: bytes = b"", timeout: float = 3.0,
                callback: Optional[Callable[[str, bytes], None]] = None) -> Optional[bytes]:
        """One-shot request/response over a private ``_INBOX``.

        Returns the reply payload or None on timeout; raises NatsError on control errors.
        """
        assert self._sock
        self._sid += 1
        inbox = "_INBOX.hermes.%d" % self._sid
        # 1) register the subscription first: TCP ordering guarantees the server has
        #    processed the SUB before the request (below) arrives.
        self._sock.sendall(("SUB %s %d\r\n" % (inbox, self._sid)).encode())
        # 2) request with reply-to
        self._send_pub_reply(subject, inbox, payload)
        # 3) read the reply; answer server heartbeat PINGs along the way
        deadline = time.time() + timeout
        self._sock.settimeout(min(timeout, 4.0))
        while time.time() < deadline:
            try:
                line = self._read_control_or_none()
            except (TimeoutError, OSError):
                break
            if line is None:
                break
            if line.startswith("MSG"):
                toks = line.split(" ")
                try:
                    size = int(toks[-1])
                except (ValueError, IndexError):
                    continue
                try:
                    body = self._read_n(size)
                except (TimeoutError, OSError):
                    break
                self._consume_crlf()
                if callback:
                    callback(subject, body)
                    return None
                return body
            if line.startswith("HMSG"):
                toks = line.split(" ")
                try:
                    hdr_len = int(toks[-2])
                    total_len = int(toks[-1])
                except (ValueError, IndexError):
                    continue
                try:
                    if hdr_len:
                        self._read_n(hdr_len)
                    body = self._read_n(total_len - hdr_len)
                except (TimeoutError, OSError):
                    break
                self._consume_crlf()
                if callback:
                    callback(subject, body)
                    return None
                return body
        return None

    def _flush(self) -> None:
        """Send PING and read until the confirmed PONG was seen."""
        assert self._sock
        self._sock.sendall(b"PING\r\n")
        deadline = time.time() + 4.0
        self._sock.settimeout(4.0)
        while time.time() < deadline:
            line = self._read_control_or_none()
            if line == "PONG" or line == "+PONG":
                return
            if line is None:
                raise NatsError("connection closed during flush")
        raise NatsError("timeout waiting for PONG in flush")

    def _read_control_or_none(self) -> Optional[str]:
        """Read one protocol line; answer server PING with PONG and raise on -ERR."""
        assert self._sock
        line = self._readline()
        if line == "PING" or line == "+PING":
            self._sock.sendall(b"PONG\r\n")
            return self._read_control_or_none()
        if line.startswith("-ERR"):
            raise NatsError("NATS-ERR: " + line)
        return line

    def _send_pub_reply(self, subject: str, reply_inbox: str, payload: bytes) -> None:
        """Send a real request/reply via HPUB (reply-to + hdr_len=0)."""
        assert self._sock
        n = len(payload)
        cmd = ("HPUB %s %s 0 %d\r\n" % (subject, reply_inbox, n)).encode()
        self._sock.sendall(cmd)
        self._sock.sendall(payload)
        self._sock.sendall(b"\r\n")

    def _readline_control(self) -> Optional[str]:
        """Alias of :meth:`_read_control_or_none` (kept for parity with the bridge client)."""
        return self._read_control_or_none()

    def _consume_crlf(self) -> None:
        # Consume the trailing CRLF after a MSG body (when present).
        try:
            if len(self._buf) >= 2:
                if self._buf[:2] == b"\r\n":
                    self._buf = self._buf[2:]
            else:
                self._read_n(2)  # discard
        except (NatsError, OSError):
            pass

    # ------------------------------------------------------------ lifecycle
    def close(self) -> None:
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *exc):
        self.close()
        return False
