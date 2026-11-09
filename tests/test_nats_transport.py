"""Wire-format unit tests for the NATS/JetStream transport modules.

A local, stdlib-only TCP dummy (a thread) emulates just enough of the NATS
protocol to check the bytes the plugin emits: ``PUB <subject>``, the ``HPUB``
header line ``NATS/1.0`` + ``Nats-Msg-Id``, request/reply, and tolerance of the
server's ``+OK`` advisories. No real server and no outbound network access.
"""

from __future__ import annotations

import base64
import json
import socket
import threading
import time

import pytest

from continuity_capture.nats_client import NatsClient, NatsError
from continuity_capture.nats_transport import NatsTransport


class DummyNats:
    """A one-connection NATS stand-in that records frames and answers JS requests."""

    def __init__(self):
        self._listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listen.bind(("127.0.0.1", 0))
        self._listen.listen(1)
        self.port = self._listen.getsockname()[1]
        self.records: list[dict] = []       # client -> server frames
        self.sent: list[bytes] = []         # server -> client control lines
        self.stream_msgs: list[tuple] = []  # (seq, subject, payload, msg_id)
        self.error: Exception | None = None
        self._stop = threading.Event()
        self._conn: socket.socket | None = None
        self._thread = threading.Thread(target=self._serve, daemon=True)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        try:
            if self._conn is not None:
                self._conn.close()
        except OSError:
            pass
        try:
            self._listen.close()
        except OSError:
            pass
        self._thread.join(timeout=2.0)
        return False

    @property
    def url(self):
        return "nats://127.0.0.1:%d" % self.port

    # --------------------------------------------------------------- server loop
    def _serve(self):
        try:
            self._listen.settimeout(5.0)
            conn, _ = self._listen.accept()
        except OSError:
            return
        self._conn = conn
        conn.settimeout(0.5)
        buf = b""

        def read_line():
            nonlocal buf
            while b"\r\n" not in buf:
                try:
                    chunk = conn.recv(4096)
                except socket.timeout:
                    return None
                if not chunk:
                    return None
                buf += chunk
            i = buf.index(b"\r\n")
            line, buf = buf[:i], buf[i + 2:]
            return line.decode("utf-8", "replace")

        def read_n(n):
            nonlocal buf
            while len(buf) < n:
                chunk = conn.recv(4096)
                if not chunk:
                    raise ConnectionError("client closed")
                buf += chunk
            out, buf = buf[:n], buf[n:]
            return out

        try:
            conn.sendall(b'INFO {"headers":true,"server_id":"dummy"}\r\n')
            while not self._stop.is_set():
                line = read_line()
                if line is None:
                    continue
                if line.startswith("CONNECT"):
                    continue
                if line == "PING":
                    conn.sendall(b"PONG\r\n")
                    self.sent.append(b"PONG\r\n")
                    continue
                if line.startswith("SUB"):
                    continue
                if line.startswith("HPUB "):
                    toks = line.split(" ")
                    reply = toks[2] if len(toks) == 5 else None
                    hdr_len = int(toks[-2]);
                    total_len = int(toks[-1])
                    hdr = read_n(hdr_len)
                    body = read_n(total_len - hdr_len)
                    read_n(2)
                    self.records.append({"kind": "HPUB", "subject": toks[1], "reply": reply,
                                         "headers": hdr.decode("utf-8", "replace"),
                                         "payload": body})
                    conn.sendall(b"+OK\r\n")
                    self.sent.append(b"+OK\r\n")
                    if toks[1].startswith("$JS.API"):
                        self._handle_js(toks[1], reply, body)
                    else:
                        self.stream_msgs.append(
                            (len(self.stream_msgs) + 1, toks[1], body, _msg_id(hdr)))
                    continue
                if line.startswith("PUB "):
                    toks = line.split(" ")
                    n = int(toks[-1])
                    body = read_n(n)
                    read_n(2)
                    self.records.append({"kind": "PUB", "subject": toks[1], "payload": body})
                    conn.sendall(b"+OK\r\n")
                    self.sent.append(b"+OK\r\n")
                    continue
        except Exception as exc:  # noqa: BLE001
            self.error = exc

    def _handle_js(self, subject, reply, body):
        if reply is None:
            return
        if subject.startswith("$JS.API.STREAM.INFO."):
            payload = {"config": {"name": subject.rsplit(".", 1)[-1]},
                       "state": {"messages": len(self.stream_msgs),
                                 "last_seq": len(self.stream_msgs)}}
        elif subject.startswith("$JS.API.STREAM.CREATE."):
            payload = {"state": {"messages": 0, "last_seq": 0}}
        elif subject.startswith("$JS.API.STREAM.MSG.GET."):
            seq = json.loads(body.decode("utf-8")).get("seq")
            if isinstance(seq, int) and 1 <= seq <= len(self.stream_msgs):
                _, ssubj, sbody, mid = self.stream_msgs[seq - 1]
                msg = {"seq": seq, "subject": ssubj,
                       "data": base64.b64encode(sbody).decode()}
                if mid:
                    hdr_block = ("NATS/1.0\r\nNats-Msg-Id: %s\r\n\r\n" % mid).encode()
                    msg["hdrs"] = base64.b64encode(hdr_block).decode()
                payload = {"message": msg}
            else:
                payload = {"error": {"code": 404}}
        else:
            payload = {"error": {"code": 404}}
        raw = json.dumps(payload).encode("utf-8")
        self._conn.sendall(("MSG %s 1 %d\r\n" % (reply, len(raw))).encode() + raw + b"\r\n")


def _msg_id(hdr: bytes) -> str | None:
    for line in hdr.decode("utf-8", "replace").splitlines():
        if line.lower().startswith("nats-msg-id:"):
            return line.split(":", 1)[1].strip()
    return None


def _closed_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _hpub(records):
    return [r for r in records if r["kind"] == "HPUB"]


def _pub(records):
    return [r for r in records if r["kind"] == "PUB"]


# --------------------------------------------------------------------- client
def test_client_connects_without_credentials():
    """Connection with no user/password must not raise (auth block is optional)."""
    with DummyNats() as dummy:
        client = NatsClient(dummy.url, connect_timeout=2.0)
        client.connect()
        client.close()


def test_tls_scheme_is_accepted_and_implies_tls():
    """``tls://`` is stripped like ``nats://`` and switches TLS on."""
    client = NatsClient("tls://nats.invalid:4333")
    assert client._parse_url(client.url) == ("nats.invalid", 4333)
    assert client._use_tls is True
    # Without the scheme the flag alone decides.
    plain = NatsClient("nats://nats.invalid:4222")
    assert plain._parse_url(plain.url) == ("nats.invalid", 4222)
    assert plain._use_tls is False
    assert NatsClient("nats://nats.invalid:4222", tls=True)._use_tls is True


def test_tls_scheme_default_port():
    client = NatsClient("tls://nats.invalid")
    assert client._parse_url(client.url) == ("nats.invalid", 4222)
    assert client._use_tls is True


def test_core_publish_wire_format():
    with DummyNats() as dummy:
        client = NatsClient(dummy.url, connect_timeout=2.0)
        client.connect()
        client.publish("hermes.continuity.events", b"hello")
        client.close()
        deadline = time.time() + 2.0
        while time.time() < deadline and not _pub(dummy.records):
            time.sleep(0.01)
        pubs = _pub(dummy.records)
        assert pubs and pubs[0]["subject"] == "hermes.continuity.events"
        assert pubs[0]["payload"] == b"hello"
        assert b"+OK\r\n" in dummy.sent


def test_request_reply_roundtrip_and_plus_ok_tolerance():
    with DummyNats() as dummy:
        client = NatsClient(dummy.url, connect_timeout=2.0)
        client.connect()
        # A core publish leaves an unread +OK in the socket; the next request must
        # still parse its MSG reply correctly (the +OK is skipped as a control line).
        client.publish("hermes.continuity.events", b"x")
        resp = client.request("$JS.API.STREAM.INFO.demo", b"", timeout=3.0)
        client.close()
        assert resp is not None
        data = json.loads(resp.decode("utf-8"))
        assert data["state"]["messages"] == 0
        reqs = [r for r in _hpub(dummy.records) if r["subject"].startswith("$JS.API")]
        assert reqs and reqs[0]["reply"] is not None  # JS requests carry a reply subject


# --------------------------------------------------------------- transport
def test_jetstream_publish_hpub_header_and_ack():
    with DummyNats() as dummy:
        transport = NatsTransport(url=dummy.url, publish_mode="jetstream",
                                  stream_name="test_stream_xyz")
        event = {"event_id": "e1", "event_type": "hermes.session.started"}
        outcome = transport.process(event)
        transport.close()

        assert outcome.published is True
        assert outcome.published_seq == 1
        assert outcome.error_message is None

        publishes = [r for r in _hpub(dummy.records)
                     if r["subject"] == "hermes.continuity.events"]
        assert publishes, "a stream publish must be an HPUB onto the events subject"
        headers = publishes[0]["headers"]
        assert headers.startswith("NATS/1.0\r\n")
        assert "Nats-Msg-Id: e1" in headers
        assert headers.endswith("\r\n\r\n")
        # The persisted payload is the exact event body.
        assert json.loads(publishes[0]["payload"].decode("utf-8"))["event_id"] == "e1"


def test_core_mode_publishes_with_pub():
    with DummyNats() as dummy:
        transport = NatsTransport(url=dummy.url, publish_mode="core")
        outcome = transport.process({"event_id": "e2", "event_type": "hermes.session.ended"})
        # Wait for the dummy server to read the frame before tearing the socket down;
        # closing immediately races the reader thread (test artifact, not a bug).
        deadline = time.time() + 2.0
        while time.time() < deadline and not _pub(dummy.records):
            time.sleep(0.01)
        transport.close()
        assert outcome.published is True
        assert outcome.publish_mode == "core"
        pubs = _pub(dummy.records)
        assert pubs and pubs[0]["subject"] == "hermes.continuity.events"


def test_unreachable_server_yields_error_without_raising():
    port = _closed_port()
    transport = NatsTransport(url="nats://127.0.0.1:%d" % port, publish_mode="jetstream",
                              connect_timeout=1.0)
    outcome = transport.process({"event_id": "e3", "event_type": "hermes.session.started"})
    transport.close()
    assert outcome.published is False
    assert outcome.error_message  # a clear message, not a silent success


def test_empty_nats_url_yields_error():
    transport = NatsTransport(url="", publish_mode="jetstream")
    outcome = transport.process({"event_id": "e4"})
    assert outcome.published is False
    assert outcome.error_message


def test_transport_never_leaks_the_password():
    transport = NatsTransport(url="nats://127.0.0.1:1", password="topsecret-value",
                              publish_mode="jetstream")
    scrubbed = transport._scrub("connect failed for topsecret-value")
    assert "topsecret-value" not in scrubbed
    assert "***" in scrubbed
