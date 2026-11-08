"""
continuity_capture.nats_transport — Persisting NATS/JetStream transport.

Adapted 1:1 from the deployed Hermes-Continuity bridge (``bridge/jetstream.py`` +
the publish path of ``bridge/bridge.py``); only the namespace/imports/error classes
were changed. It is wired into ``__init__.py`` and handed to the capture controller
as the injectable "bridge", so a real NATS/JetStream server is reached whenever
``publish_mode`` is ``jetstream`` or ``core``.

Wire-level facts preserved from the hard-won bridge client:
- Stream provisioning is idempotent (``$JS.API.STREAM.INFO`` then CREATE on 404).
- Publish is a Core-PUB onto a stream subject (this server does not answer the
  ``$JS.API.STREAM.MSG.PUB.*`` paths); the server-side acknowledgement is a
  ``$JS.API.STREAM.INFO`` poll for messages/last_seq plus an MSG.GET content match.
- Dedup uses the ``Nats-Msg-Id`` HPUB header (= event_id) within the stream's
  ``duplicate_window``.
- ``$JS.API.*`` requests need a reply subject (handled by ``NatsClient.request``).
- NO silent fallback to non-persistent core publishing when JetStream fails.

Credentials are never logged and never embedded in error messages.
"""

from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from .nats_client import NatsClient, NatsError


# ---------------------------------------------------------------- errors
class JetStreamError(Exception):
    """Base error of the JetStream module."""


class StreamMissingError(JetStreamError):
    """The requested stream does not exist (and was not provisioned)."""


class PublishFailedError(JetStreamError):
    """Publish was not acknowledged server-side."""


# ------------------------------------------------------- stream / consumer config
def _stream_config(stream_name: str, subject_events: str, subject_quality: str) -> Dict[str, Any]:
    subjects = {subject_events, subject_quality}
    wildcards = set()
    for subj in subjects:
        if "." in subj:
            wildcards.add(subj.rsplit(".", 1)[0] + ".>")
    return {
        "name": stream_name,
        "subjects": sorted(wildcards) or [subject_events],
        "retention": "limits",
        "max_consumers": -1,
        "max_msgs": 1000000,
        "max_bytes": 2147483648,
        "max_age": 7776000000000000,   # 90 days in ns
        "max_msg_size": 262144,
        "storage": "file",
        "num_replicas": 1,
        "duplicate_window": 120000000000,
    }


def _consumer_config(subject_events: str) -> Dict[str, Any]:
    return {
        "ack_policy": "explicit",
        "max_deliver": 5,
        "ack_wait": 30000000000,       # 30 s
        "deliver_policy": "all",
        "filter_subject": subject_events,
    }


class JetStream:
    """JetStream helper around an existing NatsClient (same connection, no 2nd socket)."""

    def __init__(
        self,
        client: NatsClient,
        *,
        stream_name: str = "hermes_continuity",
        subject_events: str = "hermes.continuity.events",
        subject_quality: str = "hermes.continuity.quality",
        dedup_window_s: int = 120,
    ) -> None:
        self._c = client
        self.stream_name = stream_name
        self.subject_events = subject_events
        self.subject_quality = subject_quality
        self._last_stream_state: Optional[Dict[str, Any]] = None
        self._dedup_window_s = int(dedup_window_s)
        self._recent_event_ids: Dict[str, float] = {}

    # ----------------------------------------------------- stream provisioning
    def ensure_stream(self) -> Dict[str, Any]:
        """Idempotently provision the continuity stream (CREATE on 404, else leave as-is)."""
        resp = self._api("$JS.API.STREAM.INFO." + self.stream_name, b"")
        info = _json(resp, "STREAM.INFO")
        if info.get("error"):
            code = info["error"].get("code")
            if code == 404:
                cfg = _stream_config(self.stream_name, self.subject_events, self.subject_quality)
                create = _json(
                    self._api("$JS.API.STREAM.CREATE." + self.stream_name, json.dumps(cfg).encode()),
                    "STREAM.CREATE")
                if create.get("error"):
                    raise JetStreamError("Stream-CREATE failed: " + _err(create))
                self._last_stream_state = create.get("state")
                return create
            raise JetStreamError("STREAM.INFO unexpected error: " + _err(info))
        self._last_stream_state = info.get("state")
        return info

    def stream_exists(self) -> bool:
        try:
            info = self.ensure_stream()
        except JetStreamError:
            return False
        return not info.get("error")

    # ---------------------------------------------------------------- publish
    def publish_jetstream(self, subject: str, payload: bytes,
                          event_id: Optional[str] = None) -> Dict[str, Any]:
        """Publish persistently into the stream (Core-PUB onto a stream subject).

        Server-side, content-verified acknowledgement (no silent success); raises
        :class:`PublishFailedError` when storage is not confirmed. Returns
        ``{"seq": int, "messages": int, "duplicate": bool}``.
        """
        self.ensure_stream()
        before = self.stream_state() or {}
        b_msgs = before.get("messages", 0)
        b_last = before.get("last_seq")
        now = time.time()
        seen_before = self._recent_event_ids.get(event_id) if event_id is not None else None

        if event_id:
            self._publish_with_msg_id(subject, payload, event_id)
        else:
            self._c.publish(subject, payload)

        if event_id is not None:
            self._recent_event_ids[event_id] = now
            self._prune_dedup()

        after: Optional[Dict[str, Any]] = None
        a_msgs, a_last = b_msgs, b_last
        deadline = time.time() + 3.0
        while time.time() < deadline:
            s = self.stream_state() or {}
            a_msgs, a_last = s.get("messages", 0), s.get("last_seq")
            if a_msgs != b_msgs or a_last != b_last:
                after = s
                break
            time.sleep(0.1)
        changed = bool(after is not None)

        # Case A: state advanced -> verify the individual message by content.
        if changed:
            seq, verified = self._verified_seq(b_last, a_last, payload, event_id)
            if not verified:
                raise PublishFailedError(
                    "sequence advanced without content match (foreign/late message?): "
                    "messages %s->%s, last_seq %s->%s" % (b_msgs, a_msgs, b_last, a_last))
            return {"seq": seq, "messages": a_msgs, "duplicate": False}

        # Case B: state unchanged, client-side dedup window knows the id.
        if event_id is not None and seen_before is not None:
            return {"seq": b_last, "messages": b_msgs, "duplicate": True}
        # Case B2: state unchanged, client window does not know it (restart) -> check the
        # last stored message carries exactly this id/payload (server-side dedup).
        if event_id is not None and b_last is not None:
            last = self._get_message(int(b_last))
            if last is not None and last.get("data") == payload and (
                    last.get("msg_id") == event_id or last.get("data_event_id") == event_id):
                return {"seq": b_last, "messages": b_msgs, "duplicate": True}
        # Never report a false success when storage was not confirmed.
        raise PublishFailedError(
            "server-side publish acknowledgement missing (messages %s->%s, last_seq %s->%s)"
            % (b_msgs, a_msgs, b_last, a_last))

    def _verified_seq(self, b_last: Optional[int], a_last: Optional[int],
                      payload: bytes, event_id: Optional[str]) -> tuple:
        lo = int(b_last) + 1 if b_last is not None else 1
        hi = int(a_last) if a_last is not None else lo
        for seq in range(lo, hi + 1):
            got = self._get_message(seq)
            if got is None:
                continue
            if got.get("data") != payload:
                continue
            if event_id is not None and got.get("msg_id") != event_id and \
                    got.get("data_event_id") != event_id:
                continue
            return seq, True
        return None, False

    def _get_message(self, seq: int) -> Optional[Dict[str, Any]]:
        """Read one concrete stream message via MSG.GET (seq-based)."""
        subj = "$JS.API.STREAM.MSG.GET." + self.stream_name
        r = self._api(subj, json.dumps({"seq": seq}).encode(), timeout=6.0)
        d = _json(r, "STREAM.MSG.GET")
        if d.get("error"):
            return None
        msg = d.get("message") or {}
        out = {"seq": msg.get("seq"), "data": None, "msg_id": None, "data_event_id": None}
        if msg.get("data"):
            try:
                raw = base64.b64decode(msg["data"])
                out["data"] = raw
                try:
                    out["data_event_id"] = json.loads(raw.decode("utf-8")).get("event_id")
                except (ValueError, UnicodeDecodeError):
                    pass
            except Exception:  # noqa: BLE001
                pass
        if msg.get("hdrs"):
            try:
                hb = base64.b64decode(msg["hdrs"]).decode("utf-8", "replace")
                for line in hb.splitlines():
                    if line.lower().startswith("nats-msg-id:"):
                        out["msg_id"] = line.split(":", 1)[1].strip()
            except Exception:  # noqa: BLE001
                pass
        return out

    def _prune_dedup(self) -> None:
        cutoff = time.time() - self._dedup_window_s
        for k in [k for k, ts in self._recent_event_ids.items() if ts < cutoff]:
            self._recent_event_ids.pop(k, None)

    def _publish_with_msg_id(self, subject: str, payload: bytes, msg_id: str) -> None:
        """HPUB with a ``Nats-Msg-Id`` header block (NATS/1.0 header format)."""
        sock = self._c._sock
        assert sock is not None
        hdr = ("NATS/1.0\r\nNats-Msg-Id: %s\r\n\r\n" % msg_id).encode()
        total = len(hdr) + len(payload)
        sock.sendall(("HPUB %s %d %d\r\n" % (subject, len(hdr), total)).encode())
        sock.sendall(hdr)
        sock.sendall(payload)
        sock.sendall(b"\r\n")

    def stream_state(self) -> Optional[Dict[str, Any]]:
        resp = self._api("$JS.API.STREAM.INFO." + self.stream_name, b"")
        info = _json(resp, "STREAM.INFO")
        if info.get("error"):
            return None
        self._last_stream_state = info.get("state")
        return self._last_stream_state

    # ---------------------------------------------------------------- consumer
    def ensure_consumer(self, consumer: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Idempotently create a durable pull consumer (no error when it exists)."""
        cfg = _consumer_config(self.subject_events)
        if config:
            for k in ("ack_policy", "max_deliver", "ack_wait", "deliver_policy", "filter_subject",
                      "durable_name", "opt_start_seq"):
                if k in config:
                    cfg[k] = config[k]
        cfg["durable_name"] = consumer
        subj = "$JS.API.CONSUMER.CREATE.%s.%s" % (self.stream_name, consumer)
        body = {"stream_name": self.stream_name, "config": cfg}
        resp = _json(self._api(subj, json.dumps(body).encode()), "CONSUMER.CREATE")
        if resp.get("error") and resp["error"].get("err_code") != 10062:  # 10062 = already exists
            raise JetStreamError("Consumer-CREATE failed: " + _err(resp))
        return resp

    def consumer_info(self, consumer: str) -> Optional[Dict[str, Any]]:
        subj = "$JS.API.CONSUMER.INFO.%s.%s" % (self.stream_name, consumer)
        resp = _json(self._api(subj, b""), "CONSUMER.INFO")
        if resp.get("error"):
            return None
        return resp

    def next_message(self, consumer: str, batch: int = 1, no_wait: bool = True,
                     timeout: float = 6.0) -> Optional[Dict[str, Any]]:
        """Pull one batch from the consumer (payload base64 in 'payload')."""
        subj = "$JS.API.CONSUMER.MSG.NEXT.%s.%s" % (self.stream_name, consumer)
        body = {"batch": batch, "no_wait": no_wait, "expires": int(timeout * 1e9)}
        resp = self._api(subj, json.dumps(body).encode(), timeout=timeout + 1.0)
        return _json(resp, "CONSUMER.MSG.NEXT")

    # ---------------------------------------------------------------- intern
    def _api(self, subject: str, payload: bytes, timeout: float = 8.0) -> Optional[bytes]:
        try:
            return self._c.request(subject, payload, timeout=timeout)
        except NatsError as e:
            raise JetStreamError("NATS request failed: " + str(e)) from e


def _json(resp: Optional[bytes], what: str) -> Dict[str, Any]:
    if resp is None:
        raise JetStreamError("%s: no response (timeout / not acknowledged)" % what)
    try:
        return json.loads(resp.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as e:
        raise JetStreamError("%s: unreadable response: %s" % (what, e)) from e


def _err(resp: Dict[str, Any]) -> str:
    e = resp.get("error") or {}
    return "%s/%s: %s" % (e.get("code"), e.get("err_code"), e.get("description"))


# ---------------------------------------------------------------- transport
@dataclass
class TransportOutcome:
    """Outcome of one transport delivery attempt (mirrors the bridge's BridgeOutcome)."""

    event_id: str = ""
    published: bool = False
    published_seq: Optional[int] = None
    publish_mode: str = "core"
    error_message: Optional[str] = None


class NatsTransport:
    """Injectable bridge over a real NATS server (core or JetStream).

    ``process(event)`` never raises: a connection or delivery failure yields an
    outcome with ``published=False`` and a clear ``error_message`` (the event then
    stays in the spool as ``error`` and is retried). There is NO silent fallback
    from JetStream to non-persistent core publishing.
    """

    def __init__(self, url: str = "nats://127.0.0.1:4222", *,
                 user: Optional[str] = None, password: Optional[str] = None,
                 tls: bool = False, publish_mode: str = "jetstream",
                 subject_events: str = "hermes.continuity.events",
                 subject_quality: str = "hermes.continuity.quality",
                 stream_name: str = "hermes_continuity",
                 dedup_window_s: int = 120, connect_timeout: float = 3.0) -> None:
        self.url = url
        self.user = user
        self._password = password
        self.tls = bool(tls)
        self.publish_mode = publish_mode
        self.subject_events = subject_events
        self.subject_quality = subject_quality
        self.stream_name = stream_name
        self.dedup_window_s = int(dedup_window_s)
        self.connect_timeout = connect_timeout
        self._client: Optional[NatsClient] = None
        self._js: Optional[JetStream] = None

    # BridgeSink looks for a ``process`` attribute to decide whether it is active.
    @property
    def active(self) -> bool:
        return True

    def _ensure_client(self) -> NatsClient:
        if self._client is not None:
            return self._client
        client = NatsClient(self.url, user=self.user, password=self._password,
                            name="hermes-continuity-capture", connect_timeout=self.connect_timeout,
                            tls=self.tls)
        client.connect()
        self._client = client
        return client

    def process(self, event: Dict[str, Any]) -> TransportOutcome:
        eid = str(event.get("event_id") or "")
        out = TransportOutcome(event_id=eid, publish_mode=self.publish_mode)
        try:
            body = json.dumps(event).encode("utf-8")
            client = self._ensure_client()
            if self.publish_mode == "core":
                client.publish(self.subject_events, body)
                out.published = True
                out.publish_mode = "core"
                return out
            # jetstream
            if self._js is None:
                self._js = JetStream(client, stream_name=self.stream_name,
                                     subject_events=self.subject_events,
                                     subject_quality=self.subject_quality,
                                     dedup_window_s=self.dedup_window_s)
            res = self._js.publish_jetstream(self.subject_events, body, event_id=eid or None)
            out.published = True
            out.published_seq = res.get("seq")
            out.publish_mode = "jetstream"
            return out
        except Exception as exc:  # noqa: BLE001 - never crash the sender
            # Reset a possibly broken connection; the event stays in the spool (error) for retry.
            self._reset()
            out.published = False
            out.error_message = self._scrub("%s: %s" % (type(exc).__name__, exc))
            return out

    def _scrub(self, message: str) -> str:
        """Never leak the configured password into an error message."""
        if self._password and self._password in message:
            return message.replace(self._password, "***")
        return message

    def _reset(self) -> None:
        try:
            if self._client is not None:
                self._client.close()
        except Exception:  # noqa: BLE001
            pass
        self._client = None
        self._js = None

    def close(self) -> None:
        self._reset()


def build_jetstream_transport(cfg, *, password: Optional[str] = None) -> NatsTransport:
    """Construct the transport from a :class:`CaptureConfig` (+ the secret password)."""
    return NatsTransport(
        url=cfg.nats_url,
        user=cfg.nats_user,
        password=password,
        tls=cfg.nats_tls,
        publish_mode=cfg.publish_mode,
        subject_events=cfg.bridge_subject_events,
        subject_quality=cfg.bridge_subject_quality,
        stream_name=cfg.jetstream_stream_name,
        dedup_window_s=cfg.jetstream_duplicate_window_s,
    )
