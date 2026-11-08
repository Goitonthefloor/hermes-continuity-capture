"""
continuity_capture.sender — Asynchronous bridge integration (Phase 4 / M-2 §5.2, §7).

- Hand-off to the bridge via ``publish_mode`` (jetstream | core | spool-only).
- JetStream persistence goes through the same wire path used by the deployed bridge
  (``nats_transport``): NO bypass of the A->B->C->D pipeline and NO silent fallback
  to a non-persistent Core-PUB on a JetStream error.
- ``done`` only after confirmed delivery (B-06); otherwise the row stays pending/error
  and is retried.
- Retry with backoff; idempotency via the stable event_id.
- Periodic retention sweep (``retention_sweep_seconds``, once at start; <= 0 disables).
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, Optional

from .spool import Spool

log = logging.getLogger("continuity.capture.sender")

# Minimal interface of an injectable bridge: returns an outcome carrying
# .published (bool) and .published_seq (int|None). Satisfied by the real
# NatsTransport as well as by a test stub.
class BridgeOutcomeLike:
    published: bool = False
    published_seq: Any = None
    published_mode: str = "core"
    error_message: Optional[str] = None


class BridgeSink:
    """Adapter over the injectable bridge (A->B->C->D->NATS)."""

    def __init__(self, publish_mode: str = "spool-only", *,
                 bridge: Any = None,
                 subject_events: str = "hermes.continuity.events",
                 subject_quality: str = "hermes.continuity.quality") -> None:
        self.publish_mode = publish_mode
        self._bridge = bridge
        self.subject_events = subject_events
        self.subject_quality = subject_quality

    @property
    def active(self) -> bool:
        return self._bridge is not None and getattr(self._bridge, "process", None) is not None

    def publish(self, event: Dict[str, Any]) -> bool:
        """Publish through the injected bridge; true only on confirmed delivery.

        With no bridge wired in there is nothing to deliver: return False and keep
        the event in the spool (no silent drop, no fallback to non-persistent core).
        """
        if not self.active:
            return False
        outcome = self._bridge.process(event)
        if outcome is None:
            return False
        return bool(getattr(outcome, "published", False))


class SenderThread:
    """Background thread that drains pending/error rows to the sink.

    Also runs the periodic retention sweep. Pure decoupling from the hook callback:
    no network/sink work happens in the callback itself.
    """

    def __init__(self, spool: Spool, sink: BridgeSink, *, poll_interval: float = 1.0,
                 retry_attempts: int = 5, base_delay: float = 1.0, max_backoff: float = 60.0,
                 max_batch: int = 50, retention_days: int = 90,
                 retention_sweep_seconds: int = 3600) -> None:
        self._spool = spool
        self._sink = sink
        self._poll = float(poll_interval)
        self._retries = max(int(retry_attempts), 1)
        self._base = float(base_delay)
        self._max_backoff = float(max_backoff)
        self._batch = max(int(max_batch), 1)
        self._retention_days = int(retention_days)
        self._sweep_seconds = int(retention_sweep_seconds)
        self._next_sweep: Optional[float] = None
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._iterations = 0
        self._last_error: Optional[str] = None
        # Observability for tests/status.
        self.sweeps = 0
        self.last_swept_rows = 0

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._wake.clear()
        # A sweep runs once when the thread starts (if enabled).
        self._next_sweep = time.time()
        self._thread = threading.Thread(target=self._run, name="continuity-sender", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            self._iterations += 1
            try:
                self._maybe_sweep()
                self._drain_once()
            except Exception as exc:  # Sender errors stay isolated from the agent
                self._last_error = str(exc)
                log.error("sender error: %s", exc)
            self._wake.wait(self._poll)
            self._wake.clear()

    def kick(self) -> None:
        """Wake the sender now (used by the ``flush`` command); does not block."""
        self._wake.set()

    def sweep_retention(self) -> int:
        """Run one retention pass. Returns the number of rows pruned.

        Disabled (returns 0) when ``retention_sweep_seconds <= 0``. Aged terminal
        rows (including ``error``) are pruned; only ``pending`` is kept.
        """
        if self._sweep_seconds <= 0:
            return 0
        removed = int(self._spool.cleanup_retention(self._retention_days) or 0)
        self.sweeps += 1
        self.last_swept_rows = removed
        if removed:
            log.info("retention sweep removed %d row(s)", removed)
        return removed

    def _maybe_sweep(self) -> None:
        if self._sweep_seconds <= 0:
            return
        now = time.time()
        if self._next_sweep is None or now + 1e-9 < self._next_sweep:
            return
        try:
            self.sweep_retention()
        finally:
            self._next_sweep = now + self._sweep_seconds

    def _drain_once(self) -> None:
        batch = self._spool.pending_batch(self._batch)
        for row in batch:
            eid = row["event_id"]
            attempts = row["attempts"]
            # Includes error rows: retries stop only once the attempt budget is spent.
            if attempts >= self._retries and attempts > 0:
                self._spool.mark_given_up(eid, "retry-limit")
                continue
            ok = self._deliver(row["payload"])
            if ok:
                self._spool.mark_done(eid)
            else:
                delay = min(self._base * (2 ** attempts), self._max_backoff)
                self._spool.mark_error(eid, "send-failed; retry in %.1fs" % delay)
                time.sleep(min(delay, 2.0))

    def _deliver(self, event: Dict[str, Any]) -> bool:
        if self._sink.publish_mode == "spool-only":
            # Spool-only: confirmed after local acceptance (test/dev mode). No NATS.
            return True
        try:
            # JetStream fallback is excluded: publish() must return true only on
            # confirmed delivery, else false. No fallback to core on error.
            return bool(self._sink.publish(event))
        except Exception as exc:
            log.error("deliver %s failed: %s", event.get("event_id"), exc)
            return False

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    @property
    def last_error(self) -> Optional[str]:
        return self._last_error
