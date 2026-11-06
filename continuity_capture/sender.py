"""
continuity_capture.sender — Asynchrone Bridge-Integration (Phase 4 / M-2 §5.2, §7).

- Uebergabe an die Bridge ueber publish_mode (jetstream | core | spool-only).
- JetStream-Persistenz ueber den BESTEHENDEN M-1-Pfad (bridge/jetstream.py):
  KEINE Umgehung der A->B->C->D-Pipeline, KEIN stilles Fallback auf unpersistentes
  Core-PUB bei JetStream-Fehlern.
- ``done`` NUR nach bestaetigter Zustellung (B-06); sonst bleibt Eintrag pending/error.
- Retry mit Backoff; Idempotenz ueber stabile event_id.
- Crash zwischen Publish und lokaler Bestaetigung: Wiederholung sent erneut; der
  M-1-Dedup (Nats-Msg-Id=event_id) verhindert Duplikat (B-06/P-2).

Der BridgeSink ist eine injizierbare Integrationsnaht: Eine reale ``Bridge``-Instanz
(das ``process()``-Interface der deployten ContinuityBridge) wird von aussen uebergeben.
Ohne injizierte Bridge ist der Sink inaktiv (kein Produktivbetrieb).
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, Optional

from .spool import Spool

log = logging.getLogger("continuity.capture.sender")

# Minimal-Interface einer injizierbaren Bridge: liefert ein bridge.outcome mit
# .published (bool) und .published_seq (int|None). Wird von der deployten
# ContinuityBridge (bridge.py) ebenso wie von einem Test-Stub erfuellt.
class BridgeOutcomeLike:
    published: bool = False
    published_seq: Any = None
    published_mode: str = "core"
    error_message: Optional[str] = None


class BridgeSink:
    """Adapter ueber die injizierbare Bridge (A->B->C->D->NATS)."""

    def __init__(self, publish_mode: str = "jetstream", *,
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
        """Publikation durch die injizierte Bridge; true nur bei bestaetigter Zustellung.

        Wenn keine Bridge injiziert wurde (keine produktive Aktivierung), wird False
        zurueckgegeben und das Event bleibt pending (kein stilles Loeschen).
        """
        if not self.active:
            # Keine Bridge aktiv => Integration nicht (produktiv) geschaltet.
            # Das Event bleibt im Spool. KEIN Fallback auf unpersistentes Core-PUB.
            return False
        outcome = self._bridge.process(event)
        if outcome is None:
            return False
        return bool(getattr(outcome, "published", False))


class SenderThread:
    """Background-Thread, der pending-Eintraege asynchron an den Sink uebergibt.

    Reine Entkopplung vom Hook-Callback; kein Netzwerk/Sink im Callback.
    """

    def __init__(self, spool: Spool, sink: BridgeSink, *, poll_interval: float = 1.0,
                 retry_attempts: int = 5, base_delay: float = 1.0, max_backoff: float = 60.0,
                 max_batch: int = 50) -> None:
        self._spool = spool
        self._sink = sink
        self._poll = float(poll_interval)
        self._retries = max(int(retry_attempts), 1)
        self._base = float(base_delay)
        self._max_backoff = float(max_backoff)
        self._batch = max(int(max_batch), 1)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._iterations = 0
        self._last_error: Optional[str] = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="continuity-sender", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            self._iterations += 1
            try:
                self._drain_once()
            except Exception as exc:  # Sender-Fehler isoliert vom Agenten
                self._last_error = str(exc)
                log.error("Sender-Fehler: %s", exc)
            self._stop.wait(self._poll)

    def _drain_once(self) -> None:
        batch = self._spool.pending_batch(self._batch)
        for row in batch:
            eid = row["event_id"]
            attempts = row["attempts"]
            if attempts >= self._retries and row["status"] == "pending" and attempts > 0:
                self._spool.mark_given_up(eid, "retry-limit")
                continue
            ok = self._deliver(row["payload"])
            if ok:
                self._spool.mark_done(eid)
            else:
                delay = min(self._base * (2 ** attempts), self._max_backoff)
                self._spool.mark_error(eid, f"send-failed; retry in {delay:.1f}s")
                time.sleep(min(delay, 2.0))

    def _deliver(self, event: Dict[str, Any]) -> bool:
        if self._sink.publish_mode == "spool-only":
            # Spool-only: bestaetigt nach lokaler Annahme (Test-/Dev-Modus). Kein NATS.
            return True
        try:
            # JETSTREAM fallback ausgeschlossen: publish() MUSS true nur bei bestaetigter
            # Zustellung zurueckgeben, sonst false. Kein Fallback auf core bei Fehler.
            return bool(self._sink.publish(event))
        except Exception as exc:
            log.error("deliver %s fehlgeschlagen: %s", event.get("event_id"), exc)
            return False

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    @property
    def last_error(self) -> Optional[str]:
        return self._last_error
