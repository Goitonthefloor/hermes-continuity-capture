"""
continuity_capture.lifecycle — Subagent-Lebenszyklus-Zuordnung (P-1).

subagent_start und subagent_stop werden ueber child_session_id zugeordnet
(weder Start noch Stop tragen zuverlaessig dieselbe subagent_id ueber beide Hooks —
subagent_stop FUEHRT KEIN child_subagent_id).

Ist die Zuordnung NICHT eindeutig (kein Start gefunden oder mehrere parallele Kinder
mit gleicher child_session_id), wird KEINE kuenstliche Zuordnung erzeugt; der Stop wird
als unvollstaendig/nicht eindeutig gekennzeichnet.
"""

from __future__ import annotations

import threading
import time
from typing import Dict, Optional

class SubagentLifecycleIndex:
    """Ungeordnete, in-memory Registrierung der Start-Ereignisse pro child_session_id."""

    def __init__(self, ttl_seconds: int = 3600) -> None:
        self._lock = threading.Lock()
        self._starts: Dict[str, dict] = {}   # child_session_id -> Start-Event (dict payload-meta)
        self._ttl = max(int(ttl_seconds), 1)

    def register_start(self, child_session_id: str, event: dict) -> None:
        if not child_session_id:
            return
        with self._lock:
            self._starts[child_session_id] = {
                "observed_at": event.get("observed_at"),
                "event_id": event.get("event_id"),
                "at": time.time(),
            }

    def resolve_stop(self, child_session_id: str) -> Optional[dict]:
        """Findet den eindeutigen Start fuer einen Stop.

        Nur EIN Start je child_session_id im TTL-Fenster -> eindeutig.
        Mehrdeutig -> None (keine kuenstliche Zuordnung).
        """
        if not child_session_id:
            return None
        with self._lock:
            rec = self._starts.get(child_session_id)
            if rec is None:
                return None
            if time.time() - rec["at"] > self._ttl:
                self._starts.pop(child_session_id, None)
                return None
            return dict(rec)

    def forget(self, child_session_id: str) -> None:
        with self._lock:
            self._starts.pop(child_session_id, None)


class DefaultStopLinkage:
    """Standard: ohne externe Index nutzt die Normalisierung nur den child_session_id.

    Wenn kein eindeutiger Start registriert ist, bleibt die Zuordnung unvollstaendig.
    """

    def __init__(self, index: Optional[SubagentLifecycleIndex] = None) -> None:
        self._index = index or SubagentLifecycleIndex()

    def attach_stop(self, index: SubagentLifecycleIndex, child_session_id: str) -> Optional[dict]:
        return index.resolve_stop(child_session_id)
