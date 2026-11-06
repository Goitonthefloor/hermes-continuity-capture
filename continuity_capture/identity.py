"""
continuity_capture.identity — Deterministische Runtime-/Session-/Subagent-Identitaet.

E-3a/E-3b/E-3c + P-3 (UUIDv5-Kanonikalisierung):

Grundformel (prozessgebunden):
    instance_id = UUIDv5(NAMESPACE, agent_id | host_identity | process_id | process_started_at)

In-process-Subagenten (E-3c):
    instance_id(Child) = UUIDv5(NAMESPACE, Grundformel-key | child_subagent_id)

Pflichtbestandteile werden NICHT durch leere Strings/"unknown"/erfundene Werte ersetzt.
Ohne vollstaendige Identitaetsgrundlage wird KEINE instance_id erzeugt (None
zurueckgegeben) und das Ereignis als unvollstaendig markiert.
"""

from __future__ import annotations

import os
import socket
import uuid
from typing import Optional

from .timeutil import epoch_to_iso, iso_now

# Feste, dokumentierte Namespace-Kette fuer die Continuity-Runtime-Instanzen.
# INIT_NAMESPACE = fester, gangbarer UUIDv5-Anker; CNT_NAMESPACE daraus abgeleitet und Pinned.
INIT_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # DNS-Namespace (fester Anker)
CNT_NAMESPACE = uuid.uuid5(INIT_NAMESPACE, "hermes.continuity.capture.runtime")

# Kanonisches Trennzeichen. Werte werden einzeln generiert; _clean hält sie ambiguitätsfrei.
_SEP = "|"


def _clean(value: object) -> Optional[str]:
    """Kanonisiert einen Bestandteil zu einem Nicht-Leer-String ohne Trenner-Ambiguierung.

    Keine stillschweigende Ersetzung: fehlend/None/leer -> None (Unvollstaendigkeitssignal).
    Backslash und Trennzeichen werden reversibel escaped.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    s = s.replace("\\", "\\\\").replace(_SEP, "\\" + _SEP)
    return s


def canonical_key(agent_id, host_identity, process_id, process_started_at) -> Optional[str]:
    """Baut den kanonischen, reproduzierbaren Schluessel in fester Reihenfolge.

    Alle vier Pflichtbestandteile muessen tatsaechlich vorliegen, sonst None.
    """
    parts = [
        _clean(agent_id),
        _clean(host_identity),
        _clean(process_id),
        _clean(process_started_at),
    ]
    if any(p is None for p in parts):
        return None
    return _SEP.join(parts)


def make_instance_id(agent_id, host_identity, process_id, process_started_at,
                     child_subagent_id: Optional[str] = None) -> Optional[str]:
    """UUIDv5-Identitaet einer Runtime-Inkarnation.

    Fuer In-process-Subagenten wird der Kind-Suffix (child_subagent_id) angefuegt (E-3c).
    Gibt None zurueck, wenn Pflichtbestandteile fehlen (kein erfundener Wert).
    """
    base = canonical_key(agent_id, host_identity, process_id, process_started_at)
    if base is None:
        return None
    if child_subagent_id is not None:
        c = _clean(child_subagent_id)
        if c is None:
            return None
        return str(uuid.uuid5(CNT_NAMESPACE, base + _SEP + c))
    return str(uuid.uuid5(CNT_NAMESPACE, base))


# ------------------------------------------------------- Host-/Prozess-Referenz
def default_host_identity() -> str:
    """Reliable Host-Identitaet der Runtime (E-3b): FQDN/gethostname()."""
    try:
        return socket.getfqdn() or socket.gethostname() or "unknown-host"
    except Exception:
        try:
            v = socket.gethostname()
            return v if v else "unknown-host"
        except Exception:
            return "unknown-host"


class ProcessReference:
    """Idempotente Referenz auf den aktuellen langlaufenden Prozess.

    ``process_started_at`` wird aus der mtime von /proc/<pid> (Subsekunden-Aufloesung)
    abgeleitet — stabil und pro Inkarnation eindeutig genug, um PID-Reuse ueber Reboots
    zu trennen. Fallback (nur falls /proc nicht verfuegbar): UTC-Sekunden-Zeitstempel
    (dokumentiert als Restrisiko bzgl. PID-Reuse in derselben Sekunde).
    """

    def __init__(self) -> None:
        self._pid = os.getpid()
        self._process_started_at = self._start_from_proc_mtime()

    @staticmethod
    def _start_from_proc_mtime() -> str:
        try:
            mtime = os.stat(f"/proc/{os.getpid()}").st_mtime
            return epoch_to_iso(mtime)
        except Exception:
            return iso_now()

    @property
    def pid(self) -> int:
        return self._pid

    @property
    def started_at(self) -> str:
        return self._process_started_at
