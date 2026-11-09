"""
continuity_capture.normalizer — Hook-Payload -> v2-Event (Mapping + Registry-Check).

- Geschlossene Event-Type-Registry (B-01/P-4): nicht registrierte Typen werden VOR
  dem INSERT abgewiesen (RegistryRejection).
- Identitaets-/Kausalitaets-Semantik (E-3/P-1/B-02): prov.type nur Schema-enum;
  Beobachtungsstufe ueber kind/phase/confidence; subagent_start/stop via child_session_id;
  keine kuenstliche Kausalitaet.
- Datenschutz (B-08/P-3§3.6): Redaction VOR INSERT (aus redact.apply).
- Zeitstempel (B-09): occurred_at = Quelle, observed_at = Capture, received_at = INSERT.
"""

from __future__ import annotations

import threading
import uuid
from typing import Any, Dict, Mapping, Optional

from . import registry
from . import redact
from .identity import make_instance_id, ProcessReference
from .timeutil import iso_now

# Lamport-Clock: process-weiter, thread-sicherer Zaehler (B-05).
_lamport_lock = __import__("threading").RLock()
_lamport_counter = 0


def next_lamport(seed: int = 0) -> int:
    """Liefert einen monotonen Lamport-Wert (B-05). Spawn-Initialize: child seed >= parent."""
    global _lamport_counter
    with _lamport_lock:
        _lamport_counter = max(_lamport_counter + 1, seed + 1)
        return _lamport_counter


class NormalizeResult:
    """Ergebnis der Normalisierung.

    valid: True -> Event ist schema-faehig und kann in den Spool.
    incomplete: True -> Identitaets- oder Zuordnungs-Unvollstaendigkeit (kein Event/kein
                kuenstlicher Bezug); payload ggf. als nicht zuordnungsfaehig markiert.
    event: dict (v2-Event) oder None.
    rejection: bei RegistryRejection -> event_type-Meldung, KEIN Event.
    """

    def __init__(self, *, valid=False, incomplete=False, event=None,
                 rejection: Optional[str] = None, reason: str = "") -> None:
        self.valid = valid
        self.incomplete = incomplete
        self.event = event
        self.rejection = rejection
        self.reason = reason

    def __repr__(self) -> str:  # pragma: no cover
        return (f"NormalizeResult(valid={self.valid}, incomplete={self.incomplete}, "
                f"rejection={self.rejection!r}, reason={self.reason!r})")


# Hook -> (event_type, kind, phase) Mapping (geschlossen; NICHT registrierte bleiben aus).
_HOOK_MAP = {
    "on_session_start": ("hermes.session.started", "observed", "observed", "fresh"),
    "on_session_end": ("hermes.session.ended", "observed", "observed", "fresh"),
    "on_session_finalize": ("hermes.session.ended", "observed", "observed", "fresh"),
    "subagent_start": ("hermes.agent.started", "observed", "observed", "spawn"),
    "subagent_stop": ("hermes.agent.stopped", "observed", "observed", "spawn"),
    "kanban_task_claimed": ("hermes.task.claimed", "observed", "observed", "fresh"),
    "kanban_task_completed": ("hermes.task.completed", "observed", "observed", "fresh"),
    "kanban_task_blocked": ("hermes.task.blocked", "observed", "observed", "fresh"),
    # post_tool_call: KEIN isolierter Tool-Typ (E-1); nur Metadaten an Vorgaenger.
    # -> wird nicht als eigenes v2-Event erzeugt; capture-intern (post_tool_call aggregiert).
}
# Fuer post_tool_call markieren wir explizit: kein Event-Typ -> kein v2-Event (E-1/B-01).
_TOOL_METADATA_HOOKS = {"post_tool_call"}


def normalize(hook: str, kwargs: Mapping[str, Any], *, agent_id: str,
              host_identity: str, process_ref: ProcessReference,
              now_iso: Optional[str] = None,
              stop_resolved_instance_id: Optional[str] = None) -> NormalizeResult:
    """Normalisiert einen Hook-Aufruf zu einem v2-Event.

    Raises: registry.RegistryRejection nur, wenn ein unbeabsichtigter Typ durchkriecht
            (defensive); Normalizer faengt das selbst.
    """
    if hook in _TOOL_METADATA_HOOKS:
        # E-1: keine individuellen Tool-Events. Als Metadaten nur aggregiert am Vorgaenger.
        return NormalizeResult(rejection="post_tool_call: kein isolierter Tool-Event "
                                         "(E-1/B-01); nur Capture-intrinsisch.", reason="tool-metadata")

    if hook not in _HOOK_MAP:
        # Unbekannter Hook -> NICHT registriert -> kein Event, saubere Ablehnung.
        return NormalizeResult(rejection=f"unbekannter Hook {hook!r}: nicht erfassbar "
                                         "(geschlossene Registry)", reason="unknown-hook")

    event_type, kind, phase, prov_type = _HOOK_MAP[hook]

    # --- Geschlossene Registry: nicht registrierter Typ wird vor INSERT abgewiesen (P-4).
    if not registry.is_registered(event_type):
        return NormalizeResult(rejection=f"RegistryRejection: {event_type!r} nicht registriert",
                               reason="registry")

    # --- Payload aus Hook (defensiv) ---
    p = dict(kwargs)
    session_id = p.get("session_id") or p.get("parent_session_id") or None
    child_session_id = p.get("child_session_id") or None
    child_subagent_id = p.get("child_subagent_id") or None
    parent_instance_id = p.get("parent_instance_id") or None
    task_id = p.get("task_id") or None
    turn_id = p.get("turn_id") or None

    # --- Datenschutz: Redaction/Data-Minimierung VOR INSERT (B-08) ---
    # Free-text fields are dropped here; there is no setting that keeps them.
    payload_meta = redact.apply({k: v for k, v in p.items()})

    # --- Runtime-Identitaet ---
    if hook == "subagent_stop":
        # P-1: Stop wird NICHT neu gebildet. Wenn der normalizer eine vom via
        # child_session_id gefundenen Start uebernommene instance_id erhaelt, wird sie
        # verwendet; sonst (fehlender/mehrdeutiger Start) bleibt die ID unvollstaendig.
        if not child_session_id:
            return NormalizeResult(incomplete=True,
                                   reason="subagent_stop ohne child_session_id: nicht eindeutig zuordenbar")
        if stop_resolved_instance_id:
            instance_id = stop_resolved_instance_id
        else:
            instance_id = make_instance_id(
                agent_id, host_identity, process_ref.pid, process_ref.started_at,
                child_subagent_id=child_subagent_id)
    elif hook == "subagent_start":
        # E-3c: Kind-Instanz via Grundformel + child_subagent_id
        if child_subagent_id is None:
            instance_id = None
        else:
            instance_id = make_instance_id(
                agent_id, host_identity, process_ref.pid, process_ref.started_at,
                child_subagent_id=child_subagent_id)
    else:
        instance_id = make_instance_id(
            agent_id, host_identity, process_ref.pid, process_ref.started_at)

    if instance_id is None:
        # P-3: fehlende Pflichtbestandteile -> KEINE erfundene Runtime-ID -> unvollstaendig.
        return NormalizeResult(incomplete=True,
                               reason="instance_id nicht bildbar (fehlende Pflichtbestandteile)")

    # --- Subagent-Lifecycle-Zuordnung (P-1) ---
    # (Stop-Entscheidung ist im Controller via Lifecycle-Index getroffen:
    #   nur mit eindeutigem Start via child_session_id wird instance_id uebernommen.)

    # --- Kausalitaet/Parent ---
    prov = {"type": prov_type}
    if prov_type == "spawn":
        if parent_instance_id:
            prov["parent_instance_id"] = parent_instance_id
        # continues/snapshot NICHT bei spawn (Schema-exklusiv)
    elif prov_type == "fresh":
        pass  # keine Verweisfelder

    # --- Lamport ---
    parent_lc = p.get("parent_lc")  # optional, von Subagent-Spawn-Init
    lc = next_lamport(seed=int(parent_lc) if isinstance(parent_lc, int) else 0)

    now = now_iso or iso_now()

    producer = {
        "agent_id": agent_id,
        "instance_id": instance_id,
        "prov": prov,
    }
    if session_id:
        producer["session_id"] = session_id
    elif child_session_id:
        producer["session_id"] = child_session_id
    producer["process_id"] = process_ref.pid
    producer["process_started_at"] = process_ref.started_at

    # context: kausal und IDs (Kausalitaet nur soweit belegt).
    # Schema: causal_status=root => caused_by_event_ids required + null (kein Vorgaenger).
    context = {
        "causal_status": "root",
        "caused_by_event_ids": None,
        "ingest_mode": "live",
        "timeliness": "on_time",
    }
    if turn_id:
        context["correlation_id"] = f"turn:{turn_id}"
    if child_session_id:
        context["child_session_id"] = child_session_id  # P-1 Kopplungs-Schluessel
    if task_id:
        context["task_id"] = task_id

    # lm_source.method konsistent zu prov.type (Schema R4): fresh->fresh, spawn->spawn.
    # baseline_used ist NUR bei method=resume erlaubt (true); bei fresh/spawn nie gesetzt.
    lm_method = "fresh" if prov_type in ("fresh",) else prov_type if prov_type in (
        "spawn", "clone", "restore", "resume") else "fresh"
    # Schema verlangt bei nicht-resume explizit baseline_used (false), sonst wird
    # die resume-then-Regel als Fehler gewertet (empirisch aus Reader/Reader v2 belegt).
    lm_source = {
        "method": lm_method,
        "reference_event_ids": [],
        "baseline_used": bool(lm_method == "resume"),
    }

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "schema_version": 2,
        "kind": kind,
        "phase": phase,
        "occurred_at": now,
        "observed_at": now,
        "lc": lc,
        "lm_source": lm_source,
        "producer": producer,
        "context": context,
        "payload": payload_meta if payload_meta else None,
    }
    return NormalizeResult(valid=True, event=event)
