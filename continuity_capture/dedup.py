"""
continuity_capture.dedup — Rollenspezifische Dedup-Schlüsselberechnung (B-07 / P-2).

Regeln:
- Session-Hooks:  (source_hook, session_id, turn_id)  — turn_id zwingend bei on_session_end
                  (feuert je Turn mehrfach). Ohne turn bei start/finalize: (source_hook, session_id).
- Tool-Hooks:     (source_hook, session_id, tool_call_id).
- Subagent-Hooks: (source_hook, parent_session_id, parent_turn_id, child_session_id)
                  — parallele Subagenten nie zusammengefuehrt; child_session_id als Diskriminator.
- Kanban-Hooks:   (source_hook, task_id, run_id).
- Fehlende/mehrdeutige Ids -> KEIN Dedup-Schlüssel (None) -> keine Deduplizierung (P-2).
"""

from __future__ import annotations

from typing import Any, Mapping, Optional

def _s(value: object) -> str:
    if value is None:
        return ""
    return str(value)

def _opt(*values: object) -> Optional[str]:
    """Liefert None, wenn einer der notwaendigen Werte fehlt/leer ist (kein Dedup)."""
    parts = []
    for v in values:
        s = _s(v).strip()
        if not s:
            return None
        parts.append(s)
    return "|".join(parts)


def dedup_key_for(source_hook: str, p: Mapping[str, Any]) -> Optional[str]:
    """Berechnet den rollenspezifischen Dedup-Schlüssel oder None (keine Deduplizierung)."""
    session_id = p.get("session_id") or p.get("parent_session_id") or None
    if source_hook == "on_session_start" or source_hook == "on_session_finalize":
        # einmalig (.Le synonym zu auf Gefahr); nur session_id noetig
        return _opt(source_hook, session_id)
    if source_hook == "on_session_end":
        # turn_id ZWINGEND, sonst wuerden verschiedene Turns faelschlich dedupliziert (P-2/B-07)
        return _opt(source_hook, session_id, p.get("turn_id"))
    if source_hook in ("subagent_start", "subagent_stop"):
        # P-1/P-2: child_session_id ist der robuste, in Start UND Stop vorhandene
        # Diskriminator. Parallele Subagenten -> verschiedene child_session_id -> KEIN Dedup.
        return _opt(source_hook, p.get("parent_session_id"), p.get("parent_turn_id"),
                    p.get("child_session_id"))
    if source_hook.startswith("kanban_"):
        return _opt(source_hook, p.get("task_id"), p.get("run_id"))
    if source_hook == "post_tool_call":
        return _opt(source_hook, session_id, p.get("tool_call_id"))
    # unbekannt -> kein Dedup
    return None


def asserts_same_factual_instance(source_hook: str, prev: Mapping[str, Any],
                                  cur: Mapping[str, Any]) -> bool:
    """B-07/P-2: Prueft, ob zwei Treffer dieselbe FACHLICHE Ereignisinstanz darstellen.

    Nur bei nachweislich gleicher Instanz ist Deduplizierung erlaubt. Zusaetzlich zum
    technischen Schlüssel wird hier die fachliche Identitaet abgeglichen (gleicher
    event_type + gleiche Kern-IDs, die die Instanz definieren). Zeigt Mehrdeutigkeit,
    kehrt False zurueck (keine Deduplizierung).
    """
    if prev.get("event_type") != cur.get("event_type"):
        return False
    # fachliche Identitaet: gleicher turn/task bzw. child_session — die Definition der Instanz
    if source_hook in ("on_session_end", "on_session_finalize"):
        return _s(prev.get("turn_id")) == _s(cur.get("turn_id"))
    if source_hook.startswith("subagent_"):
        # gleiche child_session_id => gleiche Kind-Instanz
        return _s(prev.get("child_session_id")) == _s(cur.get("child_session_id"))
    if source_hook.startswith("kanban_"):
        return _s(prev.get("run_id")) == _s(cur.get("run_id"))
    if source_hook == "post_tool_call":
        return _s(prev.get("tool_call_id")) == _s(cur.get("tool_call_id"))
    return True
