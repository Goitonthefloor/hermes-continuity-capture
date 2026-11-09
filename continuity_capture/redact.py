"""
continuity_capture.redact — Datenschutz/Datenminimierung VOR Spool-INSERT.

B-08/P-3/M-2 §3.6: Standardmaessig werden nur Metadaten erfasst; grosse/unsichere
Felder (args, result, child_goal, summary, conversation_history) werden NICHT uebernommen
oder nur als Laengen-/Status-Metadaten. Es gibt bewusst KEINEN Schalter, der Prompt-
oder Tool-Inhalte im Spool ablegt.
"""

from __future__ import annotations

from typing import Any, Dict

# Fields that are dropped entirely (never persisted).
# Free-text / potentially sensitive fields; only their length is kept as a
# non-sensitive signal. See README ("Privacy and safety guarantees").
_SENSITIVE_FIELDS = frozenset({
    "args", "result", "child_goal", "summary", "user_message", "conversation_history",
    "assistant_response", "error_message", "child_summary",
    "reason", "tool_call_history",
})


def strip_sensitive(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Entfernt Standard-sensible Felder.

    Liefert ein NEUES Dict ohne args/result/etc.; groesse der entfernten Felder wird
    als Metadaten (len_*) beibehalten, falls sinnvoll.
    """
    out = {}
    for k, v in payload.items():
        if k in _SENSITIVE_FIELDS:
            # Grosse Inhalte nie uebernehmen; nur Laenge als nicht-sensibles Signal.
            if isinstance(v, (str, list, dict)):
                out.setdefault("omitted_fields", {})[k] = _len_of(v)
        else:
            out[k] = v
    return out


def _len_of(v: Any) -> int:
    try:
        return len(v)
    except TypeError:
        return 0


def apply(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Standard-Pipeline: sensible Felder ausladen (Rueckgabe ist eine Kopie)."""
    return strip_sensitive(payload)
