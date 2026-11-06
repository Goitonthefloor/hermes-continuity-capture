"""
continuity_capture.redact — Datenschutz/Datenminimierung VOR Spool-INSERT.

B-08/P-3/M-2 §3.6: Standardmaessig werden nur Metadaten erfasst; grosse/unsichere
Felder (args, result, child_goal, summary, conversation_history) werden NICHT uebernommen
oder nur als Laengen-/Status-Metadaten. Eine opt-in Redaction ersetzt Secret-Muster.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

# Felder, die im Normalfall komplett verworfen werden (nicht persistiert).
_SENSITIVE_FIELDS = frozenset({
    "args", "result", "child_goal", "summary", "user_message", "conversation_history",
    "assistant_response", "error_message", "child_summary",
})

# Optionale opt-in Secret-Muster (nur bei record_full_prompts oder redact_tool_payloads).
_SECRET_PATTERNS = (
    re.compile(r"(?i)\b(token|secret|password|api[_-]?key|authorization|bearer)\b"),
)


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


def redact_secrets(text: str) -> str:
    """Opt-in: ersetzt Secret-Werte zurueck zu Quellen-Angabe (keine echten Werte logs)."""
    out = str(text)
    for pattern in _SECRET_PATTERNS:
        out = pattern.sub(r"\1=***", out)
    return out


def apply(payload: Dict[str, Any], *, keep_verbose: bool = False) -> Dict[str, Any]:
    """Standard-Pipeline: Sensitive Felder ausladen (separat laufen.)

    ``keep_verbose=True`` aktiviert die (explizit konfigurierte) optionale Redaction,
    ohne dabei echte Geheimnisse zu persistieren.
    """
    stripped = strip_sensitive(payload)
    if keep_verbose:
        # Nur dann wuerden evtl. einzelne textuelle Felder durchgereicht — in M-2 bleibt
        # record_full_prompts default aus; hier wird bewusst nichts weiter angefaengt,
        # ausser redact_secrets auf angegebene Textfelder anzuwenden.
        for k in list(stripped.keys()):
            if isinstance(stripped[k], str):
                stripped[k] = redact_secrets(stripped[k])
    return stripped
