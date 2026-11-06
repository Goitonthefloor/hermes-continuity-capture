"""hermes-continuity-capture — eigenständiges Hermes-General-Plugin (M-2 Capture).

Dokumentiert Lifecycle-Ereignisse (Session, Subagent, Task) als Continuity-v2-Events in einen
lokalen SQLite-Spool und reicht validierte Events über die Continuity-Bridge (JetStream oder
Core) an NATS weiter. Ist BEWUSST nicht produktiv aktiviert: Aktivierung über ``plugins.enabled``
erfolgt nur durch den Betreiber.

Hardening / Grenzen aus der M-2-Spezifikation:
- Geschlossene Event-Type-Registry (unbekannte Typen werden VOR dem Spool-INSERT abgewiesen).
- Deterministische Runtime-/Session-/Subagent-Identität (UUIDv5), keine erfundenen IDs.
- Rollenspezifische Deduplication; parallele Subagenten werden nie zusammengeführt.
- Redaction/data-Minimierung VOR der Persistierung; keine vollständigen Prompts/Tool-Ergebnisse.
- Fehlerisolation: Hook-Fehler blockieren nie die Agentenlaufzeit.
"""

from __future__ import annotations

import logging
from typing import Optional

log = logging.getLogger("hermes-continuity-capture")

from .continuity_capture import load_config  # noqa: E402
from .continuity_capture.controller import CaptureController  # noqa: E402

__all__ = ["register", "CaptureController"]
__version__ = "0.1.0"

_LOCK_REGISTER = __import__("threading").RLock()
_ACTIVE: Optional[CaptureController] = None


def _build(config: dict) -> CaptureController:
    global _ACTIVE
    with _LOCK_REGISTER:
        if _ACTIVE is None:
            _ACTIVE = CaptureController(load_config(config))
        return _ACTIVE


# Hooks, die der Capture-Controller beobachtet. KEINE produktive Erfassung ohne
# Aktivierung des Plugins via plugins.enabled.
_HOOKS = (
    "on_session_start",
    "on_session_end",
    "on_session_finalize",
    "post_tool_call",
    "subagent_start",
    "subagent_stop",
    "kanban_task_claimed",
    "kanban_task_completed",
    "kanban_task_blocked",
)


def register(ctx) -> None:
    """Hermes-Plugin-Einstieg. Bindet Observer-Hooks, die bei plugins.enabled feuern."""
    settings = getattr(ctx, "plugin_settings", None) or {}
    ctrl = _build(settings or {})
    for hook in _HOOKS:
        try:
            # Der Hook erhält die Payload als kwargs; Capture verarbeitet defensiv.
            ctx.register_hook(hook, (lambda h=hook, c=ctrl, **kw: c.on_hook(h, **kw)))
        except Exception as exc:  # noqa: BLE001 — Beobachter fail-open
            log.warning("Hook %s nicht registrierbar (observer fail-open): %s", hook, exc)

    # Status-/Diagnosebefehl (optional; nur falls die Runtime register_command kennt).
    if hasattr(ctx, "register_command"):
        try:
            ctx.register_command(
                "continuity-capture",
                handler=lambda: _status(ctrl),
                description="Zeigt den Capture-Spool-/Sender-Status.")
        except Exception:
            pass


def _status(ctrl: CaptureController) -> str:
    import json
    try:
        return "continuity-capture status: " + json.dumps(ctrl.status(), sort_keys=True)
    except Exception:
        return "continuity-capture: status nicht verfügbar"
