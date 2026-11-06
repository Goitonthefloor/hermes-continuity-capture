"""
continuity_capture.registry — Geschlossene Event-Type-Registry (B-01 / P-4).

Nur ausdruecklich registrierte v2-Event-Typen duerfen erzeugt werden. Die Liste ist
eine geschlossene Obermenge, abgeleitet aus dem realen Schema v2
(``$defs.registered_event_types``) + im M-2-Scope genutzten, konform registrierten Typen.

Die Registry wird NICHT dynamisch anhand unbekannter Hook-Namen oder Event-Typen erweitert.
Nicht registrierte Typen werden vor dem Spool-INSERT abgewiesen (siehe normalizer).
"""

from __future__ import annotations

from typing import FrozenSet, Optional

# ---------------------------------------------------------------- Kern-Typen
# Quell-Anker: /opt/hermes-continuity/schema/event.schema.v2.json -> $defs.registered_event_types
# Zuzueglich der im M-2-Scope erlaubten registrierten Lebenszyklus-Typen.
RESERVED_REGISTERED_TYPES: FrozenSet[str] = frozenset({
    "hermes.agent.started",
    "hermes.agent.stopped",
    "hermes.session.started",
    "hermes.session.ended",
    "hermes.task.claimed",
    "hermes.task.completed",
    "hermes.task.blocked",
    # Memory/Goal/System sind im v2-Schema registriert; im initialen M-2-Scope wird
    # von ihnen nur gespeichert, was eigene Hook-Quellen sauber liefern (hier zzgl. optional).
    "hermes.memory.created",
    "hermes.memory.updated",
    "hermes.memory.recalled",
    "hermes.system.causal_resolution",
    "hermes.system.causal_invalid",
})

# Kein offener Pattern-Match: ein Typ muss explizit in der geschlossenen Liste stehen.
_ACTIVE_TYPES: Optional[FrozenSet[str]] = None


def active_registry() -> FrozenSet[str]:
    """Liefert die geschlossene, explizite Registry (feste Obermenge)."""
    return RESERVED_REGISTERED_TYPES


def is_registered(event_type: object) -> bool:
    """False fuer alles, was nicht exakt in der geschlossenen Registry steht."""
    if not isinstance(event_type, str):
        return False
    return event_type in RESERVED_REGISTERED_TYPES


def assert_registered(event_type: object) -> None:
    """Wird vor dem INSERT aufgerufen; wirft RegistryRejection, wenn nicht registriert."""
    if not is_registered(event_type):
        raise RegistryRejection(event_type)


class RegistryRejection(Exception):
    """Nicht registrierter Event-Typ — vor dem Spool-INSERT abgewiesen."""

    def __init__(self, event_type: object) -> None:
        self.event_type = event_type
        # Sensible Nutzdaten werden NICHT protokolliert; nur den Typ-Namen.
        super().__init__(f"RegistryRejection: nicht registrierter Event-Typ {event_type!r} "
                         "wird nicht verarbeitet/publiziert.")
