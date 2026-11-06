"""
continuity_capture.config — Konfiguration des Capture-Plugins.

Felder sind bewusst klein gehalten; sensible Nutzdaten werden NIEMALS in der
Konfiguration, im Spool oder in Logs abgelegt.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class CaptureConfig:
    # --- Identitaets-Hardware -----------------------------------------------
    agent_id: Optional[str] = None                      # logische Agentenidentitaet (aus Profil)
    host_identity: Optional[str] = None            # None = zur Laufzeit via identity.default_host_identity()

    # --- Registry (geschlossen, siehe registry.py) --------------------------
    # keine Laufzeit-Oeffnung; die Registry ist fest im Modul definiert.

    # --- Spool ---------------------------------------------------------------
    spool_path: str = "data/spool.db"              # relativ zum Plugin-Datadir
    spool_cap: int = 200_000                       # maximale pending-Eintraege
    retention_days: int = 90                       # alte accepted/done-Eintraege loeschen

    # --- Sender / Bridge -----------------------------------------------------
    publish_mode: str = "jetstream"                # "jetstream" | "core" | "spool-only"
    bridge_subject_events: str = "hermes.continuity.events"
    bridge_subject_quality: str = "hermes.continuity.quality"
    send_retry_attempts: int = 5
    send_retry_base_delay_s: int = 1
    send_max_backoff_s: int = 60

    # --- Datenschutz / Redaction (B-08: VOR SQLite-INSERT) -------------------
    # Standard: nur Metadaten; keine vollstaendigen Prompts/Tool-Ergebnisse.
    record_full_prompts: bool = False             # Nur mit expliziter Aktivierung
    exclude_tools: set = field(default_factory=set)        # Tool-Namen, die nicht erfasst werden
    exclude_event_types: set = field(default_factory=set)  # registry-interne Ausschluesse

    # --- Deduplication (B-07/P-2) --------------------------------------------
    dedup_ttl_seconds: int = 300                  # rollen-spezifischer Schlüssel-Speicher-TTL


def load_config(conf: dict | None) -> CaptureConfig:
    """Validiert/eingeschraenkter YAML/dict -> CaptureConfig (keine Zugangsdaten)."""
    cfg = CaptureConfig()
    if not conf:
        return cfg
    if isinstance(conf, dict):
        for k in ("agent_id", "host_identity", "spool_path", "spool_cap",
                  "retention_days", "publish_mode", "send_retry_attempts"):
            if k in conf and conf[k] is not None:
                setattr(cfg, k, conf[k])
        for k in ("exclude_tools", "exclude_event_types"):
            if isinstance(conf.get(k), (list, set, tuple)):
                setattr(cfg, k, set(conf[k]))
        if "record_full_prompts" in conf:
            cfg.record_full_prompts = bool(conf["record_full_prompts"])
    # publish_mode-Kanelisierung
    if cfg.publish_mode not in ("jetstream", "core", "spool-only"):
        raise ValueError(f"publish_mode muss jetstream|core|spool-only sein (war {cfg.publish_mode!r})")
    return cfg
