"""
continuity_capture.config — Configuration of the capture plugin.

The fields are deliberately small; sensitive payloads are NEVER stored in the
configuration, the spool or the logs. ``nats_password`` is intentionally NOT a
field of :class:`CaptureConfig` — it is a secret and stays out of any object that
might be serialized.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# --- Bridge defaults --------------------------------------------------------
# The default publish mode is local-only; a non-local mode is only meaningful when
# an external NATS/JetStream server is configured.
DEFAULT_NATS_URL = "nats://127.0.0.1:4222"
DEFAULT_JETSTREAM_STREAM_NAME = "hermes_continuity"
DEFAULT_JETSTREAM_DUPLICATE_WINDOW_S = 120
DEFAULT_SPOOL_FILENAME = "spool.db"

_VALID_PUBLISH_MODES = ("jetstream", "core", "spool-only")


@dataclass
class CaptureConfig:
    # --- Identity hardware ---------------------------------------------------
    agent_id: Optional[str] = None                 # None = resolved from the active profile at runtime
    host_identity: Optional[str] = None            # None = identity.default_host_identity() at runtime

    # --- Spool ---------------------------------------------------------------
    spool_path: Optional[str] = None               # None = not yet resolved -> <data_dir>/spool.db
    spool_cap: int = 200_000                       # maximum pending/accepted rows
    retention_days: int = 90                       # age after which terminal rows are pruned

    # --- Sender / Bridge -----------------------------------------------------
    publish_mode: str = "spool-only"               # "jetstream" | "core" | "spool-only"
    bridge_subject_events: str = "hermes.continuity.events"
    bridge_subject_quality: str = "hermes.continuity.quality"
    send_retry_attempts: int = 5
    send_retry_base_delay_s: int = 1
    send_max_backoff_s: int = 60
    retention_sweep_seconds: int = 3600            # <= 0 disables the periodic retention sweep

    # --- NATS / JetStream bridge (external infrastructure) -------------------
    nats_url: str = DEFAULT_NATS_URL
    nats_user: Optional[str] = None
    nats_tls: bool = False
    jetstream_stream_name: str = DEFAULT_JETSTREAM_STREAM_NAME
    jetstream_duplicate_window_s: int = DEFAULT_JETSTREAM_DUPLICATE_WINDOW_S

    # --- Privacy / redaction (B-08: applied BEFORE the SQLite INSERT) --------
    # Free-text fields are always dropped; there is deliberately no setting that
    # would store them (see README "Privacy and safety guarantees").
    exclude_tools: set = field(default_factory=set)
    exclude_event_types: set = field(default_factory=set)

    # --- Deduplication (B-07/P-2) --------------------------------------------
    dedup_ttl_seconds: int = 300


# Scalar keys that are copied 1:1 from the settings mapping.
_SCALAR_KEYS = (
    "agent_id", "host_identity", "spool_cap", "retention_days",
    "publish_mode", "send_retry_attempts", "send_retry_base_delay_s",
    "send_max_backoff_s", "retention_sweep_seconds",
    "nats_url", "nats_user", "nats_tls",
    "jetstream_stream_name", "jetstream_duplicate_window_s",
)


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return bool(value)


def _within(child: Path, base: Path) -> bool:
    """True when the (already resolved) *child* path stays inside *base*."""
    return child == base or base in child.parents


def resolve_spool_path(spool_path: object, data_dir: Optional[Path | str]) -> Optional[str]:
    """Resolve the SQLite spool location safely.

    - ``None``/empty -> ``<data_dir>/spool.db`` (default). The data directory is
      created if missing. With no ``data_dir`` available the path stays ``None``
      ("not yet resolved").
    - A relative path is resolved against ``data_dir`` and MUST stay inside it;
      ``..``-escapes and symlink-escapes raise :class:`ValueError`.
    - An absolute path is allowed and used as-is.
    """
    if spool_path is None or (isinstance(spool_path, str) and not spool_path.strip()):
        if data_dir is None:
            return None
        base = Path(data_dir).resolve()
        base.mkdir(parents=True, exist_ok=True)
        return str(base / DEFAULT_SPOOL_FILENAME)

    candidate_path = Path(str(spool_path))
    if candidate_path.is_absolute():
        return str(candidate_path.resolve())

    if data_dir is None:
        # No base directory to anchor a relative path to: keep it unresolved rather
        # than silently writing relative to the process cwd.
        return None

    base = Path(data_dir).resolve()
    base.mkdir(parents=True, exist_ok=True)
    resolved = (base / candidate_path).resolve()
    if not _within(resolved, base):
        raise ValueError(
            "spool_path %r escapes the plugin data directory %s" % (str(spool_path), base)
        )
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return str(resolved)


def resolve_agent_id(configured: object, profile_name: object) -> str:
    """Resolve the logical agent id: an explicit setting wins, else the active profile."""
    if configured is not None and str(configured).strip():
        return str(configured).strip()
    name = str(profile_name).strip() if profile_name else ""
    return name or "default"


def load_config(conf: dict | None, *, data_dir: Optional[Path | str] = None) -> CaptureConfig:
    """Validate/limit a settings mapping -> :class:`CaptureConfig` (never holds secrets).

    Each key is applied individually; an unset key keeps its default. ``data_dir``
    anchors the spool path resolution (see :func:`resolve_spool_path`).
    """
    cfg = CaptureConfig()
    if conf is not None:
        if not isinstance(conf, dict):
            raise ValueError("config must be a mapping")
        for key in _SCALAR_KEYS:
            if key in conf and conf[key] is not None:
                setattr(cfg, key, conf[key])
        for key in ("exclude_tools", "exclude_event_types"):
            if isinstance(conf.get(key), (list, set, tuple)):
                setattr(cfg, key, set(conf[key]))
        cfg.nats_tls = _as_bool(cfg.nats_tls)

    # publish_mode canalization
    if cfg.publish_mode not in _VALID_PUBLISH_MODES:
        raise ValueError(
            "publish_mode must be jetstream|core|spool-only (was %r)" % (cfg.publish_mode,)
        )

    raw_spool = conf.get("spool_path") if isinstance(conf, dict) else None
    cfg.spool_path = resolve_spool_path(raw_spool, data_dir)
    return cfg
