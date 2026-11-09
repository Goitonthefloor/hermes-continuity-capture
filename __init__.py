"""hermes-continuity-capture — standalone Hermes general plugin (M-2 Capture).

Records lifecycle events (session, subagent, task) as Continuity schema-v2 events
in a local SQLite spool and hands validated events to a NATS/JetStream bridge.
It is BEWUSST not active by default: activation via ``plugins.enabled`` is an
explicit operator action.

Hardening / boundaries from the M-2 specification:
- Closed event-type registry (unknown types are rejected BEFORE the spool INSERT).
- Deterministic runtime/session/subagent identity (UUIDv5), no invented IDs.
- Role-specific deduplication; parallel subagents are never merged.
- Redaction/data minimization BEFORE persistence; no full prompts/tool results.
- Error isolation: hook errors never block the agent run.
"""

from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Any, Optional, Tuple

log = logging.getLogger("hermes-continuity-capture")

from .continuity_capture import load_config  # noqa: E402
from .continuity_capture.config import resolve_agent_id  # noqa: E402
from .continuity_capture.controller import CaptureController  # noqa: E402
from .continuity_capture.nats_transport import build_jetstream_transport  # noqa: E402

__all__ = ["register", "CaptureController"]
__version__ = "0.2.2"

_LOCK = threading.RLock()
_ACTIVE: Optional[CaptureController] = None

# Environment variable the ``nats_password`` secret field is stored under. Declared
# explicitly as ``env:`` in plugin.yaml, so the manifest and this reader can never
# drift apart; read through ``agent.secret_scope.get_secret`` (catalog rule 11).
NATS_PASSWORD_ENV = "HERMES_CONTINUITY_CAPTURE_NATS_PASSWORD"

# Every plugin setting is read individually through PluginContext.get_config; an
# unset key keeps the module default. ``nats_password`` is a secret and is read
# from the secret route instead (see NATS_PASSWORD_ENV).
_SETTING_KEYS = (
    "spool_path", "publish_mode", "spool_cap", "retention_days",
    "send_retry_attempts", "send_retry_base_delay_s", "send_max_backoff_s",
    "exclude_tools", "exclude_event_types",
    "dedup_ttl_seconds", "agent_id", "host_identity",
    "bridge_subject_events", "bridge_subject_quality",
    "nats_url", "nats_user", "nats_tls",
    "jetstream_stream_name", "jetstream_duplicate_window_s",
    "retention_sweep_seconds",
)

# Hooks the capture controller observes. No productive capture without the plugin
# being enabled via plugins.enabled.
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


def _get_config(ctx: Any, key: str, default: Any = None) -> Any:
    try:
        return ctx.get_config(key, default)
    except Exception:  # noqa: BLE001 — a bad key must never block registration
        return default


def _read_settings(ctx: Any) -> Tuple[dict, Optional[str]]:
    """Read every known key individually from Hermes' plugin-settings route.

    Returns ``(settings_dict, nats_password)``. The password is preferred from the
    secret route (``get_config('nats_password')``); only when that is empty do we
    fall back to the profile secret scope (``NATS_PASSWORD``). Credentials are never
    parsed out of ``.env`` files here and never logged.
    """
    conf = {key: _get_config(ctx, key, None) for key in _SETTING_KEYS}
    password = None
    try:
        from agent.secret_scope import get_secret
        password = get_secret(NATS_PASSWORD_ENV) or None
    except Exception:  # noqa: BLE001 — no secret route available
        password = None
    return conf, password


def _data_dir(ctx: Any) -> Optional[Path]:
    try:
        return Path(ctx.state.data_dir)
    except Exception:  # noqa: BLE001 — state facade may be absent (probe/host)
        return None


def _build_controller(ctx: Any) -> CaptureController:
    conf, password = _read_settings(ctx)
    cfg = load_config(conf, data_dir=_data_dir(ctx))
    cfg.agent_id = resolve_agent_id(cfg.agent_id, getattr(ctx, "profile_name", None))
    transport = None
    if cfg.publish_mode in ("jetstream", "core"):
        transport = build_jetstream_transport(cfg, password=password)
    return CaptureController(cfg, bridge=transport)


def _get_controller(ctx: Any) -> CaptureController:
    """Build the controller lazily (first hook/command) so register() has no side effects."""
    global _ACTIVE
    with _LOCK:
        if _ACTIVE is None:
            _ACTIVE = _build_controller(ctx)
        return _ACTIVE


def _shutdown() -> None:
    global _ACTIVE
    with _LOCK:
        ctrl, _ACTIVE = _ACTIVE, None
    if ctrl is not None:
        try:
            ctrl.shutdown()
        except Exception:  # noqa: BLE001
            pass


def register(ctx: Any) -> None:
    """Hermes plugin entry point: bind observer hooks and the status command."""
    for hook in _HOOKS:
        try:
            # The hook receives the payload as kwargs; capture handles it defensively.
            # The controller is built lazily on the first call (no file I/O here).
            ctx.register_hook(hook, _make_hook(ctx, hook))
        except Exception as exc:  # noqa: BLE001 — observer fail-open
            log.warning("continuity-capture: hook %s not registrable (observer fail-open): %s",
                        hook, exc)

    if hasattr(ctx, "register_command"):
        try:
            ctx.register_command(
                "continuity-capture",
                handler=_make_command(ctx),
                description="Show continuity-capture spool/sender status, or flush the retry queue.",
                args_hint="[status|flush]")
        except Exception:  # noqa: BLE001
            pass

    if hasattr(ctx, "on_unload"):
        try:
            ctx.on_unload(_shutdown)
        except Exception:  # noqa: BLE001
            pass


def _make_hook(ctx: Any, hook: str):
    def _callback(**kwargs: Any) -> None:
        try:
            _get_controller(ctx).on_hook(hook, **kwargs)
        except Exception as exc:  # noqa: BLE001 — observer fail-open
            log.warning("continuity-capture: %s skipped (observer fail-open): %s", hook, exc)
    return _callback


def _make_command(ctx: Any):
    def _handler(raw_args: str = "") -> str:
        try:
            return _run_command(ctx, raw_args)
        except Exception as exc:  # noqa: BLE001
            return "continuity-capture: command failed (%s)" % type(exc).__name__
    return _handler


def _run_command(ctx: Any, raw_args: str = "") -> str:
    sub = (raw_args or "").strip().lower() or "status"
    if sub == "status":
        ctrl = _get_controller(ctx)
        st = ctrl.status()
        return ("continuity-capture status: agent_id={agent}, publish_mode={mode}, "
                "transport_active={active}, spool={spool}, retention_sweeps={sweeps}, "
                "last_sender_error={err}").format(
                    agent=st["agent_id"], mode=st["publish_mode"], active=st["transport_active"],
                    spool=json.dumps(st["spool"], sort_keys=True), sweeps=st["retention_sweeps"],
                    err=st["last_sender_error"])
    if sub == "flush":
        ctrl = _get_controller(ctx)
        ctrl.flush()
        return ("continuity-capture: flush requested; the sender retries pending/error "
                "events immediately.")
    return "continuity-capture: unknown subcommand %r (use 'status' or 'flush')." % sub
