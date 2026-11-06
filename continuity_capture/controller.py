"""
continuity_capture.controller — Glue zwischen Hermes-Hooks und Kernpaket.

Hook > Normalize > Dedup > Subagent-Lifecycle > Spool > Sender. Fehlerisolierung:
ein Hook-Aufruf darf die Agentenausfuehrung nie blockieren.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from .identity import ProcessReference, default_host_identity
from .normalizer import normalize as _normalize
from .dedup import dedup_key_for
from .lifecycle import SubagentLifecycleIndex
from .spool import Spool
from .sender import BridgeSink, SenderThread

log = logging.getLogger("continuity.capture")


class CaptureController:
    def __init__(self, config, *, bridge: Any = None) -> None:
        self._cfg = config
        self._process = ProcessReference()
        self._host = config.host_identity or default_host_identity()
        self._spool = Spool(config.spool_path, spool_cap=config.spool_cap,
                            dedup_ttl_seconds=config.dedup_ttl_seconds)
        self._sink = BridgeSink(publish_mode=config.publish_mode,
                                subject_events=config.bridge_subject_events,
                                subject_quality=config.bridge_subject_quality,
                                bridge=bridge)
        self._sender = SenderThread(self._spool, self._sink,
                                    retry_attempts=config.send_retry_attempts,
                                    base_delay=config.send_retry_base_delay_s,
                                    max_backoff=config.send_max_backoff_s)
        self._lifecycle = SubagentLifecycleIndex()
        self._sender.start()

    def on_hook(self, source_hook: str, **kwargs: Any) -> None:
        try:
            self._handle(source_hook, kwargs)
        except Exception as exc:  # Fehlerisolation: nie die Agentenlaufzeit blockieren
            log.error("Capture-Hook %s fehlgeschlagen (isoliert): %s", source_hook, exc)

    def _handle(self, source_hook: str, kwargs: dict) -> None:
        p = dict(kwargs)
        # fachliche Ausschluesse
        if source_hook == "post_tool_call" and p.get("tool_name") in self._cfg.exclude_tools:
            return
        if p.get("event_type") in self._cfg.exclude_event_types:
            return

        res = _normalize(source_hook, p, agent_id=self._cfg.agent_id,
                         host_identity=self._host, process_ref=self._process)
        if res.incomplete:
            log.info("Capture: unvollstaendig (%s): %s", source_hook, res.reason)
            return
        if not res.valid or res.event is None:
            if res.rejection:
                log.info("Capture: abgewiesen (%s): %s", source_hook, res.reason)
            return

        event = res.event
        child_sid = event.get("context", {}).get("child_session_id")
        if source_hook == "subagent_start" and child_sid:
            self._lifecycle.register_start(child_sid, event)

        final_event = event
        if source_hook == "subagent_stop":
            resolved = self._lifecycle.resolve_stop(child_sid) if child_sid else None
            if resolved is None or resolved.get("event_id") is None:
                log.info("Capture: subagent_stop ohne eindeutigen Start "
                         "(child_session_id=%r) -> unvollstaendig, nicht verknuepft", child_sid)
                return
            start_ev = self._spool.get(resolved["event_id"])
            if start_ev is None:
                return
            start_instance = start_ev["payload"]["producer"]["instance_id"]
            res2 = _normalize(source_hook, p, agent_id=self._cfg.agent_id,
                              host_identity=self._host, process_ref=self._process,
                              stop_resolved_instance_id=start_instance)
            if not res2.valid or res2.event is None:
                return
            final_event = res2.event
            self._lifecycle.forget(child_sid)

        dkey = dedup_key_for(source_hook, p)
        self._spool.insert(final_event, dedup_key=dkey)

    def status(self) -> dict:
        return {"spool": self._spool.count_by_status(),
                "publish_mode": self._cfg.publish_mode,
                "last_sender_error": self._sender.last_error}

    def shutdown(self) -> None:
        try:
            self._sender.stop()
        finally:
            self._spool.close()


def wait_for_spool_done(ctrl: CaptureController, timeout: float = 3.0) -> dict:
    """Test-Hilfsmittel: wartet, bis der Sender alle pending-Eintraege abgearbeitet hat."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = ctrl._spool.count_by_status()
        if not st.get("pending") and not st.get("accepted"):
            return st
        time.sleep(0.01)
    return ctrl._spool.count_by_status()
