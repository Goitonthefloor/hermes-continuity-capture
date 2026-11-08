"""Test suite for the hermes-continuity-capture plugin.

Exercises the real modules (standard library + pytest only, no network access). Every
SQLite spool is created under pytest's ``tmp_path``, so the suite never writes into the
repository tree (in particular it never creates ``data/spool.db``).
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import sqlite3
import time

import pytest

from continuity_capture import dedup, identity, redact, registry
from continuity_capture.config import load_config, resolve_agent_id, resolve_spool_path
from continuity_capture.controller import CaptureController, wait_for_spool_done
from continuity_capture.sender import BridgeSink, SenderThread
from continuity_capture.spool import Spool

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
PLUGIN_YAML = REPO_ROOT / "plugin.yaml"

# The Hermes checkout that defines the authoritative hook surface. Resolved from
# $HERMES_AGENT_SOURCE / $HERMES_HOME, or a sibling checkout; the contract test is
# skipped when no Hermes source is present (fresh clone, CI without it).
def _hermes_plugins_py() -> pathlib.Path:
    candidates: list[pathlib.Path] = []
    source = os.environ.get("HERMES_AGENT_SOURCE") or os.environ.get("HERMES_HOME")
    if source:
        base = pathlib.Path(source).expanduser()
        candidates += [base / "hermes-agent/hermes_cli/plugins.py",
                       base / "hermes_cli/plugins.py"]
    candidates.append(REPO_ROOT.parent / "hermes-agent/hermes_cli/plugins.py")
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return candidates[0]


VALID_HOOKS_SOURCE = _hermes_plugins_py()

EXPECTED_HOOKS = {
    "on_session_start",
    "on_session_end",
    "on_session_finalize",
    "post_tool_call",
    "subagent_start",
    "subagent_stop",
    "kanban_task_claimed",
    "kanban_task_completed",
    "kanban_task_blocked",
}


# --------------------------------------------------------------------------- helpers
def _read_spool_events(db_path: pathlib.Path) -> list:
    """Decode the event payloads stored in a spool DB file."""
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute("SELECT payload_json FROM spool ORDER BY rowid").fetchall()
    finally:
        conn.close()
    return [json.loads(r[0]) for r in rows]


def _backdate(db_path: pathlib.Path, days: int) -> None:
    """Push every row's ``received_ts`` into the past by ``days`` (for retention tests)."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("UPDATE spool SET received_ts=?", (time.time() - days * 86400,))
        conn.commit()
    finally:
        conn.close()


def _make_controller(tmp_path: pathlib.Path, name: str = "s.db") -> CaptureController:
    cfg = load_config({"spool_path": str(tmp_path / name), "publish_mode": "spool-only"})
    return CaptureController(cfg)


# ----------------------------------------------------------------------- load_config
def test_load_config_defaults():
    cfg = load_config(None)
    assert cfg.publish_mode == "spool-only"       # nothing sent by default
    assert cfg.spool_path is None                 # not resolved without a data dir
    assert cfg.spool_cap == 200_000
    assert cfg.retention_days == 90
    assert cfg.dedup_ttl_seconds == 300
    assert cfg.record_full_prompts is False
    assert cfg.exclude_tools == set()
    assert cfg.exclude_event_types == set()
    assert cfg.agent_id is None                   # resolved from the profile at runtime
    assert cfg.nats_url == "nats://127.0.0.1:4222"
    assert cfg.nats_tls is False
    assert cfg.jetstream_stream_name == "hermes_continuity"
    assert cfg.jetstream_duplicate_window_s == 120
    assert cfg.retention_sweep_seconds == 3600


def test_load_config_accepts_all_valid_publish_modes():
    for mode in ("jetstream", "core", "spool-only"):
        assert load_config({"publish_mode": mode}).publish_mode == mode


def test_load_config_rejects_invalid_publish_mode():
    with pytest.raises(ValueError):
        load_config({"publish_mode": "carrier-pigeon"})


def test_load_config_applies_overrides():
    cfg = load_config({
        "spool_path": "/tmp/example.db",
        "spool_cap": 5,
        "exclude_tools": ["run_shell"],
        "record_full_prompts": True,
    })
    assert cfg.spool_path == "/tmp/example.db"
    assert cfg.spool_cap == 5
    assert cfg.exclude_tools == {"run_shell"}
    assert cfg.record_full_prompts is True


def test_load_config_reads_new_bridge_keys():
    cfg = load_config({
        "publish_mode": "jetstream",
        "nats_url": "nats://nats.invalid:4222",
        "nats_user": "svc",
        "nats_tls": True,
        "jetstream_stream_name": "test_stream",
        "jetstream_duplicate_window_s": 7,
        "retention_sweep_seconds": 5,
    })
    assert cfg.nats_url == "nats://nats.invalid:4222"
    assert cfg.nats_user == "svc"
    assert cfg.nats_tls is True
    assert cfg.jetstream_stream_name == "test_stream"
    assert cfg.jetstream_duplicate_window_s == 7
    assert cfg.retention_sweep_seconds == 5


def test_load_config_coerces_boolean_strings():
    assert load_config({"nats_tls": "yes"}).nats_tls is True
    assert load_config({"nats_tls": "false"}).nats_tls is False
    assert load_config({"record_full_prompts": "1"}).record_full_prompts is True


# ------------------------------------------------------------------- spool path (Req 2)
def test_default_spool_lives_under_data_dir(tmp_path):
    cfg = load_config(None, data_dir=tmp_path)
    assert cfg.spool_path is not None
    resolved = pathlib.Path(cfg.spool_path)
    assert resolved.parent == tmp_path.resolve()
    assert resolved.name == "spool.db"
    assert tmp_path.exists()  # the data dir is created


def test_relative_spool_path_resolved_under_data_dir(tmp_path):
    cfg = load_config({"spool_path": "nested/capture.db"}, data_dir=tmp_path)
    resolved = pathlib.Path(cfg.spool_path)
    assert resolved.parent == (tmp_path / "nested").resolve()
    assert (tmp_path / "nested").is_dir()


def test_relative_spool_path_escape_is_rejected(tmp_path):
    with pytest.raises(ValueError):
        load_config({"spool_path": "../evil.db"}, data_dir=tmp_path)
    with pytest.raises(ValueError):
        load_config({"spool_path": "a/../../evil.db"}, data_dir=tmp_path)


def test_relative_spool_path_symlink_escape_is_rejected(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (data_dir / "escape").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError):
        load_config({"spool_path": "escape/spool.db"}, data_dir=data_dir)


def test_absolute_spool_path_is_allowed(tmp_path):
    target = tmp_path / "custom.db"
    cfg = load_config({"spool_path": str(target)})
    assert pathlib.Path(cfg.spool_path) == target.resolve()


def test_resolve_spool_path_without_data_dir_stays_unresolved():
    assert resolve_spool_path(None, None) is None
    assert resolve_spool_path("relative.db", None) is None


# --------------------------------------------------------------------------- agent id
def test_resolve_agent_id_uses_profile_when_unset():
    assert resolve_agent_id(None, "alice") == "alice"
    assert resolve_agent_id("", "alice") == "alice"
    assert resolve_agent_id(None, "  ") == "default"
    assert resolve_agent_id(None, None) == "default"
    assert resolve_agent_id("explicit", "alice") == "explicit"


# ------------------------------------------------------------------------- registry
def test_registry_knows_core_types():
    for event_type in ("hermes.session.started", "hermes.agent.started", "hermes.task.claimed"):
        assert registry.is_registered(event_type) is True


def test_registry_rejects_unknown_types():
    assert registry.is_registered("hermes.bogus.thing") is False
    assert registry.is_registered("") is False
    assert registry.is_registered(None) is False


def test_registry_assert_raises_rejection_for_unknown():
    with pytest.raises(registry.RegistryRejection):
        registry.assert_registered("hermes.not.registered")
    # A registered type must not raise.
    registry.assert_registered("hermes.session.started")


def test_registry_is_a_closed_frozen_set():
    assert isinstance(registry.RESERVED_REGISTERED_TYPES, frozenset)
    assert registry.active_registry() is registry.RESERVED_REGISTERED_TYPES


# ------------------------------------------------------------------------- identity
def test_make_instance_id_is_deterministic():
    a = identity.make_instance_id("test-agent", "host1", 4242, "2026-01-01T00:00:00.000Z")
    b = identity.make_instance_id("test-agent", "host1", 4242, "2026-01-01T00:00:00.000Z")
    assert a is not None
    assert a == b


def test_make_instance_id_distinguishes_processes():
    base = identity.make_instance_id("test-agent", "host1", 4242, "t0")
    other_pid = identity.make_instance_id("test-agent", "host1", 4243, "t0")
    other_start = identity.make_instance_id("test-agent", "host1", 4242, "t1")
    assert base != other_pid
    assert base != other_start


def test_make_instance_id_distinguishes_child_subagent():
    parent = identity.make_instance_id("test-agent", "host1", 42, "t0")
    child_a = identity.make_instance_id("test-agent", "host1", 42, "t0", child_subagent_id="sa-a")
    child_b = identity.make_instance_id("test-agent", "host1", 42, "t0", child_subagent_id="sa-b")
    assert parent != child_a
    assert child_a != child_b


def test_make_instance_id_returns_none_on_missing_parts():
    assert identity.make_instance_id(None, "host1", 42, "t0") is None
    assert identity.make_instance_id("test-agent", "", 42, "t0") is None
    assert identity.make_instance_id("test-agent", "host1", 42, "t0", child_subagent_id="") is None


# ---------------------------------------------------------------------------- dedup
def test_dedup_same_factual_instance_yields_same_key():
    k1 = dedup.dedup_key_for("on_session_start", {"session_id": "s1"})
    k2 = dedup.dedup_key_for("on_session_start", {"session_id": "s1"})
    assert k1 is not None
    assert k1 == k2


def test_dedup_session_end_requires_turn_id():
    assert dedup.dedup_key_for("on_session_end", {"session_id": "s1"}) is None
    assert dedup.dedup_key_for("on_session_end", {"session_id": "s1", "turn_id": "t1"}) is not None


def test_dedup_subagent_requires_child_session_id():
    assert dedup.dedup_key_for("subagent_start", {"parent_session_id": "p"}) is None
    key = dedup.dedup_key_for(
        "subagent_start",
        {"parent_session_id": "p", "parent_turn_id": "t1", "child_session_id": "c1"},
    )
    assert key is not None


def test_dedup_distinct_children_get_distinct_keys():
    common = {"parent_session_id": "p", "parent_turn_id": "t1"}
    a = dedup.dedup_key_for("subagent_stop", {**common, "child_session_id": "c1"})
    b = dedup.dedup_key_for("subagent_stop", {**common, "child_session_id": "c2"})
    assert a is not None and b is not None and a != b


def test_dedup_unknown_hook_returns_none():
    assert dedup.dedup_key_for("some_other_hook", {"session_id": "s1"}) is None


# --------------------------------------------------------------------------- redact
def test_redact_removes_sensitive_freetext():
    payload = {"session_id": "s1", "result": "x" * 4096, "user_message": "hello"}
    out = redact.apply(payload)
    assert "result" not in out
    assert "user_message" not in out
    assert out["session_id"] == "s1"
    assert out["omitted_fields"]["result"] == 4096


def test_redact_removes_reason_and_tool_call_history():
    out = redact.apply({"session_id": "s1", "reason": "why-here",
                        "tool_call_history": [{"cmd": "ls"}, {"cmd": "pwd"}]})
    assert "reason" not in out
    assert "tool_call_history" not in out
    assert out["session_id"] == "s1"
    assert out["omitted_fields"]["reason"] == len("why-here")
    assert out["omitted_fields"]["tool_call_history"] == 2


def test_redact_len_of_handles_non_sized_values():
    assert redact._len_of(12345) == 0
    assert redact._len_of("abcd") == 4
    assert redact._len_of([1, 2, 3]) == 3


def test_redact_keep_verbose_masks_secret_keywords():
    out = redact.apply({"note": "token abc123"}, keep_verbose=True)
    assert "***" in out["note"]


# ---------------------------------------------------------------------------- spool
def test_spool_insert_get_and_status(tmp_path):
    sp = Spool(str(tmp_path / "s.db"))
    try:
        assert sp.insert({"event_id": "e1", "event_type": "hermes.session.started"}) is True
        assert sp.count_by_status() == {"pending": 1}
        got = sp.get("e1")
        assert got is not None
        assert got["event_type"] == "hermes.session.started"
        assert got["status"] == "pending"
        assert sp.get("missing") is None
    finally:
        sp.close()


def test_spool_mark_done_and_error(tmp_path):
    sp = Spool(str(tmp_path / "s.db"))
    try:
        sp.insert({"event_id": "e1", "event_type": "hermes.session.started"})
        sp.insert({"event_id": "e2", "event_type": "hermes.session.ended"})
        sp.mark_done("e1")
        sp.mark_error("e2", "boom")
        assert sp.count_by_status() == {"done": 1, "error": 1}
        assert sp.get("e1")["status"] == "done"
        assert sp.get("e2")["status"] == "error"
    finally:
        sp.close()


def test_spool_dedup_same_key_not_inserted_twice(tmp_path):
    sp = Spool(str(tmp_path / "s.db"), dedup_ttl_seconds=300)
    try:
        first = sp.insert({"event_id": "e1", "event_type": "hermes.session.started"}, dedup_key="k")
        second = sp.insert({"event_id": "e2", "event_type": "hermes.session.started"}, dedup_key="k")
        assert first is True
        assert second is False
        assert sp.count_by_status() == {"pending": 1}
    finally:
        sp.close()


def test_spool_dedup_does_not_merge_different_event_types(tmp_path):
    sp = Spool(str(tmp_path / "s.db"), dedup_ttl_seconds=300)
    try:
        sp.insert({"event_id": "e1", "event_type": "hermes.session.started"}, dedup_key="k")
        # Same key but a different factual event type must not be swallowed.
        second = sp.insert({"event_id": "e2", "event_type": "hermes.session.ended"}, dedup_key="k")
        assert second is True
        assert sp.count_by_status() == {"pending": 2}
    finally:
        sp.close()


def test_spool_cleanup_retention_keeps_recent_rows(tmp_path):
    sp = Spool(str(tmp_path / "s.db"))
    try:
        sp.insert({"event_id": "e1", "event_type": "hermes.session.started"})
        sp.mark_done("e1")
        assert sp.cleanup_retention(90) == 0
        assert sp.count_by_status() == {"done": 1}
    finally:
        sp.close()


def test_spool_cleanup_retention_prunes_aged_error_row(tmp_path):
    sp = Spool(str(tmp_path / "s.db"))
    try:
        sp.insert({"event_id": "e1", "event_type": "hermes.session.started"})
        sp.mark_error("e1", "boom")
        _backdate(tmp_path / "s.db", 100)
        assert sp.cleanup_retention(90) == 1
        assert sp.get("e1") is None
    finally:
        sp.close()


# ------------------------------------------------------- spool.pending_batch (Req 3c)
def test_spool_pending_batch_includes_error_rows(tmp_path):
    sp = Spool(str(tmp_path / "s.db"))
    try:
        sp.insert({"event_id": "e1", "event_type": "hermes.session.started"})
        sp.insert({"event_id": "e2", "event_type": "hermes.session.ended"})
        sp.mark_error("e2", "delivery failed earlier")
        ids = {row["event_id"] for row in sp.pending_batch()}
        assert ids == {"e1", "e2"}  # error rows are returned for retry
    finally:
        sp.close()


def test_spool_pending_batch_excludes_done_rows(tmp_path):
    sp = Spool(str(tmp_path / "s.db"))
    try:
        sp.insert({"event_id": "e1", "event_type": "hermes.session.started"})
        sp.mark_done("e1")
        assert sp.pending_batch() == []
    finally:
        sp.close()


# ------------------------------------------------------ sender retention sweep (Req 4)
def test_sender_retention_sweep_removes_aged_rows(tmp_path):
    sp = Spool(str(tmp_path / "s.db"))
    try:
        sp.insert({"event_id": "e1", "event_type": "hermes.session.started"})
        sp.mark_done("e1")
        _backdate(tmp_path / "s.db", 100)
        sender = SenderThread(sp, BridgeSink(publish_mode="spool-only"),
                              retention_days=90, retention_sweep_seconds=3600)
        assert sender.sweep_retention() == 1
        assert sp.get("e1") is None
        assert sender.sweeps == 1
    finally:
        sp.close()


def test_sender_retention_sweep_disabled_by_config(tmp_path):
    sp = Spool(str(tmp_path / "s.db"))
    try:
        sp.insert({"event_id": "e1", "event_type": "hermes.session.started"})
        sp.mark_done("e1")
        _backdate(tmp_path / "s.db", 100)
        sender = SenderThread(sp, BridgeSink(publish_mode="spool-only"),
                              retention_days=90, retention_sweep_seconds=0)
        assert sender.sweep_retention() == 0
        assert sp.get("e1") is not None  # nothing pruned when disabled
        assert sender.sweeps == 0
    finally:
        sp.close()


def test_sender_thread_sweeps_once_on_start(tmp_path):
    sp = Spool(str(tmp_path / "s.db"))
    try:
        sp.insert({"event_id": "e1", "event_type": "hermes.session.started"})
        sp.mark_done("e1")
        _backdate(tmp_path / "s.db", 100)
        sender = SenderThread(sp, BridgeSink(publish_mode="spool-only"), poll_interval=0.02,
                              retention_days=90, retention_sweep_seconds=3600)
        sender.start()
        deadline = time.time() + 2.0
        while time.time() < deadline and sp.get("e1") is not None:
            time.sleep(0.02)
        sender.stop()
        assert sp.get("e1") is None
        assert sender.sweeps >= 1
    finally:
        sp.close()


# ----------------------------------------------------------------------- controller
def test_controller_session_start_e2e_and_fast_shutdown(tmp_path):
    ctrl = _make_controller(tmp_path)
    try:
        ctrl.on_hook("on_session_start", session_id="s1", platform="cli",
                     user_message="secret-value")
        status = wait_for_spool_done(ctrl, timeout=5.0)
        assert status.get("done") == 1
    finally:
        started = time.monotonic()
        ctrl.shutdown()
        assert time.monotonic() - started < 2.0

    events = _read_spool_events(tmp_path / "s.db")
    assert len(events) == 1
    assert events[0]["event_type"] == "hermes.session.started"
    # Redaction ran before the INSERT: the free-text prompt never reached the DB.
    assert "secret-value" not in json.dumps(events, ensure_ascii=True)


def test_controller_does_not_store_reason_or_tool_call_history(tmp_path):
    ctrl = _make_controller(tmp_path)
    try:
        ctrl.on_hook("on_session_start", session_id="s1", reason="SECRET-REASON",
                     tool_call_history=["SECRET-TOOL-1", "SECRET-TOOL-2"])
        wait_for_spool_done(ctrl, timeout=5.0)
    finally:
        ctrl.shutdown()

    blob = json.dumps(_read_spool_events(tmp_path / "s.db"), ensure_ascii=True)
    # The values never reach the DB...
    assert "SECRET-REASON" not in blob
    assert "SECRET-TOOL" not in blob
    # ...only their length is kept as a non-sensitive signal under omitted_fields.
    events = _read_spool_events(tmp_path / "s.db")
    omitted = events[0]["payload"]["omitted_fields"]
    assert omitted["reason"] == len("SECRET-REASON")
    assert omitted["tool_call_history"] == 2


def test_controller_links_subagent_start_and_stop(tmp_path):
    ctrl = _make_controller(tmp_path)
    try:
        ctrl.on_hook("subagent_start", child_session_id="c1",
                     child_subagent_id="sa-0", parent_session_id="p")
        ctrl.on_hook("subagent_stop", child_session_id="c1",
                     parent_session_id="p", parent_turn_id="t1")
        wait_for_spool_done(ctrl, timeout=5.0)
    finally:
        ctrl.shutdown()

    events = _read_spool_events(tmp_path / "s.db")
    started = [e for e in events if e["event_type"] == "hermes.agent.started"]
    stopped = [e for e in events if e["event_type"] == "hermes.agent.stopped"]
    assert len(started) == 1
    assert len(stopped) == 1
    # The stop is linked to the start through the shared child instance_id.
    assert started[0]["producer"]["instance_id"] == stopped[0]["producer"]["instance_id"]


def test_controller_subagent_stop_without_start_emits_nothing(tmp_path):
    ctrl = _make_controller(tmp_path)
    try:
        ctrl.on_hook("subagent_stop", child_session_id="ghost", parent_session_id="p")
        wait_for_spool_done(ctrl, timeout=5.0)
        assert ctrl.status()["spool"] == {}
        # The status payload also reports transport/publish wiring.
        assert ctrl.status()["publish_mode"] == "spool-only"
        assert ctrl.status()["transport_active"] is False
    finally:
        ctrl.shutdown()

    assert _read_spool_events(tmp_path / "s.db") == []


# ------------------------------------------------------------------- plugin contract
def test_plugin_manifest_hooks_are_subset_of_hermes_valid_hooks():
    if not VALID_HOOKS_SOURCE.exists():
        pytest.skip(f"Hermes plugin source not available: {VALID_HOOKS_SOURCE}")
    text = VALID_HOOKS_SOURCE.read_text(encoding="utf-8")
    match = re.search(r"VALID_HOOKS\s*:\s*set\[str\]\s*=\s*\{", text)
    assert match, "VALID_HOOKS assignment not found in Hermes plugins.py"
    i = match.end()
    depth = 1
    while depth and i < len(text):
        ch = text[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
        i += 1
    block = text[match.end():i - 1]
    valid_hooks = set(re.findall(r'"([A-Za-z_][A-Za-z0-9_]*)"', block))
    missing = EXPECTED_HOOKS - valid_hooks
    assert not missing, f"plugin declares hooks not in Hermes VALID_HOOKS: {sorted(missing)}"


def test_plugin_yaml_declares_expected_hooks():
    if not PLUGIN_YAML.exists():
        pytest.skip("plugin.yaml not found")
    text = PLUGIN_YAML.read_text(encoding="utf-8")
    try:
        import yaml
    except ImportError:
        yaml = None
    if yaml is not None:
        meta = yaml.safe_load(text)
        assert meta["name"] == "hermes-continuity-capture"
        assert meta["version"] == "0.2.0"
        assert set(meta["provides_hooks"]) == EXPECTED_HOOKS
        # nats_password must be declared as a secret and never as a plain value.
        assert meta["config_schema"]["nats_password"]["type"] == "secret"
        assert meta["config_schema"]["publish_mode"]["default"] == "spool-only"
    else:
        # PyYAML unavailable -> minimal text assertion.
        assert "name: hermes-continuity-capture" in text
        for hook in EXPECTED_HOOKS:
            assert hook in text
