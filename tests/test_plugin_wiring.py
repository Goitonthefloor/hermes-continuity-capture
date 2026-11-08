"""Wiring tests for the plugin entry point (``__init__.py``).

These load the plugin directory exactly the way Hermes loads a directory plugin
(namespace package + ``register(ctx)``) and drive it through a fake ``PluginContext``
that mirrors the real one: ``get_config``, ``state.data_dir``, ``profile_name``,
``register_hook`` / ``register_command`` / ``on_unload``.

No network is used and every path lives under ``tmp_path``.
"""

from __future__ import annotations

import importlib.util as ilu
import pathlib
import sqlite3
import sys
import time
import types

import pytest

PLUGIN_DIR = pathlib.Path(__file__).resolve().parent.parent
_MODULE_NAME = "hermes_continuity_capture_wiring"

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


def _purge():
    for key in [k for k in sys.modules if k == _MODULE_NAME or k.startswith(_MODULE_NAME + ".")]:
        del sys.modules[key]


@pytest.fixture
def plugin():
    """Load a fresh copy of the plugin package, torn down after the test."""
    _purge()
    spec = ilu.spec_from_file_location(
        _MODULE_NAME, PLUGIN_DIR / "__init__.py",
        submodule_search_locations=[str(PLUGIN_DIR)])
    module = ilu.module_from_spec(spec)
    sys.modules[_MODULE_NAME] = module
    spec.loader.exec_module(module)
    try:
        yield module
    finally:
        try:
            module._shutdown()
        except Exception:
            pass
        _purge()


class FakeCtx:
    """Minimal stand-in for the real :class:`PluginContext`."""

    def __init__(self, settings=None, *, data_dir=None, profile_name="default"):
        self._settings = dict(settings or {})
        self.profile_name = profile_name
        self.hooks = {}
        self.commands = {}
        self.unloaded = []
        self._data_dir = pathlib.Path(data_dir) if data_dir is not None else None

    @property
    def state(self):
        if self._data_dir is None:
            raise AttributeError("state")
        return types.SimpleNamespace(data_dir=self._data_dir)

    def get_config(self, key, default=None):
        return self._settings.get(key, default)

    def register_hook(self, name, fn):
        self.hooks[name] = fn

    def register_command(self, name, handler=None, description="", args_hint="", **kwargs):
        self.commands[name] = handler

    def on_unload(self, callback):
        self.unloaded.append(callback)


def _row_count(db_path: pathlib.Path) -> int:
    if not db_path.exists():
        return 0
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            return conn.execute("SELECT COUNT(*) FROM spool").fetchone()[0]
        finally:
            conn.close()
    except sqlite3.Error:
        return 0


def _wait_for(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


# --------------------------------------------------------------------------- Req 5
def test_register_binds_hooks_command_and_unload_without_side_effects(plugin, tmp_path):
    ctx = FakeCtx({"spool_path": str(tmp_path / "s.db"), "publish_mode": "spool-only"},
                  data_dir=tmp_path / "data")
    plugin.register(ctx)
    assert set(ctx.hooks) == EXPECTED_HOOKS
    assert "continuity-capture" in ctx.commands
    assert ctx.unloaded, "on_unload cleanup must be registered"
    # register() must be side-effect free: no spool/data dir is created before a hook fires.
    assert not (tmp_path / "s.db").exists()
    assert not (tmp_path / "data").exists()


def test_command_status_and_flush_raw_args(plugin, tmp_path):
    ctx = FakeCtx({}, data_dir=tmp_path / "data")
    plugin.register(ctx)
    handler = ctx.commands["continuity-capture"]
    assert "status" in handler().lower()              # default action
    assert "status" in handler("status").lower()
    assert "flush" in handler("  FLUSH ").lower()      # trimmed + case-insensitive
    assert "unknown subcommand" in handler("bogus").lower()


# --------------------------------------------------------------- Req 1 + Req 2
def test_hook_uses_configured_spool_path(plugin, tmp_path):
    spool = tmp_path / "custom" / "s.db"
    ctx = FakeCtx({"spool_path": str(spool), "publish_mode": "spool-only"},
                  data_dir=tmp_path / "data")
    plugin.register(ctx)
    ctx.hooks["on_session_start"](session_id="s1")
    assert _wait_for(lambda: _row_count(spool) == 1)
    # The (unconfigured) default location under data_dir is never touched.
    assert not (tmp_path / "data" / "spool.db").exists()


def test_default_spool_is_under_data_dir(plugin, tmp_path):
    data_dir = tmp_path / "plugin-data"
    ctx = FakeCtx({"publish_mode": "spool-only"}, data_dir=data_dir)
    plugin.register(ctx)
    ctx.hooks["on_session_start"](session_id="s1")
    assert _wait_for(lambda: _row_count(data_dir / "spool.db") == 1)


def test_relative_spool_escape_fails_open(plugin, tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    ctx = FakeCtx({"spool_path": "../evil.db", "publish_mode": "spool-only"}, data_dir=data_dir)
    plugin.register(ctx)
    ctx.hooks["on_session_start"](session_id="s1")  # must not raise
    time.sleep(0.1)
    assert not (tmp_path / "evil.db").exists()


def test_default_publish_mode_is_spool_only_and_no_transport(plugin, tmp_path):
    ctx = FakeCtx({}, data_dir=tmp_path / "data")
    plugin.register(ctx)
    out = ctx.commands["continuity-capture"]("status")
    assert "publish_mode=spool-only" in out
    assert "transport_active=False" in out


def test_jetstream_mode_wires_a_transport(plugin, tmp_path):
    ctx = FakeCtx({"publish_mode": "jetstream", "nats_url": "nats://127.0.0.1:1"},
                  data_dir=tmp_path / "data")
    plugin.register(ctx)
    out = ctx.commands["continuity-capture"]("status")
    assert "transport_active=True" in out


# --------------------------------------------------------------------------- Req 6
def test_agent_id_comes_from_profile_name(plugin, tmp_path):
    ctx = FakeCtx({}, data_dir=tmp_path / "data", profile_name="alice")
    plugin.register(ctx)
    assert "agent_id=alice" in ctx.commands["continuity-capture"]("status")


def test_explicit_agent_id_overrides_profile(plugin, tmp_path):
    ctx = FakeCtx({"agent_id": "svc"}, data_dir=tmp_path / "data", profile_name="alice")
    plugin.register(ctx)
    assert "agent_id=svc" in ctx.commands["continuity-capture"]("status")


def test_agent_id_falls_back_to_default_when_profile_empty(plugin, tmp_path):
    ctx = FakeCtx({}, data_dir=tmp_path / "data", profile_name="")
    plugin.register(ctx)
    assert "agent_id=default" in ctx.commands["continuity-capture"]("status")
