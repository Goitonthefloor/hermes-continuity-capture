#!/usr/bin/env python3
"""verify_plugin.py - verify the standalone plugin folder without Hermes.

Loads the plugin folder the way Hermes loads directory plugins: the folder becomes a
namespace package registered in ``sys.modules`` under a Python-safe module name, so
``register(ctx)`` and the relative subpackage imports resolve. Then it runs a
spool-only capture round trip and the plugin command. Nothing is written into this
folder: the spool goes to the fake plugin data directory.
"""
import os
import sys
import tempfile
import time
from pathlib import Path
import importlib.util as _ilu

# Directory of this file == plugin root (no hardcoded paths).
PLUGIN = os.path.dirname(os.path.abspath(__file__))

# --- 1) files present ---------------------------------------------------------
for f in ("plugin.yaml", "__init__.py", "continuity_capture", "README.md", "install-to-hermes.sh"):
    assert os.path.exists(os.path.join(PLUGIN, f)), f"missing: {f}"
print("[PASS] plugin layout present (plugin.yaml, __init__.py, continuity_capture/, README, install script)")

# --- 2) plugin.yaml parses ----------------------------------------------------
try:
    import yaml
    meta = yaml.safe_load(open(os.path.join(PLUGIN, "plugin.yaml")))
    print(f"[PASS] plugin.yaml: name={meta['name']} version={meta['version']} hooks={len(meta['provides_hooks'])}")
except ImportError:
    raw = open(os.path.join(PLUGIN, "plugin.yaml")).read()
    assert "name:" in raw and "provides_hooks:" in raw
    print("[PASS] plugin.yaml present (core keys found; PyYAML not installed)")

# --- 3) load the plugin the way Hermes does -----------------------------------
module_name = "hermes_continuity_capture_plugin"   # Hermes maps '-' to '_'
sys.path.insert(0, PLUGIN)
parent_spec = _ilu.spec_from_file_location(
    module_name, os.path.join(PLUGIN, "__init__.py"),
    submodule_search_locations=[PLUGIN])
parent = _ilu.module_from_spec(parent_spec)
sys.modules[module_name] = parent
parent_spec.loader.exec_module(parent)
print(f"[PASS] plugin loaded as module {module_name!r} (version {parent.__version__})")


# --- 4) register(ctx) binds every hook ---------------------------------------
class FakeState:
    def __init__(self, data_dir: str) -> None:
        self.data_dir = Path(data_dir)


class FakeCtx:
    """Stand-in for the Hermes PluginContext surface this plugin uses."""

    def __init__(self, data_dir: str) -> None:
        self.hooks = {}
        self.commands = {}
        self.state = FakeState(data_dir)
        self.profile_name = "verify"
        self._settings = {"publish_mode": "spool-only"}

    def get_config(self, key, default=None):
        return self._settings.get(key, default)

    def register_hook(self, name, fn):
        self.hooks[name] = fn

    def register_command(self, name, handler=None, description="", args_hint="", **kw):
        self.commands[name] = (handler, description)


data_dir = tempfile.mkdtemp()
ctx = FakeCtx(data_dir)
parent.register(ctx)
expected = {"on_session_start", "on_session_end", "on_session_finalize", "post_tool_call",
            "subagent_start", "subagent_stop", "kanban_task_claimed", "kanban_task_completed",
            "kanban_task_blocked"}
missing = expected - set(ctx.hooks)
assert not missing, f"hooks not bound: {missing}"
print(f"[PASS] register bound {len(expected)} hooks; register_command: {list(ctx.commands)}")

# --- 5) hook call -> capture event (isolated, spool-only, no NATS) ------------
ctx.hooks["on_session_start"](session_id="s1", platform="cli", user_message="secret-value")
ctx.hooks["subagent_start"](child_session_id="c1", child_subagent_id="sa-0-a", parent_session_id="p")
time.sleep(0.3)

for name, (handler, _desc) in ctx.commands.items():
    status_text = handler("status")
    assert isinstance(status_text, str) and "publish_mode" in status_text, status_text
    flush_text = handler("flush")
    assert isinstance(flush_text, str) and flush_text.strip(), flush_text
    print(f"[PASS] /{name} status -> {status_text}")
    print(f"[PASS] /{name} flush  -> {flush_text}")
    break

spools = sorted(Path(data_dir).glob("*.db"))
assert spools, "no spool file was created in the plugin data directory"
print(f"[PASS] spool written outside the plugin folder: {spools[0].name}")
assert not os.path.exists(os.path.join(PLUGIN, "data")), "the plugin folder must stay clean"

print()
print("=== VERIFICATION OK: standalone plugin works (isolated, not enabled) ===")
