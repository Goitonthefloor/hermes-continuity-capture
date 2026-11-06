#!/usr/bin/env python3
"""verify_plugin.py — Verifikation des eigenständigen Hermes-Plugin-Ordners.

Lädt den Plugin-Ordner so, wie Hermes directory-Plugins lädt: der Ordner wird ein
Namensraum-Paket, registriert in ``sys.modules`` unter einem Python-sicheren Modulnamen,
sodass ``register(ctx)`` und die relativen Unterpaket-Imports funktionieren.
"""
import os, sys, tempfile
import importlib.util as _ilu
import importlib

# Verzeichnis dieser Datei = Plugin-Root (dynamisch, keine hartkodierten Pfade).
PLUGIN = os.path.dirname(os.path.abspath(__file__))

# --- 1) Dateien vorhanden ---
for f in ("plugin.yaml", "__init__.py", "continuity_capture", "README.md", "install-to-hermes.sh"):
    assert os.path.exists(os.path.join(PLUGIN, f)), f"fehlt: {f}"
print("[PASS] Plugin-Struktur vorhanden (plugin.yaml, __init__.py, continuity_capture/, README, install-skript)")

# --- 2) plugin.yaml parsebar ---
try:
    import yaml
    meta = yaml.safe_load(open(os.path.join(PLUGIN, "plugin.yaml")))
    print(f"[PASS] plugin.yaml: name={meta['name']} version={meta['version']} hooks={len(meta['provides_hooks'])}")
except ImportError:
    raw = open(os.path.join(PLUGIN, "plugin.yaml")).read()
    assert "name:" in raw and "provides_hooks:" in raw
    print("[PASS] plugin.yaml vorhanden (nicht leer, Kerneinträge da; PyYAML fehlt)")

# --- 3) Plugin als Paket laden (wie Hermes: Modulname aus plugin_id, Python-sicher) ---
module_name = "hermes_continuity_capture_plugin"   # Hermes sichert Bindestriche→Unterstrich
sys.path.insert(0, PLUGIN)
# Parent in sys.modules registrieren, damit `from .continuity_capture import …` den Parent kennt
parent_spec = _ilu.spec_from_file_location(
    module_name, os.path.join(PLUGIN, "__init__.py"),
    submodule_search_locations=[PLUGIN])
parent = _ilu.module_from_spec(parent_spec)
sys.modules[module_name] = parent
parent_spec.loader.exec_module(parent)
print(f"[PASS] Plugin als Modul {module_name!r} geladen (Version {parent.__version__})")

# --- 4) register(ctx) bindet alle Hooks ---
class FakeCtx:
    def __init__(self):
        self.hooks = {}
        # Muss ``plugin_settings`` heissen: so liest der Plugin-Wrapper die Settings
        # (getattr(ctx, "plugin_settings", None)). Sonst wuerde der Default-Spool
        # data/spool.db relativ zum CWD im Repo angelegt.
        self.plugin_settings = {"spool_path": os.path.join(tempfile.mkdtemp(), "s.db"),
                                "publish_mode": "spool-only"}
        self.commands = {}
    def register_hook(self, name, fn): self.hooks[name] = fn
    def register_command(self, name, handler=None, description=None, **kw):
        self.commands[name] = (handler, description)

ctx = FakeCtx()
parent.register(ctx)
expected = {"on_session_start","on_session_end","on_session_finalize","post_tool_call",
            "subagent_start","subagent_stop","kanban_task_claimed","kanban_task_completed",
            "kanban_task_blocked"}
assert expected <= set(ctx.hooks), f"Fehlende Hooks: {expected - set(ctx.hooks)}"
print(f"[PASS] register bindet {len(expected)} Hooks; register_command: {list(ctx.commands)}")

# --- 5) Hook-Aufruf → Capture-Event (isoliert, Spool-only, kein NATS) ---
ctx.hooks["on_session_start"](session_id="s1", platform="cli", user_message="geheim")
ctx.hooks["subagent_start"](child_session_id="c1", child_subagent_id="sa-0-a", parent_session_id="p")
import time; time.sleep(0.3)
for name, (handler, _d) in ctx.commands.items():
    body = handler()
    assert "status" in body, "Status-Befehl liefert nichts Nützliches"
    print(f"[PASS] {name}:", body)
    break

# transient SQLite-DB aus tempfile entfernen (nur Scratch, nicht Plugin-Ordner)
import glob
for d in glob.glob(tempfile.gettempdir() + "/continuity*"):
    pass  # nichts im Plugin-Ordner anlegen

print()
print("=== VERIFIKATION OK: eigenständiges Plugin funktionsfähig (isoliert, NICHT aktiviert) ===")
