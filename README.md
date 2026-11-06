# hermes-continuity-capture

A Hermes general plugin that records Hermes lifecycle events as Continuity schema-v2
events in a local SQLite spool, and hands validated events to a Continuity bridge you
run yourself. It is inactive until an operator enables it.

## What it does

The plugin subscribes to a fixed set of Hermes hooks and turns each hook call into a
schema-v2 continuity event:

- `on_session_start` / `on_session_end` / `on_session_finalize` -> `hermes.session.started` / `hermes.session.ended`
- `subagent_start` / `subagent_stop` -> `hermes.agent.started` / `hermes.agent.stopped`
- `kanban_task_claimed` / `kanban_task_completed` / `kanban_task_blocked` -> `hermes.task.claimed` / `hermes.task.completed` / `hermes.task.blocked`
- `post_tool_call` -> subscribed, but intentionally not mapped to its own event type; it
  currently produces no spool entry (no isolated tool events by design).

Each event is redacted and validated, written to a local SQLite spool file, and then a
background thread drains the spool to the bridge. Nothing is published to a network
unless a bridge is wired in and a non-local `publish_mode` is configured.

## Requirements

- Python 3.10 or newer (standard library only; no third-party runtime dependencies).
- A running Continuity bridge: NATS with JetStream (or the core NATS path) reachable
  from the host. The bridge and its NATS infrastructure are **not** part of this plugin
  and must be deployed separately.
- Hermes >= 0.21.5, which is the version that exposes the `kanban_task_*` hook surface
  (`kanban_task_claimed` / `kanban_task_completed` / `kanban_task_blocked`) this plugin binds to.

## Install

Copy this repository root into the plugin directory, then enable it explicitly. The
plugin does nothing on its own.

```bash
cp -r . ~/.hermes/plugins/hermes-continuity-capture
```

Then, as the operator:

```bash
hermes config set plugins.enabled '["hermes-continuity-capture", ...]'
sudo systemctl restart hermes-gateway.service
```

`install-to-hermes.sh` performs the copy step only. It never activates the plugin.

## Configuration

Settings are read from the plugin's settings dict and parsed into `CaptureConfig`
(`continuity_capture/config.py`). Unknown keys are ignored. The relevant fields:

| Field | Default | Meaning |
| --- | --- | --- |
| `spool_path` | `data/spool.db` | Path to the local SQLite spool file. Created if missing. |
| `publish_mode` | `jetstream` | `jetstream`, `core`, or `spool-only`. Must be one of these, otherwise loading fails with `ValueError`. |
| `spool_cap` | `200000` | Maximum number of pending/accepted rows before the oldest are moved to `given_up` (backpressure). |
| `retention_days` | `90` | Age after which accepted/done/given-up rows are pruned by `cleanup_retention`. |
| `record_full_prompts` | `false` | Opt-in only. Leave `false` to keep free-text prompts and tool results out of the spool. |
| `exclude_tools` | `{}` | Tool names for which `post_tool_call` metadata is skipped. |
| `dedup_ttl_seconds` | `300` | Window in which a repeated roll-specific event is treated as a duplicate. |

`publish_mode = "spool-only"` keeps everything local: events are written to the SQLite
file and marked done without any network delivery. This is the safe mode for testing.

## Event types

The set of event types is a closed registry (`continuity_capture/registry.py`,
`RESERVED_REGISTERED_TYPES`). An event type must be listed there to be produced; the
registry is never grown at runtime from arbitrary hook names. A type that is not in the
list is rejected before it reaches the spool.

## Privacy and safety guarantees

- Redaction happens before the SQLite INSERT (`continuity_capture/redact.py`). Free-text
  fields (`user_message`, `result`, `args`, `summary`, and similar) are dropped by default;
  only their length is kept as a non-sensitive signal.
- Full prompts and tool results are never stored unless `record_full_prompts` is explicitly enabled.
- Runtime identity is a deterministic UUIDv5 derived from agent, host, process, and start
  time; IDs are never invented. If the required inputs are missing, no ID is produced and
  the event is marked incomplete instead.
- Deduplication is role-specific. A repeated event is only collapsed when it is provably
  the same factual instance; missing or ambiguous keys disable deduplication. Parallel
  subagents are never merged into one.
- Capture failures are isolated: a failing hook never blocks the agent run.
- There is no silent fallback to the non-persistent core path when JetStream delivery
  fails. An event is only marked done after confirmed delivery.
- The plugin is not active by default. Enabling it is an explicit operator action.

## Tests

```bash
python3 -m pytest tests -q
```

The suite uses only the standard library and pytest, performs no network access, and
writes every SQLite spool to a temporary directory (it never creates `data/spool.db` in
the repository).

## License

MIT. See [LICENSE](LICENSE).
