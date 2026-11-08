# hermes-continuity-capture

A Hermes general plugin that records Hermes lifecycle events as Continuity schema-v2
events in a local SQLite spool, and can hand the validated events to a NATS/JetStream
server you run yourself. It is inactive until an operator enables it, and it ships no
credentials.

## What it does

The plugin subscribes to a fixed set of Hermes hooks and turns each hook call into a
schema-v2 continuity event:

- `on_session_start` / `on_session_end` / `on_session_finalize` -> `hermes.session.started` / `hermes.session.ended`
- `subagent_start` / `subagent_stop` -> `hermes.agent.started` / `hermes.agent.stopped`
- `kanban_task_claimed` / `kanban_task_completed` / `kanban_task_blocked` -> `hermes.task.claimed` / `hermes.task.completed` / `hermes.task.blocked`
- `post_tool_call` -> subscribed, but intentionally not mapped to its own event type; it
  currently produces no spool entry (no isolated tool events by design).

Each event is redacted and validated, written to a SQLite spool file, and then a
background sender thread drains the spool. The default `publish_mode` is `spool-only`:
events stay in the local database and nothing is sent anywhere. Set `publish_mode` to
`core` or `jetstream` to deliver events to an external NATS server.

## Requirements

- Python 3.10 or newer (standard library only; no third-party runtime dependencies).
- **Only for `publish_mode: core` or `jetstream`:** a reachable NATS server, with
  JetStream enabled for `jetstream` mode. The NATS server is **not** part of this plugin
  and must be deployed separately. In the default `spool-only` mode nothing external is
  needed.
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

Settings are read per key through `ctx.get_config("<key>")` (i.e.
`plugins.entries.hermes-continuity-capture.settings.<key>`), validated into
`CaptureConfig` (`continuity_capture/config.py`). Unset keys keep their default; an
invalid `publish_mode` fails loading with a `ValueError`.

| Field | Default | Meaning |
| --- | --- | --- |
| `agent_id` | active Hermes profile | Logical agent identity used in the event identity (`ctx.profile_name`). |
| `host_identity` | runtime hostname | Host identity; defaults to the runtime FQDN/hostname. |
| `spool_path` | `spool.db` in the plugin data dir | SQLite spool file. A relative path is resolved against the plugin data directory and must stay inside it; an escaping relative path is rejected. |
| `publish_mode` | `spool-only` | `spool-only`, `core`, or `jetstream`. Must be one of these. |
| `spool_cap` | `200000` | Maximum pending/accepted rows before the oldest are moved to `given_up` (backpressure). |
| `retention_days` | `90` | Age after which terminal spool rows (including `error` rows) are pruned. |
| `retention_sweep_seconds` | `3600` | Interval of the periodic retention sweep; `<= 0` disables it. The sweep also runs once at startup. |
| `send_retry_attempts` | `5` | Delivery attempts before a row is given up. `error` rows are retried too. |
| `record_full_prompts` | `false` | Opt-in only. Leave `false` to keep free-text prompts and tool results out of the spool. |
| `exclude_tools` | `{}` | Tool names for which `post_tool_call` metadata is skipped. |
| `exclude_event_types` | `{}` | Event types excluded from capture. |
| `dedup_ttl_seconds` | `300` | Window in which a repeated role-specific event is treated as a duplicate. |
| `bridge_subject_events` | `hermes.continuity.events` | NATS subject for main events. |
| `bridge_subject_quality` | `hermes.continuity.quality` | NATS subject for quality events. |
| `nats_url` | `nats://127.0.0.1:4222` | URL of the external NATS server (used by `core`/`jetstream` only). |
| `nats_user` | – | NATS user name (optional). |
| `nats_password` | – | NATS password. Declared as a `secret` in `plugin.yaml`, so it is resolved through Hermes' secret route (the plugin never reads `.env` files itself) and never logged or echoed in errors. |
| `nats_tls` | `false` | Require TLS for the NATS connection. |
| `jetstream_stream_name` | `hermes_continuity` | JetStream stream to publish into (provisioned idempotently). |
| `jetstream_duplicate_window_s` | `120` | Server-side dedup window; each event carries `Nats-Msg-Id = <event_id>`. |

The spool lives under the plugin's own data directory (`ctx.state.data_dir`), not in the
process working directory.

## Commands

```
/continuity-capture status   # spool counters, publish mode, transport state, last error
/continuity-capture flush    # wake the sender to retry pending and error rows now
```

## Event types

The set of event types is a closed registry (`continuity_capture/registry.py`,
`RESERVED_REGISTERED_TYPES`). An event type must be listed there to be produced; the
registry is never grown at runtime from arbitrary hook names. A type that is not in the
list is rejected before it reaches the spool.

## Privacy and safety guarantees

- Redaction happens before the SQLite INSERT (`continuity_capture/redact.py`). The
  free-text fields `user_message`, `result`, `args`, `summary`, `reason`, and
  `tool_call_history` are dropped by default; only their length is kept as a
  non-sensitive signal in `omitted_fields`.
- Full prompts and tool results are never stored unless `record_full_prompts` is explicitly enabled.
- Runtime identity is a deterministic UUIDv5 derived from agent, host, process, and start
  time; IDs are never invented. If the required inputs are missing, no ID is produced and
  the event is marked incomplete instead.
- Deduplication is role-specific. A repeated event is only collapsed when it is provably
  the same factual instance; missing or ambiguous keys disable deduplication. Parallel
  subagents are never merged into one.
- Capture failures are isolated: a failing hook never blocks the agent run.
- There is no silent fallback to the non-persistent core path when JetStream delivery
  fails. An event is only marked done after confirmed delivery; a failed delivery is
  recorded as an `error` row and retried.
- The plugin is not active by default. Enabling it is an explicit operator action.

## Tests

```bash
python3 -m pytest tests -q
```

The suite uses only the standard library and pytest, performs no network access (the NATS
transport is exercised against a local socket dummy), and writes every SQLite spool to a
temporary directory (it never creates `data/spool.db` in the repository).

## License

MIT. See [LICENSE](LICENSE).
