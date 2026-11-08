"""
continuity_capture.spool — Persistenter lokaler SQLite-Spool (Phase 3 / M-2 §5).

- Transaktionssichere Speicherung (WAL, kurze Transaktionen, PK = event_id).
- Status-Modell: accepted -> pending -> done | error | given_up | incomplete.
- ``received_at`` wird beim INSERT gesetzt (B-09).
- Deduplication (B-07/P-2): rollenspezifischer Schlüssel; nur bei sicher identifizierter,
  dieselbe fachliche Instanz darstellender Wiederholung. Fehlende/mehrdeutige Schlüssel ->
  keine Deduplizierung.
- Crash ≈ atomar (jeder Nutzen von threading.Lock + SQLite-Transaktion).
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional

from .timeutil import iso_now

log = logging.getLogger("continuity.capture.spool")

# Statuswerte
S_ACCEPTED = "accepted"
S_PENDING = "pending"
S_DONE = "done"
S_ERROR = "error"
S_GIVEN_UP = "given_up"
S_INCOMPLETE = "incomplete"
_VALID_STATUS = {S_ACCEPTED, S_PENDING, S_DONE, S_ERROR, S_GIVEN_UP, S_INCOMPLETE}


class Spool:
    """SQLite-Spool; nicht thread-safe extern — intern mit Lock abgesichert."""

    def __init__(self, path: str, *, spool_cap: int = 200_000, dedup_ttl_seconds: int = 300):
        self._path = path
        self._lock = threading.RLock()
        self._spool_cap = max(int(spool_cap), 1)
        self._dedup_ttl = max(int(dedup_ttl_seconds), 1)
        if path != ":memory:":
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS spool (
                    event_id TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,       -- observed_at (Capture-Erfassung)
                    received_at TEXT NOT NULL,      -- INSERT-Zeit (B-09)
                    received_ts REAL NOT NULL,      -- ISO -> numerisch, fuer TTL-Vergleich
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    dedup_key TEXT,                 -- rollenspezifischer Schlüssel (B-07/P-2)
                    incomplete INTEGER NOT NULL DEFAULT 0
                )
            """)
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_spool_status ON spool(status)")
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_spool_dedup ON spool(dedup_key)")

    # ------------------------------------------------------------- public
    def insert(self, event: Dict[str, Any], *, dedup_key: Optional[str] = None) -> bool:
        """Transaktionssicher INSERT. Gibt True bei neu eingefuegtem Event zurueck.

        Dedup: wenn ``dedup_key`` gesetzt und ein Eintrag mit demselben Schlüssel in
        pending/accepted/error (innerhalb TTL) existiert und dieselbe fachliche Instanz
        ist, wird NICHTS eingefuegt (wartet ggf. auf den in-flight Eintrag) — sonst
        wird neu eingefuegt. Fehlt der Schlüssel oder ist er mehrdeutig, wird nicht
        dedupliziert.
        """
        eid = event.get("event_id")
        if not isinstance(eid, str) or not eid:
            return False
        et = event.get("event_type")
        payload = json.dumps(event, sort_keys=True, ensure_ascii=True)
        now_text = iso_now()
        now_ts = time.time()
        with self._lock:
            # PK-Dedupe auf identischer event_id (echter Fehler beim Re-Insert): doppelt
            if self._exists(eid):
                return False
            # Rollenspezifische Dedup-Semantik (B-07/P-2):
            if dedup_key:
                cutoff = time.time() - self._dedup_ttl
                dup = self._find_by_dedup_key(dedup_key, cutoff)
                if dup is not None:
                    # Nur wenn wirklich dieselbe fachliche Instanz (gleicher event_type)
                    # => nicht erneut einfuegen. Andernfalls (fachlich verschieden,
                    # z. B. 2. paralleler Subagent mit gleichem parent-turn) NICHT dedupen.
                    if dup["event_type"] == et:
                        return False
            with self._conn:
                self._conn.execute(
                    "INSERT OR IGNORE INTO spool "
                    "(event_id,event_type,payload_json,status,created_at,received_at,received_ts,dedup_key,incomplete) "
                    "VALUES (?,?,?,?,?,?,?,?,0)",
                    (eid, et, payload, S_ACCEPTED,
                     event.get("observed_at") or now_text, now_text, now_ts, dedup_key))
                self._conn.execute(
                    "UPDATE spool SET status=? WHERE event_id=? AND status!=?",
                    (S_PENDING, eid, S_DONE))
            self._groom_locked()
            return True

    def mark_incomplete(self, event_id: str, reason: str) -> None:
        with self._lock, self._conn:
            self._conn.execute("UPDATE spool SET incomplete=1, last_error=? , status=? "
                               "WHERE event_id=?", (reason, S_INCOMPLETE, event_id))

    def pending_batch(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Rows to deliver: everything still owed to the bridge.

        Includes ``pending`` AND ``error`` rows so a failed delivery is retried
        (the reader/consumer side stays idempotent via the stable event_id).
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT event_id,event_type,payload_json,status,attempts,last_error,dedup_key "
                "FROM spool WHERE status IN (?,?) ORDER BY received_at LIMIT ?",
                (S_PENDING, S_ERROR, max(int(limit), 1))).fetchall()
        return [
            {"event_id": r[0], "event_type": r[1], "payload": json.loads(r[2]),
             "status": r[3], "attempts": r[4], "last_error": r[5], "dedup_key": r[6]}
            for r in rows
        ]

    def mark_done(self, event_id: str) -> None:
        with self._lock, self._conn:
            self._conn.execute("UPDATE spool SET status=? WHERE event_id=?",
                               (S_DONE, event_id))

    def mark_error(self, event_id: str, error: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE spool SET status=?, last_error=?, attempts=attempts+1 WHERE event_id=?",
                (S_ERROR, error[:2000], event_id))

    def mark_given_up(self, event_id: str, error: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE spool SET status=?, last_error=?, attempts=attempts+1 WHERE event_id=?",
                (S_GIVEN_UP, error[:2000], event_id))

    def count_by_status(self) -> Dict[str, int]:
        with self._lock:
            rows = self._conn.execute("SELECT status, COUNT(*) FROM spool GROUP BY status").fetchall()
        return {r[0]: r[1] for r in rows}

    def get(self, event_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT event_id,event_type,payload_json,status,created_at,received_at "
                "FROM spool WHERE event_id=?", (event_id,)).fetchone()
        if not row:
            return None
        payload = json.loads(row[2])
        return {"event_id": row[0], "event_type": row[1], "payload": payload,
                "status": row[3], "created_at": row[4], "received_at": row[5]}

    def cleanup_retention(self, retention_days: int) -> int:
        """Delete terminal rows older than ``retention_days``.

        ``done``/``given_up``/``accepted``/``error``/``incomplete`` age out; only
        ``pending`` (still owed to the bridge) is kept. Uses the numeric
        ``received_ts`` column (portable, unlike ``strftime`` on the ISO text).
        """
        cutoff = time.time() - max(int(retention_days), 1) * 86400
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM spool WHERE status IN (?,?,?,?,?) AND received_ts < ?",
                (S_DONE, S_GIVEN_UP, S_ACCEPTED, S_ERROR, S_INCOMPLETE, cutoff))
            return cur.rowcount

    def _groom_locked(self) -> None:
        """Backpressure-Cap: wenn pending/accepted zu viele, aelteste als given_up (Order)."""
        n = self._conn.execute(
            "SELECT COUNT(*) FROM spool WHERE status IN (?,?)",
            (S_PENDING, S_ACCEPTED)).fetchone()[0]
        if n <= self._spool_cap:
            return
        excess = self._conn.execute(
            "SELECT event_id FROM spool WHERE status IN (?,?) ORDER BY received_at "
            "LIMIT ?", (S_PENDING, S_ACCEPTED, n - self._spool_cap)).fetchall()
        with self._conn:
            for (eid,) in excess:
                self._conn.execute(
                    "UPDATE spool SET status=?, last_error='backpressure-cap-drop' "
                    "WHERE event_id=?", (S_GIVEN_UP, eid))
        log.warning("Spool-Cap erreicht: %d Eintraege nach given_up verschoben", len(excess))

    # ------------------------------------------------------------- isdustry-intern
    def _exists(self, eid: str) -> bool:
        row = self._conn.execute("SELECT 1 FROM spool WHERE event_id=?", (eid,)).fetchone()
        return row is not None

    def _find_by_dedup_key(self, dedup_key: str, cutoff_ts: float) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT event_id,event_type,status FROM spool WHERE dedup_key=? "
            "AND received_ts >= ? LIMIT 1",
            (dedup_key, cutoff_ts)).fetchone()
        if not row:
            return None
        return {"event_id": row[0], "event_type": row[1], "status": row[2]}

    def close(self) -> None:
        with self._lock:
            self._conn.close()
