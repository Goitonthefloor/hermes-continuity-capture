"""continuity_capture.timeutil — Korrekte ISO-8601-UTC-Zeitstempel.

``strftime('%f')`` ist in Python NICHT unterstuetzt (bleibt wörtlich). Deshalb wird die
Sub-Sekunden-Aufloesung aus ``time.time()`` korrekt formatiert (Millisekunden).
"""

from __future__ import annotations

import time

__all__ = ["iso_now", "epoch_to_iso"]

def epoch_to_iso(ts: float) -> str:
    """Wandelt einen epoch (float) in ISO-8601-UTC mit ms-Aufloesung um."""
    sec = int(ts)
    ms = int(round((ts - sec) * 1000.0)) % 1000
    base = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(sec))
    return f"{base}.{ms:03d}Z"

def iso_now() -> str:
    """Aktuelle Zeit als ISO-8601-UTC mit ms-Aufloesung."""
    return epoch_to_iso(time.time())
