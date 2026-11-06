"""Shared pytest configuration for the hermes-continuity-capture test suite.

Puts the repository root on ``sys.path`` so the tests can import the
``continuity_capture`` package without installing it. No test writes into the
repository tree; every SQLite spool lives under pytest's ``tmp_path``.
"""

from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
