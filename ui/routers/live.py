"""
ui/routers/live.py — Flask Blueprint for the live trading dashboard.

Routes:
    GET  /                       → live.html (dashboard)
    POST /api/live/kill          → send SIGTERM to live_trader.py
    GET  /api/live/status        → JSON status from PID file
"""
from __future__ import annotations

import json
import os
import signal
from pathlib import Path

from flask import Blueprint, jsonify, render_template, current_app

live_bp = Blueprint("live", __name__)

_DEFAULT_PID_FILE = Path("data/live_trader.pid")


def _pid_file_path() -> Path:
    return Path(current_app.config.get("LIVE_TRADER_PID_FILE", str(_DEFAULT_PID_FILE)))


def _read_pid() -> int | None:
    """Read the live trader PID from the PID file. Returns None if absent or invalid."""
    pid_file = _pid_file_path()
    try:
        text = pid_file.read_text().strip()
        return int(text) if text else None
    except (FileNotFoundError, ValueError, PermissionError):
        return None


def _process_is_alive(pid: int) -> bool:
    """Return True if a process with this PID exists (not necessarily our process)."""
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


# ── routes ────────────────────────────────────────────────────────────────────

@live_bp.get("/")
def dashboard():
    """Serve the live trading dashboard page."""
    return render_template("live.html")


@live_bp.post("/api/live/kill")
def kill_trader():
    """Send SIGTERM to the live trader process.

    Reads data/live_trader.pid (or config override LIVE_TRADER_PID_FILE).
    Returns JSON: {"ok": true, "pid": <int>} on success,
                  {"ok": false, "error": "<reason>"} on failure.
    """
    pid = _read_pid()
    if pid is None:
        return jsonify({"ok": False, "error": "PID file not found or unreadable"}), 404

    if not _process_is_alive(pid):
        return jsonify({"ok": False, "error": f"Process {pid} is not running"}), 409

    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return jsonify({"ok": False, "error": f"Process {pid} vanished before SIGTERM"}), 409
    except PermissionError:
        return jsonify({"ok": False, "error": f"Permission denied sending SIGTERM to {pid}"}), 403

    current_app.logger.info("SIGTERM sent to live_trader pid=%d", pid)
    return jsonify({"ok": True, "pid": pid})


@live_bp.get("/api/live/status")
def trader_status():
    """Return live trader process status as JSON.

    Returns:
        {"running": bool, "pid": int | null}
    """
    pid = _read_pid()
    running = pid is not None and _process_is_alive(pid)
    return jsonify({"running": running, "pid": pid})
