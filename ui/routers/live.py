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

from flask import Blueprint, jsonify, render_template, current_app, request

live_bp = Blueprint("live", __name__)

_DEFAULT_PID_FILE = Path("data/live_trader.pid")
_LOCALHOST_ADDRS = frozenset({"127.0.0.1", "::1", "localhost"})


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


def _require_localhost():
    """Return a 403 JSON response if the request is not from localhost, else None."""
    remote = request.remote_addr or ""
    if remote not in _LOCALHOST_ADDRS:
        return jsonify({"ok": False, "error": "Kill switch only accessible from localhost"}), 403
    return None


@live_bp.post("/api/live/kill")
def kill_trader():
    """Send SIGTERM to the live trader process.

    Only callable from localhost to prevent remote kill-switch abuse.
    Reads data/live_trader.pid (or config override LIVE_TRADER_PID_FILE).
    Returns JSON: {"ok": true, "pid": <int>} on success,
                  {"ok": false, "error": "<reason>"} on failure.
    """
    guard = _require_localhost()
    if guard is not None:
        return guard

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


_DEFAULT_STATE_FILE = Path("data/live_state.json")


def _read_state() -> dict | None:
    path = Path(current_app.config.get("LIVE_TRADER_STATE_FILE", str(_DEFAULT_STATE_FILE)))
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None


@live_bp.get("/api/live/state")
def trader_state():
    """Return full live trader state (position, P&L, connection, ORB data)."""
    state = _read_state()
    if state is None:
        return jsonify({"error": "state file unavailable"}), 404
    pid = _read_pid()
    state["running"] = pid is not None and _process_is_alive(pid)
    return jsonify(state)


@live_bp.get("/api/live/orb")
def orb_state():
    """Return ORB-specific data for the chart: bars, high/low, strategy state."""
    state = _read_state()
    if state is None:
        return jsonify({"error": "state file unavailable"}), 404
    return jsonify({
        "orb_high": state.get("orb_high"),
        "orb_low": state.get("orb_low"),
        "strategy_state": state.get("strategy_state", "UNKNOWN"),
        "orb_minutes": state.get("orb_minutes", 5),
        "orb_bars": state.get("orb_bars", []),
        "position": state.get("position", "FLAT"),
        "entry_price": state.get("entry_price"),
        "sl": state.get("sl"),
    })
