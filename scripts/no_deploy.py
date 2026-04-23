#!/usr/bin/env python3
"""
no_deploy.py — NO_DEPLOY lockfile management for the rithmic_engine live-trading gate.

The NO_DEPLOY file is a JSON file whose presence prevents go_live.py from promoting
the system to live trading. It is written by the pipeline when a gate fails and can
only be cleared by an authorised operator via clear_lock().

Public API
----------
is_locked(path)         -> bool
set_lock(reason, path)  -> None
clear_lock(auth, path)  -> None
get_lock_reason(path)   -> str | None
lock_required(path)     -> decorator
"""
from __future__ import annotations

import functools
import json
import logging
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

log = logging.getLogger(__name__)

# Default path is relative to the working directory; callers may override.
_DEFAULT_PATH = Path("NO_DEPLOY")


def _resolve(path: Optional[Path]) -> Path:
    return Path(path) if path is not None else _DEFAULT_PATH


# ---------------------------------------------------------------------------
# public helpers
# ---------------------------------------------------------------------------

def is_locked(path: Optional[Path] = None) -> bool:
    """Return True if the NO_DEPLOY lockfile exists at *path*."""
    return _resolve(path).exists()


def set_lock(reason: str, *, path: Optional[Path] = None) -> None:
    """Create (or overwrite) the NO_DEPLOY lockfile with *reason* and a UTC timestamp.

    Writes atomically via a sibling temp file + rename so a concurrent reader
    never sees a partially written file.
    """
    lock_path = _resolve(path)
    payload = json.dumps(
        {"reason": reason, "timestamp": datetime.now(timezone.utc).isoformat()},
        indent=2,
    )
    # atomic write: write to tmp, then rename
    tmp_fd, tmp_name = tempfile.mkstemp(
        dir=lock_path.parent if lock_path.parent != Path(".") else Path("."),
        prefix=".no_deploy_tmp_",
    )
    try:
        with os.fdopen(tmp_fd, "w") as fh:
            fh.write(payload)
        os.replace(tmp_name, lock_path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    log.warning("NO_DEPLOY lockfile set: %s", reason)


def clear_lock(authorized_by: str, *, path: Optional[Path] = None) -> None:
    """Remove the NO_DEPLOY lockfile and write an audit log entry.

    No-op (and no error) if the file does not exist.
    """
    lock_path = _resolve(path)
    if not lock_path.exists():
        log.info("clear_lock called but NO_DEPLOY not present — nothing to do")
        return
    old_reason = get_lock_reason(path)
    lock_path.unlink()
    log.warning(
        "NO_DEPLOY lockfile cleared by '%s' (was: %s)", authorized_by, old_reason
    )


def get_lock_reason(path: Optional[Path] = None) -> Optional[str]:
    """Return the reason string from the lockfile, or None if not locked / unreadable."""
    lock_path = _resolve(path)
    if not lock_path.exists():
        return None
    try:
        data = json.loads(lock_path.read_text())
        reason = data.get("reason", "")
        ts = data.get("timestamp", "")
        return f"{reason} (locked at {ts})" if ts else reason
    except (json.JSONDecodeError, OSError):
        # Non-JSON legacy lockfile — return raw text
        try:
            return lock_path.read_text().strip() or "(empty lockfile)"
        except OSError:
            return "(unreadable lockfile)"


def lock_required(
    func: Optional[Callable] = None,
    *,
    path: Optional[Path] = None,
) -> Callable:
    """Decorator: exit with code 1 if the NO_DEPLOY lockfile is present.

    Can be used with or without arguments:

        @lock_required
        def my_fn(): ...

        @lock_required(path=Path("custom/NO_DEPLOY"))
        def my_fn(): ...
    """

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            if is_locked(path):
                reason = get_lock_reason(path)
                print(
                    f"ERROR: NO_DEPLOY lockfile is active — cannot proceed.\n"
                    f"  Reason: {reason}\n"
                    f"  Clear with: scripts/no_deploy.py clear --authorized-by <name>",
                    file=sys.stderr,
                )
                sys.exit(1)
            return fn(*args, **kwargs)

        return wrapper

    if func is not None:
        # used as @lock_required (no parens)
        return decorator(func)
    # used as @lock_required(...) — return the decorator
    return decorator


# ---------------------------------------------------------------------------
# CLI for operator use
# ---------------------------------------------------------------------------

def _cli() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        prog="no_deploy",
        description="Manage the NO_DEPLOY lockfile for rithmic_engine.",
    )
    parser.add_argument(
        "--path",
        default=str(_DEFAULT_PATH),
        help="Path to the NO_DEPLOY file (default: %(default)s)",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("status", help="Print current lock status")

    p_set = sub.add_parser("set", help="Set the lockfile")
    p_set.add_argument("reason", help="Human-readable reason for the lock")

    p_clear = sub.add_parser("clear", help="Clear the lockfile")
    p_clear.add_argument(
        "--authorized-by",
        required=True,
        metavar="NAME",
        help="Operator name / identifier for audit trail",
    )

    args = parser.parse_args()
    lock_path = Path(args.path)

    if args.cmd == "status":
        if is_locked(lock_path):
            print(f"LOCKED — {get_lock_reason(lock_path)}")
            sys.exit(1)
        else:
            print("UNLOCKED — no NO_DEPLOY file present")
            sys.exit(0)

    elif args.cmd == "set":
        set_lock(args.reason, path=lock_path)
        print(f"NO_DEPLOY lockfile written to {lock_path}")

    elif args.cmd == "clear":
        clear_lock(args.authorized_by, path=lock_path)
        print(f"NO_DEPLOY lockfile cleared (authorized by: {args.authorized_by})")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
    _cli()
