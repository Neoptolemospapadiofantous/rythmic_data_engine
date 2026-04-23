"""
tests/test_ui_kill.py — Tests for POST /api/live/kill and GET /api/live/status.

Uses Flask test client; no real process signals are sent.
"""
from __future__ import annotations

import os
import signal
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

from ui.app import create_app


def _app_with_pid_file(pid_file_path: str):
    return create_app({"TESTING": True, "LIVE_TRADER_PID_FILE": pid_file_path})


class TestKillEndpoint(unittest.TestCase):

    def _write_pid(self, pid_file: str, pid: int) -> None:
        Path(pid_file).write_text(str(pid))

    def test_kill_returns_404_when_no_pid_file(self):
        app = _app_with_pid_file("/tmp/nonexistent_pid_file_xyz_abc.pid")
        with app.test_client() as c:
            res = c.post("/api/live/kill")
        self.assertEqual(res.status_code, 404)
        data = res.get_json()
        self.assertFalse(data["ok"])
        self.assertIn("PID file", data["error"])

    def test_kill_returns_409_when_process_not_running(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".pid", delete=False) as f:
            # PID 99999 is extremely unlikely to be a real process
            f.write("99999")
            pid_file = f.name
        try:
            app = _app_with_pid_file(pid_file)
            with app.test_client() as c:
                res = c.post("/api/live/kill")
            self.assertEqual(res.status_code, 409)
            data = res.get_json()
            self.assertFalse(data["ok"])
        finally:
            os.unlink(pid_file)

    def test_kill_sends_sigterm_to_running_process(self):
        """kill endpoint must call os.kill(pid, SIGTERM) for a running process."""
        own_pid = os.getpid()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".pid", delete=False) as f:
            f.write(str(own_pid))
            pid_file = f.name
        try:
            app = _app_with_pid_file(pid_file)
            sent_signals: list[tuple[int, int]] = []

            def _mock_kill(pid: int, sig: int) -> None:
                sent_signals.append((pid, sig))

            with app.test_client() as c:
                with patch("ui.routers.live.os.kill", side_effect=_mock_kill):
                    # os.kill(pid, 0) for liveness check must return normally,
                    # and os.kill(pid, SIGTERM) must be recorded.
                    # Patch at module level: first call is kill(pid, 0) → alive check,
                    # second is kill(pid, SIGTERM).
                    # We need a smarter mock that returns for 0 and records for SIGTERM.
                    pass

            # Better: patch _process_is_alive to return True, then check os.kill called
            with app.test_client() as c:
                with patch("ui.routers.live._process_is_alive", return_value=True), \
                     patch("ui.routers.live.os.kill") as mock_kill:
                    res = c.post("/api/live/kill")

            self.assertEqual(res.status_code, 200)
            data = res.get_json()
            self.assertTrue(data["ok"])
            self.assertEqual(data["pid"], own_pid)
            mock_kill.assert_called_once_with(own_pid, signal.SIGTERM)
        finally:
            os.unlink(pid_file)

    def test_kill_returns_403_on_permission_denied(self):
        """kill endpoint returns 403 when OS denies SIGTERM."""
        own_pid = os.getpid()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".pid", delete=False) as f:
            f.write(str(own_pid))
            pid_file = f.name
        try:
            app = _app_with_pid_file(pid_file)
            with app.test_client() as c:
                with patch("ui.routers.live._process_is_alive", return_value=True), \
                     patch("ui.routers.live.os.kill", side_effect=PermissionError):
                    res = c.post("/api/live/kill")
            self.assertEqual(res.status_code, 403)
            data = res.get_json()
            self.assertFalse(data["ok"])
            self.assertIn("Permission denied", data["error"])
        finally:
            os.unlink(pid_file)


    def test_kill_rejected_from_non_localhost(self):
        """Kill endpoint must return 403 when request originates from a non-localhost IP."""
        app = _app_with_pid_file("/tmp/dummy.pid")
        with app.test_client() as c:
            # environ_base overrides REMOTE_ADDR for the test request
            res = c.post("/api/live/kill", environ_base={"REMOTE_ADDR": "10.0.0.1"})
        self.assertEqual(res.status_code, 403)
        data = res.get_json()
        self.assertFalse(data["ok"])
        self.assertIn("localhost", data["error"])


class TestStatusEndpoint(unittest.TestCase):

    def test_status_returns_not_running_when_no_pid_file(self):
        app = _app_with_pid_file("/tmp/nonexistent_pid_file_xyz_abc.pid")
        with app.test_client() as c:
            res = c.get("/api/live/status")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertFalse(data["running"])
        self.assertIsNone(data["pid"])

    def test_status_returns_running_for_live_process(self):
        own_pid = os.getpid()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".pid", delete=False) as f:
            f.write(str(own_pid))
            pid_file = f.name
        try:
            app = _app_with_pid_file(pid_file)
            with app.test_client() as c:
                res = c.get("/api/live/status")
            self.assertEqual(res.status_code, 200)
            data = res.get_json()
            self.assertTrue(data["running"])
            self.assertEqual(data["pid"], own_pid)
        finally:
            os.unlink(pid_file)

    def test_status_returns_not_running_for_dead_process(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".pid", delete=False) as f:
            f.write("99999")
            pid_file = f.name
        try:
            app = _app_with_pid_file(pid_file)
            with app.test_client() as c:
                res = c.get("/api/live/status")
            data = res.get_json()
            self.assertFalse(data["running"])
        finally:
            os.unlink(pid_file)


class TestDashboardPage(unittest.TestCase):

    def test_dashboard_returns_200(self):
        app = _app_with_pid_file("/tmp/dummy.pid")
        with app.test_client() as c:
            res = c.get("/")
        self.assertEqual(res.status_code, 200)
        self.assertIn(b"EMERGENCY STOP", res.data)
        self.assertIn(b"kill-btn", res.data)

    def test_dashboard_has_kill_endpoint_call(self):
        """The HTML must reference the /api/live/kill endpoint."""
        app = _app_with_pid_file("/tmp/dummy.pid")
        with app.test_client() as c:
            res = c.get("/")
        self.assertIn(b"/api/live/kill", res.data)


if __name__ == "__main__":
    unittest.main(verbosity=2)
