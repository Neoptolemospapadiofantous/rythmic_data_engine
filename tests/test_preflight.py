#!/usr/bin/env python3
"""
test_preflight.py — TDD tests for go_live.py pre-flight gate logic and
                    scripts/no_deploy.py lockfile management.

Run: python -m pytest tests/test_preflight.py -v
"""
from __future__ import annotations

import importlib
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure repo root is on sys.path so we can import go_live and scripts.no_deploy.
_REPO_ROOT = Path(__file__).parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

pytestmark = pytest.mark.preflight


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _minimal_config(**overrides) -> dict:
    """Return a valid live_config dict; keyword args override top-level keys."""
    cfg = {
        "dry_run": True,
        "ml": {
            "enabled": True,
            "model_path": "models/orb_xgb_latest.pkl",
        },
        "prop_firm": {
            "daily_loss_limit": 2000.0,
            "max_position_size": 3,
        },
        "rithmic": {
            "ssl_cert_path": "certs/rithmic_ssl_cert_auth_params",
        },
        "no_deploy_path": "NO_DEPLOY",
        "alerts": {"enabled": False},
    }
    cfg.update(overrides)
    return cfg


def _write_config(tmp_dir: Path, cfg: dict) -> Path:
    config_dir = tmp_dir / "config"
    config_dir.mkdir(exist_ok=True)
    p = config_dir / "live_config.json"
    p.write_text(json.dumps(cfg))
    return p


# ---------------------------------------------------------------------------
# tests for scripts/no_deploy.py
# ---------------------------------------------------------------------------

class TestNoDeploy(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.lock_path = self.tmp / "NO_DEPLOY"
        # import fresh each test so state doesn't bleed
        import scripts.no_deploy as nd
        importlib.reload(nd)
        self.nd = nd

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_is_locked_false_when_no_file(self):
        self.assertFalse(self.nd.is_locked(self.lock_path))

    def test_set_lock_creates_file(self):
        self.nd.set_lock("test reason", path=self.lock_path)
        self.assertTrue(self.lock_path.exists())

    def test_set_lock_writes_reason_and_timestamp(self):
        self.nd.set_lock("gate failure in step 4", path=self.lock_path)
        content = self.lock_path.read_text()
        self.assertIn("gate failure in step 4", content)
        self.assertIn("timestamp", content.lower())

    def test_is_locked_true_after_set(self):
        self.nd.set_lock("reason", path=self.lock_path)
        self.assertTrue(self.nd.is_locked(self.lock_path))

    def test_get_lock_reason_none_when_not_locked(self):
        self.assertIsNone(self.nd.get_lock_reason(self.lock_path))

    def test_get_lock_reason_returns_reason(self):
        self.nd.set_lock("some reason", path=self.lock_path)
        reason = self.nd.get_lock_reason(self.lock_path)
        self.assertIsNotNone(reason)
        self.assertIn("some reason", reason)

    def test_clear_lock_removes_file(self):
        self.nd.set_lock("reason", path=self.lock_path)
        self.nd.clear_lock("test-operator", path=self.lock_path)
        self.assertFalse(self.lock_path.exists())

    def test_clear_lock_noop_when_not_locked(self):
        # Should not raise even when file absent
        self.nd.clear_lock("test-operator", path=self.lock_path)
        self.assertFalse(self.lock_path.exists())

    def test_lock_required_decorator_exits_when_locked(self):
        self.nd.set_lock("locked", path=self.lock_path)

        @self.nd.lock_required(path=self.lock_path)
        def _fn():
            return "should not reach"

        with self.assertRaises(SystemExit) as ctx:
            _fn()
        self.assertNotEqual(ctx.exception.code, 0)

    def test_lock_required_decorator_passes_when_unlocked(self):
        @self.nd.lock_required(path=self.lock_path)
        def _fn():
            return "reached"

        result = _fn()
        self.assertEqual(result, "reached")

    def test_set_lock_idempotent(self):
        """Calling set_lock twice must not raise and must keep the newer reason."""
        self.nd.set_lock("first", path=self.lock_path)
        self.nd.set_lock("second", path=self.lock_path)
        reason = self.nd.get_lock_reason(self.lock_path)
        self.assertIn("second", reason)


# ---------------------------------------------------------------------------
# tests for go_live.py gate logic
# ---------------------------------------------------------------------------

class TestGoLiveGates(unittest.TestCase):
    """Tests for each pre-flight gate in go_live.run_preflight()."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        # Create directory layout expected by go_live
        (self.tmp / "config").mkdir()
        (self.tmp / "certs").mkdir()
        (self.tmp / "models").mkdir()
        (self.tmp / "data").mkdir()

        # Write default valid config
        self._cfg = _minimal_config()
        self._write_cfg(self._cfg)

        # Create expected cert and model files
        (self.tmp / "certs" / "rithmic_ssl_cert_auth_params").touch()
        (self.tmp / "models" / "orb_xgb_latest.pkl").touch()

        import go_live
        importlib.reload(go_live)
        self.go_live = go_live

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write_cfg(self, cfg: dict):
        (self.tmp / "config" / "live_config.json").write_text(json.dumps(cfg))

    def _run(self, extra_args: list[str] | None = None) -> int:
        """Call go_live.main() with the tmp dir as CWD. Returns exit code."""
        args = extra_args or []
        old_cwd = os.getcwd()
        try:
            os.chdir(self.tmp)
            with self.assertRaises(SystemExit) as ctx:
                self.go_live.main(args)
            return ctx.exception.code or 0
        finally:
            os.chdir(old_cwd)

    def _run_preflight(self, extra_args: list[str] | None = None) -> tuple[int, list[dict]]:
        """Call go_live.run_preflight() directly; returns (exit_code, gate_results)."""
        args = extra_args or []
        old_cwd = os.getcwd()
        try:
            os.chdir(self.tmp)
            return self.go_live.run_preflight(args)
        finally:
            os.chdir(old_cwd)

    # ── gate A: NO_DEPLOY ─────────────────────────────────────────

    def test_locked_exits_nonzero(self):
        """If NO_DEPLOY lockfile is present, go_live must exit non-zero."""
        (self.tmp / "NO_DEPLOY").write_text(
            '{"reason": "test", "timestamp": "2026-01-01T00:00:00"}'
        )
        code = self._run()
        self.assertNotEqual(code, 0)

    def test_locked_does_not_modify_config(self):
        """NO_DEPLOY must prevent config modification even with --confirm-live."""
        (self.tmp / "NO_DEPLOY").write_text(
            '{"reason": "test", "timestamp": "2026-01-01T00:00:00"}'
        )
        original = (self.tmp / "config" / "live_config.json").read_text()
        self._run(["--confirm-live"])
        after = (self.tmp / "config" / "live_config.json").read_text()
        self.assertEqual(original, after)

    # ── gate B: config exists ─────────────────────────────────────

    def test_config_missing_gate_fails(self):
        (self.tmp / "config" / "live_config.json").unlink()
        code = self._run()
        self.assertNotEqual(code, 0)

    def test_config_invalid_json_fails(self):
        (self.tmp / "config" / "live_config.json").write_text("{ not json }")
        code = self._run()
        self.assertNotEqual(code, 0)

    # ── gate C: dry_run must be True ─────────────────────────────

    def test_already_live_gate_fails(self):
        cfg = _minimal_config(dry_run=False)
        self._write_cfg(cfg)
        code = self._run()
        self.assertNotEqual(code, 0)

    # ── gate D: PostgreSQL ────────────────────────────────────────

    def test_db_failure_gate_fails(self):
        with patch("go_live._check_db_connection", return_value=(False, "connection refused")):
            code = self._run()
        self.assertNotEqual(code, 0)

    def test_db_success_gate_passes(self):
        with patch("go_live._check_db_connection", return_value=(True, "ok")):
            code = self._run()
        # Other gates still pass; only checking DB gate doesn't veto
        # (if all gates pass with no --confirm-live → exit 0)
        self.assertEqual(code, 0)

    # ── gate E: cert file ─────────────────────────────────────────

    def test_cert_missing_gate_fails(self):
        (self.tmp / "certs" / "rithmic_ssl_cert_auth_params").unlink()
        with patch("go_live._check_db_connection", return_value=(True, "ok")):
            code = self._run()
        self.assertNotEqual(code, 0)

    # ── gate F: ML model ──────────────────────────────────────────

    def test_ml_model_missing_fails_when_ml_enabled(self):
        (self.tmp / "models" / "orb_xgb_latest.pkl").unlink()
        with patch("go_live._check_db_connection", return_value=(True, "ok")):
            code = self._run()
        self.assertNotEqual(code, 0)

    def test_ml_model_missing_ignored_when_ml_disabled(self):
        cfg = _minimal_config()
        cfg["ml"]["enabled"] = False
        self._write_cfg(cfg)
        (self.tmp / "models" / "orb_xgb_latest.pkl").unlink()
        with patch("go_live._check_db_connection", return_value=(True, "ok")):
            code = self._run()
        self.assertEqual(code, 0)

    # ── gate G: disk space ────────────────────────────────────────

    def test_disk_space_gate_fails_when_low(self):
        mock_usage = MagicMock()
        mock_usage.free = 1 * 1024 ** 3  # 1 GB — below 5 GB threshold
        with patch("shutil.disk_usage", return_value=mock_usage):
            with patch("go_live._check_db_connection", return_value=(True, "ok")):
                code = self._run()
        self.assertNotEqual(code, 0)

    def test_disk_space_gate_passes_when_sufficient(self):
        mock_usage = MagicMock()
        mock_usage.free = 10 * 1024 ** 3  # 10 GB
        with patch("shutil.disk_usage", return_value=mock_usage):
            with patch("go_live._check_db_connection", return_value=(True, "ok")):
                code = self._run()
        self.assertEqual(code, 0)

    # ── gate H: DRIFT_HALT ────────────────────────────────────────

    def test_drift_halt_present_fails(self):
        (self.tmp / "data" / "DRIFT_HALT").write_text("drift detected 2026-04-01")
        with patch("go_live._check_db_connection", return_value=(True, "ok")):
            code = self._run()
        self.assertNotEqual(code, 0)

    # ── gate I: prop firm limits ──────────────────────────────────

    def test_prop_firm_daily_loss_zero_fails(self):
        cfg = _minimal_config()
        cfg["prop_firm"]["daily_loss_limit"] = 0
        self._write_cfg(cfg)
        with patch("go_live._check_db_connection", return_value=(True, "ok")):
            code = self._run()
        self.assertNotEqual(code, 0)

    def test_prop_firm_max_position_zero_fails(self):
        cfg = _minimal_config()
        cfg["prop_firm"]["max_position_size"] = 0
        self._write_cfg(cfg)
        with patch("go_live._check_db_connection", return_value=(True, "ok")):
            code = self._run()
        self.assertNotEqual(code, 0)

    # ── promotion behaviour ───────────────────────────────────────

    def test_all_pass_no_confirm_exits_zero_dry_run_unchanged(self):
        """All gates pass, no --confirm-live → exit 0, dry_run still True."""
        with patch("go_live._check_db_connection", return_value=(True, "ok")):
            code = self._run()
        self.assertEqual(code, 0)
        cfg = json.loads((self.tmp / "config" / "live_config.json").read_text())
        self.assertTrue(cfg["dry_run"])

    def test_all_pass_with_confirm_sets_dry_run_false(self):
        """All gates pass + --confirm-live → dry_run written as False."""
        with patch("go_live._check_db_connection", return_value=(True, "ok")):
            code = self._run(["--confirm-live"])
        self.assertEqual(code, 0)
        cfg = json.loads((self.tmp / "config" / "live_config.json").read_text())
        self.assertFalse(cfg["dry_run"])

    def test_rollback_on_partial_failure(self):
        """Config must NOT be modified when any gate fails."""
        original = (self.tmp / "config" / "live_config.json").read_text()
        # Fail the DB gate
        with patch("go_live._check_db_connection", return_value=(False, "refused")):
            self._run(["--confirm-live"])
        after = (self.tmp / "config" / "live_config.json").read_text()
        self.assertEqual(original, after)

    def test_config_write_is_atomic(self):
        """Promotion must use atomic write (tmp file + rename), not direct open."""
        import go_live as gl
        # Verify the implementation uses a temp-file strategy by checking source.
        src = Path(gl.__file__).read_text()
        # Either tempfile usage or os.replace / Path.rename must appear.
        self.assertTrue(
            "os.replace" in src or ".rename(" in src or "tempfile" in src,
            "go_live.py must use atomic write for config promotion",
        )


# ---------------------------------------------------------------------------
# Gate F — ML model checksum tests
# ---------------------------------------------------------------------------

class TestGateMLChecksum(unittest.TestCase):
    """Gate F: ML model file existence + sha256 checksum verification."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        # Minimal config pointing at temp dir
        self.cfg = {
            "dry_run": True,
            "ml": {"enabled": True, "model_path": str(self.tmp / "model.pkl")},
        }

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _gate(self, checksums_path=None):
        import go_live as gl
        original = gl.CHECKSUMS_PATH
        if checksums_path is not None:
            gl.CHECKSUMS_PATH = checksums_path
        try:
            return gl._gate_ml_model(self.cfg)
        finally:
            gl.CHECKSUMS_PATH = original

    def test_model_missing_fails(self):
        result = self._gate()
        self.assertFalse(result.passed)

    def test_model_exists_no_checksums_file_passes(self):
        (self.tmp / "model.pkl").write_bytes(b"fake-model")
        result = self._gate(checksums_path=self.tmp / "nonexistent_checksums.json")
        self.assertTrue(result.passed)
        self.assertIn("no checksum file", result.detail)

    def test_model_exists_checksum_matches_passes(self):
        model = self.tmp / "model.pkl"
        model.write_bytes(b"fake-model")
        import hashlib, go_live as gl
        actual_hash = hashlib.sha256(b"fake-model").hexdigest()
        checksums_path = self.tmp / "checksums.json"
        checksums_path.write_text(json.dumps({str(model): actual_hash}))
        result = self._gate(checksums_path=checksums_path)
        self.assertTrue(result.passed)
        self.assertIn("sha256 OK", result.detail)

    def test_model_exists_checksum_mismatch_fails(self):
        model = self.tmp / "model.pkl"
        model.write_bytes(b"fake-model")
        checksums_path = self.tmp / "checksums.json"
        checksums_path.write_text(json.dumps({str(model): "a" * 64}))  # wrong hash
        result = self._gate(checksums_path=checksums_path)
        self.assertFalse(result.passed)
        self.assertIn("MISMATCH", result.detail)

    def test_model_path_not_in_checksums_file_passes_with_note(self):
        model = self.tmp / "model.pkl"
        model.write_bytes(b"fake-model")
        checksums_path = self.tmp / "checksums.json"
        checksums_path.write_text(json.dumps({"other/path.pkl": "a" * 64}))
        result = self._gate(checksums_path=checksums_path)
        self.assertTrue(result.passed)
        self.assertIn("no expected hash", result.detail)

    def test_update_checksums_writes_file(self):
        model = self.tmp / "model.pkl"
        model.write_bytes(b"fake-model")
        cfg = {"ml": {"model_path": str(model), "scaler_path": ""}}
        import go_live as gl
        original = gl.CHECKSUMS_PATH
        checksums_out = self.tmp / "checksums_out.json"
        gl.CHECKSUMS_PATH = checksums_out
        try:
            gl.update_checksums(cfg)
        finally:
            gl.CHECKSUMS_PATH = original
        data = json.loads(checksums_out.read_text())
        import hashlib
        self.assertEqual(data[str(model)], hashlib.sha256(b"fake-model").hexdigest())


# ---------------------------------------------------------------------------
# Gate J — account equity tests
# ---------------------------------------------------------------------------

class TestGateAccountEquity(unittest.TestCase):
    """Gate J: account equity above minimum (via PNL_PLANT_EQUITY env var)."""

    def _gate(self, equity_env: str | None, cfg: dict | None = None):
        import go_live as gl
        if cfg is None:
            cfg = {"prop_firm": {"trailing_drawdown_limit": 2500.0}}
        env = {}
        if equity_env is not None:
            env["PNL_PLANT_EQUITY"] = equity_env
        with patch.dict(os.environ, env, clear=False):
            # Ensure the var is absent when None
            if equity_env is None:
                os.environ.pop("PNL_PLANT_EQUITY", None)
            return gl._gate_account_equity(cfg)

    def test_env_var_absent_skips(self):
        result = self._gate(equity_env=None)
        self.assertTrue(result.passed)
        self.assertIn("SKIP", result.detail)

    def test_equity_above_minimum_passes(self):
        # trailing_drawdown_limit=2500 → minimum=1250; equity=50000 → pass
        result = self._gate(equity_env="50000")
        self.assertTrue(result.passed)
        self.assertIn("≥", result.detail)

    def test_equity_below_minimum_fails(self):
        # minimum = 2500 * 0.5 = 1250; equity=100 → fail
        result = self._gate(equity_env="100")
        self.assertFalse(result.passed)
        self.assertIn("<", result.detail)

    def test_invalid_equity_value_fails(self):
        result = self._gate(equity_env="not-a-number")
        self.assertFalse(result.passed)
        self.assertIn("not a valid number", result.detail)

    def test_zero_trailing_drawdown_no_minimum(self):
        # If no trailing drawdown limit configured, minimum=0; any positive equity passes
        result = self._gate(equity_env="1", cfg={"prop_firm": {"trailing_drawdown_limit": 0}})
        self.assertTrue(result.passed)


# ---------------------------------------------------------------------------
# integration smoke test (importability / CLI help)
# ---------------------------------------------------------------------------

class TestGoLiveCLI(unittest.TestCase):

    def test_go_live_importable(self):
        import go_live  # noqa: F401

    def test_no_deploy_importable(self):
        import scripts.no_deploy  # noqa: F401

    def test_help_exits_zero(self):
        import go_live
        importlib.reload(go_live)
        with self.assertRaises(SystemExit) as ctx:
            go_live.main(["--help"])
        self.assertEqual(ctx.exception.code, 0)

    def test_update_checksums_flag_exits_zero(self):
        """--update-checksums must exit cleanly without running preflight gates."""
        import go_live
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config" / "live_config.json"
            cfg_path.parent.mkdir()
            cfg_path.write_text(json.dumps({
                "dry_run": True,
                "ml": {"enabled": True, "model_path": "nonexistent.pkl", "scaler_path": ""},
                "prop_firm": {}, "rithmic": {}, "no_deploy_path": str(Path(td) / "NO_DEPLOY"),
            }))
            import go_live as gl
            original_config = gl.CONFIG_PATH
            original_checksums = gl.CHECKSUMS_PATH
            gl.CONFIG_PATH = cfg_path
            gl.CHECKSUMS_PATH = Path(td) / "checksums.json"
            try:
                code, results = gl.run_preflight(["--update-checksums"])
            finally:
                gl.CONFIG_PATH = original_config
                gl.CHECKSUMS_PATH = original_checksums
        self.assertEqual(code, 0)
        self.assertEqual(results, [])


if __name__ == "__main__":
    unittest.main()
