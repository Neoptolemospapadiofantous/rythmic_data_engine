"""
Tests for audit_daemon.py check functions and EscalationEngine.

Coverage:
  check_trading_constants  — PASS / WARN (missing) / CRITICAL (mismatch)
  check_drift_halt         — PASS (absent) / WARN (present)
  check_wal_health         — INFO (no file) / PASS (small) / WARN (large)
  check_disk_space         — structure and sensible value
  check_ram_usage          — structure and value consistency
  check_model_staleness    — INFO (disabled) / WARN (missing/stale) / PASS (fresh)
  check_pnl_sanity         — PASS (empty) / WARN (outlier)
  check_slippage_sanity    — INFO (no data) / PASS / WARN (excessive)
  check_data_freshness     — WARN (no ticks) / PASS (weekend 48h) / WARN (RTH stale)
  EscalationEngine         — WARN accumulation, INFO exemption, native_critical gate,
                             state persistence, auto-resolve, clean-counter reset

All tests marked @pytest.mark.fast — no live DB, no subprocess, no network.
DB-dependent checks use MagicMock connections.
Filesystem checks use tmp_path + monkeypatch to redirect ENGINE_DIR / AUDIT_HALT.
"""
from __future__ import annotations

import datetime as dt_module
import json
import os
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import audit_daemon
from audit_daemon import (
    EscalationEngine,
    check_data_freshness,
    check_disk_space,
    check_drift_halt,
    check_model_staleness,
    check_pnl_sanity,
    check_ram_usage,
    check_slippage_sanity,
    check_trading_constants,
    check_wal_health,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _mock_conn(fetchone_value=None, fetchall_value=None):
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = fetchone_value
    cur.fetchall.return_value = fetchall_value if fetchall_value is not None else []
    conn.cursor.return_value = cur
    return conn


@pytest.fixture
def escalation_engine(tmp_path, monkeypatch):
    """EscalationEngine with isolated state_file and AUDIT_HALT redirected to tmp_path."""
    state_file = tmp_path / "escalation_state.json"
    halt_file = tmp_path / "AUDIT_HALT"
    monkeypatch.setattr(audit_daemon, "AUDIT_HALT", halt_file)
    engine = EscalationEngine(state_file=state_file)
    return engine, halt_file


# ── check_trading_constants ────────────────────────────────────────────────────

@pytest.mark.fast
def test_trading_constants_pass():
    cfg = {"point_value": 2.0, "tick_value": 0.50, "symbol": "MNQ", "commission_rt": 4.0}
    r = check_trading_constants(cfg)
    assert r["check"] == "trading_constants"
    assert r["status"] == "PASS"
    assert r["value"] == pytest.approx(0.0)


@pytest.mark.fast
def test_trading_constants_warn_missing_tick_value():
    """None tick_value → WARN (missing field ≠ wrong value)."""
    cfg = {"point_value": 2.0, "tick_value": None, "symbol": "MNQ", "commission_rt": 4.0}
    r = check_trading_constants(cfg)
    assert r["status"] == "WARN"
    assert r.get("native_critical") is not True
    assert "tick_value" in r["message"]


@pytest.mark.fast
def test_trading_constants_warn_missing_both():
    cfg = {"point_value": None, "tick_value": None, "symbol": "MNQ", "commission_rt": 4.0}
    r = check_trading_constants(cfg)
    assert r["status"] == "WARN"
    assert "Missing" in r["message"]


@pytest.mark.fast
def test_trading_constants_critical_wrong_point_value():
    """NQ point_value (20.0) → CRITICAL with native_critical=True."""
    cfg = {"point_value": 20.0, "tick_value": 0.50, "symbol": "MNQ", "commission_rt": 4.0}
    r = check_trading_constants(cfg)
    assert r["status"] == "CRITICAL"
    assert r.get("native_critical") is True
    assert "point_value" in r["message"]


@pytest.mark.fast
def test_trading_constants_critical_wrong_tick_value():
    """NQ tick_value (5.0) → CRITICAL with native_critical=True."""
    cfg = {"point_value": 2.0, "tick_value": 5.0, "symbol": "MNQ", "commission_rt": 4.0}
    r = check_trading_constants(cfg)
    assert r["status"] == "CRITICAL"
    assert r.get("native_critical") is True
    assert "tick_value" in r["message"]


@pytest.mark.fast
def test_trading_constants_critical_wrong_commission():
    cfg = {"point_value": 2.0, "tick_value": 0.50, "symbol": "MNQ", "commission_rt": 8.0}
    r = check_trading_constants(cfg)
    assert r["status"] == "CRITICAL"
    assert r.get("native_critical") is True


@pytest.mark.fast
def test_trading_constants_none_config():
    """No config at all → WARN (cannot verify)."""
    r = check_trading_constants(None)
    assert r["status"] == "WARN"
    assert r["check"] == "trading_constants"


# ── check_drift_halt ───────────────────────────────────────────────────────────

@pytest.mark.fast
def test_drift_halt_absent(tmp_path, monkeypatch):
    monkeypatch.setattr(audit_daemon, "ENGINE_DIR", tmp_path)
    r = check_drift_halt()
    assert r["check"] == "drift_halt"
    assert r["status"] == "PASS"
    assert r["value"] == pytest.approx(0.0)


@pytest.mark.fast
def test_drift_halt_present(tmp_path, monkeypatch):
    monkeypatch.setattr(audit_daemon, "ENGINE_DIR", tmp_path)
    halt = tmp_path / "data" / "DRIFT_HALT"
    halt.parent.mkdir(parents=True)
    halt.write_text("model drift detected 2026-05-01")
    r = check_drift_halt()
    assert r["status"] == "WARN"
    assert r["value"] == pytest.approx(1.0)
    assert "retrain" in r["message"]


# ── check_wal_health ──────────────────────────────────────────────────────────

@pytest.mark.fast
def test_wal_health_no_file(tmp_path, monkeypatch):
    """No WAL file → INFO (normal before first run)."""
    monkeypatch.setattr(audit_daemon, "ENGINE_DIR", tmp_path)
    r = check_wal_health()
    assert r["check"] == "wal_health"
    assert r["status"] == "INFO"


@pytest.mark.fast
def test_wal_health_small_file(tmp_path, monkeypatch):
    """WAL < 1 MB → PASS."""
    monkeypatch.setattr(audit_daemon, "ENGINE_DIR", tmp_path)
    wal = tmp_path / "data" / "wal.bin"
    wal.parent.mkdir(parents=True)
    wal.write_bytes(b"x" * 1024)  # 1 KB
    r = check_wal_health()
    assert r["status"] == "PASS"
    assert r["value"] == pytest.approx(1024.0)


@pytest.mark.fast
def test_wal_health_large_file(tmp_path, monkeypatch):
    """WAL >= 1 MB → WARN with unflushed-data note."""
    monkeypatch.setattr(audit_daemon, "ENGINE_DIR", tmp_path)
    wal = tmp_path / "data" / "wal.bin"
    wal.parent.mkdir(parents=True)
    wal.write_bytes(b"x" * (1024 * 1024 + 1))
    r = check_wal_health()
    assert r["status"] == "WARN"
    assert "unflushed" in r["message"]


# ── check_disk_space ──────────────────────────────────────────────────────────

@pytest.mark.fast
def test_disk_space_returns_valid_dict():
    r = check_disk_space()
    assert r["check"] == "disk_space"
    assert r["status"] in ("PASS", "WARN")
    assert isinstance(r["value"], float)
    assert r["value"] > 0
    if r["status"] == "WARN":
        assert r["value"] <= 5.0


# ── check_ram_usage ───────────────────────────────────────────────────────────

@pytest.mark.fast
def test_ram_usage_returns_valid_dict():
    r = check_ram_usage()
    assert r["check"] == "ram_usage"
    assert r["status"] in ("PASS", "WARN", "INFO")
    assert isinstance(r["value"], float)


@pytest.mark.fast
def test_ram_usage_value_consistent_with_status():
    """If status is WARN, value must be >= 90.0."""
    r = check_ram_usage()
    if r["status"] == "WARN":
        assert r["value"] >= 90.0
    elif r["status"] == "PASS":
        assert r["value"] < 90.0


# ── check_model_staleness ─────────────────────────────────────────────────────

@pytest.mark.fast
def test_model_staleness_ml_disabled():
    r = check_model_staleness({"ml": {"enabled": False}})
    assert r["status"] == "INFO"
    assert "disabled" in r["message"]


@pytest.mark.fast
def test_model_staleness_none_config():
    r = check_model_staleness(None)
    assert r["status"] == "INFO"
    assert r["check"] == "model_staleness"


@pytest.mark.fast
def test_model_staleness_model_missing(tmp_path, monkeypatch):
    """ML enabled but model file absent → WARN."""
    monkeypatch.setattr(audit_daemon, "ENGINE_DIR", tmp_path)
    cfg = {"ml": {"enabled": True, "model_path": "models/orb_xgb_latest.pkl"}}
    r = check_model_staleness(cfg)
    assert r["status"] == "WARN"
    assert "not found" in r["message"]


@pytest.mark.fast
def test_model_staleness_fresh_model(tmp_path, monkeypatch):
    """ML enabled with freshly created model file → PASS, age < 1 day."""
    monkeypatch.setattr(audit_daemon, "ENGINE_DIR", tmp_path)
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "orb_xgb_latest.pkl").write_bytes(b"fake model")
    cfg = {"ml": {"enabled": True, "model_path": "models/orb_xgb_latest.pkl"}}
    r = check_model_staleness(cfg)
    assert r["status"] == "PASS"
    assert r["value"] < 1.0


@pytest.mark.fast
def test_model_staleness_stale_model(tmp_path, monkeypatch):
    """Model older than 30 days → WARN."""
    monkeypatch.setattr(audit_daemon, "ENGINE_DIR", tmp_path)
    (tmp_path / "models").mkdir()
    model = tmp_path / "models" / "orb_xgb_latest.pkl"
    model.write_bytes(b"fake model")
    old_mtime = time.time() - 31 * 86400
    os.utime(model, (old_mtime, old_mtime))
    cfg = {"ml": {"enabled": True, "model_path": "models/orb_xgb_latest.pkl"}}
    r = check_model_staleness(cfg)
    assert r["status"] == "WARN"
    assert r["value"] >= 30.0


# ── check_pnl_sanity ──────────────────────────────────────────────────────────

@pytest.mark.fast
def test_pnl_sanity_no_outliers():
    r = check_pnl_sanity(_mock_conn(fetchall_value=[]))
    assert r["check"] == "pnl_sanity"
    assert r["status"] == "PASS"
    assert r["value"] == pytest.approx(0.0)


@pytest.mark.fast
def test_pnl_sanity_with_outlier():
    """Trade with |pnl_usd| = 750 → WARN with trade detail in message."""
    r = check_pnl_sanity(_mock_conn(fetchall_value=[(42, 750.0)]))
    assert r["status"] == "WARN"
    assert "750" in r["message"]
    assert r["value"] == pytest.approx(750.0)


@pytest.mark.fast
def test_pnl_sanity_largest_trade_first():
    """Value reflects the largest outlier (first row, ordered by abs(pnl_usd) DESC)."""
    r = check_pnl_sanity(_mock_conn(fetchall_value=[(1, 900.0), (2, 600.0)]))
    assert r["value"] == pytest.approx(900.0)


# ── check_slippage_sanity ─────────────────────────────────────────────────────

@pytest.mark.fast
def test_slippage_sanity_no_data():
    """Row count = 0 → INFO."""
    r = check_slippage_sanity(_mock_conn(fetchone_value=(None, None, 0)))
    assert r["check"] == "slippage_sanity"
    assert r["status"] == "INFO"


@pytest.mark.fast
def test_slippage_sanity_normal():
    """Avg slippage ≤ 6 ticks on both sides → PASS."""
    r = check_slippage_sanity(_mock_conn(fetchone_value=(2.5, 3.0, 10)))
    assert r["status"] == "PASS"
    assert r["value"] == pytest.approx(3.0)  # max(2.5, 3.0)


@pytest.mark.fast
def test_slippage_sanity_excessive_exit():
    """Exit slippage > 6 ticks → WARN, value is the worse side."""
    r = check_slippage_sanity(_mock_conn(fetchone_value=(4.0, 7.5, 10)))
    assert r["status"] == "WARN"
    assert r["value"] == pytest.approx(7.5)


@pytest.mark.fast
def test_slippage_sanity_excessive_entry():
    """Entry slippage > 6 ticks → WARN."""
    r = check_slippage_sanity(_mock_conn(fetchone_value=(6.5, 2.0, 5)))
    assert r["status"] == "WARN"
    assert r["value"] == pytest.approx(6.5)


# ── check_data_freshness ──────────────────────────────────────────────────────

@pytest.mark.fast
def test_data_freshness_no_ticks():
    """No ticks in DB → WARN without touching datetime."""
    r = check_data_freshness(_mock_conn(fetchone_value=(None,)))
    assert r["status"] == "WARN"
    assert "No ticks" in r["message"]
    assert r["value"] == -1


@pytest.mark.fast
def test_data_freshness_weekend_48h_pass():
    """Weekend + 48h-old last tick → PASS (72h grace window)."""
    sunday = dt_module.datetime(2026, 5, 3, 15, 0, 0, tzinfo=dt_module.timezone.utc)
    last_tick = sunday - dt_module.timedelta(hours=48)
    conn = _mock_conn(fetchone_value=(last_tick,))
    with patch.object(audit_daemon, "datetime") as mock_dt:
        mock_dt.now.return_value = sunday
        r = check_data_freshness(conn)
    assert r["status"] == "PASS"
    assert r["value"] == pytest.approx(48 * 3600, abs=1)


@pytest.mark.fast
def test_data_freshness_weekend_80h_warn():
    """Weekend + 80h-old last tick → WARN (exceeds 72h grace)."""
    sunday = dt_module.datetime(2026, 5, 3, 15, 0, 0, tzinfo=dt_module.timezone.utc)
    last_tick = sunday - dt_module.timedelta(hours=80)
    conn = _mock_conn(fetchone_value=(last_tick,))
    with patch.object(audit_daemon, "datetime") as mock_dt:
        mock_dt.now.return_value = sunday
        r = check_data_freshness(conn)
    assert r["status"] == "WARN"


@pytest.mark.fast
def test_data_freshness_rth_stale():
    """Weekday RTH (15:00 UTC) + 400s-old tick → WARN (threshold 300s)."""
    monday_rth = dt_module.datetime(2026, 5, 4, 15, 0, 0, tzinfo=dt_module.timezone.utc)
    last_tick = monday_rth - dt_module.timedelta(seconds=400)
    conn = _mock_conn(fetchone_value=(last_tick,))
    with patch.object(audit_daemon, "datetime") as mock_dt:
        mock_dt.now.return_value = monday_rth
        r = check_data_freshness(conn)
    assert r["status"] == "WARN"
    assert r["value"] == pytest.approx(400, abs=1)


@pytest.mark.fast
def test_data_freshness_rth_fresh():
    """Weekday RTH + 100s-old tick → PASS."""
    monday_rth = dt_module.datetime(2026, 5, 4, 15, 0, 0, tzinfo=dt_module.timezone.utc)
    last_tick = monday_rth - dt_module.timedelta(seconds=100)
    conn = _mock_conn(fetchone_value=(last_tick,))
    with patch.object(audit_daemon, "datetime") as mock_dt:
        mock_dt.now.return_value = monday_rth
        r = check_data_freshness(conn)
    assert r["status"] == "PASS"


@pytest.mark.fast
def test_data_freshness_offhours_18h_pass():
    """Weekday off-hours (03:00 UTC) + 17h-old tick → PASS (threshold 18h)."""
    monday_early = dt_module.datetime(2026, 5, 4, 3, 0, 0, tzinfo=dt_module.timezone.utc)
    last_tick = monday_early - dt_module.timedelta(hours=17)
    conn = _mock_conn(fetchone_value=(last_tick,))
    with patch.object(audit_daemon, "datetime") as mock_dt:
        mock_dt.now.return_value = monday_early
        r = check_data_freshness(conn)
    assert r["status"] == "PASS"


# ── EscalationEngine ──────────────────────────────────────────────────────────

@pytest.mark.fast
def test_escalation_warn_accumulation(escalation_engine):
    """3 WARNs for the same check within the window → escalates to ERROR."""
    engine, _ = escalation_engine
    warn = [{"check": "gap_count", "status": "WARN",
             "message": "65 gaps", "value": 65.0}]
    for _ in range(EscalationEngine.WARN_THRESHOLD):
        results = engine.process(warn, conn=None)
    assert results[0]["status"] == "ERROR"
    assert "escalated" in results[0]["message"]


@pytest.mark.fast
def test_escalation_below_threshold_stays_warn(escalation_engine):
    """WARN_THRESHOLD - 1 WARNs → stays WARN (not escalated)."""
    engine, _ = escalation_engine
    warn = [{"check": "gap_count", "status": "WARN",
             "message": "65 gaps", "value": 65.0}]
    for _ in range(EscalationEngine.WARN_THRESHOLD - 1):
        results = engine.process(warn, conn=None)
    assert results[0]["status"] == "WARN"


@pytest.mark.fast
def test_escalation_info_never_escalates(escalation_engine):
    """INFO results are exempt — they never accumulate toward ERROR."""
    engine, _ = escalation_engine
    info = [{"check": "cpp_tests", "status": "INFO",
             "message": "ctest not found", "value": 0}]
    for _ in range(20):
        results = engine.process(info, conn=None)
    assert results[0]["status"] == "INFO"
    assert "cpp_tests" not in engine._warn_ts


@pytest.mark.fast
def test_escalation_no_halt_for_escalated_critical(escalation_engine):
    """Check escalated to CRITICAL via WARN accumulation must NOT write AUDIT_HALT.

    Only native_critical=True (actual value mismatch) triggers the sentinel.
    An escalated WARN lacks native_critical, so the file must stay absent.
    """
    engine, halt_file = escalation_engine
    # Seed a pre-existing ERROR so elapsed time triggers CRITICAL promotion
    engine._error_since["trading_constants"] = time.time() - EscalationEngine.ERROR_CRITICAL_SEC - 60
    warn = [{"check": "trading_constants", "status": "WARN",
             "message": "Missing: tick_value not set", "value": 1.0}]
    for _ in range(EscalationEngine.WARN_THRESHOLD):
        engine.process(warn, conn=None)
    assert not halt_file.exists(), "AUDIT_HALT must not be written for escalated (non-native) CRITICALs"


@pytest.mark.fast
def test_escalation_halt_for_native_critical(escalation_engine):
    """trading_constants with native_critical=True → AUDIT_HALT written once."""
    engine, halt_file = escalation_engine
    crit = [{"check": "trading_constants", "status": "CRITICAL",
             "native_critical": True,
             "message": "MISMATCH: point_value=20.0 (must be 2.0 for MNQ)",
             "value": 1.0}]
    engine.process(crit, conn=None)
    assert halt_file.exists()
    data = json.loads(halt_file.read_text())
    assert data["check"] == "trading_constants"
    assert "MISMATCH" in data["message"]


@pytest.mark.fast
def test_escalation_halt_written_only_once(escalation_engine):
    """Second CRITICAL cycle does not corrupt an existing AUDIT_HALT file."""
    engine, halt_file = escalation_engine
    crit = [{"check": "trading_constants", "status": "CRITICAL",
             "native_critical": True, "message": "MISMATCH: point_value=20.0",
             "value": 1.0}]
    engine.process(crit, conn=None)
    first_content = halt_file.read_text()
    engine.process(crit, conn=None)
    assert halt_file.read_text() == first_content, "AUDIT_HALT must not be overwritten once written"


@pytest.mark.fast
def test_escalation_state_persistence(tmp_path, monkeypatch):
    """Warn timestamps are saved and loaded correctly — daemon restart does not amnesty WARNs."""
    monkeypatch.setattr(audit_daemon, "AUDIT_HALT", tmp_path / "AUDIT_HALT")
    state_file = tmp_path / "escalation_state.json"

    engine1 = EscalationEngine(state_file=state_file)
    warn = [{"check": "gap_count", "status": "WARN", "message": "gaps", "value": 65.0}]
    engine1.process(warn, conn=None)
    engine1.process(warn, conn=None)

    engine2 = EscalationEngine(state_file=state_file)
    assert "gap_count" in engine2._warn_ts
    assert len(engine2._warn_ts["gap_count"]) == 2


@pytest.mark.fast
def test_escalation_error_since_persists(tmp_path, monkeypatch):
    """error_since timestamps survive a daemon restart."""
    monkeypatch.setattr(audit_daemon, "AUDIT_HALT", tmp_path / "AUDIT_HALT")
    state_file = tmp_path / "escalation_state.json"

    engine1 = EscalationEngine(state_file=state_file)
    fail = [{"check": "rejection_rate", "status": "FAIL",
             "message": "10% rejected", "value": 10.0}]
    engine1.process(fail, conn=None)
    assert "rejection_rate" in engine1._error_since

    engine2 = EscalationEngine(state_file=state_file)
    assert "rejection_rate" in engine2._error_since


@pytest.mark.fast
def test_escalation_auto_resolve(escalation_engine):
    """CLEAN_RESOLVE consecutive all-PASS runs clear error_since, warn_ts, and alerted."""
    engine, _ = escalation_engine
    engine._error_since["gap_count"] = time.time() - 100
    engine._warn_ts["gap_count"] = [time.time() - 10]
    engine._alerted.add("warn_esc:gap_count")

    clean = [{"check": "gap_count", "status": "PASS", "message": "ok", "value": 0.0}]
    for _ in range(EscalationEngine.CLEAN_RESOLVE):
        engine.process(clean, conn=None)

    assert not engine._error_since
    assert not engine._warn_ts
    assert not engine._alerted
    assert engine._consecutive_clean == 0  # reset after resolve


@pytest.mark.fast
def test_escalation_clean_counter_resets_on_fail(escalation_engine):
    """A FAIL result resets the consecutive_clean counter to 0.

    Sub-threshold WARNs do not count as 'bad' and do not reset the counter —
    only FAIL/ERROR/CRITICAL do (these set any_bad=True inside process()).
    """
    engine, _ = escalation_engine
    engine._consecutive_clean = EscalationEngine.CLEAN_RESOLVE - 1  # one pass away from resolve
    fail = [{"check": "gap_count", "status": "FAIL", "message": "gaps", "value": 65.0}]
    engine.process(fail, conn=None)
    assert engine._consecutive_clean == 0


@pytest.mark.fast
def test_escalation_subthreshold_warn_does_not_reset_clean_counter(escalation_engine):
    """A single WARN (below threshold) does not set any_bad — clean counter keeps counting."""
    engine, _ = escalation_engine
    engine._consecutive_clean = 0
    clean = [{"check": "data_freshness", "status": "PASS", "message": "ok", "value": 5.0}]
    warn = [{"check": "gap_count", "status": "WARN", "message": "gaps", "value": 65.0}]
    engine.process(clean, conn=None)  # consecutive_clean → 1
    engine.process(warn, conn=None)   # sub-threshold WARN; any_bad still False → consecutive_clean → 2
    assert engine._consecutive_clean == 2


@pytest.mark.fast
def test_escalation_non_trading_constants_no_halt(escalation_engine):
    """native_critical=True on a non-HALT_CHECKS check does NOT write AUDIT_HALT."""
    engine, halt_file = escalation_engine
    crit = [{"check": "pnl_sanity", "status": "CRITICAL",
             "native_critical": True,
             "message": "MISMATCH: something",
             "value": 1.0}]
    engine.process(crit, conn=None)
    assert not halt_file.exists()


@pytest.mark.fast
def test_escalation_result_check_names_preserved(escalation_engine):
    """process() must return exactly one result per input, preserving check names."""
    engine, _ = escalation_engine
    inputs = [
        {"check": "data_freshness", "status": "PASS", "message": "ok", "value": 10.0},
        {"check": "gap_count", "status": "WARN", "message": "gaps", "value": 60.0},
        {"check": "cpp_tests", "status": "INFO", "message": "no ctest", "value": 0.0},
    ]
    results = engine.process(inputs, conn=None)
    assert len(results) == 3
    assert [r["check"] for r in results] == ["data_freshness", "gap_count", "cpp_tests"]
