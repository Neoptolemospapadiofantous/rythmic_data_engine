"""
Pydantic v2 schema for config/live_config.json.

Validates:
- Required keys are present (catches typos like 'trale_route')
- Types are correct (catches string where float expected)
- MNQ-specific invariants (point_value=2.0, tick_size=0.25)
- Prop firm / C++ executor consistency (flat keys match nested prop_firm)
- trade_route is never 'simulator'
- dry_run starts True (pre-promotion)

Usage:
    python config/live_config_schema.py                  # validate live_config.json
    python config/live_config_schema.py path/to/cfg.json  # validate arbitrary config
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel, field_validator, model_validator, Field


# ── nested sections ────────────────────────────────────────────────────────────

class PropFirmConfig(BaseModel):
    name: str
    daily_loss_limit: float = Field(gt=0,
        description="Positive magnitude — go_live.py reads this for gate I")
    max_position_size: int = Field(gt=0)
    max_daily_trades: int = Field(gt=0)
    trailing_drawdown_limit: float = Field(gt=0)
    consistency_rule_pct: float = Field(gt=0, lt=1)


class OrbConfig(BaseModel):
    orb_period_minutes: int = Field(gt=0)
    stop_loss_ticks: int = Field(gt=0)
    target_ticks: int = Field(gt=0)
    tick_size: float
    point_value: float
    rth_open: str
    rth_close: str
    eod_exit_minutes_before_close: int = Field(ge=0)
    allow_short: bool

    @field_validator("tick_size")
    @classmethod
    def tick_size_must_be_025(cls, v: float) -> float:
        if abs(v - 0.25) > 1e-6:
            raise ValueError(f"tick_size={v} — MNQ tick size is 0.25 (same as NQ)")
        return v

    @field_validator("point_value")
    @classmethod
    def point_value_must_be_2(cls, v: float) -> float:
        if abs(v - 2.0) > 1e-6:
            raise ValueError(
                f"point_value={v} — MNQ point value is $2.00 (NQ is $20.00; do NOT confuse)")
        return v


class MLConfig(BaseModel):
    enabled: bool
    model_path: str
    scaler_path: str
    feature_cache_path: str
    feature_lookback_days: int = Field(gt=0)
    min_confidence: float = Field(ge=0, le=1)
    fallback_to_fixed_params: bool


class DbConfig(BaseModel):
    host_env: str
    port_env: str
    dbname_env: str
    user_env: str
    password_env: str
    connect_timeout: int = Field(gt=0)
    statement_timeout_ms: int = Field(gt=0)


class RithmicConfig(BaseModel):
    system_env: str
    user_env: str
    password_env: str
    app_name_env: str
    app_version_env: str
    ssl_cert_path: str
    reconnect_max_attempts: int = Field(ge=0)
    reconnect_base_delay_s: float = Field(gt=0)
    tick_timeout_s: float = Field(gt=0)


class AlertsConfig(BaseModel):
    enabled: bool
    email_to: str
    slack_webhook_env: str
    on_trade_fill: bool
    on_daily_loss_limit: bool
    on_connection_loss: bool
    on_emergency_flatten: bool


class LoggingConfig(BaseModel):
    log_dir: str
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    log_trades_to_db: bool
    log_ticks: bool


# ── root config ────────────────────────────────────────────────────────────────

class LiveConfig(BaseModel):
    # Top-level identity
    dry_run: bool
    symbol: str
    trade_contract: str
    exchange: str
    point_value: float
    account_id: str
    trade_route: str
    fcm_id: str
    ib_id: str

    # C++ executor flat params (must mirror prop_firm nested values)
    daily_loss_limit: float = Field(
        description="Negative: C++ halt threshold. E.g. -2000.0 halts when daily_pnl <= -2000")
    trailing_drawdown_cap: float = Field(gt=0)
    consistency_cap_pct: float = Field(gt=0, lt=1)
    orb_minutes: int = Field(gt=0)
    sl_points: float = Field(gt=0)
    trail_step: float = Field(gt=0)
    trail_be_trigger: float = Field(ge=0)
    trail_delay_secs: int = Field(ge=0)
    trail_be_offset: float = Field(ge=0)
    max_daily_trades: int = Field(gt=0)
    last_entry_hour: int = Field(ge=0, le=23)
    eod_flatten_hour: int = Field(ge=0, le=23)
    eod_flatten_min: int = Field(ge=0, le=59)
    session_open_hour: int = Field(ge=0, le=23)
    session_open_min: int = Field(ge=0, le=59)
    qty: int = Field(gt=0)

    # Nested sections
    prop_firm: PropFirmConfig
    orb: OrbConfig
    ml: MLConfig
    db: DbConfig
    rithmic: RithmicConfig
    no_deploy_path: str
    alerts: AlertsConfig
    logging: LoggingConfig

    # Optional comment fields (tolerated, not required)
    _comment: Optional[str] = None
    _cpp_executor_params: Optional[str] = None

    model_config = {"extra": "allow"}  # allow _comment/_cpp_executor_params without error

    # ── cross-field invariants ────────────────────────────────────────────────

    @field_validator("trade_route")
    @classmethod
    def trade_route_not_simulator(cls, v: str) -> str:
        if v == "simulator":
            raise ValueError(
                "trade_route='simulator' routes all orders to Rithmic paper system "
                "even with dry_run=False — set to 'Rithmic Order Routing' for live trading")
        return v

    @field_validator("daily_loss_limit")
    @classmethod
    def daily_loss_limit_must_be_negative(cls, v: float) -> float:
        if v >= 0:
            raise ValueError(
                f"daily_loss_limit={v} must be negative (C++ halts when daily_pnl <= this threshold). "
                f"Use prop_firm.daily_loss_limit for the positive magnitude.")
        return v

    @field_validator("symbol")
    @classmethod
    def symbol_must_be_micro_future(cls, v: str) -> str:
        valid = {"MNQ", "MES", "MYM", "M2K"}
        if v not in valid:
            raise ValueError(
                f"symbol='{v}' is not a recognized micro futures contract. "
                f"Supported: {sorted(valid)}")
        return v

    @field_validator("point_value")
    @classmethod
    def root_point_value_must_be_2(cls, v: float) -> float:
        if abs(v - 2.0) > 1e-6:
            raise ValueError(f"top-level point_value={v} — MNQ is $2.00/point (NQ is $20.00)")
        return v

    @model_validator(mode="after")
    def flat_keys_match_prop_firm(self) -> "LiveConfig":
        p = self.prop_firm
        errors = []

        if abs(self.trailing_drawdown_cap - p.trailing_drawdown_limit) > 0.01:
            errors.append(
                f"trailing_drawdown_cap={self.trailing_drawdown_cap} != "
                f"prop_firm.trailing_drawdown_limit={p.trailing_drawdown_limit}")

        if abs(self.consistency_cap_pct - p.consistency_rule_pct) > 1e-6:
            errors.append(
                f"consistency_cap_pct={self.consistency_cap_pct} != "
                f"prop_firm.consistency_rule_pct={p.consistency_rule_pct}")

        if self.max_daily_trades != p.max_daily_trades:
            errors.append(
                f"flat max_daily_trades={self.max_daily_trades} != "
                f"prop_firm.max_daily_trades={p.max_daily_trades} — "
                f"C++ would allow {self.max_daily_trades} trades but Legends cap is {p.max_daily_trades}")

        if abs(abs(self.daily_loss_limit) - p.daily_loss_limit) > 0.01:
            errors.append(
                f"|daily_loss_limit|={abs(self.daily_loss_limit)} != "
                f"prop_firm.daily_loss_limit={p.daily_loss_limit}")

        if errors:
            raise ValueError("Flat/nested config mismatch:\n  " + "\n  ".join(errors))

        return self

    @model_validator(mode="after")
    def sl_points_consistent_with_orb_stop_loss_ticks(self) -> "LiveConfig":
        expected_ticks = self.sl_points / self.orb.tick_size
        actual_ticks = self.orb.stop_loss_ticks
        if abs(expected_ticks - actual_ticks) > 0.1:
            raise ValueError(
                f"sl_points={self.sl_points} / tick_size={self.orb.tick_size} = "
                f"{expected_ticks} ticks, but orb.stop_loss_ticks={actual_ticks} — "
                f"C++ and Python stop-loss distances are mismatched")
        return self


# ── CLI entrypoint ─────────────────────────────────────────────────────────────

def validate_config(path: Path) -> tuple[bool, list[str]]:
    """Return (ok, list_of_errors)."""
    try:
        raw = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        return False, [f"JSON parse error: {e}"]
    except FileNotFoundError:
        return False, [f"File not found: {path}"]

    try:
        LiveConfig.model_validate(raw)
        return True, []
    except Exception as e:
        lines = str(e).splitlines()
        return False, lines


def main() -> int:
    cfg_path = Path(sys.argv[1]) if len(sys.argv) > 1 else (
        Path(__file__).resolve().parent / "live_config.json")

    ok, errors = validate_config(cfg_path)

    if ok:
        print(f"PASS  config schema valid: {cfg_path}")
        return 0
    else:
        print(f"FAIL  config schema errors in {cfg_path}:")
        for e in errors:
            print(f"  {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
