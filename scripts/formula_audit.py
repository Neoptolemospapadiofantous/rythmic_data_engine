#!/usr/bin/env python3
"""
Formula Audit — verifies PnL constants, formulas, and prop-firm limits for MNQ ORB.

Checks:
  1. Constants from YAML match live_config.json (point_value, tick_size, tick_value, commission)
  2. PnL formula test vectors against logic extracted from live_trader.py
  3. Prop firm limits from live_config.json are sane

Usage:
  python scripts/formula_audit.py           # run all checks
  python scripts/formula_audit.py --json    # JSON output
"""
import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

import yaml

RULES_PATH = PROJECT_ROOT / "quality_rules" / "formula_audit.yaml"
CONFIG_PATH = PROJECT_ROOT / "config" / "live_config.json"


def load_rules() -> dict:
    with open(RULES_PATH) as f:
        return yaml.safe_load(f)


def load_live_config() -> dict:
    with open(CONFIG_PATH) as f:
        return json.load(f)


def _pass(check: str, msg: str) -> dict:
    return {"check": check, "severity": "INFO", "message": msg, "status": "PASS"}


def _fail(check: str, msg: str) -> dict:
    return {"check": check, "severity": "CRITICAL", "message": msg, "status": "FAIL"}


def _warn(check: str, msg: str) -> dict:
    return {"check": check, "severity": "WARN", "message": msg, "status": "WARN"}


def check_constants(rules: dict, cfg: dict) -> list[dict]:
    """Verify YAML constants match live_config.json orb section."""
    findings = []
    yaml_consts = rules.get("constants", {})
    orb = cfg.get("orb", {})

    point_value = orb.get("point_value")
    tick_size = orb.get("tick_size")
    expected_pv = yaml_consts.get("POINT_VALUE", 2.0)
    expected_ts = yaml_consts.get("TICK_SIZE", 0.25)
    expected_tv = yaml_consts.get("TICK_VALUE", 0.50)
    commission_per_side = yaml_consts.get("COMMISSION_PER_SIDE", 2.0)
    commission_rt = yaml_consts.get("COMMISSION_RT", 4.0)

    # POINT_VALUE
    if point_value is None:
        findings.append(_fail("constants_point_value", "orb.point_value missing from live_config.json"))
    elif abs(point_value - expected_pv) > 0.001:
        findings.append(_fail("constants_point_value",
            f"orb.point_value={point_value} != YAML {expected_pv} — wrong for MNQ"))
    else:
        findings.append(_pass("constants_point_value", f"orb.point_value={point_value} matches YAML"))

    # TICK_SIZE
    if tick_size is None:
        findings.append(_fail("constants_tick_size", "orb.tick_size missing from live_config.json"))
    elif abs(tick_size - expected_ts) > 0.0001:
        findings.append(_fail("constants_tick_size",
            f"orb.tick_size={tick_size} != YAML {expected_ts}"))
    else:
        findings.append(_pass("constants_tick_size", f"orb.tick_size={tick_size} matches YAML"))

    # TICK_VALUE = POINT_VALUE * TICK_SIZE
    if point_value is not None and tick_size is not None:
        derived_tv = point_value * tick_size
        if abs(derived_tv - expected_tv) > 0.001:
            findings.append(_fail("constants_tick_value",
                f"TICK_VALUE derived {derived_tv} != YAML {expected_tv} "
                f"(POINT_VALUE={point_value} * TICK_SIZE={tick_size})"))
        else:
            findings.append(_pass("constants_tick_value",
                f"TICK_VALUE={derived_tv} = {point_value} * {tick_size}"))

    # COMMISSION_RT = 2 * COMMISSION_PER_SIDE
    derived_crt = commission_per_side * 2
    if abs(commission_rt - derived_crt) > 0.001:
        findings.append(_fail("constants_commission_rt",
            f"COMMISSION_RT={commission_rt} != 2 * COMMISSION_PER_SIDE={commission_per_side} ({derived_crt})"))
    else:
        findings.append(_pass("constants_commission_rt",
            f"COMMISSION_RT={commission_rt} = 2 * {commission_per_side}"))

    return findings


def _calc_pnl_usd(entry: float, exit_price: float, direction: str, point_value: float) -> float:
    """Replicates live_trader.py _write_trade_close formula exactly."""
    if direction.upper() == "LONG":
        pts = exit_price - entry
    else:
        pts = entry - exit_price
    return pts * point_value


def check_pnl_formulas(rules: dict, cfg: dict) -> list[dict]:
    """Run PnL formula test vectors through the formula extracted from live_trader.py."""
    findings = []
    point_value = cfg.get("orb", {}).get("point_value", 2.0)
    tick_size = cfg.get("orb", {}).get("tick_size", 0.25)
    commission_rt = rules.get("constants", {}).get("COMMISSION_RT", 4.0)

    for formula in rules.get("formulas", []):
        fid = formula["id"]
        fname = formula.get("name", fid)

        for i, tc in enumerate(formula.get("test_cases", []), 1):
            label = f"{fid}_{fname}_tc{i}"

            # Gross PnL test: entry/exit/direction
            if "entry" in tc and "exit" in tc and "direction" in tc and "expected_gross" in tc:
                qty = tc.get("qty", 1)
                gross = _calc_pnl_usd(tc["entry"], tc["exit"], tc["direction"], point_value) * qty
                expected = tc["expected_gross"]
                if abs(gross - expected) > 0.01:
                    findings.append(_fail(label,
                        f"Gross PnL: entry={tc['entry']}, exit={tc['exit']}, "
                        f"dir={tc['direction']}, qty={qty} → got {gross:.4f}, expected {expected}"))
                else:
                    findings.append(_pass(label, f"Gross PnL {gross:.2f} == {expected}"))

            # PnL points test
            elif "entry" in tc and "exit" in tc and "direction" in tc and "expected_points" in tc:
                if tc["direction"].upper() == "LONG":
                    pts = tc["exit"] - tc["entry"]
                else:
                    pts = tc["entry"] - tc["exit"]
                expected = tc["expected_points"]
                if abs(pts - expected) > 0.001:
                    findings.append(_fail(label,
                        f"PnL points: got {pts}, expected {expected}"))
                else:
                    findings.append(_pass(label, f"PnL points {pts} == {expected}"))

            # Net PnL test: gross - commission
            elif "gross" in tc and "expected_net" in tc:
                crt = tc.get("commission_rt", commission_rt)
                net = tc["gross"] - crt
                expected = tc["expected_net"]
                if abs(net - expected) > 0.01:
                    findings.append(_fail(label,
                        f"Net PnL: gross={tc['gross']} - commission={crt} → got {net}, expected {expected}"))
                else:
                    findings.append(_pass(label, f"Net PnL {net:.2f} == {expected}"))

            # Points → ticks conversion
            elif "points" in tc and "expected_ticks" in tc:
                ticks = tc["points"] / tick_size
                expected = tc["expected_ticks"]
                if abs(ticks - expected) > 0.01:
                    findings.append(_fail(label,
                        f"Ticks: {tc['points']} / {tick_size} → got {ticks}, expected {expected}"))
                else:
                    findings.append(_pass(label, f"Ticks {ticks} == {expected}"))

            # SL price test
            elif "entry" in tc and "sl_points" in tc and "expected_sl" in tc:
                direction = tc.get("direction", "long")
                if direction.upper() == "LONG":
                    sl = tc["entry"] - tc["sl_points"]
                else:
                    sl = tc["entry"] + tc["sl_points"]
                expected = tc["expected_sl"]
                if abs(sl - expected) > 0.001:
                    findings.append(_fail(label,
                        f"SL price: entry={tc['entry']} sl_pts={tc['sl_points']} dir={direction} → got {sl}, expected {expected}"))
                else:
                    findings.append(_pass(label, f"SL price {sl} == {expected}"))

            # Target price test
            elif "entry" in tc and "target_points" in tc and "expected_target" in tc:
                direction = tc.get("direction", "long")
                if direction.upper() == "LONG":
                    tgt = tc["entry"] + tc["target_points"]
                else:
                    tgt = tc["entry"] - tc["target_points"]
                expected = tc["expected_target"]
                if abs(tgt - expected) > 0.001:
                    findings.append(_fail(label,
                        f"Target price: entry={tc['entry']} tgt_pts={tc['target_points']} dir={direction} → got {tgt}, expected {expected}"))
                else:
                    findings.append(_pass(label, f"Target price {tgt} == {expected}"))

    # Run golden test vectors
    for tv in rules.get("test_vectors", {}).get("vectors", []):
        vid = tv.get("id", "TV-?")
        label_base = tv.get("label", vid)

        if "gross_pnl" in tv and "entry" in tv and "exit" in tv:
            gross = _calc_pnl_usd(tv["entry"], tv["exit"], tv["direction"], point_value)
            qty = tv.get("qty", 1)
            gross *= qty
            if abs(gross - tv["gross_pnl"]) > 0.01:
                findings.append(_fail(f"{vid}_gross",
                    f"{label_base}: gross {gross:.2f} != {tv['gross_pnl']}"))
            else:
                findings.append(_pass(f"{vid}_gross",
                    f"{label_base}: gross {gross:.2f} == {tv['gross_pnl']}"))

        if "net_pnl" in tv and "gross_pnl" in tv:
            net = tv["gross_pnl"] - commission_rt
            if abs(net - tv["net_pnl"]) > 0.01:
                findings.append(_fail(f"{vid}_net",
                    f"{label_base}: net {net:.2f} != {tv['net_pnl']}"))
            else:
                findings.append(_pass(f"{vid}_net",
                    f"{label_base}: net {net:.2f} == {tv['net_pnl']}"))

        if "pnl_points" in tv and "entry" in tv and "exit" in tv:
            if tv["direction"].upper() == "LONG":
                pts = tv["exit"] - tv["entry"]
            else:
                pts = tv["entry"] - tv["exit"]
            if abs(pts - tv["pnl_points"]) > 0.001:
                findings.append(_fail(f"{vid}_points",
                    f"{label_base}: points {pts} != {tv['pnl_points']}"))
            else:
                findings.append(_pass(f"{vid}_points",
                    f"{label_base}: points {pts} == {tv['pnl_points']}"))

    return findings


def check_prop_firm_limits(cfg: dict) -> list[dict]:
    """Verify prop firm limits from live_config.json are sane."""
    findings = []
    pf = cfg.get("prop_firm", {})

    daily_loss = pf.get("daily_loss_limit", 0)
    trailing_dd = pf.get("trailing_drawdown_limit", 0)
    max_pos = pf.get("max_position_size", 0)
    consistency_pct = pf.get("consistency_rule_pct", 0)

    if daily_loss <= 0:
        findings.append(_fail("prop_firm_daily_loss",
            f"daily_loss_limit={daily_loss} must be > 0"))
    else:
        findings.append(_pass("prop_firm_daily_loss",
            f"daily_loss_limit={daily_loss} > 0"))

    if trailing_dd <= 0:
        findings.append(_fail("prop_firm_trailing_dd",
            f"trailing_drawdown_limit={trailing_dd} must be > 0"))
    elif trailing_dd <= daily_loss:
        findings.append(_fail("prop_firm_trailing_dd",
            f"trailing_drawdown_limit={trailing_dd} must be > daily_loss_limit={daily_loss}"))
    else:
        findings.append(_pass("prop_firm_trailing_dd",
            f"trailing_drawdown_limit={trailing_dd} > daily_loss_limit={daily_loss}"))

    if max_pos <= 0:
        findings.append(_fail("prop_firm_max_pos",
            f"max_position_size={max_pos} must be > 0"))
    else:
        findings.append(_pass("prop_firm_max_pos",
            f"max_position_size={max_pos} > 0"))

    if not (0 < consistency_pct < 1):
        findings.append(_warn("prop_firm_consistency",
            f"consistency_rule_pct={consistency_pct} expected between 0 and 1"))
    else:
        findings.append(_pass("prop_firm_consistency",
            f"consistency_rule_pct={consistency_pct} in (0, 1)"))

    # Sanity: daily_loss_limit should be < account size (50K Legends)
    if daily_loss > 5000:
        findings.append(_warn("prop_firm_daily_loss_sanity",
            f"daily_loss_limit={daily_loss} seems high for a 50K account"))
    else:
        findings.append(_pass("prop_firm_daily_loss_sanity",
            f"daily_loss_limit={daily_loss} reasonable for 50K account"))

    return findings


def check_config_invariants(cfg: dict) -> list[dict]:
    """Verify INV-006/007/008: trade_route, risk flat keys, max_daily_trades consistency."""
    findings = []

    # INV-006: trade_route must not be 'simulator'
    route = cfg.get("trade_route", "")
    if route == "simulator":
        findings.append(_fail("trade_route_not_simulator",
            "trade_route='simulator' — live orders will go to Rithmic paper system "
            "even with dry_run=False. Set to 'Rithmic Order Routing'."))
    else:
        findings.append(_pass("trade_route_not_simulator",
            f"trade_route='{route}' (not simulator)"))

    # INV-007: trailing_drawdown_cap and consistency_cap_pct flat keys must exist and match prop_firm
    prop = cfg.get("prop_firm", {})
    risk_checks = [
        ("trailing_drawdown_cap", "prop_firm.trailing_drawdown_limit",
         prop.get("trailing_drawdown_limit")),
        ("consistency_cap_pct",   "prop_firm.consistency_rule_pct",
         prop.get("consistency_rule_pct")),
    ]
    for flat_key, nested_label, nested_val in risk_checks:
        flat_val = cfg.get(flat_key)
        if flat_val is None:
            findings.append(_fail(f"risk_flat_{flat_key}",
                f"Flat '{flat_key}' missing — C++ risk_manager uses hardcoded default, "
                f"not driven by config"))
        elif nested_val is not None and abs(float(flat_val) - float(nested_val)) > 0.001:
            findings.append(_fail(f"risk_flat_{flat_key}",
                f"flat '{flat_key}'={flat_val} != {nested_label}={nested_val}"))
        else:
            label = f"={nested_val}" if nested_val is not None else ""
            findings.append(_pass(f"risk_flat_{flat_key}",
                f"flat '{flat_key}'={flat_val} matches {nested_label}{label}"))

    # INV-008: flat max_daily_trades must equal prop_firm.max_daily_trades
    flat_mdt = cfg.get("max_daily_trades")
    prop_mdt = prop.get("max_daily_trades")
    if flat_mdt is None:
        findings.append(_fail("max_daily_trades_match",
            "Flat 'max_daily_trades' missing — C++ uses default"))
    elif prop_mdt is None:
        findings.append(_warn("max_daily_trades_match",
            f"prop_firm.max_daily_trades missing — cannot verify flat={flat_mdt} is within limits"))
    elif flat_mdt != prop_mdt:
        findings.append(_fail("max_daily_trades_match",
            f"flat max_daily_trades={flat_mdt} != prop_firm.max_daily_trades={prop_mdt} — "
            f"C++ will allow {flat_mdt} trades but Legends limit is {prop_mdt}"))
    else:
        findings.append(_pass("max_daily_trades_match",
            f"flat max_daily_trades={flat_mdt} == prop_firm.max_daily_trades={prop_mdt}"))

    return findings


def main():
    parser = argparse.ArgumentParser(description="Formula Audit — MNQ ORB constants and PnL formulas")
    parser.add_argument("--json", action="store_true", help="JSON output")
    args = parser.parse_args()

    try:
        rules = load_rules()
    except FileNotFoundError:
        msg = f"YAML not found: {RULES_PATH} — run Builder 2 first (T1)"
        if args.json:
            print(json.dumps([{"check": "load_rules", "status": "SKIP",
                               "severity": "WARN", "message": msg}], indent=2))
        else:
            print(f"SKIP: {msg}")
        sys.exit(0)

    try:
        cfg = load_live_config()
    except FileNotFoundError:
        msg = f"Config not found: {CONFIG_PATH}"
        if args.json:
            print(json.dumps([{"check": "load_config", "status": "FAIL",
                               "severity": "CRITICAL", "message": msg}], indent=2))
        else:
            print(f"FAIL: {msg}")
        sys.exit(1)

    all_findings: list[dict] = []
    all_findings.extend(check_constants(rules, cfg))
    all_findings.extend(check_pnl_formulas(rules, cfg))
    all_findings.extend(check_prop_firm_limits(cfg))
    all_findings.extend(check_config_invariants(cfg))

    passed = sum(1 for f in all_findings if f["status"] == "PASS")
    failed = sum(1 for f in all_findings if f["status"] == "FAIL")
    warned = sum(1 for f in all_findings if f["status"] == "WARN")

    if args.json:
        print(json.dumps(all_findings, indent=2))
        sys.exit(1 if failed > 0 else 0)

    print(f"\n{'='*60}")
    print("  FORMULA AUDIT")
    print(f"  {passed} passed, {failed} failed, {warned} warnings, {len(all_findings)} total")
    print(f"{'='*60}")

    for f in all_findings:
        if f["status"] == "FAIL":
            print(f"  FAIL  [{f['check']}] {f['message']}")
    for f in all_findings:
        if f["status"] == "WARN":
            print(f"  WARN  [{f['check']}] {f['message']}")
    for f in all_findings:
        if f["status"] == "PASS":
            print(f"  PASS  [{f['check']}] {f['message']}")

    print(f"\n  Status: {'FAIL' if failed > 0 else 'PASS'}")
    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
