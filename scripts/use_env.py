#!/usr/bin/env python3
"""
use_env.py — Rithmic environment switcher.

Reads RITHMIC_ENV_{NAME}_ORDER_* / RITHMIC_ENV_{NAME}_MD_* blocks from .env
and copies the chosen env's creds into the active RITHMIC_LEGENDS_* / RITHMIC_AMP_*
aliases that the C++ executor and audit script actually read.

Usage:
    python3 scripts/use_env.py                # show current env + status
    python3 scripts/use_env.py test           # switch to test env
    python3 scripts/use_env.py legends        # switch to Legends live
    python3 scripts/use_env.py test --verify  # switch + run login test
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ENGINE_DIR = Path(__file__).parent.parent
ENV_FILE   = ENGINE_DIR / ".env"
CONFIG_DIR = ENGINE_DIR / "config"
ENVS_DIR   = CONFIG_DIR / "envs"

GREEN  = "\033[32m"
RED    = "\033[31m"
YELLOW = "\033[33m"
CYAN   = "\033[36m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

CONFIG_FILES = ["live_config.json", "MNQ_config.json", "MES_config.json", "MYM_config.json"]

# Active alias keys written back to .env
ORDER_ALIASES: dict[str, str] = {
    "USER":     "RITHMIC_LEGENDS_USER",
    "PASSWORD": "RITHMIC_LEGENDS_PASSWORD",
    "SYSTEM":   "RITHMIC_LEGENDS_SYSTEM",
    "URL":      "RITHMIC_LEGENDS_URL",
    "ACCOUNT":  "RITHMIC_LEGENDS_ACCOUNT",
}
MD_ALIASES: dict[str, str] = {
    "USER":     "RITHMIC_AMP_USER",
    "PASSWORD": "RITHMIC_AMP_PASSWORD",
    "SYSTEM":   "RITHMIC_AMP_SYSTEM",
    "URL":      "RITHMIC_AMP_URL",
}


def _parse_env(path: Path) -> dict[str, str]:
    """Return {key: value} from .env (ignores comments and blank lines)."""
    vals: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            vals[k.strip()] = v.strip()
    return vals


def _write_env_updates(path: Path, updates: dict[str, str]) -> None:
    """Update specific keys in .env preserving all comments and blank lines."""
    lines = path.read_text().splitlines()
    updated: set[str] = set()
    new_lines: list[str] = []

    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            k = stripped.split("=", 1)[0].strip()
            if k in updates:
                new_lines.append(f"{k}={updates[k]}")
                updated.add(k)
                continue
        new_lines.append(line)

    # Append keys not already present in the file
    for k, v in updates.items():
        if k not in updated:
            new_lines.append(f"{k}={v}")

    path.write_text("\n".join(new_lines) + "\n")


def _discover_envs(env_vals: dict[str, str]) -> dict[str, dict[str, dict[str, str]]]:
    """Scan for RITHMIC_ENV_{NAME}_ORDER_* and _MD_* keys.

    Returns {name_lower: {"ORDER": {...}, "MD": {...}}}.
    """
    envs: dict[str, dict] = {}

    for key, val in env_vals.items():
        if key.startswith("RITHMIC_ENV_"):
            rest = key[len("RITHMIC_ENV_"):]
            for plant_tag in ("_ORDER_", "_MD_"):
                if plant_tag in rest:
                    name, field = rest.split(plant_tag, 1)
                    name = name.lower()
                    plant = plant_tag.strip("_")
                    envs.setdefault(name, {"ORDER": {}, "MD": {}})
                    envs[name][plant][field] = val
                    break

    return envs


def _mask(val: str) -> str:
    return "***" if val else "(not set)"


def cmd_status(env_vals: dict[str, str], envs: dict) -> None:
    active = env_vals.get("RITHMIC_ACTIVE_ENV", "(not set)")
    print(f"\n{BOLD}Active Rithmic environment:{RESET}  {CYAN}{active}{RESET}")

    print(f"\n{BOLD}ORDER_PLANT aliases  (RITHMIC_LEGENDS_*){RESET}")
    for field, key in ORDER_ALIASES.items():
        val = env_vals.get(key, "(not set)")
        display = _mask(val) if field == "PASSWORD" else (val or "(not set)")
        print(f"  {key:<38} {display}")

    print(f"\n{BOLD}TICKER_PLANT aliases  (RITHMIC_AMP_*){RESET}")
    for field, key in MD_ALIASES.items():
        val = env_vals.get(key, "(not set)")
        display = _mask(val) if field == "PASSWORD" else (val or "(not set)")
        print(f"  {key:<38} {display}")

    if envs:
        print(f"\n{BOLD}Available environments:{RESET}  {', '.join(sorted(envs))}")
    else:
        print(
            f"\n{YELLOW}No RITHMIC_ENV_* keys found in .env — "
            f"populate them first (see Builder 3 .env restructure){RESET}"
        )
    print()


def cmd_switch(name: str, env_vals: dict[str, str], envs: dict) -> bool:
    """Apply env creds to active aliases and update config JSONs. Returns True on success."""
    name_lower = name.lower()
    name_upper = name.upper()

    if name_lower not in envs:
        print(f"{RED}ERROR:{RESET} environment '{name}' not found in .env")
        if envs:
            print(f"  Available: {', '.join(sorted(envs))}")
        else:
            print("  No RITHMIC_ENV_* keys found.")
        return False

    order = envs[name_lower]["ORDER"]
    md    = envs[name_lower]["MD"]

    updates: dict[str, str] = {"RITHMIC_ACTIVE_ENV": name_upper}
    for field, alias in ORDER_ALIASES.items():
        updates[alias] = order.get(field, "")
    for field, alias in MD_ALIASES.items():
        updates[alias] = md.get(field, "")

    _write_env_updates(ENV_FILE, updates)

    print(f"\n{GREEN}✓{RESET}  Switched to {BOLD}{name_upper}{RESET}")
    print(f"  ORDER_PLANT  → user={order.get('USER','?')}  system={order.get('SYSTEM','?')}")
    md_user = md.get("USER") or "(same as ORDER)"
    print(f"  TICKER_PLANT → user={md_user}  system={md.get('SYSTEM','?')}")

    # Apply per-env config overrides
    override = ENVS_DIR / f"{name_lower}.json"
    if override.exists():
        _apply_config_overrides(override)
    else:
        print(f"  {YELLOW}No config override at {override.relative_to(ENGINE_DIR)} — skipping{RESET}")

    return True


def _apply_config_overrides(override_file: Path) -> None:
    """Merge keys from override JSON into all config/*.json files."""
    try:
        overrides: dict = json.loads(override_file.read_text())
    except Exception as exc:
        print(f"  {YELLOW}WARN: could not read override file: {exc}{RESET}")
        return

    updated: list[str] = []
    for cfg_name in CONFIG_FILES:
        cfg_path = CONFIG_DIR / cfg_name
        if not cfg_path.exists():
            continue
        try:
            cfg: dict = json.loads(cfg_path.read_text())
            changed = False
            for k, v in overrides.items():
                if k == "prop_firm" and isinstance(v, dict) and isinstance(cfg.get("prop_firm"), dict):
                    # Deep merge nested prop_firm section
                    for pk, pv in v.items():
                        if pv != "" and cfg["prop_firm"].get(pk) != pv:
                            cfg["prop_firm"][pk] = pv
                            changed = True
                else:
                    # Skip empty-string overrides — they would clobber valid existing values
                    if v != "" and cfg.get(k) != v:
                        cfg[k] = v
                        changed = True
            if changed:
                cfg_path.write_text(json.dumps(cfg, indent=2) + "\n")
                updated.append(cfg_name)
        except Exception as exc:
            print(f"  {YELLOW}WARN: could not update {cfg_name}: {exc}{RESET}")

    if updated:
        print(f"  {GREEN}✓{RESET}  Config overrides applied → {', '.join(updated)}")
    else:
        print(f"  Config files: no changes needed (already up to date)")


def cmd_verify(name: str, envs: dict) -> None:
    """Run login test for ORDER_PLANT and TICKER_PLANT using selected env creds."""
    name_lower = name.lower()
    if name_lower not in envs:
        print(f"{RED}ERROR:{RESET} environment '{name}' not found")
        return

    order = envs[name_lower]["ORDER"]
    md    = envs[name_lower]["MD"]
    login_script = ENGINE_DIR / "scripts" / "test_rithmic_login.py"

    tests = [
        ("ORDER_PLANT",  order, "ORDER_PLANT"),
        ("TICKER_PLANT", md,    "TICKER_PLANT"),
    ]

    for label, creds, plant in tests:
        user     = creds.get("USER", "")
        password = creds.get("PASSWORD", "")
        system   = creds.get("SYSTEM", "")
        url      = creds.get("URL", "wss://ritpz01001.01.rithmic.com:443")

        if not user or not password:
            print(f"\n{YELLOW}SKIP {label}:{RESET} credentials not configured")
            continue

        print(f"\n{BOLD}── Verifying {label}{RESET}  user={user}  system={system}")
        proc = subprocess.run(
            [sys.executable, str(login_script),
             f"--user={user}",
             f"--password={password}",
             f"--system={system}",
             f"--plant={plant}",
             f"--url={url}"],
            capture_output=False,
        )
        if proc.returncode == 0:
            print(f"  {GREEN}PASS{RESET}  (exit 0)")
        else:
            print(f"  {YELLOW}CHECK OUTPUT ABOVE{RESET}  (exit {proc.returncode})")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rithmic environment switcher",
        usage="%(prog)s [env_name] [--verify]",
    )
    parser.add_argument(
        "env_name", nargs="?",
        help="Environment to activate (e.g. test, legends). Omit to show status.",
    )
    parser.add_argument(
        "--verify", action="store_true",
        help="Run login test after switching (requires network access to Rithmic)",
    )
    args = parser.parse_args()

    if not ENV_FILE.exists():
        print(f"{RED}ERROR:{RESET} .env not found at {ENV_FILE}")
        sys.exit(1)

    env_vals = _parse_env(ENV_FILE)
    envs     = _discover_envs(env_vals)

    if args.env_name is None:
        cmd_status(env_vals, envs)
        return

    switched = cmd_switch(args.env_name, env_vals, envs)
    if not switched:
        sys.exit(1)

    if args.verify:
        cmd_verify(args.env_name, envs)


if __name__ == "__main__":
    main()
