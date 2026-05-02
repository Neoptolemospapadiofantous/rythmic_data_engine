"""
Unit tests for scripts/use_env.py helper functions.

Coverage:
  _parse_env          — normal, comments, blank lines, no-value lines
  _write_env_updates  — updates existing keys, appends missing, preserves comments
  _discover_envs      — finds ORDER and MD blocks, lowercases env names
  _mask               — password masking
  cmd_switch          — missing env returns False; existing env writes updates
  check_config_schema — PASS with valid live_config, WARN on invalid
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from use_env import (
    _parse_env,
    _write_env_updates,
    _discover_envs,
    _mask,
    cmd_switch,
)


# ── _parse_env ─────────────────────────────────────────────────────────────────


@pytest.mark.fast
def test_parse_env_basic(tmp_path):
    env = tmp_path / ".env"
    env.write_text("KEY1=value1\nKEY2=value2\n")
    result = _parse_env(env)
    assert result == {"KEY1": "value1", "KEY2": "value2"}


@pytest.mark.fast
def test_parse_env_ignores_comments(tmp_path):
    env = tmp_path / ".env"
    env.write_text("# This is a comment\nKEY=val\n")
    result = _parse_env(env)
    assert "KEY" in result
    assert len(result) == 1


@pytest.mark.fast
def test_parse_env_ignores_blank_lines(tmp_path):
    env = tmp_path / ".env"
    env.write_text("\n\nKEY=val\n\n")
    result = _parse_env(env)
    assert result == {"KEY": "val"}


@pytest.mark.fast
def test_parse_env_strips_whitespace(tmp_path):
    env = tmp_path / ".env"
    env.write_text("  KEY  =  value  \n")
    result = _parse_env(env)
    assert result.get("KEY") == "value"


@pytest.mark.fast
def test_parse_env_handles_value_with_equals(tmp_path):
    env = tmp_path / ".env"
    env.write_text("URL=https://example.com?a=b&c=d\n")
    result = _parse_env(env)
    assert result["URL"] == "https://example.com?a=b&c=d"


@pytest.mark.fast
def test_parse_env_empty_file(tmp_path):
    env = tmp_path / ".env"
    env.write_text("")
    result = _parse_env(env)
    assert result == {}


# ── _write_env_updates ────────────────────────────────────────────────────────


@pytest.mark.fast
def test_write_env_updates_existing_key(tmp_path):
    env = tmp_path / ".env"
    env.write_text("KEY=old_value\n")
    _write_env_updates(env, {"KEY": "new_value"})
    content = env.read_text()
    assert "KEY=new_value" in content
    assert "old_value" not in content


@pytest.mark.fast
def test_write_env_updates_preserves_comments(tmp_path):
    env = tmp_path / ".env"
    env.write_text("# My comment\nKEY=val\n")
    _write_env_updates(env, {"KEY": "new"})
    assert "# My comment" in env.read_text()


@pytest.mark.fast
def test_write_env_updates_appends_missing_key(tmp_path):
    env = tmp_path / ".env"
    env.write_text("EXISTING=val\n")
    _write_env_updates(env, {"NEW_KEY": "new_val"})
    content = env.read_text()
    assert "NEW_KEY=new_val" in content
    assert "EXISTING=val" in content


@pytest.mark.fast
def test_write_env_updates_multiple_keys(tmp_path):
    env = tmp_path / ".env"
    env.write_text("A=1\nB=2\n")
    _write_env_updates(env, {"A": "10", "B": "20"})
    content = env.read_text()
    assert "A=10" in content
    assert "B=20" in content


# ── _discover_envs ────────────────────────────────────────────────────────────


@pytest.mark.fast
def test_discover_envs_finds_order_block():
    env_vals = {
        "RITHMIC_ENV_LEGENDS_ORDER_USER": "myuser",
        "RITHMIC_ENV_LEGENDS_ORDER_PASSWORD": "secret",
        "RITHMIC_ENV_LEGENDS_ORDER_SYSTEM": "Rithmic Paper Trading",
    }
    envs = _discover_envs(env_vals)
    assert "legends" in envs
    assert envs["legends"]["ORDER"]["USER"] == "myuser"
    assert envs["legends"]["ORDER"]["PASSWORD"] == "secret"


@pytest.mark.fast
def test_discover_envs_finds_md_block():
    env_vals = {
        "RITHMIC_ENV_AMP_MD_USER": "amp_user",
        "RITHMIC_ENV_AMP_MD_SYSTEM": "Rithmic MD System",
    }
    envs = _discover_envs(env_vals)
    assert "amp" in envs
    assert envs["amp"]["MD"]["USER"] == "amp_user"


@pytest.mark.fast
def test_discover_envs_lowercases_name():
    env_vals = {"RITHMIC_ENV_MYENV_ORDER_USER": "u"}
    envs = _discover_envs(env_vals)
    assert "myenv" in envs
    assert "MYENV" not in envs


@pytest.mark.fast
def test_discover_envs_ignores_unrelated_keys():
    env_vals = {"PG_HOST": "localhost", "OTHER_KEY": "value"}
    envs = _discover_envs(env_vals)
    assert envs == {}


@pytest.mark.fast
def test_discover_envs_empty_dict():
    assert _discover_envs({}) == {}


# ── _mask ─────────────────────────────────────────────────────────────────────


@pytest.mark.fast
def test_mask_non_empty_returns_stars():
    assert _mask("secret123") == "***"


@pytest.mark.fast
def test_mask_empty_returns_not_set():
    assert _mask("") == "(not set)"


# ── cmd_switch ────────────────────────────────────────────────────────────────


@pytest.mark.fast
def test_cmd_switch_unknown_env_returns_false(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("")
    envs = {"legends": {"ORDER": {"USER": "u"}, "MD": {}}}
    # Patch ENV_FILE so it doesn't try to write to the real .env
    import use_env
    with patch.object(use_env, "ENV_FILE", env_file), \
         patch.object(use_env, "ENVS_DIR", tmp_path / "envs"):
        result = cmd_switch("nonexistent", {}, envs)
    assert result is False


@pytest.mark.fast
def test_cmd_switch_known_env_writes_updates(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "RITHMIC_LEGENDS_USER=\n"
        "RITHMIC_LEGENDS_PASSWORD=\n"
        "RITHMIC_LEGENDS_SYSTEM=\n"
        "RITHMIC_LEGENDS_URL=\n"
        "RITHMIC_LEGENDS_ACCOUNT=\n"
        "RITHMIC_AMP_USER=\n"
        "RITHMIC_AMP_PASSWORD=\n"
        "RITHMIC_AMP_SYSTEM=\n"
        "RITHMIC_AMP_URL=\n"
        "RITHMIC_ACTIVE_ENV=\n"
    )
    envs = {
        "legends": {
            "ORDER": {"USER": "testuser", "PASSWORD": "pw", "SYSTEM": "sys",
                      "URL": "url", "ACCOUNT": "acc"},
            "MD": {"USER": "mduser", "PASSWORD": "mdpw", "SYSTEM": "mdsys", "URL": "mdurl"},
        }
    }
    import use_env
    with patch.object(use_env, "ENV_FILE", env_file), \
         patch.object(use_env, "ENVS_DIR", tmp_path / "envs"), \
         patch.object(use_env, "ENGINE_DIR", tmp_path):
        result = cmd_switch("legends", {}, envs)
    assert result is True
    content = env_file.read_text()
    assert "RITHMIC_LEGENDS_USER=testuser" in content
    assert "RITHMIC_ACTIVE_ENV=LEGENDS" in content


# ── check_config_schema integration ──────────────────────────────────────────


@pytest.mark.fast
def test_config_schema_passes_for_valid_live_config():
    """check_config_schema must PASS for the real live_config.json."""
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from audit_daemon import check_config_schema
    cfg = json.loads((REPO_ROOT / "config" / "live_config.json").read_text())
    r = check_config_schema(cfg)
    assert r["status"] in ("PASS", "INFO"), f"Expected PASS or INFO, got: {r}"


@pytest.mark.fast
def test_config_schema_warns_for_invalid_config():
    """check_config_schema must WARN when point_value is wrong."""
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from audit_daemon import check_config_schema
    cfg = json.loads((REPO_ROOT / "config" / "live_config.json").read_text())
    cfg["point_value"] = 20.0  # NQ value — wrong for MNQ
    r = check_config_schema(cfg)
    assert r["status"] == "WARN", f"Expected WARN, got: {r}"
