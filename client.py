"""
client.py — Rithmic AMP connection (self-contained, no bot dependency).

Handles connection to Rithmic AMP for live tick streaming only.
Credentials come from .env in this directory.
"""
from __future__ import annotations

import os
import ssl
from dataclasses import dataclass
from pathlib import Path

from async_rithmic import RithmicClient

AMP_URL = "wss://ritpz01001.01.rithmic.com:443"


@dataclass
class RithmicConfig:
    user:        str = ""
    password:    str = ""
    system_name: str = "Rithmic 01"
    url:         str = AMP_URL
    app_name:    str = "nepa:OentexNQBot"
    app_version: str = "1.0"
    symbol:      str = "NQ"
    exchange:    str = "CME"

    @classmethod
    def from_env(cls, env_file: str = ".env") -> "RithmicConfig":
        env_path = Path(__file__).parent / env_file
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    if k.strip() not in os.environ:
                        os.environ[k.strip()] = v.strip()

        return cls(
            user=os.environ.get("RITHMIC_AMP_USER", ""),
            password=os.environ.get("RITHMIC_AMP_PASSWORD", ""),
            system_name=os.environ.get("RITHMIC_AMP_SYSTEM", "Rithmic 01"),
            url=os.environ.get("RITHMIC_AMP_URL", AMP_URL),
            app_name=os.environ.get("RITHMIC_APP_NAME", "nepa:OentexNQBot"),
            app_version=os.environ.get("RITHMIC_APP_VERSION", "1.0"),
            symbol=os.environ.get("RITHMIC_SYMBOL", "NQ"),
            exchange=os.environ.get("RITHMIC_EXCHANGE", "CME"),
        )

    def validate(self) -> list[str]:
        errors = []
        if not self.user:     errors.append("RITHMIC_AMP_USER not set")
        if not self.password: errors.append("RITHMIC_AMP_PASSWORD not set")
        return errors


def get_client(cfg: RithmicConfig) -> RithmicClient:
    errors = cfg.validate()
    if errors:
        raise ValueError(f"Config errors: {'; '.join(errors)}")

    client = RithmicClient(
        user=cfg.user,
        password=cfg.password,
        system_name=cfg.system_name,
        app_name=cfg.app_name,
        app_version=cfg.app_version,
        url=cfg.url,
    )
    client.ssl_context.check_hostname = False
    return client
