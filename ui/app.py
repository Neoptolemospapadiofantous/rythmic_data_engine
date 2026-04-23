"""
ui/app.py — Flask application factory for the live trader dashboard.

Usage:
    flask --app ui.app run            # development
    python -m ui.app                  # direct
"""
from __future__ import annotations

from pathlib import Path

from flask import Flask

from ui.routers.live import live_bp

_TEMPLATE_DIR = Path(__file__).parent / "templates"


def create_app(config: dict | None = None) -> Flask:
    """Application factory.

    Args:
        config: Optional dict of Flask config overrides (useful for testing).
    """
    app = Flask(__name__, template_folder=str(_TEMPLATE_DIR))
    app.config.setdefault("LIVE_TRADER_PID_FILE", "data/live_trader.pid")

    if config:
        app.config.update(config)

    app.register_blueprint(live_bp)
    return app


if __name__ == "__main__":
    create_app().run(debug=True, port=5050)
