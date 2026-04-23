"""
backtest.py — Backtest entry point for NQ ORB feature engineering.

Re-exports compute_features from strategy.features so that
test_feature_parity.py can import it as `backtest.compute_features`.
"""
from strategy.features import compute_features  # noqa: F401

__all__ = ["compute_features"]
