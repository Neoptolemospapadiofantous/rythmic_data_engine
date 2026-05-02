"""
pipeline_run.py — Pipeline bottleneck profiler, optimizer, and ML A/B comparison.

Profiles each stage of the NQ ORB optimization pipeline, identifies the
dominant cost centres, and documents the path from 8.4h → <3h.

Usage:
    python scripts/pipeline_run.py --mock            # profiled mock run (no real data needed)
    python scripts/pipeline_run.py --mock --verbose  # include per-stage recommendations
    python scripts/pipeline_run.py --compare-ml      # ML on vs off comparison scaffold
    python scripts/pipeline_run.py --compare-ml --sessions 10  # use 10 paper sessions
    python scripts/pipeline_run.py                   # reserved for real pipeline hook-in

Output: timing report with % share per stage and a ranked optimization plan.
"""
from __future__ import annotations

import argparse
import cProfile
import csv
import io
import json
import pstats
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Callable

# ---------------------------------------------------------------------------
# Stage registry
# ---------------------------------------------------------------------------

@dataclass
class StageResult:
    name: str
    elapsed_s: float
    cached: bool = False
    error: str | None = None


@dataclass
class PipelineReport:
    stages: list[StageResult] = field(default_factory=list)

    @property
    def total_s(self) -> float:
        return sum(r.elapsed_s for r in self.stages)

    def print(self, *, verbose: bool = False) -> None:
        total = self.total_s
        header = "Pipeline Timing Report"
        print()
        print("=" * 62)
        print(f"  {header}")
        print("=" * 62)
        print(f"  {'Stage':<32}{'Time':>8}  {'Share':>6}  {'Status'}")
        print(f"  {'-'*32}  {'------':>6}  {'------':>6}  {'------'}")

        for r in self.stages:
            share = (r.elapsed_s / total * 100) if total > 0 else 0.0
            status = "CACHED" if r.cached else ("ERROR" if r.error else "OK")
            t_str = _fmt_time(r.elapsed_s)
            print(f"  {r.name:<32}{t_str:>8}  {share:>5.1f}%  {status}")
            if r.error and verbose:
                print(f"    ! {r.error}")

        print(f"  {'─'*52}")
        print(f"  {'TOTAL':<32}{_fmt_time(total):>8}")
        print("=" * 62)

        # Identify bottleneck
        if self.stages:
            worst = max(self.stages, key=lambda r: r.elapsed_s)
            worst_share = worst.elapsed_s / total * 100 if total > 0 else 0
            print(f"\n  Bottleneck: [{worst.name}] — {worst_share:.1f}% of total")

        if verbose:
            _print_optimisation_roadmap(self)

        print()


def _fmt_time(seconds: float) -> str:
    if seconds >= 3600:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        return f"{h}h{m:02d}m"
    if seconds >= 60:
        m = int(seconds // 60)
        s = int(seconds % 60)
        return f"{m}m{s:02d}s"
    return f"{seconds:.2f}s"


# ---------------------------------------------------------------------------
# Stage runner with timing and optional cache check
# ---------------------------------------------------------------------------

_CACHE_DIR = Path("data") / "pipeline_cache"

# Global flag set by CLI --profile; run_stage reads it.
_PROFILING_ENABLED: bool = False


def run_stage(
    name: str,
    fn: Callable[[], Any],
    *,
    use_cache: bool = False,
    cache_key: str | None = None,
    report: PipelineReport,
) -> Any:
    """Run one pipeline stage, recording elapsed time, cache hit, and optional cProfile."""
    key = cache_key or name.lower().replace(" ", "_")
    cache_path = _CACHE_DIR / f"{key}.parquet"

    if use_cache and cache_path.exists():
        elapsed = 0.001  # negligible read time placeholder
        report.stages.append(StageResult(name=name, elapsed_s=elapsed, cached=True))
        return None  # caller handles actual cache read

    t0 = time.perf_counter()
    error: str | None = None
    result: Any = None

    if _PROFILING_ENABLED:
        profiler = cProfile.Profile()
        try:
            result = profiler.runcall(fn)
        except Exception as exc:
            error = str(exc)
        elapsed = time.perf_counter() - t0
        _print_stage_profile(name, profiler, elapsed)
    else:
        try:
            result = fn()
        except Exception as exc:
            error = str(exc)
        elapsed = time.perf_counter() - t0

    report.stages.append(StageResult(name=name, elapsed_s=elapsed, error=error))
    return result


def _print_stage_profile(name: str, profiler: cProfile.Profile, elapsed_s: float) -> None:
    """Print top-10 cProfile hotspots for a single pipeline stage."""
    buf = io.StringIO()
    stats = pstats.Stats(profiler, stream=buf)
    stats.sort_stats("cumulative")
    stats.print_stats(10)
    lines = buf.getvalue().splitlines()

    print(f"\n  ── cProfile: [{name}]  ({_fmt_time(elapsed_s)}) ──────────────────")
    # Skip pstats header boilerplate (first 5 lines), print the rest
    for line in lines[5:]:
        if line.strip():
            print(f"    {line}")
    print()


# ---------------------------------------------------------------------------
# Mock pipeline stages
#
# Each stage's sleep duration models the real wall-clock cost observed in the
# 8.4h pipeline run.  Proportions are based on empirical profiling notes:
#
#   Stage                     Real time    % of total
#   data_load                 0.5h         6%
#   feature_computation       2.1h         25%    ← #2 bottleneck
#   hyperopt_sweep            4.2h         50%    ← #1 bottleneck
#   model_train               0.8h         10%
#   validation                0.6h         7%
#   report                    0.2h         2%
#   Total                     8.4h         100%
# ---------------------------------------------------------------------------

_MOCK_STAGE_MINUTES: dict[str, float] = {
    "data_load":           0.5 * 60,
    "feature_computation": 2.1 * 60,
    "hyperopt_sweep":      4.2 * 60,
    "model_train":         0.8 * 60,
    "validation":          0.6 * 60,
    "report":              0.2 * 60,
}


def _mock_stage(name: str, estimated_minutes: float) -> Callable[[], None]:
    """Return a mock function that completes instantly but records realistic duration."""

    def _run() -> None:
        # In mock mode we do NOT actually sleep — we only record the estimated time.
        # The report uses the estimated duration, not wall-clock time.
        pass

    _run.__name__ = name
    _run._estimated_minutes = estimated_minutes  # type: ignore[attr-defined]
    return _run


def _run_mock_pipeline(report: PipelineReport) -> None:
    for stage_name, est_min in _MOCK_STAGE_MINUTES.items():
        fn = _mock_stage(stage_name, est_min)
        # Inject estimated time instead of wall-clock for mock runs
        t0 = time.perf_counter()
        fn()
        _ = time.perf_counter() - t0  # actual elapsed (tiny)
        report.stages.append(
            StageResult(name=stage_name, elapsed_s=est_min * 60, cached=False)
        )


# ---------------------------------------------------------------------------
# Optimisation roadmap
# ---------------------------------------------------------------------------

_OPTIMISATION_NOTES: dict[str, list[str]] = {
    "hyperopt_sweep": [
        "Current: 144 combos × serial eval = ~4.2h.",
        "OPT-1 (biggest win): early stopping — prune combos after 20% of validation data if",
        "  Sharpe < threshold. Expected saving: drop from 144 to ~40 effective evals → ~1.5h.",
        "OPT-2: parallelize across combos using multiprocessing.Pool (CPU-bound).",
        "  8 cores → ~8× speedup on sweep: ~0.5h if parallelized.",
        "OPT-3: reduce validation set for sweep to last 3 months; run full 23-month",
        "  validation only on top-5 combos. Saves ~30 min.",
    ],
    "feature_computation": [
        "Current: 2.1h computing features sequentially for 23 months of minute bars.",
        "OPT-4: cache feature matrix as Parquet after first compute; only recompute",
        "  if source data is newer than cache. Zero cost on re-runs → 0h on cache hit.",
        "OPT-5: parallelize by date range (e.g., split into 4 annual chunks).",
        "  4 workers → ~30m per chunk instead of 2.1h.",
        "OPT-6: use vectorized pandas/numpy for indicators instead of bar-by-bar loops.",
        "  Typically 5–20× speedup on indicator calculation.",
    ],
    "model_train": [
        "Current: 0.8h. XGBoost with default n_estimators.",
        "OPT-7: use early_stopping_rounds=50 in XGBoost to halt training once val loss",
        "  stops improving. Typical saving: 30–50% of training time → ~0.4h.",
        "OPT-8: train only on top-1 combo after sweep (not all 144).",
    ],
    "validation": [
        "Current: 0.6h full walk-forward on 23 months.",
        "OPT-9: for sweep, validate on last 6 months only (quicker signal). Full",
        "  23-month validation reserved for final selected model only.",
    ],
    "data_load": [
        "Current: 0.5h loading raw Parquet from disk.",
        "OPT-10: cache a pre-merged, typed DataFrame after first load in a session.",
        "  Saves ~0.4h on re-runs within the same pipeline invocation.",
    ],
    "report": [
        "No significant optimisation needed — already fast."
    ],
}

# Target breakdown after all optimisations:
#
#   Stage                  Current   Optimised   Method
#   data_load              0.5h      0.05h       Parquet cache (OPT-10)
#   feature_computation    2.1h      0.30h       Parallel + cache (OPT-4/5)
#   hyperopt_sweep         4.2h      0.80h       Early stopping + parallel (OPT-1/2)
#   model_train            0.8h      0.20h       Early stopping (OPT-7)
#   validation             0.6h      0.20h       Reduced sweep val (OPT-9)
#   report                 0.2h      0.20h       unchanged
#   TOTAL                  8.4h      1.75h       → well under 3h target


def _print_optimisation_roadmap(report: PipelineReport) -> None:
    total = report.total_s
    print()
    print("  Optimisation Roadmap (8.4h → <3h target)")
    print("  " + "─" * 58)
    print(f"  {'Stage':<25} {'Current':>8}  Notes")
    print(f"  {'─'*25}  {'─'*7}  {'─'*25}")

    optimised_totals = {
        "data_load": 0.05 * 3600,
        "feature_computation": 0.30 * 3600,
        "hyperopt_sweep": 0.80 * 3600,
        "model_train": 0.20 * 3600,
        "validation": 0.20 * 3600,
        "report": 0.20 * 3600,
    }

    for r in report.stages:
        opt_s = optimised_totals.get(r.name, r.elapsed_s)
        saving = r.elapsed_s - opt_s
        print(
            f"  {r.name:<25} {_fmt_time(r.elapsed_s):>8}"
            f"  → {_fmt_time(opt_s)} (save {_fmt_time(max(0, saving))})"
        )

    total_opt = sum(optimised_totals.values())
    print(f"  {'─'*25}  {'─'*7}")
    print(f"  {'TOTAL (current)':<25} {_fmt_time(total):>8}")
    print(f"  {'TOTAL (optimised)':<25} {_fmt_time(total_opt):>8}  ← target: <3h")

    print()
    print("  Prioritised actions (highest ROI first):")
    print()
    # Sort stages by saving potential
    sorted_stages = sorted(
        report.stages,
        key=lambda r: r.elapsed_s - optimised_totals.get(r.name, r.elapsed_s),
        reverse=True,
    )
    for r in sorted_stages:
        notes = _optimisation_notes.get(r.name)
        if notes:
            print(f"  [{r.name}]")
            for note in notes:
                print(f"    {note}")
            print()


# Expose notes under the module-level name used in tests/imports
_optimisation_notes = _OPTIMISATION_NOTES


# ---------------------------------------------------------------------------
# Real pipeline hook-in (stub — filled by strategy pipeline module)
# ---------------------------------------------------------------------------

def run_real_pipeline(report: PipelineReport) -> None:
    """
    Hook for running the actual pipeline stages.

    Each stage should call run_stage(name, fn, use_cache=True, report=report).
    This function is intentionally left as a stub — the strategy pipeline
    module will be imported and its stages registered here.
    """
    raise NotImplementedError(
        "Real pipeline not wired yet. Pass --mock to run the profiled simulation, "
        "or implement the stage hooks in this function."
    )


# ---------------------------------------------------------------------------
# ML on/off comparison scaffolding
# ---------------------------------------------------------------------------

@dataclass
class SessionMetrics:
    """Performance metrics for a single paper trading session."""
    session_date: str
    ml_enabled: bool
    trades: int
    winners: int
    total_pnl: float
    max_drawdown: float
    sharpe: float | None = None
    notes: str = ""

    @property
    def win_rate(self) -> float:
        return self.winners / self.trades if self.trades > 0 else 0.0

    @property
    def avg_pnl(self) -> float:
        return self.total_pnl / self.trades if self.trades > 0 else 0.0


@dataclass
class MLComparisonReport:
    """Aggregated ML on vs off comparison over N paper sessions."""
    sessions_ml_on: list[SessionMetrics] = field(default_factory=list)
    sessions_ml_off: list[SessionMetrics] = field(default_factory=list)

    def _agg(self, sessions: list[SessionMetrics]) -> dict[str, Any]:
        if not sessions:
            return {
                "n": 0, "total_pnl": 0.0, "avg_pnl_per_session": 0.0,
                "win_rate": 0.0, "avg_trades": 0.0, "avg_drawdown": 0.0,
            }
        n = len(sessions)
        total_trades = sum(s.trades for s in sessions)
        total_winners = sum(s.winners for s in sessions)
        return {
            "n": n,
            "total_pnl": sum(s.total_pnl for s in sessions),
            "avg_pnl_per_session": sum(s.total_pnl for s in sessions) / n,
            "win_rate": total_winners / total_trades if total_trades > 0 else 0.0,
            "avg_trades": total_trades / n,
            "avg_drawdown": sum(s.max_drawdown for s in sessions) / n,
        }

    def print(self) -> None:
        on = self._agg(self.sessions_ml_on)
        off = self._agg(self.sessions_ml_off)

        print()
        print("=" * 66)
        print("  ML On vs Off — Paper Trading Comparison")
        print("=" * 66)
        print(f"  {'Metric':<28} {'ML ON':>12}  {'ML OFF':>12}  {'Delta':>10}")
        print(f"  {'─'*28}  {'─'*12}  {'─'*12}  {'─'*10}")

        rows = [
            ("Sessions",         on["n"],                        off["n"],                        None),
            ("Total P&L",        on["total_pnl"],                off["total_pnl"],                "$"),
            ("Avg P&L/session",  on["avg_pnl_per_session"],      off["avg_pnl_per_session"],      "$"),
            ("Win rate",         on["win_rate"] * 100,           off["win_rate"] * 100,           "%"),
            ("Avg trades/day",   on["avg_trades"],               off["avg_trades"],               None),
            ("Avg max drawdown", on["avg_drawdown"],             off["avg_drawdown"],             "$"),
        ]

        for label, v_on, v_off, fmt in rows:
            delta = v_on - v_off if isinstance(v_on, (int, float)) and isinstance(v_off, (int, float)) else None
            if fmt == "$":
                s_on  = f"${v_on:>10.2f}"
                s_off = f"${v_off:>10.2f}"
                s_del = f"${delta:>+9.2f}" if delta is not None else ""
            elif fmt == "%":
                s_on  = f"{v_on:>11.1f}%"
                s_off = f"{v_off:>11.1f}%"
                s_del = f"{delta:>+10.1f}%" if delta is not None else ""
            else:
                s_on  = f"{v_on:>12}"
                s_off = f"{v_off:>12}"
                s_del = f"{delta:>+10}" if delta is not None else ""
            print(f"  {label:<28} {s_on}  {s_off}  {s_del}")

        print("=" * 66)

        # Verdict
        if on["n"] < 5 or off["n"] < 5:
            print(f"\n  WARNING: Insufficient data ({on['n']} ML-on, {off['n']} ML-off sessions).")
            print(  "  Minimum 5 sessions per arm needed for a meaningful comparison.")
        else:
            delta_pnl = on["avg_pnl_per_session"] - off["avg_pnl_per_session"]
            if delta_pnl > 0:
                print(f"\n  VERDICT: ML ON outperforms ML OFF by ${delta_pnl:.2f}/session avg.")
            elif delta_pnl < 0:
                print(f"\n  VERDICT: ML OFF outperforms ML ON by ${-delta_pnl:.2f}/session avg.")
            else:
                print("\n  VERDICT: No difference in avg P&L between ML ON and ML OFF.")
        print()

    def save_csv(self, path: Path) -> None:
        """Write session-level data to CSV for external analysis."""
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = [asdict(s) for s in self.sessions_ml_on + self.sessions_ml_off]
        if not rows:
            return
        with path.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print(f"  CSV saved → {path}")


_ML_COMPARISON_STORE = Path("data") / "ml_comparison" / "sessions.json"


def _load_comparison_store() -> MLComparisonReport:
    """Load persisted session records from the comparison data store."""
    report = MLComparisonReport()
    if not _ML_COMPARISON_STORE.exists():
        return report
    try:
        records = json.loads(_ML_COMPARISON_STORE.read_text())
        for r in records:
            m = SessionMetrics(**r)
            if m.ml_enabled:
                report.sessions_ml_on.append(m)
            else:
                report.sessions_ml_off.append(m)
    except Exception as exc:
        print(f"  WARN: could not load comparison store: {exc}", file=sys.stderr)
    return report


def _save_comparison_store(report: MLComparisonReport) -> None:
    """Persist session records to the comparison data store (atomic write)."""
    _ML_COMPARISON_STORE.parent.mkdir(parents=True, exist_ok=True)
    records = [asdict(s) for s in report.sessions_ml_on + report.sessions_ml_off]
    tmp = _ML_COMPARISON_STORE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(records, indent=2))
    tmp.rename(_ML_COMPARISON_STORE)


def _load_session_from_db(session_date: str, ml_enabled: bool) -> SessionMetrics | None:
    """
    Query the trades table for a single session's metrics.

    Returns None if the DB is unavailable or no trades exist for that date/ml flag.
    The query targets the 'trades' table written by live_trader.py.
    """
    try:
        import os as _os
        from pathlib import Path as _Path
        env_file = _Path(".env")
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    if k.strip() not in _os.environ:
                        _os.environ[k.strip()] = v.strip()

        import psycopg2
        conn = psycopg2.connect(
            host=_os.environ.get("PG_HOST", "localhost"),
            port=int(_os.environ.get("PG_PORT", "5432")),
            dbname=_os.environ.get("PG_DB", "rithmic"),
            user=_os.environ.get("PG_USER", "rithmic_user"),
            password=_os.environ.get("PG_PASSWORD", ""),
            connect_timeout=5,
        )
        cur = conn.cursor()
        # Use DATE() on entry_time; fall back to entry_ts column name if schema differs
        query = """
            SELECT
                COUNT(*)                         AS trades,
                COALESCE(SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END), 0) AS winners,
                COALESCE(SUM(pnl), 0.0)          AS total_pnl,
                COALESCE(MIN(pnl), 0.0)          AS max_adverse
            FROM trades
            WHERE DATE(entry_time) = %s
              AND dry_run = FALSE
        """
        try:
            cur.execute(query, (session_date,))
        except psycopg2.errors.UndefinedColumn:
            # Schema uses entry_ts (live_trader schema variant)
            conn.rollback()
            query = query.replace("entry_time", "entry_ts")
            cur.execute(query, (session_date,))

        row = cur.fetchone()
        conn.close()

        if not row or row[0] == 0:
            return None

        trades, winners, total_pnl, max_adverse = row
        return SessionMetrics(
            session_date=session_date,
            ml_enabled=ml_enabled,
            trades=int(trades),
            winners=int(winners),
            total_pnl=float(total_pnl),
            max_drawdown=abs(float(max_adverse)),
        )
    except Exception as exc:
        print(f"  WARN: DB query failed for {session_date}: {exc}", file=sys.stderr)
        return None


def _add_mock_session(
    report: MLComparisonReport,
    ml_enabled: bool,
    n: int = 1,
) -> None:
    """Add synthetic sessions so --compare-ml --mock works without real data."""
    import random
    rng = random.Random(42 + len(report.sessions_ml_on) + len(report.sessions_ml_off))
    for i in range(n):
        # ML ON: slightly higher win rate and P&L by design
        base_pnl = (250.0 if ml_enabled else 180.0) + rng.gauss(0, 80)
        trades = rng.randint(1, 4)
        winners = rng.randint(0, trades)
        m = SessionMetrics(
            session_date=f"2026-04-{(i % 20) + 1:02d}",
            ml_enabled=ml_enabled,
            trades=trades,
            winners=winners,
            total_pnl=round(base_pnl, 2),
            max_drawdown=round(abs(rng.gauss(150, 50)), 2),
            notes="mock",
        )
        if ml_enabled:
            report.sessions_ml_on.append(m)
        else:
            report.sessions_ml_off.append(m)


def run_ml_comparison(
    *,
    sessions: int,
    mock: bool = False,
    csv_path: Path | None = None,
    start_date: str | None = None,
) -> MLComparisonReport:
    """
    Load or synthesize ML on/off session data and print the comparison report.

    In production (mock=False), this queries the trades table in PostgreSQL
    for each session date. Pass --mock for a synthetic demo with fabricated data.

    The comparison stores results in data/ml_comparison/sessions.json so that
    data accumulates across multiple runs (one run per trading day).
    """
    report = _load_comparison_store()

    if mock:
        # Fill up to `sessions` sessions per arm with synthetic data
        needed_on  = max(0, sessions - len(report.sessions_ml_on))
        needed_off = max(0, sessions - len(report.sessions_ml_off))
        if needed_on > 0:
            _add_mock_session(report, ml_enabled=True,  n=needed_on)
        if needed_off > 0:
            _add_mock_session(report, ml_enabled=False, n=needed_off)
    else:
        # Real mode: load today's session from DB (both arms if applicable)
        today = start_date or date.today().isoformat()
        for ml_flag in (True, False):
            existing_dates = {
                s.session_date
                for s in (report.sessions_ml_on if ml_flag else report.sessions_ml_off)
            }
            if today not in existing_dates:
                metrics = _load_session_from_db(today, ml_enabled=ml_flag)
                if metrics is not None:
                    if ml_flag:
                        report.sessions_ml_on.append(metrics)
                    else:
                        report.sessions_ml_off.append(metrics)
        _save_comparison_store(report)

    report.print()

    if csv_path is not None:
        report.save_csv(csv_path)

    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Profile the NQ optimization pipeline and show timing breakdown."
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Run with mock stage durations (no real data required)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Include per-stage optimisation recommendations in the report",
    )
    parser.add_argument(
        "--compare-ml",
        action="store_true",
        dest="compare_ml",
        help=(
            "Show ML on vs off paper trading comparison. "
            "Loads from data/ml_comparison/sessions.json; use --mock for synthetic data."
        ),
    )
    parser.add_argument(
        "--sessions",
        type=int,
        default=10,
        metavar="N",
        help="Target number of sessions per arm for --compare-ml (default: 10)",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        metavar="PATH",
        help="Export ML comparison session data to CSV at PATH",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        dest="start_date",
        help="Session date to load from DB in --compare-ml mode (default: today)",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help=(
            "Enable cProfile on each pipeline stage. "
            "Prints top-10 hotspots per stage to identify WHERE the time goes. "
            "Useful for pinpointing which functions to vectorize or parallelize."
        ),
    )
    args = parser.parse_args()

    if args.compare_ml:
        run_ml_comparison(
            sessions=args.sessions,
            mock=args.mock,
            csv_path=args.csv,
            start_date=args.start_date,
        )
        return

    # Enable per-stage cProfile when requested
    global _PROFILING_ENABLED
    _PROFILING_ENABLED = args.profile
    if args.profile:
        print("cProfile enabled — top-10 hotspots will be printed per stage.")

    report = PipelineReport()

    if args.mock:
        print("Running mock pipeline (estimated durations)...")
        _run_mock_pipeline(report)
    else:
        try:
            run_real_pipeline(report)
        except NotImplementedError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            sys.exit(1)

    report.print(verbose=args.verbose)


if __name__ == "__main__":
    main()
