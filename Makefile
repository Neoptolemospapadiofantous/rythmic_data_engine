.PHONY: test test-unit test-fast test-parallel test-parity install-dev audit quality-gate hermes hermes-fast push-eod

# Default: full suite, sequential — safe for subprocess/SIGTERM tests.
# Includes scripts/kill_test_suite.py (see pytest.ini testpaths).
test:
	python3 -m pytest -q

# Pre-commit gate: fast unit tests only, no I/O, no subprocesses.
# Must complete in <2s. Run this before every commit.
test-unit:
	python3 -m pytest -m fast -q

# Pre-push gate: fast + preflight + parity (no subprocess, no DB, no C++ binary).
# Must complete in <10s.
test-fast:
	python3 -m pytest \
		-m "fast or feature_parity or preflight or live_trader" \
		-q

# Parallel CI suite — all tests except live, parallel workers via xdist.
# Requires: pip install pytest-xdist
test-parallel:
	python3 -m pytest \
		-n auto \
		--dist=worksteal \
		-q

# C++/Python signal parity only
test-parity:
	python3 -m pytest -m "feature_parity or orb_parity" -v

# Run all quality audit scripts (formula, cross-system, Python standards, C++ standards)
audit:
	python3 scripts/formula_audit.py
	python3 scripts/cross_system_audit.py
	python3 scripts/python_standards_check.py
	python3 scripts/cpp_standards_check.py
	@echo 'All audit gates passed'

# Full quality gate: fast tests + all audit checks
quality-gate:
	python3 -m pytest \
		-m "fast or feature_parity or preflight or live_trader" \
		-q
	$(MAKE) audit

# Install dev deps (includes flask, pytest-xdist, pytest-timeout)
install-dev:
	pip install -r requirements-dev.txt

# ── Hermes loop ────────────────────────────────────────────────────────────────
# Full check: tests + mypy + ruff + audit. Run this each iteration.
hermes:
	python3 scripts/hermes_session.py

# Fast check: tests (fast marks only) + mypy + ruff. Skips audit + full suite.
hermes-fast:
	python3 scripts/hermes_session.py --fast

# End-of-day push: must be fully green before this runs.
push-eod:
	@python3 scripts/hermes_session.py --fast || (echo "Not clean — fix before pushing." && exit 1)
	@echo "All gates pass. Pushing..."
	git push origin main
	@echo "Done. Oracle will pull on next deployment."
