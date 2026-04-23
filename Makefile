.PHONY: test test-parallel test-fast test-parity install-dev

# Default: full sequential suite (safe for subprocess/SIGTERM tests)
test:
	python3 -m pytest tests/

# Parallel suite — excludes audit_engine.py (cwd-relative paths break under xdist workers)
# Uses worksteal scheduler so slow tests don't bottleneck fast workers
test-parallel:
	python3 -m pytest tests/ \
		--ignore=tests/audit_engine.py \
		-n auto \
		--dist=worksteal \
		-q

# Fast pre-commit subset: parity, preflight, live_trader state machine
# Must complete in <10s; no DB, no subprocess, no C++ binary required
test-fast:
	python3 -m pytest \
		-m "feature_parity or preflight or live_trader" \
		--ignore=tests/audit_engine.py \
		-q

# CI parity suite (C++/Python + feature vector agreement)
test-parity:
	python3 -m pytest -m "feature_parity or orb_parity" -v

# Install dev deps
install-dev:
	pip install -r requirements-dev.txt
