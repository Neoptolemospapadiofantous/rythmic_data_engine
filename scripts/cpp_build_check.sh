#!/bin/bash
# cpp_build_check.sh — verify C++ source compiles cleanly (incremental cmake build)
#
# Runs cmake --build on the existing build directory (does NOT reconfigure).
# If the build directory doesn't exist, skips gracefully with a warning.
# If cmake is not installed, skips gracefully.
# Exits 1 only on actual compilation errors.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$PROJECT_ROOT/build"

echo ""
echo "============================================================"
echo "  C++ BUILD CHECK"
echo "============================================================"

# Check cmake available
if ! command -v cmake &>/dev/null; then
    echo "  SKIP  cmake not found — install cmake to enable C++ build check"
    exit 0
fi

# Check build directory exists (cmake configure must have run first)
if [ ! -f "$BUILD_DIR/CMakeCache.txt" ]; then
    echo "  SKIP  build/ not configured — run 'cmake -B build' first to enable this check"
    exit 0
fi

echo "  Build dir: $BUILD_DIR"
echo "  Targets:   nq_executor orb_strategy"
echo ""

# Incremental build — only recompiles changed files
# Redirect stderr to stdout so errors appear inline; capture exit code
BUILD_LOG=$(mktemp)
FAILED=0

build_target() {
    local target="$1"
    echo "  Building $target ..."
    if cmake --build "$BUILD_DIR" --target "$target" --parallel 4 \
             2>&1 | tee -a "$BUILD_LOG" | grep -E "error:|warning:|Building|Linking" | \
             sed 's/^/    /'; then
        echo "  PASS  $target compiled"
    else
        echo "  FAIL  $target compilation failed (see above)"
        FAILED=$((FAILED + 1))
    fi
}

# Only build the executor targets — not the test binaries or dashboard
for target in nq_executor orb_strategy; do
    if cmake --build "$BUILD_DIR" --target "$target" --parallel 4 \
             >"$BUILD_LOG" 2>&1; then
        echo "  PASS  [$target] compiled successfully"
    else
        echo "  FAIL  [$target] compilation errors:"
        grep -E "error:|undefined" "$BUILD_LOG" | head -20 | sed 's/^/    /'
        FAILED=$((FAILED + 1))
    fi
done

rm -f "$BUILD_LOG"

echo ""
echo "  Status: $([ "$FAILED" -eq 0 ] && echo PASS || echo FAIL)"

exit $FAILED
