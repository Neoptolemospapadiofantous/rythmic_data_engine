#!/bin/bash
# build.sh — compile rithmic_engine on Oracle Linux 9
set -e
cd "$(dirname "$0")"

rm -rf build
mkdir build
cd build

cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DPostgreSQL_ROOT=/usr/pgsql-16 \
    -DBOOST_ROOT=/usr/local \
    -DBoost_NO_SYSTEM_PATHS=ON \
    -DCMAKE_PREFIX_PATH=/usr/local

make -j$(nproc)
echo "=== Build complete ==="
ls -lh rithmic_engine dashboard test_connection 2>/dev/null || true
