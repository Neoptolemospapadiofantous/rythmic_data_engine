#!/bin/bash
# Full update + restart sequence for Oracle.
# Usage: bash deploy/update.sh
set -e
cd "$(dirname "$0")/.."

echo "==> Stopping service..."
sudo systemctl stop nq_executor 2>/dev/null || true

echo "==> Pulling latest code..."
git pull origin main

echo "==> Building..."
make -C build nq_executor -j"$(nproc)"

echo "==> Installing service unit..."
sudo cp deploy/nq_executor.service /etc/systemd/system/nq_executor.service
sudo systemctl daemon-reload

echo "==> Starting service..."
sudo systemctl enable nq_executor
sudo systemctl start nq_executor

echo "==> Done. Following logs (Ctrl-C to exit)..."
journalctl -u nq_executor -f
