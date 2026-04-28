#!/bin/bash
# Full update + restart sequence for Oracle.
# Usage: bash deploy/update.sh
set -e
cd "$(dirname "$0")/.."

echo "==> Stopping service..."
sudo systemctl stop nq_executor 2>/dev/null || true

echo "==> Stashing local Oracle edits..."
git stash 2>/dev/null || true

echo "==> Moving untracked config files out of the way..."
for f in config/MES_config.json config/MNQ_config.json config/MYM_config.json; do
    [ -f "$f" ] && mv "$f" /tmp/ && echo "  moved $f to /tmp/" || true
done

echo "==> Pulling latest code..."
git pull origin main

echo "==> Building..."
make -C build nq_executor -j"$(nproc)"

echo "==> Fixing SELinux label (Oracle Linux requires bin_t for systemd execution)..."
sudo chcon -t bin_t build/nq_executor

echo "==> Installing service unit..."
sudo cp deploy/nq_executor.service /etc/systemd/system/nq_executor.service
sudo systemctl daemon-reload

echo "==> Resetting failed state and starting..."
sudo systemctl reset-failed nq_executor 2>/dev/null || true
sudo systemctl enable nq_executor
sudo systemctl start nq_executor

echo "==> Done. Following logs (Ctrl-C to exit)..."
journalctl -u nq_executor -f
