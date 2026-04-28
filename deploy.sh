#!/bin/bash
# deploy.sh — Oracle Linux 9 setup AND push-to-VM automation for rithmic_engine
#
# Usage
# ─────
#   bash deploy.sh              # full first-time setup (run on Oracle VM itself)
#   bash deploy.sh setup        # same as above (explicit)
#   bash deploy.sh push [HOST]  # push C++ binaries + config + service file, restart
#
# Environment variables for push mode
#   ORACLE_VM_HOST  — ssh target, e.g. opc@152.x.x.x (overridden by CLI arg)
#   ORACLE_VM_KEY   — path to SSH private key (default: ~/.ssh/id_rsa)
#
# push mode does NOT rebuild on the remote — it pushes the local build/ artefacts.
# Run `cmake --build build --target nq_executor` locally before pushing.
# Oracle VM runs C++ only — no Python files are pushed.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── push subcommand ───────────────────────────────────────────────────────────
_do_push() {
    local host="${1:-${ORACLE_VM_HOST:-}}"
    local ssh_key="${ORACLE_VM_KEY:-$HOME/.ssh/id_rsa}"
    local remote_dir="/home/opc/rithmic_engine"

    if [[ -z "$host" ]]; then
        echo "ERROR: Oracle VM host required."
        echo "  Usage:  bash deploy.sh push opc@<ip>"
        echo "  Or set: export ORACLE_VM_HOST=opc@<ip>"
        exit 1
    fi

    # Verify nq_executor binary exists before pushing
    local binary="$SCRIPT_DIR/build/nq_executor"
    if [[ ! -f "$binary" ]]; then
        echo "ERROR: $binary not found — build first:"
        echo "  cmake --build $SCRIPT_DIR/build --target nq_executor"
        exit 1
    fi

    echo "=== Pushing to $host ==="
    local SSH_OPTS="-i $ssh_key -o StrictHostKeyChecking=no -o BatchMode=yes"

    # 1. Ensure remote directory structure exists
    echo "[1/4] Ensuring remote directories..."
    ssh $SSH_OPTS "$host" "mkdir -p $remote_dir/{build,config,certs,data/logs,deploy}"

    # 2. Push C++ binaries only — Oracle VM runs no Python
    echo "[2/4] Pushing C++ build artefacts..."
    rsync -az -e "ssh $SSH_OPTS" \
        "$SCRIPT_DIR/build/nq_executor" \
        "$host:$remote_dir/build/"
    # test_connection is useful for smoke-testing Rithmic credentials on the VM
    if [[ -f "$SCRIPT_DIR/build/test_connection" ]]; then
        rsync -az -e "ssh $SSH_OPTS" \
            "$SCRIPT_DIR/build/test_connection" \
            "$host:$remote_dir/build/"
    fi

    # 3. Push config and certs — JSON/cert files only, no Python
    echo "[3/4] Pushing config and certs..."
    rsync -az \
        --exclude='*.py' --exclude='__pycache__' --exclude='*.pyc' \
        -e "ssh $SSH_OPTS" \
        "$SCRIPT_DIR/config/" \
        "$host:$remote_dir/config/"
    rsync -az \
        -e "ssh $SSH_OPTS" \
        "$SCRIPT_DIR/certs/" \
        "$host:$remote_dir/certs/"

    # 4. Install/update systemd service files (single + template unit)
    echo "[4/4] Installing systemd units..."
    rsync -az -e "ssh $SSH_OPTS" \
        "$SCRIPT_DIR/deploy/nq_executor.service" \
        "$SCRIPT_DIR/deploy/nq_executor@.service" \
        "$host:/tmp/"
    ssh $SSH_OPTS "$host" \
        "sudo mv /tmp/nq_executor.service  /etc/systemd/system/nq_executor.service \
        && sudo mv /tmp/nq_executor@.service /etc/systemd/system/nq_executor@.service \
        && sudo chmod 644 /etc/systemd/system/nq_executor.service \
                          /etc/systemd/system/nq_executor@.service \
        && sudo systemctl daemon-reload \
        && sudo systemctl enable nq_executor"

    # Fix SELinux label on binary so systemd can exec it (Oracle Linux 9)
    echo "[SELinux] Relabelling nq_executor binary..."
    ssh $SSH_OPTS "$host" "sudo chcon -t bin_t $remote_dir/build/nq_executor || true"

    # Restart only if already running; leave stopped on fresh deploy to prevent
    # accidental live order flow before the operator has verified config.
    echo ""
    echo "--- Service state ---"
    if ssh $SSH_OPTS "$host" "sudo systemctl is-active --quiet nq_executor 2>/dev/null"; then
        ssh $SSH_OPTS "$host" "sudo systemctl restart nq_executor"
        echo "   Service restarted."
    else
        echo "   Service is stopped — not auto-starting (prevents accidental live trading)."
        echo "   To start:  ssh $host 'sudo systemctl start nq_executor'"
    fi

    echo ""
    ssh $SSH_OPTS "$host" \
        "sudo systemctl status nq_executor --no-pager -l 2>&1 | head -20" || true
    echo ""
    echo "=== Push complete → $host ==="
    echo "Logs:    ssh $host 'sudo journalctl -u nq_executor -n 50 --no-pager'"
    echo "Start:   ssh $host 'sudo systemctl start nq_executor'"
    echo "Stop:    ssh $host 'sudo systemctl stop nq_executor'"
}

# ── setup subcommand (first-time server setup, run on the Oracle VM itself) ──
_do_setup() {
    echo "=== Rithmic Engine Full Setup ==="

    # ── TimescaleDB repo + install ────────────────────────────────────────
    echo "[1/5] Installing TimescaleDB..."
    sudo tee /etc/yum.repos.d/timescale.repo > /dev/null <<'REPO'
[timescale_timescaledb]
name=timescale_timescaledb
baseurl=https://packagecloud.io/timescale/timescaledb/el/9/$basearch
repo_gpgcheck=1
gpgcheck=0
enabled=1
gpgkey=https://packagecloud.io/timescale/timescaledb/gpgkey
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
metadata_expire=300
REPO

    sudo dnf install -y timescaledb-2-postgresql-16
    sudo timescaledb-tune --quiet --yes --pg-config=/usr/pgsql-16/bin/pg_config
    sudo systemctl restart postgresql-16
    echo "   TimescaleDB OK"

    # ── TimescaleDB extension in DB ───────────────────────────────────────
    echo "[2/5] Enabling timescaledb extension..."
    sudo -u postgres /usr/pgsql-16/bin/psql -d rithmic -c \
        "CREATE EXTENSION IF NOT EXISTS timescaledb;" || true
    echo "   Extension OK"

    # ── Boost 1.83 from source (system has 1.75, need 1.82+) ─────────────
    echo "[3/5] Building Boost 1.83 from source (takes ~10 min)..."
    echo "   Disk space:"
    df -h /tmp ~ | tail -3
    cd ~
    if [ ! -f boost_1_83_0.tar.gz ]; then
        cp /tmp/boost_1_83_0.tar.gz . 2>/dev/null || {
            echo "Boost tarball not found. Copy it with:"
            echo "  scp /tmp/boost_1_83_0.tar.gz opc@<ip>:~/"
            exit 1
        }
    fi
    echo "   Tarball info: $(ls -lh boost_1_83_0.tar.gz)"
    file boost_1_83_0.tar.gz | grep -q "gzip" || \
        { echo "Tarball is corrupt — re-scp it"; exit 1; }
    rm -rf boost_1_83_0
    tar xf boost_1_83_0.tar.gz || { echo "Extraction failed"; exit 1; }
    ls -d boost_1_83_0 || { echo "Dir missing after extract"; exit 1; }
    cd boost_1_83_0
    ./bootstrap.sh --prefix=/usr/local \
        --with-libraries=system,thread,filesystem,regex,random,chrono,date_time,atomic \
        2>&1 | tail -3
    ./b2 install -j"$(nproc)" variant=release link=shared 2>&1 | tail -5
    sudo ldconfig
    echo "   Boost 1.83 OK"

    # ── Build nq_executor ─────────────────────────────────────────────────
    echo "[4/5] Building nq_executor..."
    cd ~/rithmic_engine
    rm -rf build
    mkdir build
    cd build
    cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DPostgreSQL_ROOT=/usr/pgsql-16 \
        -DBOOST_ROOT=/usr/local \
        -DBoost_NO_SYSTEM_PATHS=ON
    make -j"$(nproc)" nq_executor test_connection
    echo "   Build OK"

    # ── .env ─────────────────────────────────────────────────────────────
    echo "[5/5] Checking .env..."
    if [ ! -f ~/rithmic_engine/.env ]; then
        echo "   WARNING: ~/rithmic_engine/.env not found — create it before running"
    else
        echo "   .env found OK"
    fi

    echo ""
    echo "=== Setup complete ==="
    echo "Test:    cd ~/rithmic_engine/build && ./test_connection ../.env"
    echo "Service: sudo systemctl start nq_executor"
    echo "Logs:    sudo journalctl -u nq_executor -f"
    echo ""
    echo "To push updates from your workstation later:"
    echo "  cmake --build build --target nq_executor && bash deploy.sh push opc@<ip>"
}

# ── subcommand dispatch (must come after function definitions) ────────────────
CMD="${1:-setup}"
shift || true

case "$CMD" in
    push)  _do_push "$@"  ;;
    setup) _do_setup      ;;
    *)
        echo "Unknown command: $CMD"
        echo "Usage: bash deploy.sh [setup|push [HOST]]"
        exit 1
        ;;
esac
