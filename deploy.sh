#!/bin/bash
# deploy.sh — Oracle Linux 9 setup AND push-to-VM automation for rithmic_engine
#
# Usage
# ─────
#   bash deploy.sh              # full first-time setup (run on Oracle VM itself)
#   bash deploy.sh setup        # same as above (explicit)
#   bash deploy.sh push [HOST]  # push binaries + config + service file, restart
#
# Environment variables for push mode
#   ORACLE_VM_HOST  — ssh target, e.g. opc@152.x.x.x (overridden by CLI arg)
#   ORACLE_VM_KEY   — path to SSH private key (default: ~/.ssh/id_rsa)
#
# push mode does NOT rebuild on the remote — it pushes the local build/ artefacts.
# Run `cmake --build build --target rithmic_engine` locally before pushing.

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

    # Verify local binary exists before pushing
    local binary="$SCRIPT_DIR/build/rithmic_engine"
    if [[ ! -f "$binary" ]]; then
        echo "ERROR: $binary not found — build first:"
        echo "  cmake --build $SCRIPT_DIR/build --target rithmic_engine"
        exit 1
    fi

    echo "=== Pushing to $host ==="
    local SSH_OPTS="-i $ssh_key -o StrictHostKeyChecking=no -o BatchMode=yes"

    # 1. Ensure remote directory structure exists
    echo "[1/5] Ensuring remote directories..."
    ssh $SSH_OPTS "$host" "mkdir -p $remote_dir/{build,config,certs,data/logs,deploy}"

    # 2. Push C++ binaries
    echo "[2/5] Pushing build artefacts..."
    rsync -az --delete-after \
        -e "ssh $SSH_OPTS" \
        "$SCRIPT_DIR/build/rithmic_engine" \
        "$SCRIPT_DIR/build/test_connection" \
        "$host:$remote_dir/build/"
    # Push orb_strategy binary only if it exists (separate CMake target)
    if [[ -f "$SCRIPT_DIR/build/orb_strategy" ]]; then
        rsync -az -e "ssh $SSH_OPTS" \
            "$SCRIPT_DIR/build/orb_strategy" \
            "$host:$remote_dir/build/"
    fi

    # 3. Push config, certs, and Python trading sources
    echo "[3/5] Pushing config, certs, and Python sources..."
    rsync -az \
        -e "ssh $SSH_OPTS" \
        "$SCRIPT_DIR/config/" \
        "$host:$remote_dir/config/"
    rsync -az \
        -e "ssh $SSH_OPTS" \
        "$SCRIPT_DIR/certs/" \
        "$host:$remote_dir/certs/"
    # Push Python trading files if they exist (live_trader.py built by Builder 3)
    for f in live_trader.py go_live.py; do
        if [[ -f "$SCRIPT_DIR/$f" ]]; then
            rsync -az -e "ssh $SSH_OPTS" "$SCRIPT_DIR/$f" "$host:$remote_dir/"
        fi
    done
    if [[ -d "$SCRIPT_DIR/strategy" ]]; then
        rsync -az --delete-after \
            -e "ssh $SSH_OPTS" \
            --exclude='*.pyc' --exclude='__pycache__' \
            "$SCRIPT_DIR/strategy/" \
            "$host:$remote_dir/strategy/"
    fi

    # 4. Install/update systemd service file
    echo "[4/5] Installing systemd unit..."
    rsync -az -e "ssh $SSH_OPTS" \
        "$SCRIPT_DIR/deploy/live_trader.service" \
        "$host:/tmp/live_trader.service"
    ssh $SSH_OPTS "$host" \
        "sudo mv /tmp/live_trader.service /etc/systemd/system/live_trader.service \
        && sudo chmod 644 /etc/systemd/system/live_trader.service \
        && sudo systemctl daemon-reload \
        && sudo systemctl enable live_trader"

    # 5. Restart the service if already running; leave it stopped otherwise to
    #    avoid unintended live trading on a fresh deploy.
    echo "[5/5] Checking service state..."
    if ssh $SSH_OPTS "$host" "sudo systemctl is-active --quiet live_trader 2>/dev/null"; then
        ssh $SSH_OPTS "$host" "sudo systemctl restart live_trader"
        echo "   Service restarted."
    else
        echo "   Service is stopped — not auto-starting (prevents accidental live trading)."
        echo "   To start:  ssh $host 'sudo systemctl start live_trader'"
    fi

    echo ""
    ssh $SSH_OPTS "$host" \
        "sudo systemctl status live_trader --no-pager -l 2>&1 | head -20" || true
    echo ""
    echo "=== Push complete → $host ==="
    echo "Logs:    ssh $host 'sudo journalctl -u live_trader -n 50 --no-pager'"
    echo "Start:   ssh $host 'sudo systemctl start live_trader'"
    echo "Stop:    ssh $host 'sudo systemctl stop live_trader'"
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

    # ── Build rithmic_engine ──────────────────────────────────────────────
    echo "[4/5] Building rithmic_engine..."
    cd ~/rithmic_engine
    rm -rf build
    mkdir build
    cd build
    cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DPostgreSQL_ROOT=/usr/pgsql-16 \
        -DBOOST_ROOT=/usr/local \
        -DBoost_NO_SYSTEM_PATHS=ON
    make -j"$(nproc)"
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
    echo "Test:  cd ~/rithmic_engine/build && ./test_connection ../.env"
    echo "Run:   cd ~/rithmic_engine/build && ./rithmic_engine ../.env"
    echo ""
    echo "To push updates from your workstation later:"
    echo "  bash deploy.sh push opc@<ip>"
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
