#!/bin/bash
# deploy.sh — Oracle Linux 9 full setup for rithmic_engine
# Run as: bash deploy.sh
set -e
echo "=== Rithmic Engine Deploy ==="

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
sudo -u postgres /usr/pgsql-16/bin/psql -d rithmic -c "CREATE EXTENSION IF NOT EXISTS timescaledb;" || true
echo "   Extension OK"

# ── Boost 1.83 from source (system has 1.75, need 1.82+) ─────────────
echo "[3/5] Building Boost 1.83 from source (takes ~10 min)..."
cd /tmp
if [ ! -f boost_1_83_0.tar.gz ]; then
    echo "Boost tarball not found at /tmp/boost_1_83_0.tar.gz"
    echo "Copy it from your local machine with:"
    echo "  scp /tmp/boost_1_83_0.tar.gz opc@<ip>:/tmp/"
    exit 1
fi
echo "   Tarball info: $(ls -lh boost_1_83_0.tar.gz)"
file boost_1_83_0.tar.gz | grep -q "gzip" || { echo "Tarball is corrupt — re-scp it"; exit 1; }
BOOST_DIR=$(tar tf boost_1_83_0.tar.gz 2>/dev/null | head -1 | cut -d/ -f1)
echo "   Will extract to: $BOOST_DIR"
rm -rf "$BOOST_DIR"
tar xf boost_1_83_0.tar.gz || { echo "Extraction failed"; exit 1; }
ls -d "$BOOST_DIR" || { echo "Dir missing after extract — tarball may be corrupt"; exit 1; }
cd "$BOOST_DIR"
cd boost_1_83_0
./bootstrap.sh --prefix=/usr/local \
    --with-libraries=system,thread,filesystem,regex,random,chrono,date_time,atomic 2>&1 | tail -3
./b2 install -j$(nproc) variant=release link=shared 2>&1 | tail -5
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
make -j$(nproc)
echo "   Build OK"

# ── .env ─────────────────────────────────────────────────────────────
echo "[5/5] Checking .env..."
if [ ! -f ~/rithmic_engine/.env ]; then
    echo "   WARNING: ~/rithmic_engine/.env not found — create it before running"
else
    echo "   .env found OK"
fi

echo ""
echo "=== Deploy complete ==="
echo "Test:  cd ~/rithmic_engine/build && ./test_connection ../.env"
echo "Run:   cd ~/rithmic_engine/build && ./rithmic_engine ../.env"
