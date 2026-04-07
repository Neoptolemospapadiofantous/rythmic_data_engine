"""
sync.py — Upload the DuckDB file to Cloudflare R2 (or any S3-compatible storage).

Snapshots rithmic.duckdb and uploads to R2 every 6 hours.
Always keeps a `rithmic_latest.duckdb` key so any machine can pull the freshest DB.

Cloudflare R2 setup:
    1. Create a bucket at dash.cloudflare.com → R2
    2. Create an API token with Object Read & Write permissions
    3. Fill in .env:
        R2_ACCOUNT_ID = abc123...          (32-char Cloudflare account ID)
        R2_ACCESS_KEY = your_key_id
        R2_SECRET_KEY = your_secret
        R2_BUCKET     = your-bucket-name
        R2_PREFIX     = rithmic_engine/    (optional)

Usage:
    python sync.py           # upload once now
    python sync.py --daemon  # upload every 6 hours
    python sync.py --status  # show upload history
    python sync.py --pull    # download latest DB from R2 (restore on new machine)
    python sync.py --dry-run # show what would upload
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

os.chdir(Path(__file__).parent)

from db import DB_PATH

STATE_FILE = Path("data/sync_state.json")
LOG_FILE   = Path("data/logs/sync.log")
SNAPSHOT   = Path("data/rithmic_snapshot.duckdb")
INTERVAL   = 6 * 3600  # 6 hours


# ── Logging ───────────────────────────────────────────────────────
def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


# ── State ─────────────────────────────────────────────────────────
def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"uploads": []}


def _save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    os.replace(tmp, STATE_FILE)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_env():
    env = Path(__file__).parent / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                if k.strip() not in os.environ:
                    os.environ[k.strip()] = v.strip()


# ── Cloudflare R2 client ──────────────────────────────────────────
def _r2_client():
    """boto3 client pointed at Cloudflare R2 (S3-compatible)."""
    import boto3
    account_id = os.environ.get("R2_ACCOUNT_ID", "").strip()
    access_key = os.environ.get("R2_ACCESS_KEY", "").strip()
    secret_key = os.environ.get("R2_SECRET_KEY", "").strip()
    if not account_id:
        raise ValueError("R2_ACCOUNT_ID not set in .env")
    if not access_key or not secret_key:
        raise ValueError("R2_ACCESS_KEY / R2_SECRET_KEY not set in .env")
    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
    )


def _bucket() -> str:
    b = os.environ.get("R2_BUCKET", "").strip()
    if not b:
        raise ValueError("R2_BUCKET not set in .env")
    return b


def _prefix() -> str:
    return os.environ.get("R2_PREFIX", "rithmic_engine/").strip("/") + "/"


# ── Upload ────────────────────────────────────────────────────────
def upload_once(dry_run: bool = False) -> bool:
    """Snapshot DB and upload to R2. Returns True on success."""
    try:
        r2     = _r2_client()
        bucket = _bucket()
        prefix = _prefix()
    except ValueError as e:
        log(f"ERROR: {e}")
        return False

    if not DB_PATH.exists():
        log("No DB yet — nothing to upload")
        return False

    ts      = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key     = f"{prefix}rithmic_{ts}.duckdb"
    latest  = f"{prefix}rithmic_latest.duckdb"
    size_mb = DB_PATH.stat().st_size / (1024 * 1024)

    log(f"Snapshotting {size_mb:.1f} MB → R2 bucket '{bucket}'")

    if dry_run:
        log(f"  [DRY RUN] would upload → r2://{bucket}/{key}")
        return True

    shutil.copy2(DB_PATH, SNAPSHOT)
    sha = _sha256(SNAPSHOT)

    try:
        r2.upload_file(str(SNAPSHOT), bucket, key,
                       ExtraArgs={"ContentType": "application/octet-stream"})
        # Keep 'latest' always pointing to the most recent snapshot
        r2.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": key},
            Key=latest,
        )
        SNAPSHOT.unlink(missing_ok=True)

        state = _load_state()
        state["uploads"].append({
            "ts":      datetime.now(timezone.utc).isoformat(),
            "key":     key,
            "size_mb": round(size_mb, 2),
            "sha256":  sha,
        })
        state["uploads"] = state["uploads"][-100:]
        _save_state(state)

        log(f"  Uploaded  → r2://{bucket}/{key}")
        log(f"  Latest    → r2://{bucket}/{latest}")
        return True

    except Exception as e:
        log(f"  Upload failed: {e}")
        SNAPSHOT.unlink(missing_ok=True)
        return False


# ── Download (restore on new machine) ────────────────────────────
def pull_latest(dest: Path = DB_PATH) -> bool:
    """Download the latest DB snapshot from R2."""
    try:
        r2     = _r2_client()
        bucket = _bucket()
        prefix = _prefix()
    except ValueError as e:
        log(f"ERROR: {e}")
        return False

    key = f"{prefix}rithmic_latest.duckdb"
    log(f"Downloading r2://{bucket}/{key} → {dest}")
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(".pull.tmp")
        r2.download_file(bucket, key, str(tmp))
        os.replace(tmp, dest)
        size_mb = dest.stat().st_size / (1024 * 1024)
        log(f"  Downloaded {size_mb:.1f} MB → {dest}")
        return True
    except Exception as e:
        log(f"  Download failed: {e}")
        return False


# ── Daemon ────────────────────────────────────────────────────────
def run_daemon():
    log(f"Sync daemon started (every {INTERVAL // 3600}h, bucket={os.environ.get('R2_BUCKET','?')})")
    while True:
        upload_once()
        time.sleep(INTERVAL)


def _status():
    state = _load_state()
    uploads = state.get("uploads", [])
    if not uploads:
        print("No uploads yet")
        return
    print(f"Last {min(len(uploads), 10)} uploads:")
    for u in uploads[-10:]:
        print(f"  {u['ts']}  {u['size_mb']:.1f} MB  → {u['key']}")


# ── CLI ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Rithmic DB → Cloudflare R2 sync")
    parser.add_argument("--daemon",  action="store_true", help="Run every 6 hours")
    parser.add_argument("--dry-run", action="store_true", help="Show what would upload")
    parser.add_argument("--status",  action="store_true", help="Show upload history")
    parser.add_argument("--pull",    action="store_true", help="Download latest DB from R2")
    args = parser.parse_args()

    _load_env()

    if args.status:
        _status()
    elif args.pull:
        pull_latest()
    elif args.daemon:
        run_daemon()
    else:
        upload_once(dry_run=args.dry_run)
