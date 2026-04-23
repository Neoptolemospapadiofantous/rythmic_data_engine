# NQ ORB — Paper-to-Live Promotion Runbook

This document is the authoritative checklist for promoting the NQ ORB bot from paper trading (dry_run=true) to live trading (dry_run=false).

> **Rule:** dry_run=false must NEVER be set by hand-editing config. Use `go_live.py --confirm-live` only.

---

## Prerequisites

Before starting:
- [ ] Python 3.12+ virtual environment active (`source .venv/bin/activate`)
- [ ] `.env` file present with valid PG_HOST, PG_USER, PG_PASSWORD, PG_DB
- [ ] Oracle VM reachable (`ssh opc@<vm-ip>`)
- [ ] Rithmic credentials loaded in `.env` (RITHMIC_USER, RITHMIC_PASSWORD, RITHMIC_GATEWAY)
- [ ] Prop firm account funded (Legends 50K Master — check portal)

---

## Step 1 — Verify paper trading baseline (5 sessions)

Before promoting, the bot must complete 5 clean RTH sessions in dry_run=true mode with zero unexpected behaviour:

```bash
# Start a paper session
python live_trader.py --config config/live_config.json --dry-run

# Watch logs
tail -f data/logs/live_trader.log | python -c "import sys,json; [print(json.loads(l)) for l in sys.stdin]"
```

For each session verify in logs:
- `ORB_BUILDING` state fires at 09:30
- `WATCHING` state fires after 5 range bars
- Signal fires when breakout bar closes outside range
- `trade_open` log written with correct SL and target
- `trade_close` log written on SL/target hit or EOD
- `session_summary` row written at EOD

---

## Step 2 — Run kill tests

```bash
python -m pytest tests/ --ignore=tests/audit_engine.py -q
```

All tests must pass. 0 failures, 0 unexpected skips.

---

## Step 3 — Train or validate ML models

> Skip if using fixed ORB params (ml.enabled=false in live_config.json).

If ML is enabled, the model must be trained on recent data (≤30 days old):

```bash
# Run the ML pipeline (targeting <3h with profiling to find bottlenecks)
python scripts/pipeline_run.py --config config/live_config.json --profile

# After pipeline completes, update model checksums
python go_live.py --update-checksums
```

Verify:
- `models/orb_xgb_latest.pkl` size > 1KB (not a stub)
- `config/model_checksums.json` updated with non-PLACEHOLDER sha256 values

---

## Step 4 — Pre-flight dry run (all 9 gates)

```bash
# Run go_live.py WITHOUT --confirm-live first — just check gates
python go_live.py --config config/live_config.json
```

All 9 gates must show PASS:

| Gate | Check |
|------|-------|
| A | NO_DEPLOY lockfile absent |
| B | config/live_config.json valid JSON with required keys |
| C | dry_run currently True (paper mode) |
| D | PostgreSQL reachable |
| E | TLS certificate file present |
| F | ML model file + sha256 checksum |
| G | Prop firm daily loss limit > 0 |
| H | Prop firm max position size > 0 |
| I | Disk space > 5 GB free |

If any gate fails, fix the issue and re-run before proceeding.

---

## Step 5 — Update checksums (if models retrained)

Only needed when model files have changed since the last `--update-checksums` run:

```bash
python go_live.py --update-checksums
```

This writes the current sha256 hashes of all model files to `config/model_checksums.json`. Commit the updated checksums file.

---

## Step 6 — Promote to live

```bash
python go_live.py --config config/live_config.json --confirm-live
```

This command:
1. Re-runs all 9 gates
2. Atomically writes `dry_run: false` to `config/live_config.json` (via tempfile + rename)
3. Removes the NO_DEPLOY lockfile if present
4. Prints a promotion summary

> **Rollback:** If promotion fails mid-way, the config is restored to its previous state automatically. To manually rollback: `git checkout config/live_config.json`

---

## Step 7 — Deploy to Oracle VM

```bash
# Push binary, config, and service file; restart the systemd unit
bash deploy.sh push
```

The `push` subcommand:
- rsync's the C++ binary and `config/` to the VM
- Copies `deploy/live_trader.service` to `/etc/systemd/system/`
- Runs `systemctl daemon-reload && systemctl restart live_trader`

---

## Step 8 — Start the systemd service

If deploying fresh (first time on this VM):

```bash
ssh opc@<vm-ip>
sudo systemctl enable live_trader
sudo systemctl start live_trader
```

For subsequent restarts after a deploy:

```bash
sudo systemctl restart live_trader
```

---

## Step 9 — Verify the service started cleanly

```bash
# On the VM
sudo systemctl status live_trader
journalctl -u live_trader -f

# Expect within 60s:
# - "startup complete — entering trading loop"
# - "position_reconciliation: no open position found" (first start)
# - systemd reports: Active: active (running)
```

If `ExecStartPre` fails (NO_DEPLOY lockfile present), run `python scripts/no_deploy.py clear` before attempting to start.

---

## Step 10 — Monitor first live session

**Dashboard:** `python -m ui.app` (localhost:5050)

The dashboard shows:
- Current position (LONG/SHORT/FLAT), entry price, stop loss, open P&L
- Daily P&L vs limit
- Feed health: connection state, last tick time, consecutive reconnect failures
- Emergency stop button (SIGTERM → emergency flatten)

**Logs on VM:**
```bash
journalctl -u live_trader -f --output=json | python -c "
import sys, json
for line in sys.stdin:
    try:
        r = json.loads(line)
        msg = json.loads(r.get('MESSAGE', '{}'))
        print(msg.get('ts',''), msg.get('level',''), msg.get('msg',''))
    except: pass
"
```

**Key log events to watch:**

| Event | Meaning |
|-------|---------|
| `position_reconciliation: no open position` | Clean start, no previous position |
| `position_reconciliation: found open position` | Restarted mid-trade — state restored |
| `trade_open id=... dry_run=False` | Real order submitted (not paper) |
| `trade_close id=... reason=SL_OR_TARGET` | Stop or target hit |
| `EOD: flattening all positions` | Clean end-of-day |
| `emergency_flatten` | SIGTERM received — flatten initiated |
| `shutdown signal ... received` | Clean shutdown |

---

## Emergency procedures

### Kill the live trader immediately

```bash
# Via dashboard kill switch (localhost:5050) — triggers emergency flatten

# Or via systemd
sudo systemctl stop live_trader

# Or via signal if systemd is unresponsive
kill -SIGTERM $(cat data/live_trader.pid)
```

SIGTERM triggers `_emergency_flatten()` in live_trader.py which:
1. Closes any open position at latest tick price
2. Writes the session summary to DB
3. Exits cleanly

### Revert to paper mode

```bash
# On the VM — edit config atomically
python -c "
import json, tempfile, os
cfg = json.loads(open('config/live_config.json').read())
cfg['dry_run'] = True
with tempfile.NamedTemporaryFile(mode='w', suffix='.tmp', delete=False, dir='config') as f:
    json.dump(cfg, f, indent=2)
    tmp = f.name
os.replace(tmp, 'config/live_config.json')
print('dry_run set to True')
"
sudo systemctl restart live_trader
```

### Set NO_DEPLOY lockfile (prevent restart)

```bash
python scripts/no_deploy.py set "Manual hold — investigating position discrepancy"
```

### Check current NO_DEPLOY status

```bash
python scripts/no_deploy.py status
```

---

## Definition of done

The Coordinator signs off when all of the following are true:

- [ ] 5 clean paper sessions completed (RTH start to close)
- [ ] All kill tests pass (`pytest tests/ -q`)
- [ ] Position reconciliation tested with simulated mid-session restart
- [ ] Feature parity test green in CI (`pytest -m feature_parity`)
- [ ] C++/Python ORB parity test green in CI (`pytest -m orb_parity`)
- [ ] `go_live.py --confirm-live` exists and requires explicit flag
- [ ] `dry_run: false` set by script, never by hand-editing config
- [ ] Model checksums updated with real (non-stub) model files
