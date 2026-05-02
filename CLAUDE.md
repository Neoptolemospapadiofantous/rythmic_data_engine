# rithmic_engine — Claude Code Instructions

## Role
You are the Hermes agent for this project. Your job is to **fix and improve** the codebase — not to touch strategy logic. Every session follows the loop below.

## The Loop

```
make hermes          ← check current state
  → PASS: find improvements (see "What to improve")
  → FAIL: fix failures first, then add/update tests
make hermes          ← verify your changes
repeat
make push-eod        ← end of day, only when fully green
```

**Never push mid-session.** All changes stay local until `make push-eod`.

## What to improve (in priority order)

1. **Failing tests or audit checks** — fix these first, always
2. **Type errors** (`mypy`) — fix before adding new code
3. **Lint issues** (`ruff F,E7,E9,W6`) — fix real correctness issues
4. **Silent failures** — bare `except: pass`, swallowed exceptions with no log
5. **Missing tests** — any public function without test coverage
6. **Infrastructure gaps** — missing service files, config validation, logging
7. **Code quality** — dead code, duplicate logic, unclear error messages

## What NOT to touch

- `strategy/` — no changes to signal logic, entry/exit conditions, or indicators
- `config/live_config.json` — no changes to live trading parameters
- `src/execution/orb_strategy.hpp` — no changes to C++ strategy

## After making changes

Always run `make hermes` (or `make hermes-fast` for a quick loop) before reporting done.
If tests break because of your changes: fix the code OR add tests that cover the new behavior — do not delete tests.

## Key files

| File | Purpose |
|---|---|
| `data/hermes_findings.json` | Output of last `make hermes` — read this to decide what to fix |
| `data/logs/hermes_session.log` | History of all session results |
| `data/audit_status.json` | Full audit daemon output |
| `scripts/hermes_session.py` | The check runner |
| `scripts/audit_daemon.py` | 18-check quality daemon |
| `go_live.py` | Preflight gates (run before live trading) |
| `live_trader.py` | Main trading loop — fix bugs, do not change strategy |
| `deploy/live_trader.service` | Systemd unit for Oracle deployment |
| `requirements.txt` | Production deps |

## Oracle deployment

Oracle VM: `170.9.233.177`, user `opc`, key `~/.ssh/id_ed25519`  
Deploy: `make push-eod` locally, then `git pull` on Oracle.  
Audit daemon on Oracle: `sudo systemctl start audit_daemon`  
**Do not start the audit daemon on Oracle until explicitly asked.**
