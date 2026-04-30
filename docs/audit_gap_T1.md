# T1: C++ Core Security Audit — Findings

**Date:** 2026-04-30  
**Auditor:** Builder 2 (BridgeSwarm)  
**Scope:** src/audit.cpp, src/audit.hpp, src/client.cpp, src/client.hpp, src/db.cpp, src/db.hpp, src/validator.hpp, src/wal.hpp, src/log.hpp, src/config.hpp, src/collector.cpp, src/collector.hpp, src/main.cpp, src/dashboard.cpp

---

## Summary

| Severity | Count |
|----------|-------|
| HIGH     | 3     |
| MED      | 7     |
| LOW      | 5     |
| **Total**| **15**|

---

## Findings

---

### F-01 — SQL Injection in `AuditLog::flush()` (event and severity fields)

- **File:Line:** `src/audit.cpp:80-83`
- **Severity:** HIGH
- **Category:** SQL Injection

**Description:**  
`flush()` builds a multi-row `INSERT` by string-concatenating `batch[i].event` and `batch[i].severity` without escaping or parameterization:

```cpp
sql += "('" + batch[i].ts + "',"
       "'" + batch[i].event + "',"       // ← no escaping
       "'" + batch[i].severity + "',"   // ← no escaping
       + esc_details(batch[i].details) + ")";
```

Only `details` has a hand-rolled single-quote escaper. `event` and `severity` are concatenated raw.
The public `AuditLog::log(event, details, severity)` API accepts arbitrary `std::string` event names from any caller. A caller passing `O''Brien'); DROP TABLE audit_log;--` as the event name would execute arbitrary SQL against the audit database connection.

**Recommended Fix:**  
Use `PQexecParams()` with parameterized queries, or at minimum apply the same `esc_details` escaper to `event` and `severity` as a stop-gap. Prefer the parameterized approach.

```cpp
// Parameterized INSERT with UNNEST — avoids all injection risks
const char* sql =
    "INSERT INTO audit_log (ts, event, severity, details)"
    " SELECT * FROM unnest($1::timestamptz[], $2::varchar[],"
    "                      $3::varchar[], $4::text[])";
```

---

### F-02 — PostgreSQL Password Exposed via `--status` Mode

- **File:Line:** `src/config.hpp:35-43`, `src/main.cpp:44`, `src/db.hpp:52`
- **Severity:** HIGH
- **Category:** Credential Exposure

**Description:**  
`pg_connstr()` builds a libpq keyword=value string containing the plaintext password:

```cpp
return "host=" + pg_host + " ... password=" + pg_password + " ...";
```

This string is stored in `DBSummary::connstr` and printed to stdout when running `./rithmic_engine --status`:

```cpp
std::printf("DB:       %s\n", s.connstr.c_str());  // prints password!
```

Any user who can run `--status` or redirect stdout can harvest the database password from the terminal/logs.

**Recommended Fix:**  
Either strip the password from `DBSummary::connstr` before storing it, or print a redacted version:

```cpp
// In DBSummary population:
// Store connstr without password, or replace password value with "***"
s.connstr = redact_pg_password(connstr_);
```

---

### F-03 — libpq Connection String Injection via Password Field

- **File:Line:** `src/config.hpp:35-43`
- **Severity:** HIGH
- **Category:** Connection String Injection

**Description:**  
`pg_connstr()` uses the old-style libpq keyword=value format without quoting or escaping values:

```cpp
" password=" + pg_password + " connect_timeout=10"
```

If `pg_password` contains a space followed by a keyword (e.g., `secret dbname=postgres`), libpq will parse `dbname=postgres` as an additional connection parameter, overriding the configured database. This allows connecting to an unintended PostgreSQL database.

More broadly: unquoted values in libpq keyword=value strings break if they contain spaces, backslashes, or single quotes. A password like `it's fine` would malform the connection string.

**Recommended Fix:**  
Use `PQconnectdbParams()` with separate arrays of keywords and values — values are never parsed for special characters:

```cpp
const char* keys[]   = {"host","port","dbname","user","password",
                         "connect_timeout","application_name", nullptr};
const char* values[] = {pg_host.c_str(), pg_port.c_str(), pg_db.c_str(),
                         pg_user.c_str(), pg_password.c_str(),
                         "10", "rithmic_engine", nullptr};
conn_ = PQconnectdbParams(keys, values, 0);
```

---

### F-04 — WAL Contains No Integrity Check (No CRC/Hash)

- **File:Line:** `src/wal.hpp:40-83`, `src/wal.hpp:99-124`
- **Severity:** MED
- **Category:** Data Integrity

**Description:**  
The WAL is plain CSV with no per-entry checksum, CRC, or HMAC. A partial disk write, bit flip, or filesystem corruption can produce a WAL entry that parses as a structurally valid `TickRow` with silently wrong values (e.g., corrupted price or timestamp). The parser only skips lines that throw an exception:

```cpp
} catch (...) {
    // Skip malformed/partial lines (tail of file at crash boundary)
}
```

Silent corruption that produces valid-looking numbers is not detected and will be replayed to the DB.

**Recommended Fix:**  
Append a CRC-32 or Adler-32 checksum to each WAL line. On replay, verify the checksum and discard lines that fail:

```
ts_micros,price,size,is_buy,symbol,exchange,crc32\n
```

---

### F-05 — Log Injection: `log.hpp` Does Not Sanitize Control Characters

- **File:Line:** `src/log.hpp:16-25`
- **Severity:** MED
- **Category:** Log Injection

**Description:**  
The `Logger::write()` method writes the message string directly to stdout and a log file without stripping newlines (`\n`, `\r`) or other control characters:

```cpp
void write(const std::string& msg) {
    auto line = timestamp() + " " + msg;
    std::puts(line.c_str());   // no sanitization
    // ...
    f << line << '\n';
}
```

Server-side data (Rithmic wire messages) is logged in several places. For example in `client.cpp:157`:

```cpp
throw std::runtime_error("Login failed: " + resp.rp_code(0));
```

If `resp.rp_code(0)` from the wire contains `\n[FAKE TIMESTAMP] INFO connection.established`, it would inject a fake log line into the log file. A compromised upstream server could use this to forge audit-relevant log entries.

**Recommended Fix:**  
Add a sanitizer in `Logger::write()` that replaces `\n`, `\r`, and other non-printable characters:

```cpp
static std::string sanitize(const std::string& s) {
    std::string out; out.reserve(s.size());
    for (unsigned char c : s)
        out += (c < 0x20 || c == 0x7f) ? '?' : (char)c;
    return out;
}
```

---

### F-06 — WAL Symbol/Exchange Not Quoted: Comma Injection Corrupts Replay

- **File:Line:** `src/wal.hpp:47-62`, `src/wal.hpp:99-124`
- **Severity:** MED
- **Category:** Data Integrity / Parsing

**Description:**  
The WAL writes `symbol` and `exchange` as bare unquoted CSV fields:

```cpp
buf += r.symbol;   // no quoting
buf += ',';
buf += r.exchange;
buf += '\n';
```

`TickValidator` only checks that symbol characters are in the printable ASCII range `[0x20, 0x7E]`. A comma (0x2C) is printable and passes validation. A symbol like `NQ,CME` would produce the WAL line:

```
1234567890,12345.00,10,1,NQ,CME,CME
```

The replay parser (`std::getline(ss, r.symbol, ',')`) would parse `symbol="NQ"`, `exchange="CME"` and silently drop the last field — or misparse the line structure entirely.

While no current Rithmic symbols contain commas, `TickValidator::valid()` should explicitly reject commas in symbol/exchange fields, or the WAL writer should quote fields.

**Recommended Fix:**  
Add comma/newline rejection to `TickValidator`, or quote fields in WAL writes.

---

### F-07 — Missing Audit Events: Connection Established, Auth Failure, Reconnect, Tick Rejection

- **File:Line:** `src/collector.cpp`, `src/client.cpp`
- **Severity:** MED
- **Category:** Audit Coverage Gap

**Description:**  
The audit log records `collector.start`, `collector.stop`, `connection.lost`, `wal.replay`, `ticks.written`, and write errors. The following operationally significant events are **not audited**:

| Missing Event | Location | Why It Matters |
|---|---|---|
| `connection.established` | `client.cpp:162` — only `LOG()`, no `audit_->info()` | No audit trail when connectivity is restored |
| `auth.failed` | `client.cpp:157` — only throws, no audit entry | Login failures from the broker are invisible in the audit table |
| `connection.reconnect_attempt` | `client.cpp:573` — only `LOG()` | Can't distinguish intentional stops from cascading failures in the audit table |
| `tick.rejected` | `collector.cpp:63-68` — only `LOG()` | Individual rejection reasons not stored; only aggregate counts in quality_metrics |
| `db.reconnect.success/fail` | `collector.cpp:89-93` — only `LOG()` | DB failover events are not audited |
| `wal.dirty_at_startup` | `collector.cpp:31` — only `LOG()` | Recovery path not captured in audit trail |

**Recommended Fix:**  
Add `audit_->info()` / `audit_->warn()` / `audit_->error()` calls at each of the above sites, following the same pattern as `ticks.written`.

---

### F-08 — BBO and Depth Data Have No WAL Protection

- **File:Line:** `src/collector.cpp:182-238`
- **Severity:** MED
- **Category:** Data Loss Risk

**Description:**  
Tick data is protected by a WAL: before each DB write, ticks are `fdatasync()`-ed to disk. If the process crashes between WAL write and DB commit, ticks are replayed on next start.

BBO (`flush_bbo()`) and Depth (`flush_depth()`) data have no such protection — they are written directly to the DB with no pre-write WAL. A crash between receiving BBO/depth and the DB flush loses that data permanently.

For a market-data recorder, BBO and depth data are equally important as tick data. Missing BBO snapshots create gaps in the bid-ask spread history; missing depth events leave the L3 order book reconstruction incomplete.

**Recommended Fix:**  
Extend WAL to support BBO and depth rows (with separate WAL files or a multiplexed WAL with a row-type prefix), or accept the BBO/depth loss risk explicitly in documentation.

---

### F-09 — No Unbounded WAL Growth Protection

- **File:Line:** `src/wal.hpp`, `src/collector.cpp:108-158`
- **Severity:** MED
- **Category:** Resource Exhaustion

**Description:**  
During a sustained DB outage, tick data accumulates in the WAL file without limit. There is no maximum WAL size check, no alerting when WAL grows beyond a threshold, and no circuit-breaker to stop accepting ticks. A prolonged outage (e.g., hours on a high-velocity feed) could fill the disk and crash the entire host.

**Recommended Fix:**  
Add a WAL size check in `flush()`. When the WAL exceeds a configurable limit (e.g., 100 MB), emit a `CRITICAL` sentinel alert and optionally stop accepting new ticks until the backlog clears.

---

### F-10 — `cmd_audit()` Uses Raw `PQexec` on a Read-Write Connection

- **File:Line:** `src/main.cpp:60-61`
- **Severity:** MED
- **Category:** Least-Privilege Violation

**Description:**  
The `--audit` CLI command creates a `TickDB` and calls `PQexec` directly on `db.conn()`:

```cpp
TickDB db(cfg.pg_connstr(), /*read_only=*/true);
PGresult* res = PQexec(db.conn(),
    "SELECT ts, severity, event, details FROM audit_log ORDER BY ts DESC LIMIT 20");
```

The `read_only=true` flag only prevents `ensure_schema()` from running — the underlying libpq connection is a full read-write connection to the database. Any code with access to `db.conn()` can execute arbitrary DML/DDL. The flag name is misleading and provides no actual access restriction.

**Recommended Fix:**  
To achieve true read-only access, connect with a PostgreSQL role that has only `SELECT` privileges, or append `options=-c default_transaction_read_only=on` to the connection string.

---

### F-11 — `DataSentinel::emit()` Uses `operator[]` Causing Unintended Map Insertion

- **File:Line:** `src/validator.hpp:215-216`
- **Severity:** LOW
- **Category:** Logic Bug

**Description:**  
In `emit()`, `elapsed` is computed using `last_alert_time_[check]` which **inserts** a default (epoch) time_point if the key is absent. The subsequent `last_alert_time_.count(check)` will then always return `1` (key now exists), defeating the intent of the existence check:

```cpp
double elapsed = std::chrono::duration<double>(
    now - last_alert_time_[check]).count();  // inserts default if absent
if (elapsed < cfg_.alert_cooldown_sec && last_alert_time_.count(check))
    return; // count() is always 1 after the line above
```

In practice this works correctly because: if the key was absent, `last_alert_time_[check]` returns epoch, `elapsed` is years-large, `elapsed < cooldown` is false, so we don't rate-limit (correct). But the logic is fragile and the intent is obscured. It also permanently pollutes the map with default entries for every unique check string.

**Recommended Fix:**  
Use `find()` to avoid the unintended insertion:

```cpp
auto it = last_alert_time_.find(check);
if (it != last_alert_time_.end()) {
    double elapsed = std::chrono::duration<double>(now - it->second).count();
    if (elapsed < cfg_.alert_cooldown_sec) return;
}
```

---

### F-12 — Password Not Validated: Empty/Whitespace Password Passes `Config::validate()`

- **File:Line:** `src/config.hpp:65-71`
- **Severity:** LOW
- **Category:** Input Validation

**Description:**  
`Config::validate()` only checks `password.empty()` for the Rithmic password and `pg_password.empty()` for the PostgreSQL password:

```cpp
if (password.empty())    errs.push_back("RITHMIC_AMP_PASSWORD not set");
if (pg_password.empty()) errs.push_back("PG_PASSWORD not set");
```

A whitespace-only string like `"   "` (common from misconfigured `.env` files after `trim()`) is not caught by `empty()`. The `trim()` function is called during `load_dotenv()`, so actual leading/trailing whitespace is stripped. However a value of literally `" "` (one space) would survive trim and still be reported as non-empty, causing a confusing authentication failure downstream rather than a clear configuration error.

Additionally, `cert_path` is not validated — if the file doesn't exist, the error surfaces only at TLS handshake time with a cryptic OpenSSL error.

**Recommended Fix:**  
Check `password.find_first_not_of(" \t") == std::string::npos` and validate that `cert_path` exists.

---

### F-13 — `TickValidator` Does Not Reject Comma or Newline in Symbol/Exchange

- **File:Line:** `src/validator.hpp:41-49`
- **Severity:** LOW
- **Category:** Input Validation Gap

**Description:**  
`TickValidator::valid()` accepts any printable ASCII character (0x20–0x7E) in `symbol` and `exchange`:

```cpp
for (char c : r.symbol)
    if (c < 0x20 || c > 0x7e) return fail("symbol non-printable");
```

Commas (0x2C) and spaces (0x20) pass this check but would corrupt the WAL CSV format (see F-06) and could cause issues in SQL strings that construct queries using symbol values. Expected valid symbols are 2–6 uppercase alphanumeric characters (e.g. `NQ`, `ES`, `CL`, `RTY`).

**Recommended Fix:**  
Tighten the character allowlist to `[A-Z0-9]` for symbol and `[A-Z0-9_]` for exchange:

```cpp
for (char c : r.symbol)
    if (!std::isupper(static_cast<unsigned char>(c)) &&
        !std::isdigit(static_cast<unsigned char>(c)))
        return fail("symbol contains invalid character");
```

---

### F-14 — Dashboard Pipeline Does Not Subscribe BBO/Depth Callbacks

- **File:Line:** `src/dashboard.cpp:444`
- **Severity:** LOW
- **Category:** Feature Gap / Audit Coverage

**Description:**  
The `Pipeline` struct in `dashboard.cpp` only registers the tick callback:

```cpp
client->set_on_tick([this](TickRow r) { on_tick(r); });
```

There is no `set_on_bbo()` or `set_on_depth()` registration. When the dashboard is running, all BBO and depth events received from Rithmic are silently discarded — they are not stored to the database. This means the dashboard mode provides no BBO or depth data collection despite the client being subscribed to both (see `client.cpp:557-558` where both are subscribed in `run()`).

**Recommended Fix:**  
Either register BBO/depth callbacks in `Pipeline` (or document that dashboard mode intentionally drops BBO/depth).

---

### F-15 — `localtime()` in `log.hpp` is Not Thread-Safe

- **File:Line:** `src/log.hpp:38`
- **Severity:** LOW
- **Category:** Thread Safety

**Description:**  
`Logger::timestamp()` uses `std::localtime()` which is not thread-safe (it returns a pointer to a shared static `tm` struct):

```cpp
std::strftime(buf, sizeof(buf), "[%Y-%m-%d %H:%M:%S]", std::localtime(&t));
```

`Logger::write()` holds `mu_` when calling `timestamp()` so concurrent writes are serialized, mitigating the race within this class. However, if `timestamp()` is ever called outside the lock (or if the logger is used in a context where `localtime()` is called concurrently from another translation unit), the result could be corrupted.

**Recommended Fix:**  
Use `std::gmtime_r()` (POSIX thread-safe) as already used in `audit.cpp` and `validator.hpp`:

```cpp
struct tm tm_local;
localtime_r(&t, &tm_local);
std::strftime(buf, sizeof(buf), "[%Y-%m-%d %H:%M:%S]", &tm_local);
```

---

## Audit Event Coverage Map

Events currently logged to `audit_log` table:

| Event | Severity | Code Location |
|---|---|---|
| `collector.start` | INFO | `collector.cpp:26` |
| `collector.stop` | INFO | `collector.cpp:355` |
| `wal.replay` | INFO | `collector.cpp:38` |
| `ticks.written` | INFO | `collector.cpp:132` |
| `ticks.write_error` | ERROR | `collector.cpp:156` |
| `bbo.write_error` | ERROR | `collector.cpp:197` |
| `depth.write_error` | ERROR | `collector.cpp:234` |
| `connection.lost` | ERROR | `collector.cpp:328` |

**Missing events (see F-07 for details):**

| Missing Event | Recommended Severity |
|---|---|
| `connection.established` | INFO |
| `auth.failed` | ERROR |
| `connection.reconnect_attempt` | WARN |
| `tick.rejected` | WARN |
| `db.reconnect.success` | INFO |
| `db.reconnect.failed` | ERROR |
| `wal.recovery_started` | WARN |
| `wal.size_threshold_exceeded` | CRITICAL |

---

## Positive Observations

- **TLS properly configured:** `ssl_ctx_.set_verify_mode(ssl::verify_peer)` and `ssl_ctx_.load_verify_file()` are both set — no `verify_none` shortcuts.
- **Tick data WAL is solid:** `fdatasync()` before DB write, replay on startup, dedup via `ON CONFLICT DO NOTHING`.
- **Parameterized queries everywhere except audit flush:** `write()`, `write_bbo()`, `write_depth()`, `start_session()`, `end_session()`, `write_metric()`, `write_sentinel_alerts()`, `write_gate_result()` all use `PQexecParams()`.
- **Thread safety:** All shared state in `Collector` and `DataSentinel` is properly mutex-protected.
- **Input validation:** `TickValidator` provides a solid structural gate for the hot path.
- **Buffer overflow protection:** All `snprintf` calls use properly bounded buffer sizes.
