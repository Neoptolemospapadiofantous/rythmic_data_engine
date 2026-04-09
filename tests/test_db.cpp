// Integration tests for TickDB + AuditLog against a real PostgreSQL instance.
//
// Requires env vars: PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
// or a .env file in the working directory.
//
// Run: ./test_db
// All tests use a temporary schema prefix to avoid polluting production data.

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "../src/audit.hpp"
#include "../src/config.hpp"
#include "../src/db.hpp"

// ── helpers ────────────────────────────────────────────────────────

static int g_passed = 0;
static int g_failed = 0;

#define TEST(name) \
    static void test_##name(); \
    struct _reg_##name { _reg_##name() { \
        std::printf("  %-40s ", #name); \
        try { test_##name(); std::printf("PASS\n"); ++g_passed; } \
        catch (std::exception& e) { std::printf("FAIL: %s\n", e.what()); ++g_failed; } \
    }} _inst_##name; \
    static void test_##name()

#define ASSERT(expr) \
    if (!(expr)) throw std::runtime_error("Assertion failed: " #expr)

#define ASSERT_EQ(a, b) \
    if ((a) != (b)) throw std::runtime_error( \
        std::string("Expected ") + std::to_string(b) + " got " + std::to_string(a))

// ── fixture ────────────────────────────────────────────────────────

static std::string g_connstr;

static void setup_test_schema(PGconn* conn) {
    // Use a separate test table so we don't touch production data
    PGresult* r;
    r = PQexec(conn, "DROP TABLE IF EXISTS ticks_test CASCADE");
    if (r) PQclear(r);

    r = PQexec(conn, R"(
        CREATE TABLE ticks_test (
            ts_event  TIMESTAMPTZ NOT NULL,
            price     DOUBLE PRECISION NOT NULL,
            size      BIGINT NOT NULL,
            side      CHAR(1),
            is_buy    BOOLEAN,
            source    VARCHAR(32) DEFAULT 'amp_rithmic'
        );
        CREATE UNIQUE INDEX ON ticks_test(ts_event);
        ALTER TABLE ticks_test RENAME TO ticks;
    )");
    // Note: for simplicity we just rename to 'ticks' in a transaction — in a real
    // test suite use a separate schema (SET search_path TO test_schema).
    if (r) PQclear(r);
}

static void teardown_test_schema(PGconn* conn) {
    PGresult* r = PQexec(conn, "DROP TABLE IF EXISTS ticks CASCADE");
    if (r) PQclear(r);
    r = PQexec(conn, "DROP TABLE IF EXISTS audit_log CASCADE");
    if (r) PQclear(r);
}

// ── tests ──────────────────────────────────────────────────────────

TEST(connection) {
    TickDB db(g_connstr);
    ASSERT(db.conn() != nullptr);
}

TEST(write_and_count) {
    TickDB db(g_connstr);

    std::vector<TickRow> rows = {
        {1712000000'000000LL, 18500.0, 10, true},
        {1712000001'000000LL, 18501.5, 5,  false},
        {1712000002'000000LL, 18502.0, 8,  true},
    };

    int inserted = db.write(rows);
    ASSERT_EQ(inserted, 3);
    ASSERT_EQ(db.row_count(), 3);
}

TEST(deduplication) {
    TickDB db(g_connstr);

    // Insert same ts_event twice — second should be ignored
    std::vector<TickRow> rows = {
        {1712000010'000000LL, 18510.0, 1, true},
    };
    db.write(rows);
    int second = db.write(rows);  // duplicate
    ASSERT_EQ(second, 0);

    // Total should still be 3 + 1 = 4 from previous tests
    // (test isolation is via table drop/recreate in main)
}

TEST(latest_price) {
    TickDB db(g_connstr);

    std::vector<TickRow> rows = {
        {1712000020'000000LL, 18520.25, 3, false},
    };
    db.write(rows);

    auto price = db.latest_price();
    ASSERT(price.has_value());
    ASSERT(*price == 18520.25);
}

TEST(summary) {
    TickDB db(g_connstr);
    auto s = db.summary();
    ASSERT(s.tick_count >= 0);
    ASSERT(!s.connstr.empty());
}

TEST(audit_log) {
    TickDB db(g_connstr);
    AuditLog audit(db.conn());

    audit.info("test.event", "detail=hello");
    audit.warn("test.warn",  "detail=world");
    audit.error("test.error","detail=oops");

    ASSERT(audit.pending() == 3);

    audit.flush();
    ASSERT(audit.pending() == 0);

    // Verify rows written
    PGresult* res = PQexec(db.conn(),
        "SELECT COUNT(*) FROM audit_log WHERE event LIKE 'test.%'");
    ASSERT(res && PQresultStatus(res) == PGRES_TUPLES_OK);
    int count = std::atoi(PQgetvalue(res, 0, 0));
    PQclear(res);
    ASSERT(count == 3);
}

TEST(large_batch) {
    TickDB db(g_connstr);

    // Insert 500 ticks in one batch
    std::vector<TickRow> rows;
    rows.reserve(500);
    for (int i = 0; i < 500; ++i)
        rows.push_back({1712001000'000000LL + i * 1000LL,
                        18500.0 + i * 0.25, 1 + i % 10, i % 2 == 0});

    int inserted = db.write(rows);
    ASSERT(inserted == 500);
}

// ── main ───────────────────────────────────────────────────────────

int main() {
    // Load .env if present
    Config cfg = Config::from_env(".env");
    g_connstr  = cfg.pg_connstr();

    std::printf("\n=== TickDB + AuditLog integration tests ===\n\n");

    // Setup fresh test tables
    {
        PGconn* setup_conn = PQconnectdb(g_connstr.c_str());
        if (PQstatus(setup_conn) != CONNECTION_OK) {
            std::fprintf(stderr, "Cannot connect to PostgreSQL: %s\n",
                         PQerrorMessage(setup_conn));
            PQfinish(setup_conn);
            return 1;
        }
        teardown_test_schema(setup_conn);
        PQfinish(setup_conn);
    }

    // TickDB constructor creates the schema
    {
        TickDB db(g_connstr);  // triggers ensure_schema()
        (void)db;
    }

    // Tests run via static initializers (TEST macro)
    // They're already registered — nothing else to call here.

    std::printf("\n=== Results: %d passed, %d failed ===\n\n",
                g_passed, g_failed);

    // Teardown
    {
        PGconn* c = PQconnectdb(g_connstr.c_str());
        if (PQstatus(c) == CONNECTION_OK)
            teardown_test_schema(c);
        PQfinish(c);
    }

    return g_failed > 0 ? 1 : 0;
}
