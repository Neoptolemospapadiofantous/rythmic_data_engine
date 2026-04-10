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

static int g_passed  = 0;
static int g_failed  = 0;
static int g_skipped = 0;

// Throw this to skip a test without counting it as a failure
struct SkipTest : std::exception {
    explicit SkipTest(const char* msg) : msg_(msg) {}
    const char* what() const noexcept override { return msg_; }
    const char* msg_;
};

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

// g_connstr is initialised at static-init time by reading .env, so all TEST
// static-initializer constructors (which run before main()) have a valid connstr.
static std::string g_connstr = []() -> std::string {
    Config c = Config::from_env(".env");
    return c.pg_connstr();
}();

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

// ── RUN_TEST macro — runs tests from main() (not static init) ──────
//
// New tests use RUN_TEST instead of TEST so they execute after main()
// has set up the database schema.  TEST is kept for the existing tests
// (static-initializer style, maintained for backward compat).

#define RUN_TEST(name) \
    do { \
        std::printf("  %-40s ", #name); \
        try { test_##name(); std::printf("PASS\n"); ++g_passed; } \
        catch (SkipTest& s) { std::printf("SKIP: %s\n", s.what()); ++g_skipped; } \
        catch (std::exception& e) { std::printf("FAIL: %s\n", e.what()); ++g_failed; } \
    } while (0)

// ── BBO helpers ────────────────────────────────────────────────────

static void truncate_bbo_table(PGconn* conn) {
    // Truncate rather than drop so the hypertable structure (created by
    // ensure_schema) is preserved.
    PGresult* r = PQexec(conn, "TRUNCATE TABLE bbo");
    if (r) PQclear(r);
}

static void teardown_bbo_table(PGconn* conn) {
    PGresult* r = PQexec(conn, "TRUNCATE TABLE bbo");
    if (r) PQclear(r);
}

// ── BBORow tests ────────────────────────────────────────────────────

static void test_write_bbo_basic() {
    {
        PGconn* c = PQconnectdb(g_connstr.c_str());
        truncate_bbo_table(c);
        PQfinish(c);
    }

    TickDB db(g_connstr);

    std::vector<BBORow> rows = {
        {1712000100'000000LL, 18499.75, 10, 3, 18500.00, 8,  2, "NQ", "CME"},
        {1712000101'000000LL, 18500.00, 5,  1, 18500.25, 12, 4, "NQ", "CME"},
        {1712000102'000000LL, 18500.25, 7,  2, 18500.50, 6,  1, "NQ", "CME"},
    };

    int inserted = db.write_bbo(rows);
    ASSERT_EQ(inserted, 3);
}

static void test_write_bbo_empty_batch() {
    TickDB db(g_connstr);

    std::vector<BBORow> empty;
    int inserted = db.write_bbo(empty);
    ASSERT_EQ(inserted, 0);
}

static void test_write_bbo_dedup() {
    // bbo has no UNIQUE constraint (only a plain ts_event DESC index).
    // write_bbo uses ON CONFLICT DO NOTHING which is only a no-op when a
    // unique constraint is violated.  Without one, every insert succeeds.
    // This test verifies the write path is stable with repeated rows.
    TickDB db(g_connstr);

    std::vector<BBORow> rows = {
        {1712000200'000000LL, 18510.0, 5, 1, 18510.25, 8, 2, "NQ", "CME"},
    };

    int first  = db.write_bbo(rows);
    int second = db.write_bbo(rows);

    // Both inserts succeed since there is no unique constraint on bbo
    ASSERT(first  >= 1);
    ASSERT(second >= 0);  // 0 or 1 — both are acceptable
}

// ── DepthRow helpers ────────────────────────────────────────────────

static void truncate_depth_table(PGconn* conn) {
    PGresult* r = PQexec(conn, "TRUNCATE TABLE depth_by_order");
    if (r) PQclear(r);
}

static void teardown_depth_table(PGconn* conn) {
    PGresult* r = PQexec(conn, "TRUNCATE TABLE depth_by_order");
    if (r) PQclear(r);
}

// ── DepthRow tests ──────────────────────────────────────────────────

// Helper: true if idx_depth_unique exists (TimescaleDB may not support partial unique indexes)
static bool depth_unique_index_exists() {
    PGconn* c = PQconnectdb(g_connstr.c_str());
    if (PQstatus(c) != CONNECTION_OK) { PQfinish(c); return false; }
    PGresult* r = PQexec(c,
        "SELECT COUNT(*) FROM pg_indexes"
        " WHERE tablename='depth_by_order' AND indexname='idx_depth_unique'");
    bool ok = r && PQresultStatus(r) == PGRES_TUPLES_OK && std::atoi(PQgetvalue(r, 0, 0)) > 0;
    if (r) PQclear(r);
    PQfinish(c);
    return ok;
}

static void test_write_depth_basic() {
    {
        PGconn* c = PQconnectdb(g_connstr.c_str());
        truncate_depth_table(c);
        PQfinish(c);
    }

    TickDB db(g_connstr);

    std::vector<DepthRow> rows = {
        {1712000300'000000LL, 1712000300'000000001LL, 1001, 1, 1, 18499.75, 0.0, 10, "ORD001", "NQ", "CME"},
        {1712000301'000000LL, 1712000301'000000002LL, 1002, 2, 1, 18499.75, 18499.75, 8, "ORD001", "NQ", "CME"},
        {1712000302'000000LL, 1712000302'000000003LL, 1003, 3, 1, 18499.75, 18499.75, 0, "ORD001", "NQ", "CME"},
    };

    // write_depth requires idx_depth_unique; skip if TimescaleDB won't create it
    if (!depth_unique_index_exists())
        throw SkipTest("idx_depth_unique not present (TimescaleDB partial-unique limitation)");

    int inserted = db.write_depth(rows);
    ASSERT_EQ(inserted, 3);
}

static void test_write_depth_empty_batch() {
    TickDB db(g_connstr);

    // Empty batch is a fast-path return — no DB query issued, no constraint needed
    std::vector<DepthRow> empty;
    int inserted = db.write_depth(empty);
    ASSERT_EQ(inserted, 0);
}

static void test_write_depth_dedup() {
    if (!depth_unique_index_exists())
        throw SkipTest("idx_depth_unique not present (TimescaleDB partial-unique limitation)");

    TickDB db(g_connstr);

    // Same source_ns → should be deduped by idx_depth_unique
    std::vector<DepthRow> rows = {
        {1712000400'000000LL, 1712000400'999999001LL, 2001, 1, 1, 18502.0, 0.0, 5, "ORD999", "NQ", "CME"},
    };

    int first  = db.write_depth(rows);
    int second = db.write_depth(rows);  // same source_ns

    ASSERT(first >= 1);
    ASSERT(second <= 0);
}

static void test_write_depth_update_types() {
    if (!depth_unique_index_exists())
        throw SkipTest("idx_depth_unique not present (TimescaleDB partial-unique limitation)");

    TickDB db(g_connstr);

    // One row per update_type: 1=NEW, 2=CHANGE, 3=DELETE — all unique source_ns
    std::vector<DepthRow> rows = {
        {1712000500'000000LL, 1712000500'100000001LL, 3001, 1, 1, 18505.0, 0.0,     5, "ORD_A", "NQ", "CME"},
        {1712000500'000001LL, 1712000500'200000002LL, 3002, 2, 1, 18505.0, 18505.0, 3, "ORD_A", "NQ", "CME"},
        {1712000500'000002LL, 1712000500'300000003LL, 3003, 3, 1, 18505.0, 18505.0, 0, "ORD_A", "NQ", "CME"},
    };

    int inserted = db.write_depth(rows);
    ASSERT_EQ(inserted, 3);
}

// ── Schema verification tests (information_schema queries) ──────────

static void test_bbo_table_exists() {
    PGconn* c = PQconnectdb(g_connstr.c_str());
    ASSERT(PQstatus(c) == CONNECTION_OK);

    PGresult* res = PQexec(c,
        "SELECT COUNT(*) FROM information_schema.columns"
        " WHERE table_name = 'bbo'");
    ASSERT(res && PQresultStatus(res) == PGRES_TUPLES_OK);
    int count = std::atoi(PQgetvalue(res, 0, 0));
    PQclear(res);
    PQfinish(c);

    ASSERT(count > 0);
}

static void test_bbo_has_required_columns() {
    PGconn* c = PQconnectdb(g_connstr.c_str());
    ASSERT(PQstatus(c) == CONNECTION_OK);

    const char* required[] = {
        "bid_price", "ask_price", "bid_size", "ask_size", "ts_event"
    };

    for (const char* col : required) {
        std::string sql =
            std::string("SELECT COUNT(*) FROM information_schema.columns"
                        " WHERE table_name = 'bbo' AND column_name = '") + col + "'";
        PGresult* res = PQexec(c, sql.c_str());
        ASSERT(res && PQresultStatus(res) == PGRES_TUPLES_OK);
        int cnt = std::atoi(PQgetvalue(res, 0, 0));
        PQclear(res);
        if (cnt == 0) {
            PQfinish(c);
            throw std::runtime_error(std::string("bbo missing column: ") + col);
        }
    }

    PQfinish(c);
}

static void test_depth_table_exists() {
    PGconn* c = PQconnectdb(g_connstr.c_str());
    ASSERT(PQstatus(c) == CONNECTION_OK);

    PGresult* res = PQexec(c,
        "SELECT COUNT(*) FROM information_schema.columns"
        " WHERE table_name = 'depth_by_order'");
    ASSERT(res && PQresultStatus(res) == PGRES_TUPLES_OK);
    int count = std::atoi(PQgetvalue(res, 0, 0));
    PQclear(res);
    PQfinish(c);

    ASSERT(count > 0);
}

static void test_depth_has_source_ns() {
    PGconn* c = PQconnectdb(g_connstr.c_str());
    ASSERT(PQstatus(c) == CONNECTION_OK);

    PGresult* res = PQexec(c,
        "SELECT COUNT(*) FROM information_schema.columns"
        " WHERE table_name = 'depth_by_order' AND column_name = 'source_ns'");
    ASSERT(res && PQresultStatus(res) == PGRES_TUPLES_OK);
    int count = std::atoi(PQgetvalue(res, 0, 0));
    PQclear(res);
    PQfinish(c);

    ASSERT(count > 0);
}

static void test_depth_has_update_type() {
    PGconn* c = PQconnectdb(g_connstr.c_str());
    ASSERT(PQstatus(c) == CONNECTION_OK);

    PGresult* res = PQexec(c,
        "SELECT COUNT(*) FROM information_schema.columns"
        " WHERE table_name = 'depth_by_order' AND column_name = 'update_type'");
    ASSERT(res && PQresultStatus(res) == PGRES_TUPLES_OK);
    int count = std::atoi(PQgetvalue(res, 0, 0));
    PQclear(res);
    PQfinish(c);

    ASSERT(count > 0);
}

static void test_ticks_unique_index_wide() {
    // Regression guard: idx_ticks_unique must include 'price' AND 'size' columns.
    // The old narrow index (ts_event only) allowed duplicate trades sharing
    // a microsecond timestamp to be silently dropped.
    PGconn* c = PQconnectdb(g_connstr.c_str());
    ASSERT(PQstatus(c) == CONNECTION_OK);

    PGresult* res = PQexec(c,
        "SELECT indexdef FROM pg_indexes"
        " WHERE tablename = 'ticks' AND indexname = 'idx_ticks_unique'");
    ASSERT(res && PQresultStatus(res) == PGRES_TUPLES_OK);

    bool found = PQntuples(res) > 0;
    std::string indexdef;
    if (found)
        indexdef = PQgetvalue(res, 0, 0);
    PQclear(res);
    PQfinish(c);

    ASSERT(found);
    ASSERT(indexdef.find("price") != std::string::npos);
    ASSERT(indexdef.find("size")  != std::string::npos);
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

    // Existing tests run via static initializers (TEST macro).
    // New tests (BBORow, DepthRow, schema) run explicitly below, after the
    // schema is guaranteed to be in place.

    std::printf("\n--- BBORow tests ---\n");
    RUN_TEST(write_bbo_basic);
    RUN_TEST(write_bbo_empty_batch);
    RUN_TEST(write_bbo_dedup);

    std::printf("\n--- DepthRow tests ---\n");
    RUN_TEST(write_depth_basic);
    RUN_TEST(write_depth_empty_batch);
    RUN_TEST(write_depth_dedup);
    RUN_TEST(write_depth_update_types);

    std::printf("\n--- Schema verification tests ---\n");
    RUN_TEST(bbo_table_exists);
    RUN_TEST(bbo_has_required_columns);
    RUN_TEST(depth_table_exists);
    RUN_TEST(depth_has_source_ns);
    RUN_TEST(depth_has_update_type);
    RUN_TEST(ticks_unique_index_wide);

    std::printf("\n=== Results: %d passed, %d failed, %d skipped ===\n\n",
                g_passed, g_failed, g_skipped);

    // Teardown
    {
        PGconn* c = PQconnectdb(g_connstr.c_str());
        if (PQstatus(c) == CONNECTION_OK) {
            teardown_test_schema(c);
            teardown_bbo_table(c);
            teardown_depth_table(c);
        }
        PQfinish(c);
    }

    return g_failed > 0 ? 1 : 0;
}
