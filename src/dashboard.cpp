// dashboard — live ncurses TUI for the Rithmic tick engine.
//
// Runs the full data pipeline internally (Rithmic + PostgreSQL) while
// displaying real-time metrics in a terminal dashboard:
//
//   CONNECTION  |  LIVE TICK
//   RATES       |  1-MIN BAR
//   PIPELINE    |  BUFFER
//   AUDIT LOG
//
// Usage:  ./build/dashboard  [path/to/.env]
// Quit:   q or Ctrl-C

// ncurses.h must come LAST — it defines macros (timeout, erase, etc.)
// that corrupt Boost header parsing if included first.

#include <atomic>
#include <chrono>
#include <clocale>
#include <csignal>
#include <cstdio>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/steady_timer.hpp>

#include "audit.hpp"
#include "client.hpp"   // also declares global use_awaitable
#include "config.hpp"
#include "db.hpp"

// ncurses macros (timeout, erase, etc.) — included last to avoid conflicts
#include <ncurses.h>

namespace asio = boost::asio;

// ── ncurses color pairs ────────────────────────────────────────────
enum : int {
    C_HEADER = 1,   // cyan bold  — section titles
    C_OK     = 2,   // green      — connected / BUY
    C_ERR    = 3,   // red        — error / SELL
    C_LABEL  = 4,   // yellow     — field labels
    C_VALUE  = 5,   // white      — data values
    C_DIM    = 6,   // dim white  — secondary info
};

// ── Shared state (Asio thread writes, ncurses thread reads) ────────
struct Metrics {
    std::mutex mu;

    // Connection
    bool        rithmic_up = false;
    bool        pg_up      = false;
    std::string status_msg = "starting...";

    // Latest tick
    double  price   = 0;
    int64_t qty     = 0;
    bool    is_buy  = false;
    int64_t wire_us = 0;   // exchange timestamp → now (microseconds)

    // Rate window — system-clock ms of each received tick (last 60 s)
    std::deque<int64_t> rate_ms;

    // Totals
    int64_t session_ticks = 0;
    int64_t db_ticks      = 0;

    // Pipeline
    int     buf_queued    = 0;
    int64_t last_write_ms = 0;

    // 1-min OHLCV (from continuous aggregate, refreshed every 5 s)
    double  bar_o=0, bar_h=0, bar_l=0, bar_c=0;
    int64_t bar_vol = 0;
    std::string bar_ts;

    // Audit tail — last 5 formatted rows from audit_log
    std::vector<std::string> audit_tail;
};

static Metrics           g_metrics;
static std::atomic<bool> g_stop{false};

// ── Pipeline (runs in Asio background thread) ──────────────────────

static constexpr int    FLUSH_N   = 200;
static constexpr double FLUSH_SEC = 30.0;

struct Pipeline {
    const Config&             cfg;
    std::unique_ptr<TickDB>   db;
    std::unique_ptr<AuditLog> audit;
    asio::io_context          ioc{1};
    std::unique_ptr<RithmicClient> client;

    std::vector<TickRow>                  buf;
    std::chrono::steady_clock::time_point last_flush;
    std::chrono::steady_clock::time_point last_audit_flush;

    explicit Pipeline(const Config& c) : cfg(c) {
        db     = std::make_unique<TickDB>(cfg.pg_connstr());
        audit  = std::make_unique<AuditLog>(db->conn());
        client = std::make_unique<RithmicClient>(ioc, cfg);
        last_flush = last_audit_flush = std::chrono::steady_clock::now();
        {
            std::lock_guard lk(g_metrics.mu);
            g_metrics.pg_up = true;
        }
    }

    void flush_buf() {
        if (buf.empty()) return;
        auto t0 = std::chrono::steady_clock::now();
        try {
            db->write(buf);
        } catch (std::exception& e) {
            audit->error("ticks.write_error", e.what());
        }
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - t0).count();
        {
            std::lock_guard lk(g_metrics.mu);
            g_metrics.last_write_ms = ms;
            g_metrics.buf_queued    = 0;
        }
        audit->info("ticks.written", "count=" + std::to_string(buf.size()));
        buf.clear();
        last_flush = std::chrono::steady_clock::now();
    }

    // Called from Asio thread — no additional locking needed for buf
    void on_tick(TickRow r) {
        // Compute wire latency: now − exchange timestamp
        auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::system_clock::now().time_since_epoch()).count();
        int64_t wire = now_us - r.ts_micros;

        {
            std::lock_guard lk(g_metrics.mu);
            g_metrics.price     = r.price;
            g_metrics.qty       = r.size;
            g_metrics.is_buy    = r.is_buy;
            g_metrics.wire_us   = wire;
            ++g_metrics.session_ticks;

            if (!g_metrics.rithmic_up) {
                g_metrics.rithmic_up = true;
                g_metrics.status_msg =
                    "streaming " + cfg.symbol + "/" + cfg.exchange;
            }

            // Rolling 60-second rate window
            int64_t now_ms = now_us / 1000;
            g_metrics.rate_ms.push_back(now_ms);
            while (!g_metrics.rate_ms.empty() &&
                   now_ms - g_metrics.rate_ms.front() > 60000)
                g_metrics.rate_ms.pop_front();
        }

        buf.push_back(r);

        // Trigger count- or time-based flush
        double elapsed_s = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - last_flush).count();
        if (static_cast<int>(buf.size()) >= FLUSH_N || elapsed_s >= FLUSH_SEC)
            flush_buf();

        {
            std::lock_guard lk(g_metrics.mu);
            g_metrics.buf_queued = static_cast<int>(buf.size());
        }

        // Periodic audit flush (every 60 s)
        double ae = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - last_audit_flush).count();
        if (ae >= 60.0) {
            audit->flush();
            last_audit_flush = std::chrono::steady_clock::now();
        }
    }

    // Periodic DB queries — runs inside Asio thread
    asio::awaitable<void> db_poll() {
        auto ex = co_await asio::this_coro::executor;
        asio::steady_timer timer(ex);

        while (!g_stop.load()) {
            timer.expires_after(std::chrono::seconds(5));
            co_await timer.async_wait(use_awaitable);
            if (g_stop.load()) break;

            // Total tick count
            try {
                int64_t n = db->row_count();
                std::lock_guard lk(g_metrics.mu);
                g_metrics.db_ticks = n;
            } catch (...) {}

            // Latest completed 1-min bar from continuous aggregate
            try {
                PGresult* res = PQexec(db->conn(),
                    "SELECT to_char(ts,'HH24:MI'),"
                    "  open, high, low, close, volume"
                    " FROM bars_1min"
                    " ORDER BY ts DESC LIMIT 1");
                if (res && PQresultStatus(res) == PGRES_TUPLES_OK &&
                    PQntuples(res) > 0) {
                    std::lock_guard lk(g_metrics.mu);
                    g_metrics.bar_ts  = PQgetvalue(res, 0, 0);
                    g_metrics.bar_o   = std::stod(PQgetvalue(res, 0, 1));
                    g_metrics.bar_h   = std::stod(PQgetvalue(res, 0, 2));
                    g_metrics.bar_l   = std::stod(PQgetvalue(res, 0, 3));
                    g_metrics.bar_c   = std::stod(PQgetvalue(res, 0, 4));
                    g_metrics.bar_vol = std::stoll(PQgetvalue(res, 0, 5));
                }
                if (res) PQclear(res);
            } catch (...) {}

            // Audit tail
            try {
                PGresult* res = PQexec(db->conn(),
                    "SELECT to_char(ts,'HH24:MI:SS') || ' '"
                    "    || severity || '  '"
                    "    || event    || '  '"
                    "    || COALESCE(details, '')"
                    "  FROM audit_log"
                    "  ORDER BY ts DESC LIMIT 5");
                if (res && PQresultStatus(res) == PGRES_TUPLES_OK) {
                    std::vector<std::string> tail;
                    for (int i = 0; i < PQntuples(res); ++i)
                        tail.emplace_back(PQgetvalue(res, i, 0));
                    std::lock_guard lk(g_metrics.mu);
                    g_metrics.audit_tail = std::move(tail);
                }
                if (res) PQclear(res);
            } catch (...) {}
        }
    }

    void run() {
        client->set_on_tick([this](TickRow r) { on_tick(r); });

        {
            std::lock_guard lk(g_metrics.mu);
            g_metrics.status_msg = "connecting to " + cfg.url + "...";
        }

        asio::co_spawn(ioc, client->run(), asio::detached);
        asio::co_spawn(ioc, db_poll(), asio::detached);
        ioc.run();
    }
};

// ── ncurses helpers ────────────────────────────────────────────────

static void set_color(int pair, int attrs = A_NORMAL) {
    attrset(COLOR_PAIR(pair) | attrs);
}

static void draw_section(int row, int col, const char* title) {
    set_color(C_HEADER, A_BOLD);
    mvprintw(row, col, "%s", title);
    set_color(C_VALUE);
}

static void draw_label(int row, int col, const char* label) {
    set_color(C_LABEL);
    mvprintw(row, col, "%-18s", label);
    set_color(C_VALUE);
}

// ── Render one frame ───────────────────────────────────────────────

static void render() {
    // Snapshot all shared state under one lock
    bool        rithmic_up, pg_up;
    std::string status_msg;
    double      price;
    int64_t     qty;
    bool        is_buy;
    int64_t     wire_us;
    int64_t     session_ticks, db_ticks;
    int         buf_queued;
    int64_t     last_write_ms;
    double      bar_o, bar_h, bar_l, bar_c;
    int64_t     bar_vol;
    std::string bar_ts;
    std::vector<std::string> audit_tail;
    double tps5 = 0, tpm60 = 0;

    {
        std::lock_guard lk(g_metrics.mu);
        rithmic_up    = g_metrics.rithmic_up;
        pg_up         = g_metrics.pg_up;
        status_msg    = g_metrics.status_msg;
        price         = g_metrics.price;
        qty           = g_metrics.qty;
        is_buy        = g_metrics.is_buy;
        wire_us       = g_metrics.wire_us;
        session_ticks = g_metrics.session_ticks;
        db_ticks      = g_metrics.db_ticks;
        buf_queued    = g_metrics.buf_queued;
        last_write_ms = g_metrics.last_write_ms;
        bar_o  = g_metrics.bar_o;  bar_h = g_metrics.bar_h;
        bar_l  = g_metrics.bar_l;  bar_c = g_metrics.bar_c;
        bar_vol = g_metrics.bar_vol; bar_ts = g_metrics.bar_ts;
        audit_tail = g_metrics.audit_tail;

        if (!g_metrics.rate_ms.empty()) {
            int64_t now_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
            // ticks/sec: count ticks in last 5 s, divide by 5
            int cnt5 = 0;
            for (auto ts : g_metrics.rate_ms)
                if (now_ms - ts <= 5000) ++cnt5;
            tps5  = cnt5 / 5.0;
            tpm60 = static_cast<double>(g_metrics.rate_ms.size());
        }
    }

    int cols   = getmaxx(stdscr);
    int midcol = 40;

    erase();

    // ── Title bar ────────────────────────────────────────────────
    set_color(C_HEADER, A_BOLD | A_REVERSE);
    mvhline(0, 0, ' ', cols);
    mvprintw(0, 2, " RITHMIC ENGINE  live dashboard "
                   " (q=quit)");
    set_color(C_VALUE);

    // ── Row 2-5: CONNECTION (left) / LIVE TICK (right) ───────────
    int r = 2;
    draw_section(r, 1, "CONNECTION");
    draw_section(r, midcol, "LIVE TICK");

    // Rithmic dot
    r = 3;
    draw_label(r, 1, "Rithmic:");
    if (rithmic_up) { set_color(C_OK);  printw("[UP] CONNECTED"); }
    else            { set_color(C_ERR); printw("[..] waiting");   }

    // PostgreSQL dot
    draw_label(r+1, 1, "PostgreSQL:");
    if (pg_up) { set_color(C_OK);  printw("[UP] OK");    }
    else       { set_color(C_ERR); printw("[!!] error"); }

    // Status message
    set_color(C_DIM);
    mvprintw(r+2, 1, "%.*s", midcol - 3, status_msg.c_str());

    // Price
    set_color(C_VALUE);
    draw_label(r, midcol, "Price:");
    if (price > 0) printw("%.2f", price);
    else           printw("--");

    // Side / qty
    draw_label(r+1, midcol, "Side / Qty:");
    if (price > 0) {
        if (is_buy) { set_color(C_OK);  printw("BUY  ^"); }
        else        { set_color(C_ERR); printw("SELL v"); }
        set_color(C_VALUE);
        printw("   %lld", (long long)qty);
    } else {
        printw("--");
    }

    // Wire latency
    set_color(C_VALUE);
    draw_label(r+2, midcol, "Wire latency:");
    if (wire_us > 0)
        printw("%lld us  (%.1f ms)", (long long)wire_us, wire_us / 1000.0);
    else
        printw("--");

    // ── Separator ────────────────────────────────────────────────
    r = 7;
    set_color(C_DIM);
    mvhline(r, 0, ACS_HLINE, cols);

    // ── Row 8-12: RATES (left) / 1-MIN BAR (right) ───────────────
    r = 8;
    draw_section(r, 1, "RATES");
    {
        std::string bar_hdr = "1-MIN BAR";
        if (!bar_ts.empty()) bar_hdr += "  [" + bar_ts + "]";
        draw_section(r, midcol, bar_hdr.c_str());
    }

    draw_label(r+1, 1, "Ticks/sec:");
    set_color(C_VALUE); printw("%.1f", tps5);

    draw_label(r+2, 1, "Ticks/min:");
    set_color(C_VALUE); printw("%.0f", tpm60);

    draw_label(r+3, 1, "Session:");
    set_color(C_VALUE); printw("%lld", (long long)session_ticks);

    draw_label(r+4, 1, "DB total:");
    set_color(C_VALUE); printw("%lld", (long long)db_ticks);

    if (bar_o > 0) {
        set_color(C_VALUE);
        mvprintw(r+1, midcol, "O: %-10.2f  H: %.2f", bar_o, bar_h);
        mvprintw(r+2, midcol, "L: %-10.2f  C: %.2f", bar_l, bar_c);
        mvprintw(r+3, midcol, "Vol: %lld",            (long long)bar_vol);
    } else {
        set_color(C_DIM);
        mvprintw(r+1, midcol, "no bar data yet");
        mvprintw(r+2, midcol, "(refreshes every 1 min)");
    }

    // ── Separator ────────────────────────────────────────────────
    r = 13;
    set_color(C_DIM);
    mvhline(r, 0, ACS_HLINE, cols);

    // ── Row 14-16: PIPELINE (left) / BUFFER (right) ──────────────
    r = 14;
    draw_section(r, 1, "PIPELINE");
    draw_section(r, midcol, "BUFFER");

    draw_label(r+1, 1, "Last DB write:");
    set_color(C_VALUE);
    if (last_write_ms > 0) printw("%lld ms", (long long)last_write_ms);
    else                   printw("--");

    draw_label(r+1, midcol, "Queued ticks:");
    set_color(C_VALUE);
    printw("%d / %d", buf_queued, FLUSH_N);

    // Progress bar
    {
        int  bar_w  = 22;
        int  filled = buf_queued * bar_w / FLUSH_N;
        if (filled > bar_w) filled = bar_w;
        mvprintw(r+2, midcol, "[");
        set_color(buf_queued > FLUSH_N * 3 / 4 ? C_ERR : C_OK);
        for (int i = 0; i < bar_w; ++i)
            addch(i < filled ? '#' : '-');
        set_color(C_VALUE);
        addch(']');
    }

    // ── Separator ────────────────────────────────────────────────
    r = 17;
    set_color(C_DIM);
    mvhline(r, 0, ACS_HLINE, cols);

    // ── Row 18+: AUDIT LOG ────────────────────────────────────────
    r = 18;
    draw_section(r, 1, "AUDIT LOG");

    if (audit_tail.empty()) {
        set_color(C_DIM);
        mvprintw(r+1, 1, "(no events yet)");
    } else {
        for (int i = 0; i < static_cast<int>(audit_tail.size()); ++i) {
            set_color(C_DIM);
            int max_w = cols - 3;
            if (max_w < 0) max_w = 0;
            mvprintw(r+1+i, 1, "%.*s", max_w, audit_tail[i].c_str());
        }
    }

    // ── Footer ───────────────────────────────────────────────────
    int bottom = getmaxy(stdscr) - 1;
    set_color(C_DIM);
    mvhline(bottom, 0, ACS_HLINE, cols);
    mvprintw(bottom, 2, " q=quit  Ctrl-C=quit ");

    refresh();
}

// ── Signal handler ─────────────────────────────────────────────────

static void handle_signal(int) { g_stop.store(true); }

// ── main ───────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    const char* env_path = argc > 1 ? argv[1] : ".env";

    Config cfg;
    try {
        cfg = Config::from_env(env_path);
    } catch (std::exception& e) {
        std::fprintf(stderr, "Config error: %s\n", e.what());
        return 1;
    }
    {
        auto errs = cfg.validate();
        if (!errs.empty()) {
            for (auto& e : errs) std::fprintf(stderr, "  %s\n", e.c_str());
            return 1;
        }
    }

    // ── Connect to PostgreSQL before starting ncurses ─────────────
    std::unique_ptr<Pipeline> pipeline;
    try {
        pipeline = std::make_unique<Pipeline>(cfg);
    } catch (std::exception& e) {
        std::fprintf(stderr, "PostgreSQL error: %s\n", e.what());
        return 1;
    }

    std::signal(SIGINT,  handle_signal);
    std::signal(SIGTERM, handle_signal);

    // ── Launch Asio pipeline in background thread ─────────────────
    std::thread asio_thread([&] { pipeline->run(); });

    // ── ncurses init ──────────────────────────────────────────────
    setlocale(LC_ALL, "");   // required for correct character width calculation
    initscr();
    cbreak();
    noecho();
    curs_set(0);
    keypad(stdscr, TRUE);
    nodelay(stdscr, TRUE);

    if (has_colors()) {
        start_color();
        use_default_colors();
        init_pair(C_HEADER, COLOR_CYAN,   -1);
        init_pair(C_OK,     COLOR_GREEN,  -1);
        init_pair(C_ERR,    COLOR_RED,    -1);
        init_pair(C_LABEL,  COLOR_YELLOW, -1);
        init_pair(C_VALUE,  COLOR_WHITE,  -1);
        init_pair(C_DIM,    COLOR_WHITE,  -1);
    }

    // ── Render loop at 10 fps ─────────────────────────────────────
    while (!g_stop.load()) {
        int ch = getch();
        if (ch == 'q' || ch == 'Q') {
            g_stop.store(true);
            break;
        }
        render();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // ── Cleanup ───────────────────────────────────────────────────
    endwin();

    pipeline->client->stop();
    pipeline->ioc.stop();
    if (asio_thread.joinable()) asio_thread.join();

    return 0;
}
