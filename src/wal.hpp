#pragma once
// wal.hpp — Write-Ahead Log for crash-safe tick recovery.
//
// Lifecycle:
//   1. Before every DB flush: write_batch(buf)        — appends + fdatasyncs
//   2. After successful DB write: commit()            — truncates + fdatasyncs
//   3. On startup: replay() → vector<TickRow>         — returns any unflushed ticks
//
// If the process crashes between steps 1 and 2, the unflushed ticks are
// replayed at the next start.  Duplicates are handled by the DB's
// ON CONFLICT (symbol, exchange, ts_event) DO NOTHING clause.
//
// Format: one CSV line per tick, \n terminated.
//   ts_micros,price,size,is_buy,symbol,exchange
//
// Uses POSIX open/write/fdatasync for true crash safety.
// std::ofstream flush() only reaches the kernel buffer; fdatasync() forces
// the data to stable storage so a kernel crash cannot lose a committed WAL.

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "db.hpp"

class Wal {
public:
    explicit Wal(const std::string& path) : path_(path) {}

    // Append a batch to the WAL file (called before DB write).
    // Guarantees the data reaches stable storage (fdatasync) before returning.
    void write_batch(const std::vector<TickRow>& rows) {
        if (rows.empty()) return;

        // Build the entire CSV block in memory first
        std::string buf;
        buf.reserve(rows.size() * 72);
        for (auto& r : rows) {
            buf += std::to_string(r.ts_micros);
            buf += ',';
            // Use fixed-precision to avoid locale-dependent decimal separator
            char pbuf[32];
            std::snprintf(pbuf, sizeof(pbuf), "%.6f", r.price);
            buf += pbuf;
            buf += ',';
            buf += std::to_string(r.size);
            buf += ',';
            buf += (r.is_buy ? '1' : '0');
            buf += ',';
            buf += r.symbol;
            buf += ',';
            buf += r.exchange;
            buf += '\n';
        }

        int fd = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0)
            throw std::runtime_error("WAL open failed: " + path_ +
                                     " (" + std::strerror(errno) + ")");

        const char* p   = buf.data();
        std::size_t rem = buf.size();
        while (rem > 0) {
            ssize_t n = ::write(fd, p, rem);
            if (n < 0) {
                ::close(fd);
                throw std::runtime_error(std::string("WAL write failed: ") +
                                         std::strerror(errno));
            }
            p   += n;
            rem -= static_cast<std::size_t>(n);
        }
        ::fdatasync(fd);   // force to stable storage
        ::close(fd);
    }

    // Truncate the WAL to zero after a confirmed DB write.
    void commit() {
        int fd = ::open(path_.c_str(),
                        O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd >= 0) {
            ::fdatasync(fd);
            ::close(fd);
        }
        // If open fails the file simply doesn't exist — already clean.
    }

    // Read all ticks from the WAL (called once at startup, and before each
    // flush to catch up accumulated missed batches).
    std::vector<TickRow> replay() const {
        std::vector<TickRow> rows;
        std::ifstream f(path_);
        if (!f) return rows;   // no WAL file — nothing to replay

        std::string line;
        while (std::getline(f, line)) {
            if (line.empty()) continue;
            std::istringstream ss(line);
            std::string tok;
            TickRow r;
            try {
                std::getline(ss, tok, ','); r.ts_micros = std::stoll(tok);
                std::getline(ss, tok, ','); r.price     = std::stod(tok);
                std::getline(ss, tok, ','); r.size      = std::stoll(tok);
                std::getline(ss, tok, ','); r.is_buy    = (tok == "1");
                std::getline(ss, r.symbol,   ',');
                std::getline(ss, r.exchange);
                if (!r.symbol.empty() && !r.exchange.empty())
                    rows.push_back(std::move(r));
            } catch (...) {
                // Skip malformed/partial lines (tail of file at crash boundary)
            }
        }
        return rows;
    }

    // True if the WAL file exists and is non-empty (dirty = data to replay)
    bool dirty() const {
        struct stat st{};
        return ::stat(path_.c_str(), &st) == 0 && st.st_size > 0;
    }

    bool exists() const {
        return ::access(path_.c_str(), F_OK) == 0;
    }

    const std::string& path() const { return path_; }

private:
    std::string path_;
};
