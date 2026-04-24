#pragma once
/*  log.hpp — local stub for unit test builds (no spdlog dependency).
    On the Oracle VM this file lives in src/ alongside client.hpp / config.hpp.
    Here we provide a minimal fprintf-based implementation so the execution
    module compiles and tests run without the full rithmic_lib build. */
#include <cstdio>
#include <ctime>

// printf-style log to stderr with timestamp prefix
#define LOG(fmt, ...) do { \
    struct timespec _ts; \
    clock_gettime(CLOCK_REALTIME, &_ts); \
    fprintf(stderr, "[%.3f] " fmt "\n", \
            (double)_ts.tv_sec + _ts.tv_nsec * 1e-9, ##__VA_ARGS__); \
} while(0)
