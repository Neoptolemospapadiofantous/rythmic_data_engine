#pragma once
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>

namespace fs = std::filesystem;

class Logger {
public:
    explicit Logger(fs::path log_file = "data/logs/collector.log")
        : file_(std::move(log_file)) {}

    void write(const std::string& msg) {
        auto line = timestamp() + " " + msg;
        std::lock_guard lock(mu_);
        std::puts(line.c_str());
        std::fflush(stdout);
        try {
            fs::create_directories(file_.parent_path());
            std::ofstream f(file_, std::ios::app);
            f << line << '\n';
        } catch (...) {}
    }

    template <class... Args>
    void log(const char* fmt, Args&&... args) {
        char buf[2048];
        std::snprintf(buf, sizeof(buf), fmt, std::forward<Args>(args)...);
        write(buf);
    }

private:
    static std::string timestamp() {
        std::time_t t = std::time(nullptr);
        char buf[32];
        std::strftime(buf, sizeof(buf), "[%Y-%m-%d %H:%M:%S]", std::localtime(&t));
        return buf;
    }

    fs::path file_;
    std::mutex mu_;
};

inline Logger& global_log() {
    static Logger inst;
    return inst;
}

#define LOG(fmt, ...) global_log().log(fmt, ##__VA_ARGS__)
