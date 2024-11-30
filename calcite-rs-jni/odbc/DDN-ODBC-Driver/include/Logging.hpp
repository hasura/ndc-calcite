#pragma once

#include <string>
#include <memory>
#include <fstream>
#include <mutex>

class Logger {
private:
    static std::unique_ptr<Logger> instance;
    static std::mutex mutex;

    std::ofstream logFile;
    std::string logPath;

    Logger();

public:
    static Logger& getInstance();

    void log(const std::string& message);
    void logf(const char* format, ...);
    void setLogPath(const std::string& path);

    // Delete copy/move operations
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;
    Logger(Logger&&) = delete;
    Logger& operator=(Logger&&) = delete;
};

// #ifdef DEBUG
#define LOG(msg) Logger::getInstance().log(msg)
#define LOGF(...) Logger::getInstance().logf(__VA_ARGS__)
// #else
// #define LOG(msg) (void)0
// #define LOGF(...) (void)0
// #endif
