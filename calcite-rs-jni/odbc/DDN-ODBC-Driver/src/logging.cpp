#include "../include/logging.hpp"
#include <cstdarg>
#include <ctime>
#include <iomanip>
#include <sstream>

std::unique_ptr<Logger> Logger::instance = nullptr;
std::mutex Logger::mutex;

Logger::Logger() : logPath("c:\\temp\\odbc_driver.log") {
    logFile.open(logPath, std::ios::app);
}

Logger& Logger::getInstance() {
    std::lock_guard<std::mutex> lock(mutex);
    if (!instance) {
        instance = std::unique_ptr<Logger>(new Logger());
    }
    return *instance;
}

void Logger::log(const std::string& message) {
    std::lock_guard<std::mutex> lock(mutex);
    if (!logFile.is_open()) return;

    auto now = std::time(nullptr);
    auto tm = *std::localtime(&now);

    logFile << std::put_time(&tm, "%Y-%m-%d %H:%M:%S")
        << " | " << message << std::endl;
    logFile.flush();
}

void Logger::logf(const char* format, ...) {
    char buffer[2048];
    va_list args;
    va_start(args, format);
    vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);

    log(buffer);
}

void Logger::setLogPath(const std::string& path) {
    std::lock_guard<std::mutex> lock(mutex);
    if (logFile.is_open()) {
        logFile.close();
    }
    logPath = path;
    logFile.open(logPath, std::ios::app);
}