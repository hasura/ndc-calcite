#pragma once
#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include <sql.h>
#include <sqlext.h>
#include <string>
#include <memory>
#include <stdexcept>
#include "../include/Logging.hpp"
#include "../include/Error.hpp"

class Environment  {
private:
    

public:
    SQLINTEGER envVersion;
    Environment()
        : envVersion(SQL_OV_ODBC3) {
        LOG("Environment instance created");
    }

    ~Environment() {
        LOG("Environment instance destroyed");
    }

    [[nodiscard]] static const Error& Environment::getLastError() {
        static const Error defaultError("HY000", "Environment general error", 1);
        return defaultError;
    }
};

