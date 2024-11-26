#pragma once
#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <string>
#include <memory>
#include <vector>
#include <stdexcept>
#include "../include/logging.hpp"

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
};

