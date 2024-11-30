#pragma once
#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include <string>
#include <sql.h>

struct Error {
    std::string sqlState;
    std::string message;
    SQLINTEGER nativeError;

    Error(std::string state = "HY000",
          std::string msg = "General error",
          SQLINTEGER native = 0)
        : sqlState(state)
        , message(msg)
        , nativeError(native) {}
};