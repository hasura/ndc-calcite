#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include "../include/Connection.hpp"
#include "../include/Logging.hpp"
#include "../include/Environment.hpp"
#include "../include/Statement.hpp"

extern "C" {
SQLRETURN SQL_API SQLFreeHandle(
    SQLSMALLINT handleType,
    SQLHANDLE handle) {
    LOG("SQLFreeHandle called");

    if (!handle) {
        return SQL_ERROR;
    }

    try {
        switch (handleType) {
            case SQL_HANDLE_ENV:
                delete static_cast<Environment *>(handle);
                break;

            case SQL_HANDLE_DBC:
                delete static_cast<Connection *>(handle);
                break;

            case SQL_HANDLE_STMT:
                delete static_cast<Statement *>(handle);
                break;

            default:
                return SQL_ERROR;
        }
        return SQL_SUCCESS;
    } catch (const std::exception &e) {
        LOGF("Error in SQLFreeHandle: %s", e.what());
        return SQL_ERROR;
    }
}
} // extern "C"
