#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include "../include/Connection.hpp"
#include "../include/Logging.hpp"
#include "../include/Environment.hpp"
#include "../include/Statement.hpp"

extern "C" {
SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT handleType, SQLHANDLE inputHandle, SQLHANDLE *outputHandle) {
    LOGF("SQLAllocHandle called, handleType: ", handleType);

    *outputHandle = SQL_NULL_HANDLE;

    switch (handleType) {
        case SQL_HANDLE_ENV:
            try {
                *outputHandle = reinterpret_cast<SQLHANDLE>(new Environment());
                LOG("Environment handle allocated successfully");
                return SQL_SUCCESS;
            } catch (const std::exception &e) {
                LOGF("Failed to allocate environment handle: %s", e.what());
                return SQL_ERROR;
            }

        case SQL_HANDLE_DBC:
            try {
                if (!inputHandle) {
                    LOG("Invalid environment handle passed to SQL_HANDLE_DBC allocation");
                    return SQL_INVALID_HANDLE;
                }
                *outputHandle = reinterpret_cast<SQLHANDLE>(new Connection());
                LOG("Connection handle allocated successfully");
                return SQL_SUCCESS;
            } catch (const std::exception &e) {
                LOGF("Failed to allocate connection handle: %s", e.what());
                return SQL_ERROR;
            }

        case SQL_HANDLE_STMT:
            try {
                if (!inputHandle) {
                    LOG("Invalid database connection handle passed to SQL_HANDLE_STMT allocation");
                    return SQL_INVALID_HANDLE;
                }
                *outputHandle = reinterpret_cast<SQLHANDLE>(new Statement(static_cast<Connection *>(inputHandle)));
                LOG("Statement handle allocated successfully");
                return SQL_SUCCESS;
            } catch (const std::exception &e) {
                LOGF("Failed to allocate statement handle: %s", e.what());
                return SQL_ERROR;
            }

        default:
            LOG("SQLAllocHandle received unknown handle type");
            return SQL_ERROR;
    }
}
} // extern "C"
