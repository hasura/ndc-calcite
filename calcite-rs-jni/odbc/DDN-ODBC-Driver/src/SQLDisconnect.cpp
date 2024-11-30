#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include "../include/Connection.hpp"
#include "../include/Logging.hpp"
#include "../include/Environment.hpp"
#include "../include/Statement.hpp"

extern "C" {
SQLRETURN SQL_API SQLDisconnect(SQLHDBC ConnectionHandle) {
    LOG("SQLDisconnect called");
    if (!ConnectionHandle) {
        LOG("Invalid connection handle in SQLDisconnect");
        return SQL_INVALID_HANDLE;
    }

    try {
        auto *conn = static_cast<Connection *>(ConnectionHandle);
        SQLRETURN ret = conn->disconnect();

        if (ret == SQL_SUCCESS) {
            LOG("SQLDisconnect completed successfully");
        } else {
            LOG("SQLDisconnect failed");
        }

        return ret;
    } catch (const std::exception &e) {
        LOGF("Exception in SQLDisconnect: %s", e.what());
        return SQL_ERROR;
    }
    catch (...) {
        LOG("Unknown exception in SQLDisconnect");
        return SQL_ERROR;
    }
}
} // extern "C"
