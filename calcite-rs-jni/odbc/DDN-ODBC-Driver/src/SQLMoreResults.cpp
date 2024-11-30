#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include "../include/Connection.hpp"
#include "../include/Logging.hpp"
#include "../include/Environment.hpp"
#include "../include/Statement.hpp"

extern "C" {
SQLRETURN SQL_API SQLMoreResults(
    SQLHSTMT StatementHandle) {
    LOGF("SQLMoreResults called");

    auto stmt = static_cast<Statement *>(StatementHandle);

    if (!stmt) {
        LOG("Invalid statement handle");
        return SQL_INVALID_HANDLE;
    }

    if (!stmt->hasResult) {
        LOG("No result set available");
        return SQL_NO_DATA;
    }

    // if (stmt->currentRow < stmt->resultData.size()) {
    //     LOG("Current result set has more rows to fetch");
    //     return SQL_SUCCESS;
    // }
    LOG("No more result sets available");
    return SQL_NO_DATA;
}
}
