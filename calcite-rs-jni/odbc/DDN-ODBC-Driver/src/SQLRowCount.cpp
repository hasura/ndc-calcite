#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include "../include/Connection.hpp"
#include "../include/Logging.hpp"
#include "../include/Environment.hpp"
#include "../include/Statement.hpp"

// Result set information functions
extern "C" {
    SQLRETURN SQL_API SQLRowCount(
        SQLHSTMT StatementHandle,
        SQLLEN *RowCount) {
        LOG("SQLRowCount called");

        auto stmt = static_cast<Statement *>(StatementHandle);
        if (!stmt) {
            LOG("Invalid statement handle");
            return SQL_INVALID_HANDLE;
        }

        if (!RowCount) {
            LOG("Null RowCount pointer");
            return SQL_ERROR;
        }

        // Initialize to 0 by default
        *RowCount = 0;

        if (!stmt->hasResult) {
            LOG("No result set available");
            return SQL_NO_DATA;  // Changed to return SQL_NO_DATA instead of SQL_SUCCESS
        }

        *RowCount = static_cast<SQLLEN>(stmt->resultData.size());
        LOGF("Returning row count: %lld", static_cast<long long>(*RowCount));
        return SQL_SUCCESS;
    }

SQLRETURN SQL_API SQLNumResultCols(
    SQLHSTMT StatementHandle,
    SQLSMALLINT *ColumnCount) {
    LOG("SQLNumResultCols called");

    auto stmt = static_cast<Statement *>(StatementHandle);
    if (!stmt) {
        LOG("Invalid statement handle");
        return SQL_INVALID_HANDLE;
    }

    if (!ColumnCount) {
        LOG("Null ColumnCount pointer");
        return SQL_ERROR;
    }

    if (!stmt->hasResult) {
        LOG("No result set available");
        *ColumnCount = 0;
        return SQL_SUCCESS;
    }

    *ColumnCount = static_cast<SQLSMALLINT>(stmt->resultColumns.size());
    LOGF("Returning column count: %d", *ColumnCount);
    return SQL_SUCCESS;
}
} // extern "C"
