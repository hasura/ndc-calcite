#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include "../include/connection.hpp"
#include "../include/logging.hpp"
//#include "../include/httplib.h"
#include "../include/globals.hpp"
#include "../include/environment.hpp"
#include "../include/statement.hpp"

// Result set information functions
extern "C" {

    SQLRETURN SQL_API SQLRowCount(
        SQLHSTMT        StatementHandle,
        SQLLEN*         RowCount)
    {
        LOG("SQLRowCount called");

        auto stmt = static_cast<Statement*>(StatementHandle);
        if (!stmt) {
            LOG("Invalid statement handle");
            return SQL_INVALID_HANDLE;
        }

        if (!RowCount) {
            LOG("Null RowCount pointer");
            return SQL_ERROR;
        }

        if (!stmt->hasResult) {
            LOG("No result set available");
            *RowCount = 0;
            return SQL_SUCCESS;
        }

        *RowCount = static_cast<SQLLEN>(stmt->resultData.size());
        LOGF("Returning row count: %zd", *RowCount);
        return SQL_SUCCESS;
    }

    SQLRETURN SQL_API SQLNumResultCols(
        SQLHSTMT        StatementHandle,
        SQLSMALLINT*    ColumnCount)
    {
        LOG("SQLNumResultCols called");

        auto stmt = static_cast<Statement*>(StatementHandle);
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