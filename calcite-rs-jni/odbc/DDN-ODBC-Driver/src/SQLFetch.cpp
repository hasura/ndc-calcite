#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include "../include/connection.hpp"
#include "../include/logging.hpp"
//#include "../include/httplib.h"
#include "../include/globals.hpp"
#include "../include/environment.hpp"
#include "../include/statement.hpp"

extern "C" {

    SQLRETURN SQL_API SQLFetch(SQLHSTMT StatementHandle) {
        LOG("SQLFetch called");

        auto stmt = static_cast<Statement*>(StatementHandle);
        if (!stmt) {
            LOG("Invalid statement handle");
            return SQL_INVALID_HANDLE;
        }

        if (!stmt->hasResult) {
            LOG("No result set available");
            return SQL_ERROR;
        }

        LOGF("Current row: %zu, Total rows: %zu",
             stmt->currentRow, stmt->resultData.size());

        // Check if we've reached the end of the result set
        if (stmt->currentRow >= stmt->resultData.size()) {
            LOG("No more rows available (SQL_NO_DATA)");
            return SQL_NO_DATA;
        }

        // Move to next row
        stmt->currentRow++;
        LOGF("Advanced to row %zu", stmt->currentRow);

        return SQL_SUCCESS;
    }

} // extern "C"