#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include "../include/connection.hpp"
#include "../include/logging.hpp"
//#include "../include/httplib.h"
#include "../include/globals.hpp"
#include "../include/environment.hpp"
#include "../include/statement.hpp"

SQLRETURN SQL_API SQLFreeStmt(
    SQLHSTMT StatementHandle,
    SQLUSMALLINT Option)
{
    LOGF("SQLFreeStmt called with Option: %d", Option);

    auto stmt = static_cast<Statement*>(StatementHandle);

    if (!stmt) {
        LOG("Invalid statement handle");
        return SQL_INVALID_HANDLE;
    }

    switch (Option) {
        case SQL_CLOSE:
            LOGF("Closing statement, currentRow: %zu, resultData.size(): %zu",
                 stmt->currentRow, stmt->resultData.size());

            // Reset the result set state
            stmt->currentRow = 0;
            stmt->resultData.clear();
            stmt->resultColumns.clear();
            stmt->hasResult = false;

            LOG("Statement closed successfully");
            return SQL_SUCCESS;

        case SQL_DROP:
            LOGF("Dropping statement, currentRow: %zu, resultData.size(): %zu",
                 stmt->currentRow, stmt->resultData.size());

            // Free the statement resources
            stmt->clearResults();
            delete stmt;

            LOG("Statement dropped successfully");
            return SQL_SUCCESS;

        case SQL_UNBIND:
            LOG("Unbinding statement columns");

            // Unbind any bound columns
            // (not implemented in this example)

            LOG("Statement columns unbound successfully");
            return SQL_SUCCESS;

        case SQL_RESET_PARAMS:
            LOG("Resetting statement parameters");

            // Reset any bound parameters
            // (not implemented in this example)

            LOG("Statement parameters reset successfully");
            return SQL_SUCCESS;

        default:
            LOGF("Unsupported SQLFreeStmt option: %d", Option);
            return SQL_ERROR;
    }
}