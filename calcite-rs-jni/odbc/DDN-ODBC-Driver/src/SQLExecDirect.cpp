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

    SQLRETURN SQL_API SQLExecDirect(
        SQLHSTMT    StatementHandle,
        SQLCHAR*    StatementText,
        SQLINTEGER  TextLength) {

        LOGF("SQLExecDirect called with query: %s", StatementText);

        auto* stmt = static_cast<Statement*>(StatementHandle);
        if (!stmt || !stmt->conn) {
            LOG("Invalid statement handle or connection");
            return SQL_INVALID_HANDLE;
        }

        // Convert to string based on TextLength
        std::string query;
        if (TextLength == SQL_NTS) {
            query = reinterpret_cast<const char*>(StatementText);
        } else {
            query = std::string(reinterpret_cast<const char*>(StatementText), TextLength);
        }

        LOGF("Executing query: %s", query.c_str());
        return stmt->conn->Query(query, stmt);
    }

    // And the Unicode version
    SQLRETURN SQL_API SQLExecDirectW(
        SQLHSTMT     StatementHandle,
        SQLWCHAR*    StatementText,
        SQLINTEGER   TextLength) {

        auto* stmt = static_cast<Statement*>(StatementHandle);
        if (!stmt || !stmt->conn) {
            LOG("Invalid statement handle or connection");
            return SQL_INVALID_HANDLE;
        }

        // Convert wide string to UTF-8
        std::wstring wquery;
        if (TextLength == SQL_NTS) {
            wquery = reinterpret_cast<const wchar_t*>(StatementText);
        } else {
            wquery = std::wstring(reinterpret_cast<const wchar_t*>(StatementText), TextLength);
        }

        std::string query = WideStringToString(wquery);
        LOGF("SQLExecDirectW executing query: %s", query.c_str());

        return stmt->conn->Query(query, stmt);
    }
} // extern "C"