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

    SQLRETURN SQL_API SQLExecDirect_A(
    SQLHSTMT        hstmt,
    SQLCHAR*        szSqlStr,
    SQLINTEGER      cbSqlStr)
    {
        std::string sqlStr(reinterpret_cast<char*>(szSqlStr),
            cbSqlStr == SQL_NTS ? strlen(reinterpret_cast<const char*>(szSqlStr)) : cbSqlStr);

        auto stmt = static_cast<Statement*>(hstmt);
        if (!stmt) {
            return SQL_INVALID_HANDLE;
        }

        return SQL_ERROR;
    }

    SQLRETURN SQL_API SQLExecDirect_W(
        SQLHSTMT        hstmt,
        SQLWCHAR*       szSqlStr,
        SQLINTEGER      cbSqlStr)
    {
        // Get the actual string length
        size_t actualLength = 0;
        while (szSqlStr[actualLength] != L'\0' &&
               !(szSqlStr[actualLength] == L'\\' && szSqlStr[actualLength + 1] == L'0')) {
            actualLength++;
               }

        // Create wide string excluding the "\ 0" terminator
        std::wstring wsqlStr(szSqlStr, actualLength);
        std::string sqlStr = WideStringToString(wsqlStr);

        LOGF("SQLExecDirect_W: Original SQL: '%s'", sqlStr.c_str());

        // Transform the query
        std::string transformedSql = sqlStr;

        // Replace INFORMATION_SCHEMA.TABLES
        size_t pos = transformedSql.find("INFORMATION_SCHEMA.TABLES");
        if (pos != std::string::npos) {
            transformedSql.replace(pos, std::string("INFORMATION_SCHEMA.TABLES").length(), "metadata.TABLES");
        }

        // Replace INFORMATION_SCHEMA.COLUMNS
        pos = transformedSql.find("INFORMATION_SCHEMA.COLUMNS");
        if (pos != std::string::npos) {
            transformedSql.replace(pos, std::string("INFORMATION_SCHEMA.COLUMNS").length(), "metadata.COLUMNS");
        }

        LOGF("SQLExecDirect_W: Transformed SQL: '%s'", transformedSql.c_str());

        auto stmt = static_cast<Statement*>(hstmt);
        if (!stmt) {
            LOG("SQLExecDirect_W: Invalid statement handle");
            return SQL_INVALID_HANDLE;
        }

        return SQL_ERROR;
    }
} // extern "C"