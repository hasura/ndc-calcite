#define NOMINMAX
#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include <algorithm>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <sqlucode.h>
#include "../include/connection.hpp"
#include "../include/logging.hpp"
//#include "../include/httplib.h"
#include "../include/globals.hpp"
#include "../include/environment.hpp"
#include "../include/statement.hpp"

extern "C" {

// Common implementation of SQLColumns
SQLRETURN SQLColumns_Impl(
    SQLHSTMT        hstmt,
    const char*     szCatalogName,
    SQLSMALLINT     cbCatalogName,
    const char*     szSchemaName,
    SQLSMALLINT     cbSchemaName,
    const char*     szTableName,
    SQLSMALLINT     cbTableName,
    const char*     szColumnName,
    SQLSMALLINT     cbColumnName)
{
    LOG("SQLColumns_Impl called");

    auto stmt = static_cast<Statement*>(hstmt);
    if (!stmt) {
        return SQL_INVALID_HANDLE;
    }

    std::string catalogName = szCatalogName && cbCatalogName != 0
                                ? std::string(szCatalogName, cbCatalogName == SQL_NTS ? strlen(szCatalogName) : cbCatalogName)
                                : "";
    std::string schemaName = szSchemaName && cbSchemaName != 0
                                ? std::string(szSchemaName, cbSchemaName == SQL_NTS ? strlen(szSchemaName) : cbSchemaName)
                                : "";
    std::string tableName = szTableName && cbTableName != 0
                                ? std::string(szTableName, cbTableName == SQL_NTS ? strlen(szTableName) : cbTableName)
                                : "";
    std::string columnName = szColumnName && cbColumnName != 0
                                ? std::string(szColumnName, cbColumnName == SQL_NTS ? strlen(szColumnName) : cbColumnName)
                                : "";

    LOGF("Fetching columns with catalog: %s, schema: %s, table: %s, column: %s",
         catalogName.c_str(), schemaName.c_str(), tableName.c_str(), columnName.c_str());

    // Fetch columns using JVM's getColumns method
    stmt->conn->GetColumns(catalogName, schemaName, tableName, columnName, stmt);
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLColumns_A(
    SQLHSTMT        hstmt,
    SQLCHAR*        szCatalogName,
    SQLSMALLINT     cbCatalogName,
    SQLCHAR*        szSchemaName,
    SQLSMALLINT     cbSchemaName,
    SQLCHAR*        szTableName,
    SQLSMALLINT     cbTableName,
    SQLCHAR*        szColumnName,
    SQLSMALLINT     cbColumnName)
{
    return SQLColumns_Impl(hstmt,
                        reinterpret_cast<const char*>(szCatalogName),
                        cbCatalogName,
                        reinterpret_cast<const char*>(szSchemaName),
                        cbSchemaName,
                        reinterpret_cast<const char*>(szTableName),
                        cbTableName,
                        reinterpret_cast<const char*>(szColumnName),
                        cbColumnName);
}

SQLRETURN SQL_API SQLColumns_W(
    SQLHSTMT        hstmt,
    SQLWCHAR*       szCatalogName,
    SQLSMALLINT     cbCatalogName,
    SQLWCHAR*       szSchemaName,
    SQLSMALLINT     cbSchemaName,
    SQLWCHAR*       szTableName,
    SQLSMALLINT     cbTableName,
    SQLWCHAR*       szColumnName,
    SQLSMALLINT     cbColumnName)
{
    // Convert each wide string to std::wstring and then to std::string
    std::wstring wsCatalogName = (szCatalogName && cbCatalogName != 0)
        ? std::wstring(szCatalogName, cbCatalogName == SQL_NTS ? wcslen(szCatalogName) : cbCatalogName)
        : L"";
    std::wstring wsSchemaName = (szSchemaName && cbSchemaName != 0)
        ? std::wstring(szSchemaName, cbSchemaName == SQL_NTS ? wcslen(szSchemaName) : cbSchemaName)
        : L"";
    std::wstring wsTableName = (szTableName && cbTableName != 0)
        ? std::wstring(szTableName, cbTableName == SQL_NTS ? wcslen(szTableName) : cbTableName)
        : L"";
    std::wstring wsColumnName = (szColumnName && cbColumnName != 0)
        ? std::wstring(szColumnName, cbColumnName == SQL_NTS ? wcslen(szColumnName) : cbColumnName)
        : L"";

    std::string sCatalogName = WideStringToString(wsCatalogName);
    std::string sSchemaName = WideStringToString(wsSchemaName);
    std::string sTableName = WideStringToString(wsTableName);
    std::string sColumnName = WideStringToString(wsColumnName);

    return SQLColumns_Impl(hstmt,
                        sCatalogName.c_str(),
                        cbCatalogName,
                        sSchemaName.c_str(),
                        cbSchemaName,
                        sTableName.c_str(),
                        cbTableName,
                        sColumnName.c_str(),
                        cbColumnName);
}

} // extern "C"