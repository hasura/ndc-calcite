#define NOMINMAX
#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include <algorithm>
#include <sql.h>
#include <sqltypes.h>
#include "../include/connection.hpp"
#include "../include/Logging.hpp"
#include "../include/Globals.hpp"
#include "../include/Environment.hpp"
#include "../include/statement.hpp"

extern "C" {

SQLRETURN SQLTables_Impl(
    SQLHSTMT        hstmt,
    const char*     szCatalogName,
    SQLSMALLINT     cbCatalogName,
    const char*     szSchemaName,
    SQLSMALLINT     cbSchemaName,
    const char*     szTableName,
    SQLSMALLINT     cbTableName,
    const char*     szTableType,
    SQLSMALLINT     cbTableType)
{
    LOG("SQLTables_Impl called");

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
    std::string tableType = szTableType && cbTableType != 0
                                ? std::string(szTableType, cbTableType == SQL_NTS ? strlen(szTableType) : cbTableType)
                                : "";

    LOGF("Fetching tables with catalog: %s, schema: %s, table: %s, type: %s",
         catalogName.c_str(), schemaName.c_str(), tableName.c_str(), tableType.c_str());

    // Fetch tables using JVM's getTables method
    stmt->conn->GetTables(catalogName, schemaName, tableName, tableType, stmt);

    LOG("SQLTables_Impl RETURNS SQL_SUCCESS");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLTables_A(
    SQLHSTMT        hstmt,
    SQLCHAR*        szCatalogName,
    SQLSMALLINT     cbCatalogName,
    SQLCHAR*        szSchemaName,
    SQLSMALLINT     cbSchemaName,
    SQLCHAR*        szTableName,
    SQLSMALLINT     cbTableName,
    SQLCHAR*        szTableType,
    SQLSMALLINT     cbTableType)
{
    return SQLTables_Impl(hstmt,
                          reinterpret_cast<const char*>(szCatalogName),
                          cbCatalogName,
                          reinterpret_cast<const char*>(szSchemaName),
                          cbSchemaName,
                          reinterpret_cast<const char*>(szTableName),
                          cbTableName,
                          reinterpret_cast<const char*>(szTableType),
                          cbTableType);
}

    SQLRETURN SQL_API SQLTables_W(
        SQLHSTMT        hstmt,
        SQLWCHAR*       szCatalogName,
        SQLSMALLINT     cbCatalogName,
        SQLWCHAR*       szSchemaName,
        SQLSMALLINT     cbSchemaName,
        SQLWCHAR*       szTableName,
        SQLSMALLINT     cbTableName,
        SQLWCHAR*       szTableType,
        SQLSMALLINT     cbTableType)
{
    // Convert each wide string to std::wstring and then to std::string
    std::wstring wsCatalogName(szCatalogName, cbCatalogName == SQL_NTS ? wcslen(szCatalogName) : cbCatalogName);
    std::wstring wsSchemaName(szSchemaName, cbSchemaName == SQL_NTS ? wcslen(szSchemaName) : cbSchemaName);
    std::wstring wsTableName(szTableName, cbTableName == SQL_NTS ? wcslen(szTableName) : cbTableName);
    std::wstring wsTableType(szTableType, cbTableType == SQL_NTS ? wcslen(szTableType) : cbTableType);

    std::string sCatalogName = WideStringToString(wsCatalogName);
    std::string sSchemaName = WideStringToString(wsSchemaName);
    std::string sTableName = WideStringToString(wsTableName);
    std::string sTableType = WideStringToString(wsTableType);

    return SQLTables_Impl(hstmt,
                          sCatalogName.c_str(),
                          cbCatalogName,
                          sSchemaName.c_str(),
                          cbSchemaName,
                          sTableName.c_str(),
                          cbTableName,
                          sTableType.c_str(),
                          cbTableType);
}

}