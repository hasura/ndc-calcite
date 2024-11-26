#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include "../include/connection.hpp"
#include "../include/logging.hpp"
#include "../include/globals.hpp"
#include "../include/environment.hpp"
#include "../include/statement.hpp"

extern "C" {
SQLRETURN SQL_API SQLDriverConnect_A(
    SQLHDBC hdbc,
    SQLHWND hwnd,
    SQLCHAR* szConnStrIn,
    SQLSMALLINT cbConnStrIn,
    SQLCHAR* szConnStrOut,
    SQLSMALLINT cbConnStrOutMax,
    SQLSMALLINT* pcbConnStrOut,
    SQLUSMALLINT fDriverCompletion)
{
    auto conn = static_cast<Connection*>(hdbc);
    if (!conn) {
        return SQL_INVALID_HANDLE;
    }

    std::string connStr(reinterpret_cast<char*>(szConnStrIn), 
                       cbConnStrIn == SQL_NTS ? strlen(reinterpret_cast<const char*>(szConnStrIn)) : cbConnStrIn);
    LOGF("Assigning connection string: %s", connStr.c_str());

    conn->setConnectionString(connStr);
    if (!conn->connect()) {
        LOG("Failed to start Java process in SQLDriverConnect_A.");
        return SQL_ERROR;
    }

    if (szConnStrOut && cbConnStrOutMax > 0) {
        strncpy(reinterpret_cast<char*>(szConnStrOut), connStr.c_str(), cbConnStrOutMax - 1);
        szConnStrOut[cbConnStrOutMax - 1] = '\0';
        if (pcbConnStrOut) {
            *pcbConnStrOut = static_cast<SQLSMALLINT>(connStr.length());
        }
    }

    // Return SQL_SUCCESS_WITH_INFO to prompt driver manager to call SQLGetInfo
    LOG("Returning SQL_SUCCESS_WITH_INFO to trigger SQLGetInfo calls");
    return SQL_SUCCESS_WITH_INFO;
}

    SQLRETURN SQL_API SQLDriverConnect_W(
        SQLHDBC hdbc,
        SQLHWND hwnd,
        SQLWCHAR* szConnStrIn,
        SQLSMALLINT cbConnStrIn,
        SQLWCHAR* szConnStrOut,
        SQLSMALLINT cbConnStrOutMax,
        SQLSMALLINT* pcbConnStrOut,
        SQLUSMALLINT fDriverCompletion)
{
    LOGF("SQLDriverConnect_W called with fDriverCompletion: %d", fDriverCompletion);

    auto conn = static_cast<Connection*>(hdbc);
    if (!conn) {
        LOG("Invalid connection handle");
        return SQL_INVALID_HANDLE;
    }

    std::wstring wConnStr(szConnStrIn, cbConnStrIn == SQL_NTS ? wcslen(szConnStrIn) : cbConnStrIn);
    std::string connStr = WideStringToString(wConnStr);
    LOGF("Connection string: %s", connStr.c_str());

    conn->setConnectionString(connStr);
    SQLRETURN connectResult = conn->connect();
    if (connectResult != SQL_SUCCESS) {
        LOGF("Failed to connect with result: %d", connectResult);
        return connectResult;
    }

    LOG("Connection successful");

    // Handle output connection string
    const std::string& connStrOut = conn->getConnectionString();
    std::wstring wConnStrOut(connStrOut.begin(), connStrOut.end());

    if (szConnStrOut && cbConnStrOutMax > 0) {
        size_t copyLength = std::min<size_t>(wConnStrOut.size(), cbConnStrOutMax - 1);
        wcsncpy(szConnStrOut, wConnStrOut.c_str(), copyLength);
        szConnStrOut[copyLength] = L'\0';

        if (pcbConnStrOut) {
            *pcbConnStrOut = static_cast<SQLSMALLINT>(copyLength + 1);
            LOGF("Set output connection string length: %d", *pcbConnStrOut);
        }
    }

    // Return SQL_SUCCESS_WITH_INFO to prompt driver manager to call SQLGetInfo
    LOG("Returning SQL_SUCCESS_WITH_INFO to trigger SQLGetInfo calls");
    return SQL_SUCCESS_WITH_INFO;
}

}