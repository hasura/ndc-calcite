#define NOMINMAX
#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include <algorithm>
#include "../include/Connection.hpp"
#include "../include/Logging.hpp"
#include "../include/Globals.hpp"
#include "../include/Environment.hpp"
#include "../include/Statement.hpp"
std::wstring MaskSensitiveInfo(const SQLWCHAR *str, SQLSMALLINT len) {
    if (!str) return L"";

    std::wstring input = (len == SQL_NTS) ? str : std::wstring(str, len);

    // List of keywords to mask
    const std::vector<std::wstring> sensitiveKeys = {
        L"PWD=", L"PASSWORD=", L"PASSWD=", L"AUTH="
    };

    std::wstring masked = input;
    for (const auto &key: sensitiveKeys) {
        size_t pos = masked.find(key);
        if (pos != std::wstring::npos) {
            pos += key.length();
            size_t end = masked.find(L';', pos);
            if (end == std::wstring::npos) end = masked.length();
            masked.replace(pos, end - pos, L"*****");
        }
    }

    return masked;
}


extern "C" {

void LogConnectionString(const SQLWCHAR *str, SQLSMALLINT len) {
    if (!str) {
        LOG("Connection string: <NULL>");
        return;
    }

    if (len == SQL_NTS) {
        LOGF("Connection string length: SQL_NTS");
    } else {
        LOGF("Connection string length: %d", len);
    }

    // Only log string content if we have a valid pointer and length
    if (str && (len == SQL_NTS || len > 0)) {
        // Mask password in logging
        std::wstring masked = MaskSensitiveInfo(str, len);
        LOGF("Connection string (masked): %ls", masked.c_str());
    }
}

SQLRETURN SQL_API SQLDriverConnect_A(
    SQLHDBC hdbc,
    SQLHWND hwnd,
    SQLCHAR *szConnStrIn,
    SQLSMALLINT cbConnStrIn,
    SQLCHAR *szConnStrOut,
    SQLSMALLINT cbConnStrOutMax,
    SQLSMALLINT *pcbConnStrOut,
    SQLUSMALLINT fDriverCompletion) {
    auto conn = static_cast<Connection *>(hdbc);
    if (!conn) {
        return SQL_INVALID_HANDLE;
    }

    std::string connStr(reinterpret_cast<char *>(szConnStrIn),
                        cbConnStrIn == SQL_NTS ? strlen(reinterpret_cast<const char *>(szConnStrIn)) : cbConnStrIn);
    LOGF("Assigning connection string: %s", connStr.c_str());

    conn->setConnectionString(connStr);

    if (!conn->connect()) {
        LOG("Failed to start Java process in SQLDriverConnect_A.");
        return SQL_ERROR;
    }

    if (szConnStrOut && cbConnStrOutMax > 0) {
        strncpy(reinterpret_cast<char *>(szConnStrOut), connStr.c_str(), cbConnStrOutMax - 1);
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
    SQLWCHAR *szConnStrIn,
    SQLSMALLINT cbConnStrIn,
    SQLWCHAR *szConnStrOut,
    SQLSMALLINT cbConnStrOutMax,
    SQLSMALLINT *pcbConnStrOut,
    SQLUSMALLINT fDriverCompletion) {

    LOGF("SQLDriverConnect_W called with fDriverCompletion: %d", fDriverCompletion);

    auto conn = static_cast<Connection *>(hdbc);
    if (!conn) {
        LOG("Invalid connection handle");
        return SQL_INVALID_HANDLE;
    }

    // Handle input string length. Handle SQL_NTS and negative values
    try {
        std::wstring wConnStr;
        if (szConnStrIn) {
            if (cbConnStrIn == SQL_NTS) {
                wConnStr = szConnStrIn;  // Will read until null terminator
            } else if (cbConnStrIn > 0) {
                wConnStr = std::wstring(szConnStrIn, cbConnStrIn);
            } else {
                // Treat negative length as SQL_NTS as per ODBC spec
                wConnStr = szConnStrIn;
            }
        }

        std::string connStr = WideStringToString(wConnStr);
        LOGF("Connection string: %s", connStr.c_str());

        conn->setConnectionString(connStr);
        SQLRETURN connectResult = conn->connect();
        if (connectResult != SQL_SUCCESS) {
            return connectResult;
        }

        // Handle output connection string
        std::wstring wConnStrOut = StringToWideString(conn->getConnectionString());
        SQLSMALLINT totalLenNeeded = static_cast<SQLSMALLINT>(wConnStrOut.length() * sizeof(WCHAR) + sizeof(WCHAR));

        // If output buffer provided
        if (szConnStrOut && cbConnStrOutMax > 0) {
            SQLSMALLINT copyLen = static_cast<SQLSMALLINT>(
                std::min<size_t>(
                    (cbConnStrOutMax - sizeof(WCHAR)) / sizeof(WCHAR),
                    wConnStrOut.length()
                )
            );

            wmemcpy(szConnStrOut, wConnStrOut.c_str(), copyLen);
            szConnStrOut[copyLen] = L'\0';
        }

        // Always return required length if pointer provided
        if (pcbConnStrOut) {
            *pcbConnStrOut = totalLenNeeded;
        }

        return SQL_SUCCESS_WITH_INFO;

    } catch (const std::exception& e) {
        LOGF("Exception in SQLDriverConnect_W: %s", e.what());
        return SQL_ERROR;
    }
}
}
