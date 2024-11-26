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
    SQLRETURN SQL_API SQLConnect_A(
        SQLHDBC         hdbc,
        SQLCHAR*        szDSN,
        SQLSMALLINT     cbDSN,
        SQLCHAR*        szUID,
        SQLSMALLINT     cbUID,
        SQLCHAR*        szAuthStr,
        SQLSMALLINT     cbAuthStr)
    {
        auto conn = static_cast<Connection*>(hdbc);
        if (!conn) {
            return SQL_INVALID_HANDLE;
        }

        std::string dsn(reinterpret_cast<char*>(szDSN),
                       cbDSN == SQL_NTS ? strlen(reinterpret_cast<const char*>(szDSN)) : cbDSN);
        std::string uid(reinterpret_cast<char*>(szUID),
                       cbUID == SQL_NTS ? strlen(reinterpret_cast<const char*>(szUID)) : cbUID);
        std::string authStr(reinterpret_cast<char*>(szAuthStr),
                           cbAuthStr == SQL_NTS ? strlen(reinterpret_cast<const char*>(szAuthStr)) : cbAuthStr);

        conn->setConnectionString(dsn, uid, authStr);

        return conn->connect();
    }

    SQLRETURN SQL_API SQLConnect_W(
        const SQLHDBC         hdbc,
        const SQLWCHAR*       szDSN,
        const SQLSMALLINT     cbDSN,
        const SQLWCHAR*       szUID,
        const SQLSMALLINT     cbUID,
        const SQLWCHAR*       szAuthStr,
        const SQLSMALLINT     cbAuthStr)
    {
        auto conn = static_cast<Connection*>(hdbc);
        if (!conn) {
            return SQL_INVALID_HANDLE;
        }

        std::wstring wdsn(szDSN, cbDSN == SQL_NTS ? wcslen(szDSN) : cbDSN);
        std::wstring wuid(szUID, cbUID == SQL_NTS ? wcslen(szUID) : cbUID);
        std::wstring wauthStr(szAuthStr, cbAuthStr == SQL_NTS ? wcslen(szAuthStr) : cbAuthStr);

        std::string dsn = WideStringToString(wdsn);
        std::string uid = WideStringToString(wuid);
        std::string authStr = WideStringToString(wauthStr);

        conn->setConnectionString(dsn, uid, authStr);

        return conn->connect();
    }
}