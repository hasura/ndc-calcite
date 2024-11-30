#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include "../include/Connection.hpp"
#include "../include/Logging.hpp"
#include "../include/Globals.hpp"
#include "../include/Environment.hpp"
#include "../include/Statement.hpp"

extern "C" {
SQLRETURN SQL_API SQLConnect_A(
    SQLHDBC hdbc,
    SQLCHAR *szDSN,
    SQLSMALLINT cbDSN,
    SQLCHAR *szUID,
    SQLSMALLINT cbUID,
    SQLCHAR *szAuthStr,
    SQLSMALLINT cbAuthStr) {
    return SQL_NO_DATA;
}

SQLRETURN SQL_API SQLConnect_W(
    const SQLHDBC hdbc,
    const SQLWCHAR *szDSN,
    const SQLSMALLINT cbDSN,
    const SQLWCHAR *szUID,
    const SQLSMALLINT cbUID,
    const SQLWCHAR *szAuthStr,
    const SQLSMALLINT cbAuthStr) {
    return SQL_NO_DATA;
}
}
