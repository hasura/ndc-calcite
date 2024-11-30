#define NOMINMAX  // Ensure NOMINMAX is defined before including windows.h to prevent min/max macro conflicts
#include <winsock2.h>
#include <windows.h>
#include <algorithm>
#include <string>
#define WIN32_LEAN_AND_MEAN // Reduce size of the Win32 header files
#include "../include/Connection.hpp"
#include "../include/Logging.hpp"
#include "../include/Environment.hpp"
#include "../include/Statement.hpp"

extern "C" {
    SQLRETURN SQL_API SQLGetDiagFieldW(SQLSMALLINT handleType, SQLHANDLE handle, SQLSMALLINT recNumber,
                                       SQLSMALLINT diagIdentifier, SQLPOINTER diagInfo,
                                       SQLSMALLINT bufferLength, SQLSMALLINT* stringLengthPtr) {
        return diagMgr->getDiagField(recNumber, diagIdentifier, diagInfo, bufferLength, stringLengthPtr);
    }
    SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT handleType, SQLHANDLE handle, SQLSMALLINT recNumber,
                                      SQLSMALLINT diagIdentifier, SQLPOINTER diagInfo,
                                      SQLSMALLINT bufferLength, SQLSMALLINT* stringLengthPtr) {

        return diagMgr->getDiagField(recNumber, diagIdentifier, diagInfo, bufferLength, stringLengthPtr);
    }
}

