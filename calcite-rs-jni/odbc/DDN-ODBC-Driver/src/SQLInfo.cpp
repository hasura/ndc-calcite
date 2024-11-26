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

// Helper function to handle string information types
static SQLRETURN HandleStringInfo(
    bool isUnicode,
    SQLPOINTER infoValue,
    SQLSMALLINT bufferLength,
    SQLSMALLINT* stringLength,
    const char* narrowStr,
    const wchar_t* wideStr)
{
    if (isUnicode) {
        // For Unicode, we need to get the number of characters
        size_t charCount = wcslen(wideStr);
        // Set the length in bytes (not including null terminator)
        if (stringLength) {
            *stringLength = static_cast<SQLSMALLINT>(charCount * sizeof(WCHAR));
            LOGF("Setting Unicode string length: %d bytes for %zu chars",
                 *stringLength, charCount);
        }
        if (infoValue && bufferLength > 0) {
            // Calculate how many characters we can copy including null terminator
            size_t maxChars = bufferLength / sizeof(WCHAR);
            size_t copyChars = std::min(charCount, maxChars - 1);
            wcsncpy(static_cast<WCHAR*>(infoValue), wideStr, copyChars);
            static_cast<WCHAR*>(infoValue)[copyChars] = L'\0';
            LOGF("Copied %zu Unicode chars to buffer", copyChars);
        }
    } else {
        size_t strLen = strlen(narrowStr);
        if (stringLength) {
            *stringLength = static_cast<SQLSMALLINT>(strLen);
            LOGF("Setting ANSI string length: %d", *stringLength);
        }
        if (infoValue && bufferLength > 0) {
            size_t copyLen = std::min<size_t>(strLen, static_cast<size_t>(bufferLength - 1));
            strncpy(static_cast<char*>(infoValue), narrowStr, copyLen);
            static_cast<char*>(infoValue)[copyLen] = '\0';
            LOGF("Copied %zu ANSI chars to buffer", copyLen);
        }
    }
    return SQL_SUCCESS;
}

// Helper function that handles the common logic
static SQLRETURN SQL_API SQLGetInfo_Internal(
    SQLHDBC         hdbc,
    SQLUSMALLINT    infoType,
    SQLPOINTER      infoValue,
    SQLSMALLINT     bufferLength,
    SQLSMALLINT*    stringLength,
    bool            isUnicode)
{
    LOGF("=== SQLGetInfo_Internal Entry ===");
    LOGF("InfoType: %d (0x%04X), BufferLength: %d, isUnicode: %d",
         infoType, infoType, bufferLength, isUnicode);
    LOGF("InfoValue ptr: %p, StringLength ptr: %p", (void*)infoValue, (void*)stringLength);

    auto* conn = static_cast<Connection*>(hdbc);
    if (!conn) {
        LOG("Invalid connection handle");
        return SQL_INVALID_HANDLE;
    }

    try {
        switch (infoType) {
            case SQL_DRIVER_ODBC_VER: {
                const char* ver = "03.80";
                LOGF("SQL_DRIVER_ODBC_VER: Reporting version %s", ver);
                if (infoValue && bufferLength > 0) {
                    strncpy(static_cast<char*>(infoValue), ver, bufferLength);
                    static_cast<char*>(infoValue)[bufferLength - 1] = '\0';
                }
                if (stringLength) {
                    *stringLength = static_cast<SQLSMALLINT>(strlen(ver));
                }
                return SQL_SUCCESS;
            }

            case SQL_DRIVER_NAME: {
                const char* narrowName = "DDN-ODBC-Driver";
                const wchar_t* wideName = L"DDN-ODBC-Driver";
                LOGF("SQL_DRIVER_NAME: Reporting %s", narrowName);
                return HandleStringInfo(isUnicode, infoValue, bufferLength, stringLength,
                                     narrowName, wideName);
            }

            case SQL_DRIVER_VER: {
                const char* narrowVer = "01.00.0000";
                const wchar_t* wideVer = L"01.00.0000";
                LOGF("SQL_DRIVER_VER: Reporting %s", narrowVer);
                return HandleStringInfo(isUnicode, infoValue, bufferLength, stringLength,
                                     narrowVer, wideVer);
            }

            case SQL_DBMS_NAME: {
                const char* narrowName = "Hasura DDN";
                const wchar_t* wideName = L"Hasura DDN";
                LOGF("SQL_DBMS_NAME: Reporting %s", narrowName);
                return HandleStringInfo(isUnicode, infoValue, bufferLength, stringLength,
                                     narrowName, wideName);
            }

            case SQL_DBMS_VER: {
                const char* narrowVer = "1.0";
                const wchar_t* wideVer = L"1.0";
                LOGF("SQL_DBMS_VER: Reporting %s", narrowVer);
                return HandleStringInfo(isUnicode, infoValue, bufferLength, stringLength,
                                     narrowVer, wideVer);
            }

            case SQL_UNICODE: {
                SQLUINTEGER unicodeSupport = SQL_TRUE;
                if (infoValue) {
                    *static_cast<SQLUINTEGER*>(infoValue) = unicodeSupport;
                }
                LOGF("SQL_UNICODE: Reporting support = %u", unicodeSupport);
                return SQL_SUCCESS;
            }

            case SQL_GETDATA_EXTENSIONS: {
                SQLUINTEGER extensions = SQL_GD_ANY_COLUMN |
                                       SQL_GD_ANY_ORDER |
                                       SQL_GD_BLOCK |
                                       SQL_GD_BOUND;
                if (infoValue) {
                    *static_cast<SQLUINTEGER*>(infoValue) = extensions;
                }
                LOGF("SQL_GETDATA_EXTENSIONS: Reporting extensions = 0x%X", extensions);
                return SQL_SUCCESS;
            }

            case SQL_CONVERT_FUNCTIONS: {
                SQLUINTEGER functions = SQL_FN_CVT_CAST | SQL_FN_CVT_CONVERT;
                if (infoValue) {
                    *static_cast<SQLUINTEGER*>(infoValue) = functions;
                }
                LOGF("SQL_CONVERT_FUNCTIONS: Reporting functions = 0x%X", functions);
                return SQL_SUCCESS;
            }

            case SQL_STRING_FUNCTIONS: {
                SQLUINTEGER functions = SQL_FN_STR_CONCAT |
                                      SQL_FN_STR_LENGTH |
                                      SQL_FN_STR_CHAR_LENGTH |
                                      SQL_FN_STR_ASCII |
                                      SQL_FN_STR_SPACE;
                if (infoValue) {
                    *static_cast<SQLUINTEGER*>(infoValue) = functions;
                }
                LOGF("SQL_STRING_FUNCTIONS: Reporting functions = 0x%X", functions);
                return SQL_SUCCESS;
            }

            case SQL_DATA_SOURCE_READ_ONLY:
            case SQL_ACCESSIBLE_PROCEDURES:
            case SQL_ACCESSIBLE_TABLES: {
                if (infoValue && bufferLength > 0) {
                    if (isUnicode) {
                        *static_cast<wchar_t*>(infoValue) = L'N';
                        if (stringLength) *stringLength = sizeof(wchar_t);
                    } else {
                        *static_cast<char*>(infoValue) = 'N';
                        if (stringLength) *stringLength = 1;
                    }
                }
                LOGF("Boolean property %d: Reporting N", infoType);
                return SQL_SUCCESS;
            }

            default: {
                LOGF("Unsupported info type: %d (0x%04X)", infoType, infoType);
                if (infoValue) {
                    if (bufferLength >= sizeof(SQLUINTEGER)) {
                        *static_cast<SQLUINTEGER*>(infoValue) = 0;
                        LOG("Set default DWORD value to 0");
                    } else if (bufferLength > 0) {
                        *static_cast<char*>(infoValue) = '\0';
                        LOG("Set empty string");
                    }
                }
                return SQL_SUCCESS;
            }
        }
    }
    catch (const std::exception& e) {
        LOGF("Exception in SQLGetInfo_Internal: %s", e.what());
        return SQL_ERROR;
    }
}

extern "C" {

SQLRETURN SQL_API SQLGetInfo_W(
    SQLHDBC         hdbc,
    SQLUSMALLINT    infoType,
    SQLPOINTER      infoValue,
    SQLSMALLINT     bufferLength,
    SQLSMALLINT*    stringLength)
{
    LOGF("SQLGetInfo_W Entry - InfoType: %d (0x%04X)", infoType, infoType);
    SQLRETURN ret = SQLGetInfo_Internal(hdbc, infoType, infoValue, bufferLength, stringLength, true);
    LOGF("SQLGetInfo_W Exit - Return: %d", ret);
    return ret;
}

SQLRETURN SQL_API SQLGetInfo_A(
    SQLHDBC         hdbc,
    SQLUSMALLINT    infoType,
    SQLPOINTER      infoValue,
    SQLSMALLINT     bufferLength,
    SQLSMALLINT*    stringLength)
{
    LOGF("SQLGetInfo_A Entry - InfoType: %d (0x%04X)", infoType, infoType);
    SQLRETURN ret = SQLGetInfo_Internal(hdbc, infoType, infoValue, bufferLength, stringLength, false);
    LOGF("SQLGetInfo_A Exit - Return: %d", ret);
    return ret;
}

SQLRETURN SQL_API SQLGetFunctions(
    SQLHDBC         hdbc,
    SQLUSMALLINT    FunctionId,
    SQLUSMALLINT*   Supported)
{
    LOGF("SQLGetFunctions Entry - FunctionId: %d (0x%04X)", FunctionId, FunctionId);

    if (!hdbc || !Supported) {
        return SQL_ERROR;
    }

    // Handle SQL_API_ALL_FUNCTIONS (0)
    if (FunctionId == SQL_API_ALL_FUNCTIONS) {
        // Fill in the 100-element array
        memset(Supported, SQL_FALSE, sizeof(SQLUSMALLINT) * 100);

        // Set supported functions to SQL_TRUE
        Supported[SQL_API_SQLGETDATA] = SQL_TRUE;
        Supported[SQL_API_SQLGETINFO] = SQL_TRUE;
        Supported[SQL_API_SQLGETSTMTATTR] = SQL_TRUE;
        Supported[SQL_API_SQLGETDESCFIELD] = SQL_TRUE;

        LOGF("Reported all function support status");
        return SQL_SUCCESS;
    }

    // Handle individual function checks
    *Supported = SQL_FALSE;
    switch (FunctionId) {
        case SQL_API_SQLGETDATA:
        case SQL_API_SQLGETINFO:
        case SQL_API_SQLGETSTMTATTR:
        case SQL_API_SQLGETDESCFIELD:
            *Supported = SQL_TRUE;
            break;
    }

    LOGF("Function %d support status: %d", FunctionId, *Supported);
    return SQL_SUCCESS;
}

} // extern "C"