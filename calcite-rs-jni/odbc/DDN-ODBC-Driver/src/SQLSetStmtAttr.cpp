#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include "../include/Connection.hpp"
#include "../include/Logging.hpp"
#include "../include/Environment.hpp"
#include "../include/Statement.hpp"

#ifndef SQL_SOPT_SS_PARAM_FOCUS
#define SQL_SOPT_SS_PARAM_FOCUS 1227
#endif

template<typename CHAR_TYPE>
SQLRETURN SetStmtAttr_Template(
    SQLHSTMT     StatementHandle,
    SQLINTEGER   Attribute,
    SQLPOINTER   ValuePtr,
    SQLINTEGER   StringLength,
    bool         isUnicode) {

    // Validate handle
    if (!StatementHandle) {
        return SQL_INVALID_HANDLE;
    }

    auto* stmt = static_cast<Statement*>(StatementHandle);
    SQLRETURN ret = SQL_SUCCESS;

    // Handle the various statement attributes
    switch (Attribute) {

        case SQL_ATTR_ROW_ARRAY_SIZE:
            // Allow but silently handle as single row
                break;

        case SQL_ATTR_ROWS_FETCHED_PTR:
        case SQL_ATTR_ROW_STATUS_PTR:
            // Support basic array operations
            if (ValuePtr != nullptr) {
                // Store pointers for later use
                if (Attribute == SQL_ATTR_ROWS_FETCHED_PTR) {
                    stmt->setRowsFetchedPtr(static_cast<SQLULEN*>(ValuePtr));
                } else {
                    stmt->setRowStatusPtr(static_cast<SQLUSMALLINT*>(ValuePtr));
                }
                break;
            }
        return SQL_SUCCESS;
        case SQL_SOPT_SS_PARAM_FOCUS:
            if (reinterpret_cast<SQLLEN>(ValuePtr) > 1) {
                stmt->setError("IM001", "Driver does not support SQLServer-specific attribute SQL_SOPT_SS_PARAM_FOCUS", 0);
                return SQL_ERROR;
            }
        break;

        case SQL_ATTR_CONCURRENCY:
            if (reinterpret_cast<SQLLEN>(ValuePtr) != SQL_CONCUR_READ_ONLY) {
                stmt->setError("HYC00", "Only read-only cursors are supported", 0);
                return SQL_ERROR;
            }
        break;

        case SQL_ATTR_CURSOR_TYPE:
            if (reinterpret_cast<SQLLEN>(ValuePtr) != SQL_CURSOR_FORWARD_ONLY) {
                stmt->setError("HYC00", "Only forward-only cursors are supported", 0);
                return SQL_ERROR;
            }
        break;

        case SQL_ATTR_PARAM_BIND_TYPE:
            if (reinterpret_cast<SQLLEN>(ValuePtr) != SQL_PARAM_BIND_BY_COLUMN) {
                stmt->setError("HYC00", "Only column-wise binding is supported", 0);
                return SQL_ERROR;
            }
        break;

        case SQL_ATTR_ROW_BIND_TYPE:
            if (reinterpret_cast<SQLLEN>(ValuePtr) != SQL_BIND_BY_COLUMN) {
                stmt->setError("HYC00", "Only column-wise binding is supported", 0);
                return SQL_ERROR;
            }
        break;

        case SQL_ATTR_ASYNC_ENABLE:
            if (reinterpret_cast<SQLLEN>(ValuePtr) != SQL_ASYNC_ENABLE_OFF) {
                stmt->setError("HYC00", "Asynchronous execution not supported", 0);
                return SQL_ERROR;
            }
        break;

        case SQL_ATTR_FETCH_BOOKMARK_PTR:
            stmt->setError("HYC00", "Bookmarks not supported", 0);
        return SQL_ERROR;

        case SQL_ATTR_RETRIEVE_DATA:
            stmt->setRetrieveData(reinterpret_cast<SQLLEN>(ValuePtr) == SQL_RD_ON);
            break;

        case SQL_ATTR_NOSCAN:
            break; // Accept but ignore

        case SQL_ATTR_APP_ROW_DESC:
            stmt->setError("HYC00", "Optional feature not implemented", 0);
        return SQL_ERROR;

        case SQL_ATTR_MAX_LENGTH:
            stmt->setMaxLength(static_cast<SQLULEN>(reinterpret_cast<SQLLEN>(ValuePtr)));
            break;

        case SQL_ATTR_MAX_ROWS:
            stmt->setMaxRows(static_cast<SQLULEN>(reinterpret_cast<SQLLEN>(ValuePtr)));
            break;

        case SQL_ATTR_QUERY_TIMEOUT:
            stmt->setQueryTimeout(static_cast<SQLULEN>(reinterpret_cast<SQLLEN>(ValuePtr)));
            break;

        default:
            LOGF("Unsupported statement attribute: %d", Attribute);
            stmt->setError("HYC00", "Unsupported statement attribute", 0);
            return SQL_ERROR;
    }

    return ret;
}

extern "C" {

SQLRETURN SQL_API SQLSetStmtAttrW(
    SQLHSTMT     StatementHandle,
    SQLINTEGER   Attribute,
    SQLPOINTER   ValuePtr,
    SQLINTEGER   StringLength) {

    return SetStmtAttr_Template<SQLWCHAR>(
        StatementHandle,
        Attribute,
        ValuePtr,
        StringLength,
        true  // isUnicode = true
    );
}

SQLRETURN SQL_API SQLSetStmtAttr(
    SQLHSTMT     StatementHandle,
    SQLINTEGER   Attribute,
    SQLPOINTER   ValuePtr,
    SQLINTEGER   StringLength) {

    return SetStmtAttr_Template<SQLCHAR>(
        StatementHandle,
        Attribute,
        ValuePtr,
        StringLength,
        false  // isUnicode = false
    );
}

} // extern "C"