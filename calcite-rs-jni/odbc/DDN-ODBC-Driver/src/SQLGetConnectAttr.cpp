#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include "../include/Connection.hpp"
#include "../include/Logging.hpp"
#include "../include/Environment.hpp"
#include "../include/Statement.hpp"

template<typename CHAR_TYPE>
SQLRETURN GetConnectAttr_Template(
    SQLHDBC        ConnectionHandle,
    SQLINTEGER     Attribute,
    SQLPOINTER     ValuePtr,
    SQLINTEGER     BufferLength,
    SQLINTEGER*    StringLengthPtr,
    bool           isUnicode) {

    // Validate handle
    if (!ConnectionHandle) {
        return SQL_INVALID_HANDLE;
    }

    auto* conn = static_cast<Connection*>(ConnectionHandle);
    SQLRETURN ret = SQL_SUCCESS;

    switch (Attribute) {
        case SQL_ATTR_LOGIN_TIMEOUT: {
            if (ValuePtr) {
                *static_cast<SQLUINTEGER*>(ValuePtr) = conn->getLoginTimeout();
            }
            break;
        }

        case SQL_ATTR_CONNECTION_TIMEOUT: {
            if (ValuePtr) {
                *static_cast<SQLUINTEGER*>(ValuePtr) = conn->getConnectionTimeout();
            }
            break;
        }

        case SQL_ATTR_ACCESS_MODE: {
            if (ValuePtr) {
                *static_cast<SQLUINTEGER*>(ValuePtr) = SQL_MODE_READ_ONLY; // Driver is read-only
            }
            break;
        }

        case SQL_ATTR_CURRENT_CATALOG: {
            // Set proper error state for unsupported catalog functionality
            conn->setError(
                "HYC00",
                "Catalogs are not supported by this driver",
                0
            );

            if (ValuePtr && BufferLength > 0) {
                // Return empty string
                if (isUnicode) {
                    *static_cast<SQLWCHAR*>(ValuePtr) = L'\0';
                    if (StringLengthPtr) {
                        *StringLengthPtr = 0;
                    }
                } else {
                    *static_cast<SQLCHAR*>(ValuePtr) = '\0';
                    if (StringLengthPtr) {
                        *StringLengthPtr = 0;
                    }
                }
            } else if (StringLengthPtr) {
                *StringLengthPtr = 0;
            }

            return SQL_ERROR;
        }

        case SQL_ATTR_ASYNC_ENABLE: {
            if (ValuePtr) {
                *static_cast<SQLUINTEGER*>(ValuePtr) = SQL_ASYNC_ENABLE_OFF;
            }
            break;
        }

        case SQL_ATTR_AUTO_IPD: {
            if (ValuePtr) {
                *static_cast<SQLUINTEGER*>(ValuePtr) = SQL_FALSE;
            }
            break;
        }

        default:
            return SQL_ERROR;
    }

    return ret;
}

extern "C" {

SQLRETURN SQL_API SQLGetConnectAttrA(
    SQLHDBC      ConnectionHandle,
    SQLINTEGER   Attribute,
    SQLPOINTER   ValuePtr,
    SQLINTEGER   BufferLength,
    SQLINTEGER*  StringLengthPtr) {

    return GetConnectAttr_Template<SQLCHAR>(
        ConnectionHandle,
        Attribute,
        ValuePtr,
        BufferLength,
        StringLengthPtr,
        false  // isUnicode = false
    );
}

SQLRETURN SQL_API SQLGetConnectAttrW(
    SQLHDBC      ConnectionHandle,
    SQLINTEGER   Attribute,
    SQLPOINTER   ValuePtr,
    SQLINTEGER   BufferLength,
    SQLINTEGER*  StringLengthPtr) {

    return GetConnectAttr_Template<SQLWCHAR>(
        ConnectionHandle,
        Attribute,
        ValuePtr,
        BufferLength,
        StringLengthPtr,
        true  // isUnicode = true
    );
}

}