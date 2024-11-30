#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include "../include/Connection.hpp"
#include "../include/Logging.hpp"
#include "../include/Environment.hpp"
#include "../include/Statement.hpp"

// Common internal function to set connection attributes
SQLRETURN SetConnectAttrInternal(SQLHDBC hdbc, SQLINTEGER Attribute, SQLPOINTER Value, SQLINTEGER StringLength) {
    LOG("Calling SetConnectAttrInternal");

    // Initial handle and pointer checks
    if (!hdbc) {
        LOG("SQLSetConnectAttrInternal: Invalid connection handle");
        return SQL_ERROR;
    }
    LOG("SQLSetConnectAttrInternal: Valid connection handle");

    Connection *conn = static_cast<Connection*>(hdbc);
    if (!conn) {
        LOG("SQLSetConnectAttrInternal: Failed to cast connection handle");
        return SQL_ERROR;
    }
    LOG("SQLSetConnectAttrInternal: Successfully cast connection handle");

    if (!Value) {
        LOG("SQLSetConnectAttrInternal: Value pointer is null");
        return SQL_ERROR;
    }
    LOGF("SQLSetConnectAttrInternal: Value pointer is not null, value address: %p", Value);

    // Check and log the raw value pointed to by Value
    SQLINTEGER intValue = reinterpret_cast<SQLINTEGER>(Value);
    LOGF("SQLSetConnectAttrInternal: Interpreted value: %d", intValue);

    // Switch case to handle attributes
    switch (Attribute) {
        case SQL_ATTR_AUTOCOMMIT:
            LOG("SQLSetConnectAttrInternal: Handling SQL_ATTR_AUTOCOMMIT");
            LOGF("SQLSetConnectAttr: Setting autocommit to %u", intValue);
            if (intValue == SQL_AUTOCOMMIT_ON || intValue == SQL_AUTOCOMMIT_OFF) {
                conn->setAutoCommit(intValue == SQL_AUTOCOMMIT_ON);
                LOG("SQLSetConnectAttrInternal: Successfully set autocommit");
            } else {
                LOG("SQLSetConnectAttrInternal: Invalid value for autocommit");
                return SQL_ERROR;
            }
            break;

        case SQL_ATTR_CONNECTION_TIMEOUT:
            LOG("SQLSetConnectAttrInternal: Handling SQL_ATTR_CONNECTION_TIMEOUT");
            LOGF("SQLSetConnectAttr: Setting connection timeout to %d", intValue);
            conn->setConnectionTimeout(intValue);
            LOG("SQLSetConnectAttrInternal: Successfully set connection timeout");
            break;

        case SQL_ATTR_LOGIN_TIMEOUT:
            LOG("SQLSetConnectAttrInternal: Handling SQL_ATTR_LOGIN_TIMEOUT");
            LOGF("SQLSetConnectAttr: Setting login timeout to %d", intValue);
            conn->setLoginTimeout(intValue);
            LOG("SQLSetConnectAttrInternal: Successfully set login timeout");
            break;

        default:
            LOGF("SQLSetConnectAttr: Unknown attribute requested: %d", Attribute);
            conn->setError("HYC00", "SQLSetConnectAttr: Unknown attribute requested", 0);
            return SQL_ERROR;
    }

    LOG("SQLSetConnectAttrInternal: Successfully completed");
    return SQL_SUCCESS;
}

extern "C" {
    SQLRETURN SQL_API SQLSetConnectAttrW(SQLHDBC hdbc, SQLINTEGER Attribute, SQLPOINTER Value, SQLINTEGER StringLength) {
        LOG("Calling SQLSetConnectAttrW");
        return SetConnectAttrInternal(hdbc, Attribute, Value, StringLength);
    }
    SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC hdbc, SQLINTEGER Attribute, SQLPOINTER Value, SQLINTEGER StringLength) {
        LOG("Calling SQLSetConnectAttr");
        return SetConnectAttrInternal(hdbc, Attribute, Value, StringLength);
    }
}
