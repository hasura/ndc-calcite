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

    SQLRETURN SQL_API SQLGetStmtAttr(
    SQLHSTMT     StatementHandle,
    SQLINTEGER   Attribute,
    SQLPOINTER   Value,
    SQLINTEGER   BufferLength,
    SQLINTEGER*  StringLength) {

        LOG("SQLGetStmtAttr called");
        auto* stmt = static_cast<Statement*>(StatementHandle);
        if (!stmt) {
            LOG("Invalid statement handle");
            return SQL_INVALID_HANDLE;
        }

        LOGF("Getting attribute: %d", Attribute);

        switch (Attribute) {
            case SQL_ATTR_IMP_ROW_DESC:
                LOGF("Getting SQL_ATTR_IMP_ROW_DESC, value ptr: %p", Value);
            // Since Statement is itself the IRD handle in our implementation
            *static_cast<SQLHDESC*>(Value) = StatementHandle;
            LOG("Returning statement handle as IRD");
            return SQL_SUCCESS;

            case SQL_ATTR_IMP_PARAM_DESC:
                LOGF("Getting SQL_ATTR_IMP_PARAM_DESC, value ptr: %p", Value);
            // Same for IPD
            *static_cast<SQLHDESC*>(Value) = StatementHandle;
            return SQL_SUCCESS;

            case SQL_ATTR_APP_ROW_DESC:
                LOGF("Getting SQL_ATTR_APP_ROW_DESC, value ptr: %p", Value);
            // Same for ARD
            *static_cast<SQLHDESC*>(Value) = StatementHandle;
            return SQL_SUCCESS;

            case SQL_ATTR_APP_PARAM_DESC:
                LOGF("Getting SQL_ATTR_APP_PARAM_DESC, value ptr: %p", Value);
            // Same for APD
            *static_cast<SQLHDESC*>(Value) = StatementHandle;
            return SQL_SUCCESS;

            // Add other attributes as needed

            default:
                LOGF("Unsupported attribute: %d", Attribute);
            return SQL_ERROR;
        }
    }

} // extern "C"