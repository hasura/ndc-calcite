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
    SQLRETURN SQL_API SQLBindParameter(
        SQLHSTMT           StatementHandle,
        SQLUSMALLINT      ParameterNumber,
        SQLSMALLINT       InputOutputType,
        SQLSMALLINT       ValueType,
        SQLSMALLINT       ParameterType,
        SQLULEN           ColumnSize,
        SQLSMALLINT       DecimalDigits,
        SQLPOINTER        ParameterValuePtr,
        SQLLEN            BufferLength,
        SQLLEN            *StrLen_or_IndPtr
    ) {
        if (!StatementHandle) {
            return SQL_ERROR;
        }

        Statement* stmt = reinterpret_cast<Statement*>(StatementHandle);

        return stmt->bindParameter(
            ParameterNumber,
            InputOutputType,
            ValueType,
            ParameterType,
            ColumnSize,
            DecimalDigits,
            ParameterValuePtr,
            BufferLength,
            StrLen_or_IndPtr
        );
    }
}