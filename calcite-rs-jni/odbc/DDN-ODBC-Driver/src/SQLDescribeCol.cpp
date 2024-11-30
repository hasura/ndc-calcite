#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include "../include/Connection.hpp"
#include "../include/Logging.hpp"
#include "../include/Environment.hpp"
#include "../include/Statement.hpp"

extern "C" {

SQLRETURN SQL_API SQLDescribeCol(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT ColumnNumber,
    SQLCHAR*     ColumnName,
    SQLSMALLINT  BufferLength,
    SQLSMALLINT* NameLengthPtr,
    SQLSMALLINT* DataTypePtr,
    SQLULEN*     ColumnSizePtr,
    SQLSMALLINT* DecimalDigitsPtr,
    SQLSMALLINT* NullablePtr) {

    auto* stmt = static_cast<Statement*>(StatementHandle);
    LOGF("SQLDescribeCol called - Column: %u", ColumnNumber);

    if (!stmt) {
        LOG("Invalid statement handle");
        return SQL_INVALID_HANDLE;
    }

    if (!stmt->hasResult) {
        LOG("No result set available");
        stmt->setError("HY000", "No result set available", 0);
        return SQL_ERROR;
    }

    // Column numbers are 1-based in ODBC
    if (ColumnNumber < 1 || ColumnNumber > stmt->resultColumns.size()) {
        LOGF("Invalid column number: %u", ColumnNumber);
        stmt->setError("07009", "Invalid descriptor index", 0);
        return SQL_ERROR;
    }

    try {
        const auto& col = stmt->resultColumns[ColumnNumber - 1];

        // Handle column name
        if (ColumnName && BufferLength > 0) {
            size_t copyLen = std::min<size_t>(strlen(col.name), static_cast<size_t>(BufferLength - 1));
            strncpy(reinterpret_cast<char*>(ColumnName), col.name, copyLen);
            ColumnName[copyLen] = '\0';
        }

        if (NameLengthPtr) {
            *NameLengthPtr = static_cast<SQLSMALLINT>(strlen(col.name));
        }

        // Set the SQL data type
        if (DataTypePtr) {
            *DataTypePtr = col.sqlType;
        }

        // Set the column size
        if (ColumnSizePtr) {
            *ColumnSizePtr = col.columnSize;
        }

        // Set decimal digits (precision for numeric types)
        if (DecimalDigitsPtr) {
            *DecimalDigitsPtr = col.precision;
        }

        // Set nullable attribute
        if (NullablePtr) {
            *NullablePtr = col.nullable;
        }

        return SQL_SUCCESS;

    } catch (const std::exception& e) {
        stmt->setError("HY000", e.what(), 0);
        return SQL_ERROR;
    }
}

SQLRETURN SQL_API SQLDescribeColW(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT ColumnNumber,
    SQLWCHAR*    ColumnName,
    SQLSMALLINT  BufferLength,
    SQLSMALLINT* NameLengthPtr,
    SQLSMALLINT* DataTypePtr,
    SQLULEN*     ColumnSizePtr,
    SQLSMALLINT* DecimalDigitsPtr,
    SQLSMALLINT* NullablePtr) {

    LOGF("SQLDescribeColW called - Column: %u", ColumnNumber);

    // Get the column info from statement
    auto* stmt = static_cast<Statement*>(StatementHandle);
    if (!stmt || ColumnNumber < 1 || ColumnNumber > stmt->resultColumns.size()) {
        return SQL_ERROR;
    }

    try {
        const auto& col = stmt->resultColumns[ColumnNumber - 1];

        // Handle the string conversion for column name
        if (ColumnName && BufferLength > 0) {
            size_t numChars = 0;
            errno_t err = mbstowcs_s(&numChars,
                                   ColumnName,
                                   BufferLength,
                                   col.name,
                                   _TRUNCATE);

            if (err != 0) {
                stmt->setError("HY000", "Unicode conversion failed", 0);
                return SQL_ERROR;
            }

            if (NameLengthPtr) {
                *NameLengthPtr = static_cast<SQLSMALLINT>((numChars - 1) * sizeof(SQLWCHAR));
            }
        } else if (NameLengthPtr) {
            // Just return required length if no buffer provided
            *NameLengthPtr = static_cast<SQLSMALLINT>(strlen(col.name) * sizeof(SQLWCHAR));
        }

        // Rest of the parameters are the same as ANSI version
        if (DataTypePtr) {
            *DataTypePtr = col.sqlType;
        }
        if (ColumnSizePtr) {
            *ColumnSizePtr = col.columnSize;
        }
        if (DecimalDigitsPtr) {
            *DecimalDigitsPtr = col.precision;
        }
        if (NullablePtr) {
            *NullablePtr = col.nullable;
        }

        return SQL_SUCCESS;

    } catch (const std::exception& e) {
        stmt->setError("HY000", e.what(), 0);
        return SQL_ERROR;
    }
}

} // extern "C"