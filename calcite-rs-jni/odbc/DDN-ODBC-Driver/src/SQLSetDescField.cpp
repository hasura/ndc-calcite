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

extern "C" {

SQLRETURN SQL_API SQLSetDescField(
    SQLHDESC    DescriptorHandle,
    SQLSMALLINT RecNumber,
    SQLSMALLINT FieldIdentifier,
    SQLPOINTER  Value,
    SQLINTEGER  BufferLength) {

    LOG("SQLSetDescField called");
    auto* stmt = static_cast<Statement*>(DescriptorHandle);
    if (!stmt) {
        LOG("Invalid descriptor handle");
        return SQL_INVALID_HANDLE;
    }

    LOGF("Setting field: %d for record: %d", FieldIdentifier, RecNumber);

    // For record 0, only SQL_DESC_COUNT is valid
    if (RecNumber == 0) {
        if (FieldIdentifier != SQL_DESC_COUNT) {
            LOG("Invalid field identifier for record 0");
            return SQL_ERROR;
        }
        size_t count = reinterpret_cast<size_t>(Value);
        LOGF("Setting column count to: %zu", count);
        stmt->resultColumns.resize(count);
        return SQL_SUCCESS;
    }

    // For other records
    size_t colIdx = RecNumber - 1;
    if (colIdx >= stmt->resultColumns.size()) {
        LOG("Column index out of range");
        return SQL_ERROR;
    }

    switch (FieldIdentifier) {
        case SQL_DESC_TYPE:
        case SQL_DESC_CONCISE_TYPE:
            stmt->resultColumns[colIdx].sqlType = reinterpret_cast<SQLSMALLINT>(Value);
            break;

        case SQL_DESC_NAME:
            if (Value) {
                stmt->resultColumns[colIdx].name = static_cast<const char*>(Value);
                stmt->resultColumns[colIdx].nameLength =
                    (BufferLength == SQL_NTS) ? strlen(stmt->resultColumns[colIdx].name) : BufferLength;
            }
            break;

        case SQL_DESC_NULLABLE:
            stmt->resultColumns[colIdx].nullable = reinterpret_cast<SQLSMALLINT>(Value);
            break;

        case SQL_DESC_LENGTH:
            stmt->resultColumns[colIdx].columnSize = reinterpret_cast<SQLULEN>(Value);
            break;

        case SQL_DESC_PRECISION:
            stmt->resultColumns[colIdx].decimalDigits = reinterpret_cast<SQLSMALLINT>(Value);
            break;

        default:
            LOGF("Unsupported field identifier: %d", FieldIdentifier);
            return SQL_ERROR;
    }

    return SQL_SUCCESS;
}

    SQLRETURN SQL_API SQLSetDescFieldW(
        SQLHDESC    DescriptorHandle,
        SQLSMALLINT RecNumber,
        SQLSMALLINT FieldIdentifier,
        SQLPOINTER  Value,
        SQLINTEGER  BufferLength) {

    if (FieldIdentifier == SQL_DESC_NAME && Value != nullptr) {
        // Convert wide string to narrow for internal storage
        const wchar_t* wstr = static_cast<const wchar_t*>(Value);
        std::string name = WideStringToString(std::wstring(wstr));
        return SQLSetDescField(DescriptorHandle, RecNumber, FieldIdentifier,
                             const_cast<char*>(name.c_str()), SQL_NTS);
    }

    return SQLSetDescField(DescriptorHandle, RecNumber, FieldIdentifier, Value, BufferLength);
}
}