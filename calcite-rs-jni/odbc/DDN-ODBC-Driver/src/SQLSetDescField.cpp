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
        case SQL_COLUMN_TYPE:
        case SQL_DESC_TYPE:
        // case SQL_DESC_CONCISE_TYPE:
            stmt->resultColumns[colIdx].sqlType = reinterpret_cast<SQLSMALLINT>(Value);
            break;

        case SQL_COLUMN_NAME:
        case SQL_DESC_NAME:
            if (Value) {
                LOGF("Setting column name to: %s", static_cast<const char*>(Value));
                stmt->resultColumns[colIdx].name = static_cast<const char*>(Value);
                stmt->resultColumns[colIdx].nameLength =
                    (BufferLength == SQL_NTS) ? strlen(stmt->resultColumns[colIdx].name) : BufferLength;
                LOGF("Set column name to: %s", stmt->resultColumns[colIdx].name);
            }
            break;

        // case SQL_COLUMN_LABEL:
        case SQL_DESC_LABEL:
            if (Value) {
                stmt->resultColumns[colIdx].label = static_cast<const char*>(Value);
                stmt->resultColumns[colIdx].labelLength =
                    (BufferLength == SQL_NTS) ? strlen(stmt->resultColumns[colIdx].label) : BufferLength;
            }
        break;

        case SQL_COLUMN_NULLABLE:
        case SQL_DESC_NULLABLE:
            stmt->resultColumns[colIdx].nullable = reinterpret_cast<SQLSMALLINT>(Value);
            break;
        case SQL_COLUMN_LENGTH:
        case SQL_DESC_LENGTH:
            stmt->resultColumns[colIdx].columnSize = reinterpret_cast<SQLULEN>(Value);
            break;

        case SQL_COLUMN_PRECISION:
        case SQL_DESC_PRECISION:
            stmt->resultColumns[colIdx].decimalDigits = reinterpret_cast<SQLSMALLINT>(Value);
            break;

        case SQL_COLUMN_SCALE:
        case SQL_DESC_SCALE:
            stmt->resultColumns[colIdx].scale = reinterpret_cast<SQLSMALLINT>(Value);
            break;

        case SQL_CATALOG_NAME:
        case SQL_DESC_CATALOG_NAME:
            if (Value) {
                stmt->resultColumns[colIdx].catalogName = static_cast<const char*>(Value);
                stmt->resultColumns[colIdx].catalogNameLength =
                    (BufferLength == SQL_NTS) ? strlen(stmt->resultColumns[colIdx].catalogName) : BufferLength;
            }
            break;

        case SQL_DESC_SCHEMA_NAME:
            if (Value) {
                stmt->resultColumns[colIdx].schemaName = static_cast<const char*>(Value);
                stmt->resultColumns[colIdx].schemaNameLength =
                    (BufferLength == SQL_NTS) ? strlen(stmt->resultColumns[colIdx].schemaName) : BufferLength;
            }
            break;

        // case SQL_COLUMN_TABLE_NAME:
        case SQL_DESC_TABLE_NAME:
            if (Value) {
                stmt->resultColumns[colIdx].tableName = static_cast<const char*>(Value);
                stmt->resultColumns[colIdx].tableNameLength =
                    (BufferLength == SQL_NTS) ? strlen(stmt->resultColumns[colIdx].tableName) : BufferLength;
            }
            break;

        case SQL_DESC_BASE_COLUMN_NAME:
            if (Value) {
                stmt->resultColumns[colIdx].baseColumnName = static_cast<const char*>(Value);
                stmt->resultColumns[colIdx].baseColumnNameLength =
                    (BufferLength == SQL_NTS) ? strlen(stmt->resultColumns[colIdx].baseColumnName) : BufferLength;
            }
            break;

        case SQL_DESC_BASE_TABLE_NAME:
            if (Value) {
                stmt->resultColumns[colIdx].baseTableName = static_cast<const char*>(Value);
                stmt->resultColumns[colIdx].baseTableNameLength =
                    (BufferLength == SQL_NTS) ? strlen(stmt->resultColumns[colIdx].baseTableName) : BufferLength;
            }
            break;

        case SQL_DESC_LITERAL_PREFIX:
            if (Value) {
                stmt->resultColumns[colIdx].literalPrefix = static_cast<const char*>(Value);
                stmt->resultColumns[colIdx].literalPrefixLength =
                    (BufferLength == SQL_NTS) ? strlen(stmt->resultColumns[colIdx].literalPrefix) : BufferLength;
            }
            break;

        case SQL_DESC_LITERAL_SUFFIX:
            if (Value) {
                stmt->resultColumns[colIdx].literalSuffix = static_cast<const char*>(Value);
                stmt->resultColumns[colIdx].literalSuffixLength =
                    (BufferLength == SQL_NTS) ? strlen(stmt->resultColumns[colIdx].literalSuffix) : BufferLength;
            }
            break;

        case SQL_DESC_LOCAL_TYPE_NAME:
            if (Value) {
                stmt->resultColumns[colIdx].localTypeName = static_cast<const char*>(Value);
                stmt->resultColumns[colIdx].localTypeNameLength =
                    (BufferLength == SQL_NTS) ? strlen(stmt->resultColumns[colIdx].localTypeName) : BufferLength;
            }
            break;

        case SQL_DESC_UNNAMED:
            stmt->resultColumns[colIdx].unnamed = reinterpret_cast<SQLSMALLINT>(Value);
            break;

        case SQL_DESC_DISPLAY_SIZE:
            stmt->resultColumns[colIdx].displaySize = reinterpret_cast<SQLULEN>(Value);
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
        auto wstr = static_cast<const wchar_t*>(Value);
        std::string name = WideStringToString(wstr);
        LOGF("Converted name: %s", name.c_str());
        auto result = SQLSetDescField(DescriptorHandle, RecNumber, FieldIdentifier,
                             const_cast<char*>(name.c_str()), SQL_NTS);
        Statement *s = static_cast<Statement *>(DescriptorHandle);
        LOGF("Post SQLSetDescField: %s", s->resultColumns[RecNumber -1].name);
        return result;
    }

    if (FieldIdentifier == SQL_COLUMN_NAME && Value != nullptr) {
        // Convert wide string to narrow for internal storage
        const wchar_t* wstr = static_cast<const wchar_t*>(Value);
        std::string name = WideStringToString(std::wstring(wstr));
        return SQLSetDescField(DescriptorHandle, RecNumber, FieldIdentifier,
                             const_cast<char*>(name.c_str()), SQL_NTS);
    }

    if (FieldIdentifier == SQL_DESC_CATALOG_NAME && Value != nullptr) {
        const wchar_t* wstr = static_cast<const wchar_t*>(Value);
        std::string catalogName = WideStringToString(std::wstring(wstr));
        return SQLSetDescField(DescriptorHandle, RecNumber, FieldIdentifier,
                             const_cast<char*>(catalogName.c_str()), SQL_NTS);
    }

    if (FieldIdentifier == SQL_DESC_SCHEMA_NAME && Value != nullptr) {
        const wchar_t* wstr = static_cast<const wchar_t*>(Value);
        std::string schemaName = WideStringToString(std::wstring(wstr));
        return SQLSetDescField(DescriptorHandle, RecNumber, FieldIdentifier,
                             const_cast<char*>(schemaName.c_str()), SQL_NTS);
    }

    if (FieldIdentifier == SQL_DESC_TABLE_NAME && Value != nullptr) {
        const wchar_t* wstr = static_cast<const wchar_t*>(Value);
        std::string tableName = WideStringToString(std::wstring(wstr));
        return SQLSetDescField(DescriptorHandle, RecNumber, FieldIdentifier,
                             const_cast<char*>(tableName.c_str()), SQL_NTS);
    }

    if (FieldIdentifier == SQL_DESC_BASE_COLUMN_NAME && Value != nullptr) {
        const wchar_t* wstr = static_cast<const wchar_t*>(Value);
        std::string baseColumnName = WideStringToString(std::wstring(wstr));
        return SQLSetDescField(DescriptorHandle, RecNumber, FieldIdentifier,
                             const_cast<char*>(baseColumnName.c_str()), SQL_NTS);
    }

    if (FieldIdentifier == SQL_DESC_BASE_TABLE_NAME && Value != nullptr) {
        const wchar_t* wstr = static_cast<const wchar_t*>(Value);
        std::string baseTableName = WideStringToString(std::wstring(wstr));
        return SQLSetDescField(DescriptorHandle, RecNumber, FieldIdentifier,
                             const_cast<char*>(baseTableName.c_str()), SQL_NTS);
    }

    if (FieldIdentifier == SQL_DESC_LOCAL_TYPE_NAME && Value != nullptr) {
        const wchar_t* wstr = static_cast<const wchar_t*>(Value);
        std::string localTypeName = WideStringToString(std::wstring(wstr));
        return SQLSetDescField(DescriptorHandle, RecNumber, FieldIdentifier,
                             const_cast<char*>(localTypeName.c_str()), SQL_NTS);
    }

    if (FieldIdentifier == SQL_DESC_LABEL && Value != nullptr) {
        const wchar_t* wstr = static_cast<const wchar_t*>(Value);
        std::string label = WideStringToString(std::wstring(wstr));
        return SQLSetDescField(DescriptorHandle, RecNumber, FieldIdentifier,
                             const_cast<char*>(label.c_str()), SQL_NTS);
    }


    return SQLSetDescField(DescriptorHandle, RecNumber, FieldIdentifier, Value, BufferLength);
}
}