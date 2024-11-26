#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include "../include/connection.hpp"
#include "../include/logging.hpp"
//#include "../include/httplib.h"
#include "../include/globals.hpp"
#include "../include/environment.hpp"
#include "../include/statement.hpp"

// Column attribute functions
extern "C" {
SQLRETURN SQL_API SQLColAttribute(
    SQLHSTMT StatementHandle,
    SQLUSMALLINT ColumnNumber,
    SQLUSMALLINT FieldIdentifier,
    SQLPOINTER CharacterAttribute,
    SQLSMALLINT BufferLength,
    SQLSMALLINT *StringLength,
    SQLLEN *NumericAttribute) {
    auto *stmt = static_cast<Statement *>(StatementHandle);
    LOGF("SQLColAttribute called - Column: %u, Field: %u, Total Size: %u", ColumnNumber, FieldIdentifier, stmt->resultColumns.size());
    if (!stmt) {
        LOG("Invalid statement handle");
        return SQL_INVALID_HANDLE;
    }

    if (!stmt->hasResult) {
        LOG("No result set available");
        return SQL_ERROR;
    }

    LOGF("Accessing column %u of %zu", ColumnNumber, stmt->resultColumns.size());
    if (ColumnNumber <= 0 || ColumnNumber > stmt->resultColumns.size()) {
        LOG("Invalid column number");
        return SQL_ERROR;
    }

    const auto &col = stmt->resultColumns[ColumnNumber - 1];

    switch (FieldIdentifier) {
        // Basic column metadata
        case SQL_COLUMN_COUNT: // 0
            if (NumericAttribute) {
                *NumericAttribute = stmt->resultColumns.size();
            }
            break;

        case SQL_COLUMN_NAME: // 1
        case SQL_DESC_NAME: // SQL_COLUMN_NAME
            if (CharacterAttribute && BufferLength > 0) {
                LOGF("Returning column name: %s, %u", col.name, col.nameLength);
                strncpy((char *) CharacterAttribute, col.name, BufferLength);
                if (StringLength) {
                    *StringLength = col.nameLength;
                }
            } else {
                LOG("Did not return column name");
            }
            break;

        case SQL_DESC_LABEL: // SQL_COLUMN_LABEL
            if (CharacterAttribute && BufferLength > 0) {
                strncpy((char *) CharacterAttribute, col.label, BufferLength);
                if (StringLength) {
                    *StringLength = col.labelLength;
                }
            }
            break;

        // Type information
        case SQL_COLUMN_TYPE: // 2
        case SQL_DESC_TYPE: // 1002
            // case SQL_DESC_CONCISE_TYPE:         // SQL_COLUMN_TYPE
            if (NumericAttribute) {
                *NumericAttribute = col.sqlType;
            }
            break;

        // Size and length information
        case SQL_COLUMN_LENGTH: // 3
        case SQL_DESC_LENGTH: // SQL_COLUMN_LENGTH
        case SQL_DESC_OCTET_LENGTH: // 1013
            if (NumericAttribute) {
                *NumericAttribute = col.columnSize;
            }
            break;

        case SQL_COLUMN_DISPLAY_SIZE: // 6
            if (NumericAttribute) {
                *NumericAttribute = col.displaySize;
            }
            break;

        case SQL_COLUMN_PRECISION: // 4
        case SQL_DESC_PRECISION: // SQL_COLUMN_PRECISION
            if (NumericAttribute) {
                *NumericAttribute = col.decimalDigits;
            }
            break;

        case SQL_COLUMN_SCALE: // 5
        case SQL_DESC_SCALE: // SQL_COLUMN_SCALE
            if (NumericAttribute) {
                *NumericAttribute = col.scale; // VARCHAR doesn't have scale
            }
            break;

        // Nullability
        case SQL_COLUMN_NULLABLE: // 7
        case SQL_DESC_NULLABLE: // SQL_COLUMN_NULLABLE
            if (NumericAttribute) {
                *NumericAttribute = col.nullable;
            }
            break;

        // Type characteristics
        case SQL_COLUMN_UNSIGNED: // 8
            // case SQL_DESC_UNSIGNED:             // SQL_COLUMN_UNSIGNED
            if (NumericAttribute) {
                *NumericAttribute = SQL_TRUE; // VARCHAR is unsigned
            }
            break;

        case SQL_COLUMN_MONEY: // 9
        case SQL_COLUMN_AUTO_INCREMENT: // 11
            // case SQL_DESC_AUTO_UNIQUE_VALUE:    // SQL_COLUMN_AUTO_INCREMENT
            if (NumericAttribute) {
                *NumericAttribute = SQL_FALSE; // VARCHAR is not auto-increment
            }
            break;

        case SQL_COLUMN_UPDATABLE: // 10
            // case SQL_DESC_UPDATABLE:            // SQL_COLUMN_UPDATABLE
            if (NumericAttribute) {
                *NumericAttribute = SQL_ATTR_READONLY; // Catalog results are read-only
            }
            break;

        case SQL_COLUMN_CASE_SENSITIVE: // 12
            // case SQL_DESC_CASE_SENSITIVE:       // SQL_COLUMN_CASE_SENSITIVE
            if (NumericAttribute) {
                *NumericAttribute = SQL_TRUE; // VARCHAR is case sensitive
            }
            break;

        case SQL_COLUMN_SEARCHABLE: // 13
            // case SQL_DESC_SEARCHABLE:           // SQL_COLUMN_SEARCHABLE
            if (NumericAttribute) {
                *NumericAttribute = SQL_SEARCHABLE; // VARCHAR is fully searchable
            }
            break;

        // Type names and descriptions
        case SQL_COLUMN_TYPE_NAME: // 14
            // case SQL_DESC_TYPE_NAME:            // SQL_COLUMN_TYPE_NAME
            if (CharacterAttribute && BufferLength > 0) {
                strncpy((char *) CharacterAttribute, "VARCHAR", BufferLength);
                if (StringLength) {
                    *StringLength = 7;
                }
            }
            break;

        // Table information
        case SQL_COLUMN_TABLE_NAME: // 15
            // case SQL_DESC_TABLE_NAME:           // SQL_COLUMN_TABLE_NAME
            if (CharacterAttribute && BufferLength > 0) {
                strncpy((char *) CharacterAttribute, col.tableName, BufferLength);
                if (StringLength) {
                    *StringLength = col.tableNameLength;
                }
            }
            break;

        case SQL_COLUMN_OWNER_NAME: // 16
            // case SQL_DESC_SCHEMA_NAME:          // SQL_COLUMN_OWNER_NAME
            if (CharacterAttribute && BufferLength > 0) {
                strncpy((char *) CharacterAttribute, col.schemaName, BufferLength);
                if (StringLength) {
                    *StringLength = col.schemaNameLength;
                }
            }
            break;

        case SQL_COLUMN_QUALIFIER_NAME: // 17
            // case SQL_DESC_CATALOG_NAME:         // SQL_COLUMN_QUALIFIER_NAME
            if (CharacterAttribute && BufferLength > 0) {
                strncpy((char *) CharacterAttribute, col.catalogName, BufferLength);
                if (StringLength) {
                    *StringLength = col.catalogNameLength;
                }
            }
            break;

        // SQL literal formatting
        case SQL_DESC_LITERAL_PREFIX: // 27
            if (CharacterAttribute && BufferLength > 0) {
                strncpy((char *) CharacterAttribute, col.literalPrefix, BufferLength);
                if (StringLength) {
                    *StringLength = col.literalPrefixLength;
                }
            }
            break;

        case SQL_DESC_LITERAL_SUFFIX: // 28
            if (CharacterAttribute && BufferLength > 0) {
                strncpy((char *) CharacterAttribute, col.literalSuffix, BufferLength);
                if (StringLength) {
                    *StringLength = col.literalSuffixLength;
                }
            }
            break;

        case SQL_DESC_LOCAL_TYPE_NAME: // 29
            if (CharacterAttribute && BufferLength > 0) {
                strncpy((char *) CharacterAttribute, col.localTypeName, BufferLength);
                if (StringLength) {
                    *StringLength = col.localTypeNameLength;
                }
            }
            break;

        case SQL_DESC_NUM_PREC_RADIX: // 32
            if (NumericAttribute) {
                *NumericAttribute = 0; // Not applicable for VARCHAR
            }
            break;

        case SQL_DESC_UNNAMED: // 1089
            if (NumericAttribute) {
                *NumericAttribute = col.unnamed;
            }
            break;

        case SQL_DESC_BASE_COLUMN_NAME: // 1025
            if (CharacterAttribute && BufferLength > 0) {
                strncpy((char *) CharacterAttribute, col.baseColumnName, BufferLength);
                if (StringLength) {
                    *StringLength = col.baseColumnNameLength;
                }
            }
            break;

        case SQL_DESC_BASE_TABLE_NAME: // 1026
            if (CharacterAttribute && BufferLength > 0) {
                strncpy((char *) CharacterAttribute, col.baseTableName, BufferLength);
                if (StringLength) {
                    *StringLength = col.baseTableNameLength;
                }
            }
            break;

        default:
            LOGF("Unknown field identifier: %u", FieldIdentifier);
            if (NumericAttribute) {
                *NumericAttribute = 0;
            }
            if (StringLength) {
                *StringLength = 0;
            }
            break;
    }

    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLColAttributeW(
    SQLHSTMT StatementHandle,
    SQLUSMALLINT ColumnNumber,
    SQLUSMALLINT FieldIdentifier,
    SQLPOINTER CharacterAttribute,
    SQLSMALLINT BufferLength,
    SQLSMALLINT *StringLength,
    SQLLEN *NumericAttribute) {
    LOG("SQLColAttributeW called");

    // For non-string attributes, just delegate to the ANSI version
    switch (FieldIdentifier) {
        // String attributes that need Unicode conversion
        case SQL_COLUMN_NAME: // 1
        case SQL_COLUMN_TYPE_NAME: // 14
        case SQL_COLUMN_TABLE_NAME: // 15
        case SQL_COLUMN_OWNER_NAME: // 16
        case SQL_COLUMN_QUALIFIER_NAME: // 17
        case SQL_COLUMN_LABEL: // 18
        case SQL_DESC_NAME: // SQL_COLUMN_NAME
        // case SQL_DESC_TYPE_NAME:            // SQL_COLUMN_TYPE_NAME
        // case SQL_DESC_TABLE_NAME:           // SQL_COLUMN_TABLE_NAME
        // case SQL_DESC_SCHEMA_NAME:          // SQL_COLUMN_OWNER_NAME
        // case SQL_DESC_CATALOG_NAME:         // SQL_COLUMN_QUALIFIER_NAME
        // case SQL_DESC_LABEL:                // SQL_COLUMN_LABEL
        case SQL_DESC_LITERAL_PREFIX: // 27
        case SQL_DESC_LITERAL_SUFFIX: // 28
        case SQL_DESC_LOCAL_TYPE_NAME: // 29
        case SQL_DESC_BASE_COLUMN_NAME: // 1025
        case SQL_DESC_BASE_TABLE_NAME: // 1026
            break; // Handle these below with Unicode conversion

        // All other attributes can go directly to ANSI version
        default:
            return SQLColAttribute(StatementHandle, ColumnNumber, FieldIdentifier,
                                   CharacterAttribute, BufferLength, StringLength,
                                   NumericAttribute);
    }

    // Get the ANSI string
    char ansiBuffer[SQL_MAX_MESSAGE_LENGTH];
    SQLSMALLINT ansiLength = 0;

    SQLRETURN ret = SQLColAttribute(StatementHandle, ColumnNumber, FieldIdentifier,
                                    ansiBuffer, sizeof(ansiBuffer), &ansiLength, NumericAttribute);

    if (!SQL_SUCCEEDED(ret)) {
        return ret;
    }

    // Convert to wide char if we have a buffer
    if (SQL_SUCCEEDED(ret) && CharacterAttribute && BufferLength > 0) {
        if (ansiLength > 0) {
            size_t numChars = 0;
            errno_t err = mbstowcs_s(&numChars, (wchar_t *)CharacterAttribute,
                                     BufferLength / sizeof(wchar_t), ansiBuffer, _TRUNCATE);
            if (err == 0 && StringLength) {
                *StringLength = static_cast<SQLSMALLINT>(numChars * sizeof(wchar_t));
            }
            LOGF("Converted string to Unicode. Original length: %d, Wide length: %zu", ansiLength, numChars);

        } else {
            // Empty string case
            if (BufferLength >= sizeof(wchar_t)) {
                *((wchar_t *) CharacterAttribute) = L'\0';
            }
            if (StringLength) {
                *StringLength = 0;
            }
            LOG("Set empty Unicode string");
        }
    } else if (StringLength) {
        // Just return required length if no buffer provided
        *StringLength = ansiLength * sizeof(wchar_t);
        LOGF("Returning required buffer length: %d bytes", *StringLength);
    }

    return ret;
}
} // extern "C"
