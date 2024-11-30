#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include "../include/Connection.hpp"
#include "../include/Logging.hpp"
#include "../include/Environment.hpp"
#include "../include/Statement.hpp"

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
    // Get the statement object from the provided handle
    auto *stmt = static_cast<Statement *>(StatementHandle);

    // Log the function call for debugging purposes
    LOGF("SQLColAttribute called - Column: %u, Field: %u, Total Size: %u", ColumnNumber, FieldIdentifier,
         stmt->resultColumns.size());

    // Validate the statement handle and ensure a result set is available
    if (!stmt || !stmt->hasResult) {
        LOG("Invalid statement handle or no result set available");
        stmt->setError("HY000", "Invalid statement handle or no result set available", 0);
        return SQL_ERROR;
    }

    // Ensure the column number is within the valid range of the result set
    if (ColumnNumber <= 0 || ColumnNumber > stmt->resultColumns.size()) {
        LOG("Invalid column number");
        stmt->setError("07009", "Invalid descriptor index", 0);
        return SQL_ERROR;
    }

    const auto &col = stmt->resultColumns[ColumnNumber - 1];
    SQLRETURN ret = SQL_SUCCESS;

    try {
        switch (FieldIdentifier) {
            case SQL_COLUMN_COUNT:
                if (NumericAttribute) {
                    *NumericAttribute = stmt->resultColumns.size();
                }
                break;

            case SQL_COLUMN_NAME:
            case SQL_DESC_NAME:
                if (CharacterAttribute && BufferLength > 0) {
                    LOGF("Returning column name: %s, %u", col.name, col.nameLength);
                    strncpy(static_cast<char *>(CharacterAttribute), col.name, BufferLength - 1);
                    static_cast<char *>(CharacterAttribute)[BufferLength - 1] = '\0';
                    if (StringLength) {
                        *StringLength = col.nameLength;
                    }
                } else if (StringLength) {
                    *StringLength = col.nameLength;
                }
                break;

            case SQL_DESC_LABEL:
                if (CharacterAttribute && BufferLength > 0) {
                    strncpy(static_cast<char *>(CharacterAttribute), col.label, BufferLength - 1);
                    static_cast<char *>(CharacterAttribute)[BufferLength - 1] = '\0';
                    if (StringLength) {
                        *StringLength = col.labelLength;
                    }
                } else if (StringLength) {
                    *StringLength = col.labelLength;
                }
                break;

            case SQL_COLUMN_TYPE_NAME:
                // case SQL_DESC_TYPE_NAME:
                if (CharacterAttribute && BufferLength > 0) {
                    LOGF("Returning column type name: %s, %u", col.typeName, col.typeNameLength);
                    strncpy(static_cast<char *>(CharacterAttribute), col.typeName, BufferLength - 1);
                    static_cast<char *>(CharacterAttribute)[BufferLength - 1] = '\0';
                    if (StringLength) {
                        *StringLength = col.typeNameLength;
                    }
                } else if (StringLength) {
                    *StringLength = col.typeNameLength;
                }
                break;

            case SQL_COLUMN_TABLE_NAME:
                // case SQL_DESC_TABLE_NAME:
                if (CharacterAttribute && BufferLength > 0) {
                    LOGF("Returning column table name: %s, %u", col.tableName, col.tableNameLength);
                    strncpy(static_cast<char *>(CharacterAttribute), col.tableName, BufferLength - 1);
                    static_cast<char *>(CharacterAttribute)[BufferLength - 1] = '\0';
                    if (StringLength) {
                        *StringLength = col.tableNameLength;
                    }
                } else if (StringLength) {
                    *StringLength = col.tableNameLength;
                }
                break;

            case SQL_COLUMN_OWNER_NAME:
                // case SQL_DESC_SCHEMA_NAME:
                if (CharacterAttribute && BufferLength > 0) {
                    LOGF("Returning column schema name: %s, %u", col.schemaName, col.schemaNameLength);
                    strncpy(static_cast<char *>(CharacterAttribute), col.schemaName, BufferLength - 1);
                    static_cast<char *>(CharacterAttribute)[BufferLength - 1] = '\0';
                    if (StringLength) {
                        *StringLength = col.schemaNameLength;
                    }
                } else if (StringLength) {
                    *StringLength = col.schemaNameLength;
                }
                break;

            case SQL_COLUMN_QUALIFIER_NAME:
                // case SQL_DESC_CATALOG_NAME:
                if (CharacterAttribute && BufferLength > 0) {
                    strncpy(static_cast<char *>(CharacterAttribute), col.catalogName, BufferLength - 1);
                    static_cast<char *>(CharacterAttribute)[BufferLength - 1] = '\0';
                    if (StringLength) {
                        *StringLength = col.catalogNameLength;
                    }
                } else if (StringLength) {
                    *StringLength = col.catalogNameLength;
                }
                break;

            case SQL_DESC_LITERAL_PREFIX:
                if (CharacterAttribute && BufferLength > 0) {
                    strncpy(static_cast<char *>(CharacterAttribute), col.literalPrefix, BufferLength - 1);
                    static_cast<char *>(CharacterAttribute)[BufferLength - 1] = '\0';
                    if (StringLength) {
                        *StringLength = col.literalPrefixLength;
                    }
                } else if (StringLength) {
                    *StringLength = col.literalPrefixLength;
                }
                break;

            case SQL_DESC_LITERAL_SUFFIX:
                if (CharacterAttribute && BufferLength > 0) {
                    strncpy(static_cast<char *>(CharacterAttribute), col.literalSuffix, BufferLength - 1);
                    static_cast<char *>(CharacterAttribute)[BufferLength - 1] = '\0';
                    if (StringLength) {
                        *StringLength = col.literalSuffixLength;
                    }
                } else if (StringLength) {
                    *StringLength = col.literalSuffixLength;
                }
                break;

            case SQL_DESC_LOCAL_TYPE_NAME:
                if (CharacterAttribute && BufferLength > 0) {
                    strncpy(static_cast<char *>(CharacterAttribute), col.localTypeName, BufferLength - 1);
                    static_cast<char *>(CharacterAttribute)[BufferLength - 1] = '\0';
                    if (StringLength) {
                        *StringLength = col.localTypeNameLength;
                    }
                } else if (StringLength) {
                    *StringLength = col.localTypeNameLength;
                }
                break;

            case SQL_DESC_BASE_COLUMN_NAME:
                if (CharacterAttribute && BufferLength > 0) {
                    strncpy(static_cast<char *>(CharacterAttribute), col.baseColumnName, BufferLength - 1);
                    static_cast<char *>(CharacterAttribute)[BufferLength - 1] = '\0';
                    if (StringLength) {
                        *StringLength = col.baseColumnNameLength;
                    }
                } else if (StringLength) {
                    *StringLength = col.baseColumnNameLength;
                }
                break;

            case SQL_DESC_BASE_TABLE_NAME:
                if (CharacterAttribute && BufferLength > 0) {
                    strncpy(static_cast<char *>(CharacterAttribute), col.baseTableName, BufferLength - 1);
                    static_cast<char *>(CharacterAttribute)[BufferLength - 1] = '\0';
                    if (StringLength) {
                        *StringLength = col.baseTableNameLength;
                    }
                } else if (StringLength) {
                    *StringLength = col.baseTableNameLength;
                }
                break;

            case SQL_COLUMN_TYPE:
            case SQL_DESC_TYPE:
                if (NumericAttribute) {
                    LOGF("Returning Column type: %d", col.sqlType);
                    *NumericAttribute = col.sqlType;
                }
                break;

            case SQL_COLUMN_LENGTH:
            case SQL_DESC_LENGTH: if (NumericAttribute) {
                    const auto columnSize = static_cast<SQLLEN>(col.columnSize);
                    LOGF("Returning Length (column size): %d", columnSize);
                    LOGF("NumericAttribute address before setting: %p", NumericAttribute);
                    LOGF("NumericAttribute content before setting: %d", *NumericAttribute);
                    *NumericAttribute = columnSize;
                    LOGF("NumericAttribute content after setting: %d", *NumericAttribute);
                } else { LOG("Warning: NumericAttribute is null"); }
                break;

            case SQL_DESC_OCTET_LENGTH:
                if (NumericAttribute) {
                    *NumericAttribute = col.octetLength;
                }
                break;

            case SQL_COLUMN_DISPLAY_SIZE:
                if (NumericAttribute) {
                    LOGF("Returning Display size: %d", col.displaySize);
                    *NumericAttribute = col.displaySize;
                }
                break;

            case SQL_COLUMN_PRECISION:
            case SQL_DESC_PRECISION:
                if (NumericAttribute) {
                    LOGF("Returning precision: %d", col.precision);
                    *NumericAttribute = col.precision;
                }
                break;

            case SQL_COLUMN_SCALE:
            case SQL_DESC_SCALE:
                if (NumericAttribute) {
                    *NumericAttribute = col.scale;
                }
                break;

            case SQL_COLUMN_NULLABLE:
            case SQL_DESC_NULLABLE:
                if (NumericAttribute) {
                    *NumericAttribute = col.nullable;
                }
                break;

            case SQL_COLUMN_UNSIGNED:
                if (NumericAttribute) {
                    *NumericAttribute = col._signed ? SQL_FALSE : SQL_TRUE;
                }
                break;

            case SQL_COLUMN_MONEY:
                if (NumericAttribute) {
                    *NumericAttribute = col.currency;
                }
                break;

            case SQL_COLUMN_AUTO_INCREMENT:
                if (NumericAttribute) {
                    *NumericAttribute = col.autoIncrement;
                }
                break;

            case SQL_COLUMN_UPDATABLE:
                if (NumericAttribute) {
                    *NumericAttribute = !col.readOnly;
                }
                break;

            case SQL_COLUMN_CASE_SENSITIVE:
                if (NumericAttribute) {
                    *NumericAttribute = col.caseSensitive;
                }
                break;

            case SQL_COLUMN_SEARCHABLE:
                if (NumericAttribute) {
                    *NumericAttribute = col.searchable;
                }
                break;

            case SQL_DESC_NUM_PREC_RADIX:
                if (NumericAttribute) {
                    switch (col.sqlType) {
                        case SQL_DECIMAL:
                        case SQL_NUMERIC:
                        case SQL_FLOAT:
                        case SQL_REAL:
                        case SQL_DOUBLE:
                            *NumericAttribute = 10;
                            break;
                        case SQL_BINARY:
                        case SQL_VARBINARY:
                            *NumericAttribute = 2;
                            break;
                        default:
                            *NumericAttribute = 0;
                            break;
                    }
                }
                break;

            case SQL_DESC_UNNAMED:
                if (NumericAttribute) {
                    *NumericAttribute = col.unnamed;
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
                ret = SQL_SUCCESS;
                break;
        }
    } catch (const std::exception &e) {
        stmt->setError("HY000", e.what(), 0);
        return SQL_ERROR;
    }

    return ret;
}

SQLRETURN SQL_API SQLColAttributeW(
    SQLHSTMT StatementHandle,
    SQLUSMALLINT ColumnNumber,
    SQLUSMALLINT FieldIdentifier,
    SQLPOINTER CharacterAttribute,
    SQLSMALLINT BufferLength,
    SQLSMALLINT *StringLength,
    SQLLEN *NumericAttribute) {
    LOGF("SQLColAttributeW - Column: %d, Field: %d, Buffer: %p, Length: %d",
         ColumnNumber, FieldIdentifier, CharacterAttribute, BufferLength);

    // For non-string attributes, delegate to the ANSI version
    switch (FieldIdentifier) {
        case SQL_COLUMN_NAME:
        case SQL_DESC_NAME:
        case SQL_COLUMN_TYPE_NAME:
        case SQL_COLUMN_TABLE_NAME:
        case SQL_COLUMN_OWNER_NAME:
        case SQL_COLUMN_QUALIFIER_NAME:
        case SQL_COLUMN_LABEL:
        case SQL_DESC_LITERAL_PREFIX:
        case SQL_DESC_LITERAL_SUFFIX:
        case SQL_DESC_LOCAL_TYPE_NAME:
        case SQL_DESC_BASE_COLUMN_NAME:
        case SQL_DESC_BASE_TABLE_NAME:
            // Handle string attributes below
            break;

        default:
            // Non-string attributes can be handled directly by ANSI version
            return SQLColAttribute(StatementHandle, ColumnNumber, FieldIdentifier,
                                   CharacterAttribute, BufferLength, StringLength,
                                   NumericAttribute);
    }

    // Get statement object and validate
    auto *stmt = static_cast<Statement *>(StatementHandle);
    if (!stmt || !stmt->hasResult) {
        LOG("Invalid statement handle or no result set");
        return SQL_ERROR;
    }

    // Validate column number
    if (ColumnNumber <= 0 || ColumnNumber > stmt->resultColumns.size()) {
        LOG("Invalid column number");
        return SQL_ERROR;
    }

    // Get column descriptor
    const auto &col = stmt->resultColumns[ColumnNumber - 1];

    // First get the ANSI string length
    SQLSMALLINT ansiLength = 0;
    const char *sourceStr = nullptr;

    // Determine which string attribute to retrieve
    switch (FieldIdentifier) {
        case SQL_COLUMN_NAME:
        case SQL_DESC_NAME:
            sourceStr = col.name;
            ansiLength = col.nameLength;
            break;
        case SQL_COLUMN_LABEL:
            sourceStr = col.label;
            ansiLength = col.labelLength;
            break;
        case SQL_COLUMN_TYPE_NAME:
            sourceStr = col.typeName;
            ansiLength = col.typeNameLength;
            break;
        case SQL_COLUMN_TABLE_NAME:
            sourceStr = col.tableName;
            ansiLength = col.tableNameLength;
            break;
        case SQL_COLUMN_OWNER_NAME:
            sourceStr = col.schemaName;
            ansiLength = col.schemaNameLength;
            break;
        case SQL_COLUMN_QUALIFIER_NAME:
            sourceStr = col.catalogName;
            ansiLength = col.catalogNameLength;
            break;
        case SQL_DESC_LITERAL_PREFIX:
            sourceStr = col.literalPrefix;
            ansiLength = col.literalPrefixLength;
            break;
        case SQL_DESC_LITERAL_SUFFIX:
            sourceStr = col.literalSuffix;
            ansiLength = col.literalSuffixLength;
            break;
        case SQL_DESC_LOCAL_TYPE_NAME:
            sourceStr = col.localTypeName;
            ansiLength = col.localTypeNameLength;
            break;
        case SQL_DESC_BASE_COLUMN_NAME:
            sourceStr = col.baseColumnName;
            ansiLength = col.baseColumnNameLength;
            break;
        case SQL_DESC_BASE_TABLE_NAME:
            sourceStr = col.baseTableName;
            ansiLength = col.baseTableNameLength;
            break;
        default:
            LOG("Unexpected string attribute type");
            return SQL_ERROR;
    }

    // Handle null source string
    if (!sourceStr) {
        LOGF("Source string is null for field %d", FieldIdentifier);
        if (StringLength) {
            *StringLength = 0;
        }
        if (CharacterAttribute && BufferLength >= sizeof(wchar_t)) {
            *static_cast<wchar_t *>(CharacterAttribute) = L'\0';
        }
        return SQL_SUCCESS;
    }

    // Calculate required buffer size in bytes for wide-char string
    // Add 1 for null terminator
    size_t requiredBytes = (ansiLength + 1) * sizeof(wchar_t);

    // If only asking for length
    if (!CharacterAttribute || BufferLength == 0) {
        if (StringLength) {
            *StringLength = static_cast<SQLSMALLINT>(requiredBytes);
            LOGF("Returning required buffer size: %d bytes", *StringLength);
        }
        return SQL_SUCCESS;
    }

    // Convert to wide char
    size_t charsWritten = 0;
    auto wcharBuf = static_cast<wchar_t *>(CharacterAttribute);
    size_t maxChars = BufferLength / sizeof(wchar_t);

    LOGF("Converting string '%s' to Unicode (max chars: %zu)", sourceStr, maxChars);

    errno_t err = mbstowcs_s(&charsWritten, wcharBuf, maxChars, sourceStr, _TRUNCATE);

    if (err != 0 && err != STRUNCATE) {
        LOGF("Unicode conversion failed with error: %d", err);
        return SQL_ERROR;
    }

    // Set the string length in bytes
    if (StringLength) {
        // Subtract 1 to not count null terminator
        *StringLength = static_cast<SQLSMALLINT>((charsWritten - 1) * sizeof(wchar_t));
        LOGF("Set string length to %d bytes", *StringLength);
    }

    // Check if data was truncated
    if (charsWritten == maxChars && ansiLength >= static_cast<SQLSMALLINT>(maxChars)) {
        LOG("String was truncated");
        return SQL_SUCCESS_WITH_INFO;
    }

    LOGF("Successfully converted string, wrote %zu chars", charsWritten);
    return SQL_SUCCESS;
}
} // extern "C"
