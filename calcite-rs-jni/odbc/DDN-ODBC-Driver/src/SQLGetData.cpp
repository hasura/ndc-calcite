#define NOMINMAX  // Ensure NOMINMAX is defined before including windows.h to prevent min/max macro conflicts
#include <winsock2.h>
#include <windows.h>
#include <algorithm>
#include <string>
#define WIN32_LEAN_AND_MEAN // Reduce size of the Win32 header files

#include "../include/connection.hpp"
#include "../include/logging.hpp"
//#include "../include/httplib.h"
#include "../include/globals.hpp"
#include "../include/environment.hpp"
#include "../include/statement.hpp"

std::string WideCharToString(const std::wstring& wstr) {
    int size = WideCharToMultiByte(CP_UTF8, 0, wstr.c_str(), -1, nullptr, 0, nullptr, nullptr);
    if (size <= 0) {
        return {};
    }

    std::string str(size - 1, '\0');
    WideCharToMultiByte(CP_UTF8, 0, wstr.c_str(), -1, str.data(), size - 1, nullptr, nullptr);
    return str;
}

extern "C" {

// Common validation function
static SQLRETURN ValidateGetDataCall(
    Statement* stmt,
    SQLUSMALLINT ColumnNumber,
    const char** ppColumnData,
    const ColumnDesc** ppColumnDesc)
{
    if (!stmt) {
        LOG("Invalid statement handle");
        return SQL_INVALID_HANDLE;
    }

    if (!stmt->hasResult) {
        LOG("No result set available");
        return SQL_ERROR;
    }

    if (stmt->currentRow == 0 || stmt->currentRow > stmt->resultData.size()) {
        LOG("Invalid row position (SQLFetch not called or no more rows)");
        return SQL_ERROR;
    }

    if (ColumnNumber < 1 || ColumnNumber > stmt->resultColumns.size()) {
        LOGF("Invalid column number %d (valid range: 1-%zu)",
             ColumnNumber, stmt->resultColumns.size());
        return SQL_ERROR;
    }

    const auto& columnData = stmt->resultData[stmt->currentRow - 1][ColumnNumber - 1];
    const auto& columnDesc = stmt->resultColumns[ColumnNumber - 1];

    LOGF("Getting data for column %s (%s)",
         columnDesc.name,
         columnData.isNull ? "NULL" : "NOT NULL");

    *ppColumnData = columnData.data.c_str();
    *ppColumnDesc = &columnDesc;

    return SQL_SUCCESS;
}

// Common numeric conversion function
static SQLRETURN ConvertNumeric(
    const char* sourceData,
    SQLSMALLINT TargetType,
    SQLPOINTER TargetValue,
    SQLLEN* StrLen_or_Ind)
{
    switch (TargetType) {
        case SQL_C_LONG:
        case SQL_C_SLONG:
        case SQL_C_LONG + SQL_UNSIGNED_OFFSET: {
            LOG("Converting to LONG");
            try {
                long value = std::stol(sourceData);
                *static_cast<SQLLEN*>(TargetValue) = value;
                if (StrLen_or_Ind) {
                    *StrLen_or_Ind = sizeof(long);
                }
                LOGF("Converted to long value: %ld", value);
                return SQL_SUCCESS;
            } catch (const std::exception& e) {
                LOGF("Failed to convert to long: %s", e.what());
                return SQL_ERROR;
            }
        }

        case SQL_C_DOUBLE: {
            LOG("Converting to DOUBLE");
            try {
                double value = std::stod(sourceData);
                *static_cast<double*>(TargetValue) = value;
                if (StrLen_or_Ind) {
                    *StrLen_or_Ind = sizeof(double);
                }
                LOGF("Converted to double value: %f", value);
                return SQL_SUCCESS;
            } catch (const std::exception& e) {
                LOGF("Failed to convert to double: %s", e.what());
                return SQL_ERROR;
            }
        }

        default:
            LOGF("Unsupported numeric target type: %d", TargetType);
            return SQL_ERROR;
    }
}

SQLRETURN SQL_API SQLGetData_A(
    SQLHSTMT       StatementHandle,
    SQLUSMALLINT   ColumnNumber,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValue,
    SQLLEN         BufferLength,
    SQLLEN*        StrLen_or_Ind)
{
    LOGF("SQLGetData called - Column: %d, TargetType: %d, BufferLength: %zd",
         ColumnNumber, TargetType, static_cast<size_t>(BufferLength));

    auto stmt = static_cast<Statement*>(StatementHandle);
    const char* sourceData;
    const ColumnDesc* columnDesc;

    SQLRETURN validationResult = ValidateGetDataCall(stmt, ColumnNumber, &sourceData, &columnDesc);
    if (validationResult != SQL_SUCCESS) {
        return validationResult;
    }

    // Handle NULL value
    if (sourceData == nullptr || *sourceData == '\0') {
        if (StrLen_or_Ind) {
            *StrLen_or_Ind = SQL_NULL_DATA;
            LOGF("NULL value for column %d", ColumnNumber);
        }

        // For string types, we should write an empty string
        if (TargetValue && BufferLength > 0) {
            switch (TargetType) {
                case SQL_C_WCHAR: {
                    if (BufferLength >= sizeof(WCHAR)) {
                        *static_cast<WCHAR*>(TargetValue) = L'\0';
                    }
                    break;
                }
                case SQL_C_CHAR: {
                    *static_cast<char*>(TargetValue) = '\0';
                    break;
                }
            }
        }
        return SQL_SUCCESS;
    }

    LOGF("Source data: '%s'", sourceData);

    // Handle SQL_C_WCHAR
    if (TargetType == SQL_C_WCHAR) {
        size_t sourceLen = strlen(sourceData);

        // Calculate required buffer size in bytes (including null terminator)
        int wideCharsRequired = MultiByteToWideChar(
            CP_UTF8,
            0,
            sourceData,
            -1,  // null-terminated string
            nullptr,
            0
        );

        if (wideCharsRequired == 0) {
            LOGF("MultiByteToWideChar sizing failed: %lu", GetLastError());
            return SQL_ERROR;
        }

        // Set the required size (in bytes, not including null terminator)
        SQLLEN bytesNeeded = (wideCharsRequired - 1) * sizeof(WCHAR);
        if (StrLen_or_Ind) {
            *StrLen_or_Ind = bytesNeeded;
            LOGF("Set StrLen_or_Ind to %zd bytes", bytesNeeded);
        }

        // If no buffer or zero length, just return required size
        if (!TargetValue || BufferLength <= 0) {
            return SQL_SUCCESS;
        }

        // Calculate how many wide chars we can store (including null terminator)
        int bufferWideChars = BufferLength / sizeof(WCHAR);
        if (bufferWideChars == 0) {
            return SQL_ERROR;
        }

        // Convert the string
        int charsConverted = MultiByteToWideChar(
            CP_UTF8,
            0,
            sourceData,
            -1,
            static_cast<LPWSTR>(TargetValue),
            bufferWideChars
        );

        if (charsConverted == 0) {
            LOGF("MultiByteToWideChar conversion failed: %lu", GetLastError());
            return SQL_ERROR;
        }

        // Log what we wrote
        LOGF("Converted %d wide chars from '%s'", charsConverted - 1, sourceData);

        // Check if truncation occurred
        if (charsConverted < wideCharsRequired) {
            // Ensure null termination
            static_cast<WCHAR*>(TargetValue)[bufferWideChars - 1] = L'\0';
            LOGF("Data truncated from %d to %d chars", wideCharsRequired - 1, charsConverted - 1);
            return SQL_SUCCESS_WITH_INFO;
        }

        return SQL_SUCCESS;
    }

    // Handle non-Unicode string types
    if (TargetType == SQL_C_CHAR) {
        size_t sourceLen = strlen(sourceData);
        if (StrLen_or_Ind) {
            *StrLen_or_Ind = static_cast<SQLLEN>(sourceLen);
        }

        if (TargetValue && BufferLength > 0) {
            size_t copyLen = std::min<size_t>(sourceLen, BufferLength - 1);
            strncpy(static_cast<char*>(TargetValue), sourceData, copyLen);
            static_cast<char*>(TargetValue)[copyLen] = '\0';

            if (copyLen < sourceLen) {
                return SQL_SUCCESS_WITH_INFO;
            }
        }
        return SQL_SUCCESS;
    }

    return ConvertNumeric(sourceData, TargetType, TargetValue, StrLen_or_Ind);
}

SQLRETURN SQL_API SQLGetData_W(
    SQLHSTMT       StatementHandle,
    SQLUSMALLINT   ColumnNumber,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValue,
    SQLLEN         BufferLength,
    SQLLEN*        StrLen_or_Ind)
{
    LOGF("SQLGetDataW (Unicode) called - Column: %d, TargetType: %d, BufferLength: %zd",
         ColumnNumber, TargetType, static_cast<size_t>(BufferLength));

    auto stmt = static_cast<Statement*>(StatementHandle);
    const char* sourceData;
    const ColumnDesc* columnDesc;

    SQLRETURN validationResult = ValidateGetDataCall(stmt, ColumnNumber, &sourceData, &columnDesc);
    if (validationResult != SQL_SUCCESS) {
        return validationResult;
    }

    // Handle NULL value
    if (*sourceData == '\0') {
        if (StrLen_or_Ind) {
            *StrLen_or_Ind = SQL_NULL_DATA;
            LOG("Returning NULL value indicator");
        }
        return SQL_SUCCESS;
    }

    // For Unicode string types (SQL_C_WCHAR)
    if (TargetType == SQL_C_WCHAR) {
        LOGF("Converting to SQL_C_WCHAR, source data: '%s'", sourceData);

        // First convert source UTF-8 to wide chars to get required buffer size
        int wideCharsRequired = MultiByteToWideChar(
            CP_UTF8,
            0,
            sourceData,
            -1,  // null-terminated string
            nullptr,
            0
        );

        if (wideCharsRequired == 0) {
            LOGF("Failed to get required buffer size for wide char conversion: %lu", GetLastError());
            return SQL_ERROR;
        }

        // wideCharsRequired includes null terminator
        if (StrLen_or_Ind) {
            *StrLen_or_Ind = (wideCharsRequired - 1) * sizeof(WCHAR);  // Size in bytes, excluding null terminator
        }

        // If no buffer provided or zero length, just return required size
        if (!TargetValue || BufferLength == 0) {
            return SQL_SUCCESS;
        }

        // Calculate how many wide chars we can actually store
        int bufferWideChars = BufferLength / sizeof(WCHAR);
        if (bufferWideChars == 0) {
            return SQL_ERROR;
        }

        // Convert the string
        int charsConverted = MultiByteToWideChar(
            CP_UTF8,
            0,
            sourceData,
            -1,
            static_cast<LPWSTR>(TargetValue),
            bufferWideChars
        );

        if (charsConverted == 0) {
            LOGF("Failed to convert string to wide char: %lu", GetLastError());
            return SQL_ERROR;
        }

        // Check if data was truncated
        if (charsConverted < wideCharsRequired) {
            // Ensure null termination on truncation
            static_cast<WCHAR*>(TargetValue)[bufferWideChars - 1] = L'\0';
            LOG("Data truncated (SQL_SUCCESS_WITH_INFO)");
            return SQL_SUCCESS_WITH_INFO;
        }

        return SQL_SUCCESS;
    }
    // For non-string types, use the original numeric conversion
    else {
        return ConvertNumeric(sourceData, TargetType, TargetValue, StrLen_or_Ind);
    }
}

} // extern "C"