#define NOMINMAX  // Ensure NOMINMAX is defined before including windows.h to prevent min/max macro conflicts
#include <winsock2.h>
#include <windows.h>
#include <algorithm>
#include <string>
#define WIN32_LEAN_AND_MEAN // Reduce size of the Win32 header files
#include "../include/Connection.hpp"
#include "../include/Logging.hpp"
#include "../include/Environment.hpp"
#include "../include/Statement.hpp"

std::string WideCharToString(const std::wstring &wstr) {
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
    Statement *stmt,
    SQLUSMALLINT ColumnNumber,
    const char **ppColumnData,
    const ColumnDesc **ppColumnDesc) {
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

    const auto &columnData = stmt->resultData[stmt->currentRow - 1][ColumnNumber - 1];
    const auto &columnDesc = stmt->resultColumns[ColumnNumber - 1];

    LOGF("Getting data for column %s (%s)",
         columnDesc.name,
         columnData.isNull ? "NULL" : "NOT NULL");

    *ppColumnData = columnData.data.c_str();
    *ppColumnDesc = &columnDesc;

    return SQL_SUCCESS;
}

// Common numeric conversion function
static SQLRETURN ConvertNumeric(
    const char *sourceData,
    SQLSMALLINT TargetType,
    SQLPOINTER TargetValue,
    SQLLEN *StrLen_or_Ind) {
    try {
        switch (TargetType) {
            case SQL_C_TYPE_TIMESTAMP: {
                auto* ts = static_cast<SQL_TIMESTAMP_STRUCT*>(TargetValue);
                // Format is: YYYY-MM-DDThh:mm:ss.fff

                try {
                    std::string str(sourceData);
                    // Parse date part
                    ts->year = std::stoi(str.substr(0, 4));
                    ts->month = std::stoi(str.substr(5, 2));
                    ts->day = std::stoi(str.substr(8, 2));

                    // Parse time part (after T)
                    ts->hour = std::stoi(str.substr(11, 2));
                    ts->minute = std::stoi(str.substr(14, 2));
                    ts->second = std::stoi(str.substr(17, 2));

                    // Parse milliseconds and convert to nanoseconds
                    if (str.length() > 20 && str[19] == '.') {
                        std::string fraction = str.substr(20);
                        // Pad with zeros to ensure we have nanosecond precision
                        fraction.append(9 - fraction.length(), '0');
                        ts->fraction = std::stoi(fraction);
                    } else {
                        ts->fraction = 0;
                    }

                    if (StrLen_or_Ind) *StrLen_or_Ind = sizeof(SQL_TIMESTAMP_STRUCT);
                    return SQL_SUCCESS;
                } catch (...) {
                    LOGF("Failed to parse timestamp string: %s", sourceData);
                    return SQL_ERROR;
                }
            }

            case SQL_C_LONG:
            case SQL_C_SLONG: {
                long value = std::stol(sourceData);
                *static_cast<SQLLEN *>(TargetValue) = value;
                if (StrLen_or_Ind) *StrLen_or_Ind = sizeof(SQLLEN);
                return SQL_SUCCESS;
            }

            case SQL_C_ULONG: {
                unsigned long value = std::stoul(sourceData);
                *static_cast<SQLULEN *>(TargetValue) = value;
                if (StrLen_or_Ind) *StrLen_or_Ind = sizeof(SQLULEN);
                return SQL_SUCCESS;
            }

            case SQL_C_SHORT:
            case SQL_C_SSHORT: {
                short value = static_cast<short>(std::stoi(sourceData));
                *static_cast<SQLSMALLINT *>(TargetValue) = value;
                if (StrLen_or_Ind) *StrLen_or_Ind = sizeof(SQLSMALLINT);
                return SQL_SUCCESS;
            }

            case SQL_C_USHORT: {
                unsigned short value = static_cast<unsigned short>(std::stoul(sourceData));
                *static_cast<SQLUSMALLINT *>(TargetValue) = value;
                if (StrLen_or_Ind) *StrLen_or_Ind = sizeof(SQLUSMALLINT);
                return SQL_SUCCESS;
            }

            case SQL_C_TINYINT:
            case SQL_C_STINYINT: {
                char value = static_cast<char>(std::stoi(sourceData));
                *static_cast<SQLSCHAR *>(TargetValue) = value;
                if (StrLen_or_Ind) *StrLen_or_Ind = sizeof(SQLSCHAR);
                return SQL_SUCCESS;
            }

            case SQL_C_UTINYINT: {
                unsigned char value = static_cast<unsigned char>(std::stoul(sourceData));
                *static_cast<SQLCHAR *>(TargetValue) = value;
                if (StrLen_or_Ind) *StrLen_or_Ind = sizeof(SQLCHAR);
                return SQL_SUCCESS;
            }

            case SQL_C_SBIGINT: {
                long long value = std::stoll(sourceData);
                *static_cast<SQLBIGINT *>(TargetValue) = value;
                if (StrLen_or_Ind) *StrLen_or_Ind = sizeof(SQLBIGINT);
                return SQL_SUCCESS;
            }

            case SQL_C_UBIGINT: {
                unsigned long long value = std::stoull(sourceData);
                *static_cast<SQLUBIGINT *>(TargetValue) = value;
                if (StrLen_or_Ind) *StrLen_or_Ind = sizeof(SQLUBIGINT);
                return SQL_SUCCESS;
            }

            case SQL_C_FLOAT: {
                float value = std::stof(sourceData);
                *static_cast<SQLREAL *>(TargetValue) = value;
                if (StrLen_or_Ind) *StrLen_or_Ind = sizeof(SQLREAL);
                return SQL_SUCCESS;
            }

            case SQL_C_DOUBLE: {
                double value = std::stod(sourceData);
                *static_cast<SQLDOUBLE *>(TargetValue) = value;
                if (StrLen_or_Ind) *StrLen_or_Ind = sizeof(SQLDOUBLE);
                return SQL_SUCCESS;
            }

            case SQL_C_BIT: {
                bool value = (sourceData[0] == '1' ||
                            tolower(sourceData[0]) == 't' ||
                            tolower(sourceData[0]) == 'y');
                *static_cast<unsigned char *>(TargetValue) = value ? 1 : 0;
                if (StrLen_or_Ind) *StrLen_or_Ind = sizeof(unsigned char);
                return SQL_SUCCESS;
            }

            default:
                LOGF("Unsupported numeric target type: %d", TargetType);
                return SQL_ERROR;
        }
    } catch (...) {
        return SQL_ERROR;
    }
}

SQLRETURN SQL_API SQLGetData_A(
    SQLHSTMT StatementHandle,
    SQLUSMALLINT ColumnNumber,
    SQLSMALLINT TargetType,
    SQLPOINTER TargetValue,
    SQLLEN BufferLength,
    SQLLEN *StrLen_or_Ind) {

    LOGF("SQLGetData_A - Column: %d, Type: %d, Buffer: %zd",
         ColumnNumber, TargetType, static_cast<size_t>(BufferLength));

    auto stmt = static_cast<Statement *>(StatementHandle);
    const char *sourceData;
    const ColumnDesc *columnDesc;

    SQLRETURN validationResult = ValidateGetDataCall(stmt, ColumnNumber, &sourceData, &columnDesc);
    if (validationResult != SQL_SUCCESS) return validationResult;

    // Handle NULL value
    if (!sourceData || !*sourceData) {
        if (StrLen_or_Ind) *StrLen_or_Ind = SQL_NULL_DATA;

        if (TargetValue && BufferLength > 0) {
            switch (TargetType) {
                case SQL_C_WCHAR:
                    if (BufferLength >= sizeof(WCHAR))
                        *static_cast<WCHAR *>(TargetValue) = L'\0';
                    break;
                case SQL_C_CHAR:
                    *static_cast<char *>(TargetValue) = '\0';
                    break;
                case SQL_C_GUID:
                    if (BufferLength >= sizeof(GUID))
                        memset(TargetValue, 0, sizeof(GUID));
                    break;
                case SQL_C_TYPE_TIMESTAMP:
                    if (BufferLength >= sizeof(SQL_TIMESTAMP_STRUCT))
                        memset(TargetValue, 0, sizeof(SQL_TIMESTAMP_STRUCT));
                break;
            }
        }
        return SQL_SUCCESS;
    }

    switch (TargetType) {
        case SQL_C_GUID: {
            if (StrLen_or_Ind) *StrLen_or_Ind = sizeof(GUID);
            if (TargetValue && BufferLength >= sizeof(GUID)) {
                GUID guid;
                if (UuidFromStringA((RPC_CSTR)sourceData, &guid) == RPC_S_OK) {
                    memcpy(TargetValue, &guid, sizeof(GUID));
                } else {
                    memset(TargetValue, 0, sizeof(GUID));
                }
            }
            return SQL_SUCCESS;
        }

        case SQL_C_WCHAR: {
            int wideCharsRequired = MultiByteToWideChar(CP_UTF8, 0, sourceData, -1, nullptr, 0);
            if (!wideCharsRequired) return SQL_ERROR;

            if (StrLen_or_Ind)
                *StrLen_or_Ind = (wideCharsRequired - 1) * sizeof(WCHAR);

            if (!TargetValue || BufferLength <= 0) return SQL_SUCCESS;

            int bufferWideChars = BufferLength / sizeof(WCHAR);
            if (!bufferWideChars) return SQL_ERROR;

            int charsConverted = MultiByteToWideChar(CP_UTF8, 0, sourceData, -1,
                static_cast<LPWSTR>(TargetValue), bufferWideChars);

            if (!charsConverted) return SQL_ERROR;

            if (charsConverted < wideCharsRequired) {
                static_cast<WCHAR *>(TargetValue)[bufferWideChars - 1] = L'\0';
                return SQL_SUCCESS_WITH_INFO;
            }
            return SQL_SUCCESS;
        }

        case SQL_C_CHAR: {
            size_t sourceLen = strlen(sourceData);
            if (StrLen_or_Ind) *StrLen_or_Ind = static_cast<SQLLEN>(sourceLen);

            if (TargetValue && BufferLength > 0) {
                size_t copyLen = std::min<size_t>(sourceLen, BufferLength - 1);
                strncpy(static_cast<char *>(TargetValue), sourceData, copyLen);
                static_cast<char *>(TargetValue)[copyLen] = '\0';
                if (copyLen < sourceLen) return SQL_SUCCESS_WITH_INFO;
            }
            return SQL_SUCCESS;
        }

        case SQL_C_BINARY: {
            // If this is a boolean/bit column
            if (columnDesc->sqlType == SQL_BIT) {
                if (StrLen_or_Ind) *StrLen_or_Ind = 1; // One byte for boolean

                if (TargetValue && BufferLength > 0) {
                    // Convert the string representation to a single byte
                    unsigned char value = (sourceData[0] == '1' ||
                                        tolower(sourceData[0]) == 't' ||
                                        tolower(sourceData[0]) == 'y') ? 1 : 0;
                    *static_cast<unsigned char*>(TargetValue) = value;
                }
                return SQL_SUCCESS;
            }
            // For other types, handle binary conversion or return error
            LOGF("SQL_C_BINARY conversion not supported for type: %d", columnDesc->sqlType);
            return SQL_ERROR;
        }

        default:
            return ConvertNumeric(sourceData, TargetType, TargetValue, StrLen_or_Ind);
    }
}

SQLRETURN SQL_API SQLGetData_W(
    SQLHSTMT StatementHandle,
    SQLUSMALLINT ColumnNumber,
    SQLSMALLINT TargetType,
    SQLPOINTER TargetValue,
    SQLLEN BufferLength,
    SQLLEN *StrLen_or_Ind) {

    LOGF("SQLGetDataW called - Column: %d, TargetType: %d, BufferLength: %zd",
         ColumnNumber, TargetType, static_cast<size_t>(BufferLength));

    auto stmt = static_cast<Statement *>(StatementHandle);
    const char *sourceData;
    const ColumnDesc *columnDesc;

    SQLRETURN validationResult = ValidateGetDataCall(stmt, ColumnNumber, &sourceData, &columnDesc);
    if (validationResult != SQL_SUCCESS) return validationResult;

    // Handle NULL value
    if (*sourceData == '\0') {
        if (StrLen_or_Ind) *StrLen_or_Ind = SQL_NULL_DATA;
        if (TargetValue && BufferLength > 0) {
            switch (TargetType) {
                case SQL_C_WCHAR:
                    if (BufferLength >= sizeof(WCHAR)) {
                        *static_cast<WCHAR *>(TargetValue) = L'\0';
                    }
                    break;
                case SQL_C_CHAR:
                    *static_cast<char *>(TargetValue) = '\0';
                    break;
                case SQL_C_TYPE_TIMESTAMP:
                    if (BufferLength >= sizeof(SQL_TIMESTAMP_STRUCT))
                        memset(TargetValue, 0, sizeof(SQL_TIMESTAMP_STRUCT));
                break;
            }
        }
        return SQL_SUCCESS;
    }

    switch (TargetType) {
        case SQL_C_GUID: {
            if (StrLen_or_Ind) *StrLen_or_Ind = sizeof(GUID);
            if (TargetValue && BufferLength >= sizeof(GUID)) {
                GUID guid;
                if (UuidFromStringA((RPC_CSTR)sourceData, &guid) == RPC_S_OK) {
                    memcpy(TargetValue, &guid, sizeof(GUID));
                } else {
                    // Zero out the GUID if conversion fails
                    memset(TargetValue, 0, sizeof(GUID));
                }
            }
            return SQL_SUCCESS;
        }

        case SQL_C_WCHAR: {
            int wideCharsRequired = MultiByteToWideChar(CP_UTF8, 0, sourceData, -1, nullptr, 0);
            if (wideCharsRequired == 0) {
                LOGF("Failed to get required buffer size: %lu", GetLastError());
                return SQL_ERROR;
            }

            if (StrLen_or_Ind) {
                *StrLen_or_Ind = (wideCharsRequired - 1) * sizeof(WCHAR);
            }

            if (!TargetValue || BufferLength == 0) return SQL_SUCCESS;

            int bufferWideChars = BufferLength / sizeof(WCHAR);
            if (bufferWideChars == 0) return SQL_ERROR;

            int charsConverted = MultiByteToWideChar(
                CP_UTF8, 0, sourceData, -1,
                static_cast<LPWSTR>(TargetValue), bufferWideChars
            );

            if (charsConverted == 0) {
                LOGF("Failed to convert string: %lu", GetLastError());
                return SQL_ERROR;
            }

            if (charsConverted < wideCharsRequired) {
                static_cast<WCHAR *>(TargetValue)[bufferWideChars - 1] = L'\0';
                return SQL_SUCCESS_WITH_INFO;
            }
            return SQL_SUCCESS;
        }

        case SQL_C_CHAR: {
            size_t sourceLen = strlen(sourceData);
            if (StrLen_or_Ind) *StrLen_or_Ind = static_cast<SQLLEN>(sourceLen);

            if (TargetValue && BufferLength > 0) {
                size_t copyLen = std::min<size_t>(sourceLen, BufferLength - 1);
                strncpy(static_cast<char *>(TargetValue), sourceData, copyLen);
                static_cast<char *>(TargetValue)[copyLen] = '\0';

                if (copyLen < sourceLen) return SQL_SUCCESS_WITH_INFO;
            }
            return SQL_SUCCESS;
        }

        case SQL_C_BINARY: {
            // If this is a boolean/bit column
            if (columnDesc->sqlType == SQL_BIT) {
                if (StrLen_or_Ind) *StrLen_or_Ind = 1; // One byte for boolean

                if (TargetValue && BufferLength > 0) {
                    // Convert the string representation to a single byte
                    unsigned char value = (sourceData[0] == '1' ||
                                        tolower(sourceData[0]) == 't' ||
                                        tolower(sourceData[0]) == 'y') ? 1 : 0;
                    *static_cast<unsigned char*>(TargetValue) = value;
                }
                return SQL_SUCCESS;
            }
            // For other types, handle binary conversion or return error
            LOGF("SQL_C_BINARY conversion not supported for type: %d", columnDesc->sqlType);
            return SQL_ERROR;
        }

        default:
            return ConvertNumeric(sourceData, TargetType, TargetValue, StrLen_or_Ind);
    }
}
} // extern "C"
