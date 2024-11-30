#pragma once

#include <Error.hpp>
#include <windows.h>
#include <sql.h>
#include <vector>
#include <string>

struct DiagnosticRecord {
    std::wstring sqlState;
    SQLINTEGER nativeError;
    std::wstring errorMsg;
};

class DiagnosticManager {
public:
    void addDiagnostic(const std::wstring& sqlState, SQLINTEGER nativeError, const std::wstring& errorMsg) {
        DiagnosticRecord record{ sqlState, nativeError, errorMsg };
        records.push_back(record);
    }

    SQLRETURN getDiagField(SQLSMALLINT recNumber, SQLSMALLINT diagIdentifier, SQLPOINTER diagInfo, SQLSMALLINT bufferLength, SQLSMALLINT* stringLengthPtr) {
        if (recNumber < 1 || recNumber > records.size()) {
            return SQL_NO_DATA;
        }

        const DiagnosticRecord& record = records[recNumber - 1];

        switch (diagIdentifier) {
            case SQL_DIAG_SQLSTATE:
                if (diagInfo) {
                    std::wcsncpy(static_cast<SQLWCHAR*>(diagInfo), record.sqlState.c_str(), bufferLength / sizeof(SQLWCHAR));
                    if (stringLengthPtr) {
                        *stringLengthPtr = static_cast<SQLSMALLINT>(record.sqlState.size() * sizeof(SQLWCHAR));
                    }
                }
            break;

            case SQL_DIAG_NATIVE:
                if (diagInfo) {
                    *static_cast<SQLINTEGER*>(diagInfo) = record.nativeError;
                }
            if (stringLengthPtr) {
                *stringLengthPtr = sizeof(SQLINTEGER);
            }
            break;

            case SQL_DIAG_MESSAGE_TEXT:
                if (diagInfo) {
                    std::wcsncpy(static_cast<SQLWCHAR*>(diagInfo), record.errorMsg.c_str(), bufferLength / sizeof(SQLWCHAR));
                    if (stringLengthPtr) {
                        *stringLengthPtr = static_cast<SQLSMALLINT>(record.errorMsg.size() * sizeof(SQLWCHAR));
                    }
                }
            break;

            default:
                return SQL_ERROR;
        }

        return SQL_SUCCESS;
    }

private:
    std::vector<DiagnosticRecord> records;
};

