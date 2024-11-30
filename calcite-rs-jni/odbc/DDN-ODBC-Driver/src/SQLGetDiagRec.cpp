#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include "../include/Connection.hpp"
#include "../include/Logging.hpp"
#include "../include/Globals.hpp"
#include "../include/Environment.hpp"
#include "../include/Statement.hpp"


template<typename CHAR_TYPE>
SQLRETURN GetDiagRec_Template(
    SQLSMALLINT handleType,
    SQLHANDLE handle,
    SQLSMALLINT recNumber,
    CHAR_TYPE *sqlState,
    SQLINTEGER *nativeErrorPtr,
    CHAR_TYPE *messageText,
    SQLSMALLINT bufferLength,
    SQLSMALLINT *textLengthPtr,
    bool isUnicode) {

    // Validate handle
    if (!handle) {
        return SQL_INVALID_HANDLE;
    }

    // Only return the first record (most drivers only support one)
    if (recNumber <= 0 || recNumber > 1) {
        return SQL_NO_DATA;
    }

    // Get the error info based on handle type
    std::string state;
    std::string message;
    SQLINTEGER nativeError = 0;

    switch (handleType) {
        case SQL_HANDLE_ENV: {
            auto env = static_cast<Environment*>(handle);
            if (!env) return SQL_INVALID_HANDLE;

            state = env->getLastError().sqlState;
            message = env->getLastError().message;
            nativeError = env->getLastError().nativeError;
            break;
        }

        case SQL_HANDLE_DBC: {
            auto conn = static_cast<Connection*>(handle);
            if (!conn) return SQL_INVALID_HANDLE;

            state = conn->getLastError().sqlState;
            message = conn->getLastError().message;
            nativeError = conn->getLastError().nativeError;
            break;
        }

        case SQL_HANDLE_STMT: {
            auto stmt = static_cast<Statement*>(handle);
            if (!stmt) return SQL_INVALID_HANDLE;

            state = stmt->getLastError().sqlState;
            message = stmt->getLastError().message;
            nativeError = stmt->getLastError().nativeError;
            break;
        }

        default:
            return SQL_INVALID_HANDLE;
    }

    // If no error recorded
    if (state.empty()) {
        return SQL_NO_DATA;
    }

    // Copy SQL state if buffer provided
    if (sqlState) {
        if (isUnicode) {
            std::wstring wstate = StringToWideString(state);
            wcsncpy(reinterpret_cast<wchar_t*>(sqlState), wstate.c_str(), 5);
            reinterpret_cast<wchar_t*>(sqlState)[5] = L'\0';
        } else {
            strncpy(reinterpret_cast<char*>(sqlState), state.c_str(), 5);
            reinterpret_cast<char*>(sqlState)[5] = '\0';
        }
    }

    // Set native error if pointer provided
    if (nativeErrorPtr) {
        *nativeErrorPtr = nativeError;
    }

    // Handle message text
    if (messageText && bufferLength > 0) {
        if (isUnicode) {
            std::wstring wmessage = StringToWideString(message);
            SQLSMALLINT copyLength = std::min<SQLSMALLINT>(
                static_cast<SQLSMALLINT>(wmessage.length()),
                static_cast<SQLSMALLINT>(bufferLength - 1));

            wcsncpy(reinterpret_cast<wchar_t*>(messageText), wmessage.c_str(), copyLength);
            reinterpret_cast<wchar_t*>(messageText)[copyLength] = L'\0';

            if (textLengthPtr) {
                *textLengthPtr = static_cast<SQLSMALLINT>(wmessage.length());
            }

            // Check if truncation occurred
            if (wmessage.length() >= static_cast<size_t>(bufferLength)) {
                return SQL_SUCCESS_WITH_INFO;
            }
        } else {
            SQLSMALLINT copyLength = std::min<SQLSMALLINT>(
                static_cast<SQLSMALLINT>(message.length()),
                static_cast<SQLSMALLINT>(bufferLength - 1));

            strncpy(reinterpret_cast<char*>(messageText), message.c_str(), copyLength);
            reinterpret_cast<char*>(messageText)[copyLength] = '\0';

            if (textLengthPtr) {
                *textLengthPtr = static_cast<SQLSMALLINT>(message.length());
            }

            // Check if truncation occurred
            if (message.length() >= static_cast<size_t>(bufferLength)) {
                return SQL_SUCCESS_WITH_INFO;
            }
        }
    } else if (textLengthPtr) {
        // If no buffer provided but length pointer is present,
        // return total length needed
        *textLengthPtr = static_cast<SQLSMALLINT>(
            isUnicode ? StringToWideString(message).length() : message.length()
        );
    }

    return SQL_SUCCESS;
}

extern "C" {

SQLRETURN SQL_API SQLGetDiagRec(
    SQLSMALLINT handleType,
    SQLHANDLE handle,
    SQLSMALLINT recNumber,
    SQLCHAR *sqlState,
    SQLINTEGER *nativeErrorPtr,
    SQLCHAR *messageText,
    SQLSMALLINT bufferLength,
    SQLSMALLINT *textLengthPtr) {

    return GetDiagRec_Template<SQLCHAR>(
        handleType,
        handle,
        recNumber,
        sqlState,
        nativeErrorPtr,
        messageText,
        bufferLength,
        textLengthPtr,
        false  // isUnicode = false
    );
}

SQLRETURN SQL_API SQLGetDiagRecW(
    SQLSMALLINT handleType,
    SQLHANDLE handle,
    SQLSMALLINT recNumber,
    SQLWCHAR *sqlState,
    SQLINTEGER *nativeErrorPtr,
    SQLWCHAR *messageText,
    SQLSMALLINT bufferLength,
    SQLSMALLINT *textLengthPtr) {

    return GetDiagRec_Template<SQLWCHAR>(
        handleType,
        handle,
        recNumber,
        sqlState,
        nativeErrorPtr,
        messageText,
        bufferLength,
        textLengthPtr,
        true  // isUnicode = true
    );
}

} // extern "C"
