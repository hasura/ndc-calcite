// First, include Windows headers
#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN

// Then SQL headers in this specific order
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>

// Standard C++ includes
#include <vector>
#include <string>

// Your project headers
#include "../include/Statement.hpp"

#include <codecvt>
#include <locale>

#include "Connection.hpp"
#include "JVMSingleton.hpp"
#include "../include/Logging.hpp"

SQLRETURN Statement::setArrowResult(jobject schemaRoot, const std::vector<ColumnDesc> &columnDescriptors) {
    LOGF("Starting setArrowResult with %zu columns", columnDescriptors.size());

    if (!conn || !schemaRoot) {
        LOG("Invalid parameters");
        return SQL_ERROR;
    }

    try {
        clearResults();

        // Process the Arrow data
        jclass rootClass = JVMSingleton::getEnv()->GetObjectClass(schemaRoot);
        if (!rootClass) {
            LOG("Failed to get root class");
            return SQL_ERROR;
        }
        jmethodID getRowCountMethod = JVMSingleton::getEnv()->GetMethodID(rootClass, "getRowCount", "()I");
        if (!getRowCountMethod) {
            LOG("Failed to get getRowCount method");
            JVMSingleton::getEnv()->DeleteLocalRef(rootClass);
            return SQL_ERROR;
        }
        jmethodID getFieldVectorsMethod = JVMSingleton::getEnv()->GetMethodID(rootClass, "getFieldVectors", "()Ljava/util/List;");
        if (!getFieldVectorsMethod) {
            LOG("Failed to get getFieldVectors method");
            JVMSingleton::getEnv()->DeleteLocalRef(rootClass);
            return SQL_ERROR;
        }

        jint rowCount = JVMSingleton::getEnv()->CallIntMethod(schemaRoot, getRowCountMethod);
        LOGF("Row count: %d", rowCount);

        jobject vectorsList = JVMSingleton::getEnv()->CallObjectMethod(schemaRoot, getFieldVectorsMethod);
        if (!vectorsList) {
            LOG("Failed to get vector list");
            JVMSingleton::getEnv()->DeleteLocalRef(rootClass);
            return SQL_ERROR;
        }
        jclass listClass = JVMSingleton::getEnv()->GetObjectClass(vectorsList);
        if (!listClass) {
            LOG("Failed to get list class");
            JVMSingleton::getEnv()->DeleteLocalRef(vectorsList);
            JVMSingleton::getEnv()->DeleteLocalRef(rootClass);
            return SQL_ERROR;
        }
        jmethodID getMethod = JVMSingleton::getEnv()->GetMethodID(listClass, "get", "(I)Ljava/lang/Object;");
        if (!getMethod) {
            LOG("Failed to get get method");
            JVMSingleton::getEnv()->DeleteLocalRef(listClass);
            JVMSingleton::getEnv()->DeleteLocalRef(vectorsList);
            JVMSingleton::getEnv()->DeleteLocalRef(rootClass);
            return SQL_ERROR;
        }
        jmethodID sizeMethod = JVMSingleton::getEnv()->GetMethodID(listClass, "size", "()I");
        if (!sizeMethod) {
            LOG("Failed to get size method");
            JVMSingleton::getEnv()->DeleteLocalRef(listClass);
            JVMSingleton::getEnv()->DeleteLocalRef(vectorsList);
            JVMSingleton::getEnv()->DeleteLocalRef(rootClass);
            return SQL_ERROR;
        }

        jint vectorCount = JVMSingleton::getEnv()->CallIntMethod(vectorsList, sizeMethod);
        LOGF("Vector count: %d", vectorCount);

        // Initialize result data
        resultData.resize(rowCount);
        for (jint row = 0; row < rowCount; row++) {
            resultData[row].resize(vectorCount);
        }

        // Process all vectors
        for (jint col = 0; col < vectorCount; col++) {
            LOGF("Processing column %d", col);
            jobject fieldVector = JVMSingleton::getEnv()->CallObjectMethod(vectorsList, getMethod, col);
            if (!fieldVector) {
                LOGF("Null field vector for column %d", col);
                continue;
            }

            jclass vectorClass = JVMSingleton::getEnv()->GetObjectClass(fieldVector);
            if (!vectorClass) {
                LOGF("Failed to get vector class for column %d", col);
                JVMSingleton::getEnv()->DeleteLocalRef(fieldVector);
                continue;
            }
            jmethodID getObjectMethod = JVMSingleton::getEnv()->GetMethodID(vectorClass, "getObject", "(I)Ljava/lang/Object;");
            if (!getObjectMethod) {
                LOGF("Failed to get getObject method for column %d", col);
                JVMSingleton::getEnv()->DeleteLocalRef(vectorClass);
                JVMSingleton::getEnv()->DeleteLocalRef(fieldVector);
                continue;
            }
            jmethodID isNullMethod = JVMSingleton::getEnv()->GetMethodID(vectorClass, "isNull", "(I)Z");
            if (!isNullMethod) {
                LOGF("Failed to get isNull method for column %d", col);
                JVMSingleton::getEnv()->DeleteLocalRef(vectorClass);
                JVMSingleton::getEnv()->DeleteLocalRef(fieldVector);
                continue;
            }

            for (jint row = 0; row < rowCount; row++) {
                // Check if the value is null
                jboolean isNull = JVMSingleton::getEnv()->CallBooleanMethod(fieldVector, isNullMethod, row);
                if (isNull || JVMSingleton::getEnv()->ExceptionCheck()) {
                    if (JVMSingleton::getEnv()->ExceptionCheck()) {
                        JVMSingleton::getEnv()->ExceptionDescribe();
                        JVMSingleton::getEnv()->ExceptionClear();
                    }
                    LOGF("Null value at row %d, col %d", row, col);
                    resultData[row][col].isNull = true;
                    resultData[row][col].data.clear();
                    continue;
                }

                // Get the value
                jobject value = JVMSingleton::getEnv()->CallObjectMethod(fieldVector, getObjectMethod, row);
                if (!value) {
                    LOGF("Null value at row %d, col %d", row, col);
                    resultData[row][col].isNull = true;
                    resultData[row][col].data.clear();
                    continue;
                }

                // Convert value to string
                jclass stringClass = JVMSingleton::getEnv()->FindClass("java/lang/String");
                if (!stringClass) {
                    LOGF("Failed to find String class at row %d, col %d", row, col);
                    JVMSingleton::getEnv()->DeleteLocalRef(value);
                    continue;
                }
                jmethodID toStringMethod = JVMSingleton::getEnv()->GetMethodID(stringClass, "toString", "()Ljava/lang/String;");
                if (!toStringMethod) {
                    LOGF("Failed to get toString method at row %d, col %d", row, col);
                    JVMSingleton::getEnv()->DeleteLocalRef(stringClass);
                    JVMSingleton::getEnv()->DeleteLocalRef(value);
                    continue;
                }

                if (auto strValue = reinterpret_cast<jstring>(JVMSingleton::getEnv()->CallObjectMethod(value, toStringMethod))) {
                    if (const char *chars = JVMSingleton::getEnv()->GetStringUTFChars(strValue, nullptr)) {
                        resultData[row][col].isNull = false;
                        resultData[row][col].data = chars;
                        JVMSingleton::getEnv()->ReleaseStringUTFChars(strValue, chars);
                        LOGF("Set value at [%d,%d]: %s", row, col, resultData[row][col].data.c_str());
                    } else {
                        resultData[row][col].isNull = true;
                        resultData[row][col].data.clear();
                    }
                    JVMSingleton::getEnv()->DeleteLocalRef(strValue);
                } else {
                    resultData[row][col].isNull = true;
                    resultData[row][col].data.clear();
                }

                JVMSingleton::getEnv()->DeleteLocalRef(value);
                JVMSingleton::getEnv()->DeleteLocalRef(stringClass);
            }

            JVMSingleton::getEnv()->DeleteLocalRef(vectorClass);
            JVMSingleton::getEnv()->DeleteLocalRef(fieldVector);
        }

        JVMSingleton::getEnv()->DeleteLocalRef(listClass);
        JVMSingleton::getEnv()->DeleteLocalRef(vectorsList);
        JVMSingleton::getEnv()->DeleteLocalRef(rootClass);

        hasResult = true;
        currentRow = 0;

        LOGF("Successfully set up result set with %d rows and %d columns", rowCount, vectorCount);
        return SQL_SUCCESS;
    } catch (const std::exception &e) {
        LOGF("Exception in setArrowResult: %s", e.what());
        if (JVMSingleton::getEnv()->ExceptionCheck()) {
            JVMSingleton::getEnv()->ExceptionDescribe();
            JVMSingleton::getEnv()->ExceptionClear();
        }
        clearResults();
        return SQL_ERROR;
    } catch (...) {
        LOG("Unknown exception in setArrowResult");
        if (conn != nullptr && JVMSingleton::getEnv() != nullptr) {
            if (JVMSingleton::getEnv()->ExceptionCheck()) {
                JVMSingleton::getEnv()->ExceptionDescribe();
                JVMSingleton::getEnv()->ExceptionClear();
            }
        }
        clearResults();
        return SQL_ERROR;
    }
}

SQLRETURN Statement::fetch() {
    if (!hasResult) return SQL_ERROR;
    if (currentRow >= resultData.size()) return SQL_NO_DATA;

    // Update rows fetched pointer
    if (rowsFetchedPtr) *rowsFetchedPtr = 1;

    // Update row status
    if (rowStatusPtr) *rowStatusPtr = SQL_ROW_SUCCESS;

    currentRow++;
    return SQL_SUCCESS;
}

SQLRETURN Statement::getFetchStatus() const {
    if (!hasResult) {
        return SQL_ERROR;
    }

    if (currentRow >= resultData.size()) {
        return SQL_NO_DATA;
    }

    return SQL_SUCCESS;
}

bool Statement::hasData() const {
    return hasResult && currentRow < resultData.size();
}

// Additional helper method for fetching data from the result set
SQLRETURN Statement::getData(SQLUSMALLINT colNum, SQLSMALLINT targetType,
                             SQLPOINTER targetValue, SQLLEN bufferLength,
                             SQLLEN *strLengthOrIndicator) {
    LOGF("getData called for column %d", colNum);

    // Validate state and parameters
    if (!hasResult || currentRow == 0 || currentRow > resultData.size() ||
        colNum == 0 || colNum > resultColumns.size()) {
        LOG("Invalid state or parameters");
        return SQL_ERROR;
    }

    const auto &colData = resultData[currentRow - 1][colNum - 1];
    LOGF("Fetching data for row %d, column %d", currentRow - 1, colNum - 1);

    // Handle NULL values
    if (colData.isNull) {
        LOGF("NULL value in column %d", colNum);
        if (strLengthOrIndicator) {
            *strLengthOrIndicator = SQL_NULL_DATA;
        }
        return SQL_SUCCESS;
    }

    // Handle string data
    switch (targetType) {
        case SQL_C_WCHAR: {
            LOGF("Converting to WCHAR: '%s'", colData.data.c_str());

            const int requiredSize = MultiByteToWideChar(
                                         CP_UTF8, 0, colData.data.c_str(), -1, nullptr, 0
                                     ) * sizeof(WCHAR);

            if (strLengthOrIndicator) {
                *strLengthOrIndicator = requiredSize - sizeof(WCHAR);
            }

            if (!targetValue || bufferLength <= 0) {
                return SQL_SUCCESS;
            }

            int maxChars = bufferLength / sizeof(WCHAR);
            if (maxChars == 0) {
                return SQL_ERROR;
            }

            int charsWritten = MultiByteToWideChar(
                CP_UTF8, 0, colData.data.c_str(), -1,
                static_cast<LPWSTR>(targetValue), maxChars
            );

            if (charsWritten == 0) {
                LOGF("Conversion failed: %lu", GetLastError());
                return SQL_ERROR;
            }

            // Check for truncation
            if (charsWritten == maxChars) {
                static_cast<WCHAR *>(targetValue)[maxChars - 1] = L'\0';
                return SQL_SUCCESS_WITH_INFO;
            }

            return SQL_SUCCESS;
        }

        default:
            LOGF("Unsupported target type: %d", targetType);
            return SQL_ERROR;
    }
}

void Statement::clearResults() {
    LOG("Called clearResults()");
    hasResult = false;
    currentRow = 0;
    resultData.clear();
    boundParams.clear();
}

SQLRETURN Statement::bindParameter(SQLUSMALLINT parameterNumber,
                                   SQLSMALLINT inputOutputType,
                                   SQLSMALLINT valueType,
                                   SQLSMALLINT parameterType,
                                   SQLULEN columnSize,
                                   SQLSMALLINT decimalDigits,
                                   SQLPOINTER parameterValuePtr,
                                   SQLLEN bufferLength,
                                   SQLLEN *strLen_or_IndPtr) {
    LOGF("Binding parameter %d of type %d", parameterNumber, valueType);

    if (!conn) {
        LOG("Invalid connection or environment");
        return SQL_ERROR;
    }

    // Parameter numbers are 1-based
    if (parameterNumber < 1) {
        LOG("Invalid parameter number");
        return SQL_ERROR;
    }

    // Check for null indicator
    if (strLen_or_IndPtr && *strLen_or_IndPtr == SQL_NULL_DATA) {
        // Handle NULL parameter - could use a special JniParam constructor for NULL
        if (parameterNumber > boundParams.size()) {
            boundParams.resize(parameterNumber);
        }
        // You might want to add a setNull method to JniParam
        return SQL_SUCCESS;
    }

    try {
        // Convert ODBC parameter to JniParam based on valueType
        switch (valueType) {
            case SQL_C_CHAR: {
                if (!parameterValuePtr) return SQL_ERROR;
                std::string value(static_cast<char *>(parameterValuePtr));
                if (parameterNumber > boundParams.size()) {
                    boundParams.resize(parameterNumber);
                }
                boundParams[parameterNumber - 1] = JniParam(value);
                break;
            }

            case SQL_C_WCHAR: {
                if (!parameterValuePtr) return SQL_ERROR;
                wchar_t *wstr = static_cast<wchar_t *>(parameterValuePtr);
                // Convert wide string to UTF-8
                int requiredSize = WideCharToMultiByte(CP_UTF8, 0, wstr, -1, nullptr, 0, nullptr, nullptr);
                if (requiredSize == 0) return SQL_ERROR;

                std::string utf8str(requiredSize, '\0');
                if (WideCharToMultiByte(CP_UTF8, 0, wstr, -1, &utf8str[0], requiredSize, nullptr, nullptr) == 0) {
                    return SQL_ERROR;
                }
                utf8str.resize(strlen(utf8str.c_str())); // Remove trailing null

                if (parameterNumber > boundParams.size()) {
                    boundParams.resize(parameterNumber);
                }
                boundParams[parameterNumber - 1] = JniParam(utf8str);
                break;
            }

            case SQL_C_LONG:
            case SQL_C_SLONG: {
                if (!parameterValuePtr) return SQL_ERROR;
                int value = *static_cast<SQLINTEGER *>(parameterValuePtr);
                if (parameterNumber > boundParams.size()) {
                    boundParams.resize(parameterNumber);
                }
                boundParams[parameterNumber - 1] = JniParam(value);
                break;
            }

            case SQL_C_FLOAT: {
                if (!parameterValuePtr) return SQL_ERROR;
                float value = *static_cast<float *>(parameterValuePtr);
                if (parameterNumber > boundParams.size()) {
                    boundParams.resize(parameterNumber);
                }
                boundParams[parameterNumber - 1] = JniParam(value);
                break;
            }

            case SQL_C_DOUBLE: {
                if (!parameterValuePtr) return SQL_ERROR;
                double value = *static_cast<double *>(parameterValuePtr);
                if (parameterNumber > boundParams.size()) {
                    boundParams.resize(parameterNumber);
                }
                boundParams[parameterNumber - 1] = JniParam(value);
                break;
            }

            case SQL_C_BIT: {
                if (!parameterValuePtr) return SQL_ERROR;
                bool value = (*static_cast<unsigned char *>(parameterValuePtr)) != 0;
                if (parameterNumber > boundParams.size()) {
                    boundParams.resize(parameterNumber);
                }
                boundParams[parameterNumber - 1] = JniParam(value);
                break;
            }

            default:
                LOGF("Unsupported parameter type: %d", valueType);
                return SQL_ERROR;
        }

        return SQL_SUCCESS;
    } catch (const std::exception &e) {
        LOGF("Exception in bindParameter: %s", e.what());
        return SQL_ERROR;
    }
}

std::string Statement::escapeString(const std::string &str) const {
    std::string escaped;
    escaped.reserve(str.length() + str.length() / 8); // Reserve extra space for escapes

    for (char c: str) {
        switch (c) {
            case '\'': escaped += "''";
                break; // Double single quotes for SQL
            default: escaped += c;
                break;
        }
    }
    return escaped;
}

std::string Statement::buildInterpolatedQuery() const {
    std::string result = originalQuery;

    // Apply LIMIT if maxRows is set
    if (maxRows > 0) {
        result += " LIMIT " + std::to_string(maxRows);
    }

    // Find all ? parameters and replace them
    size_t paramIndex = 0;
    size_t pos = 0;

    while ((pos = result.find('?', pos)) != std::string::npos) {
        if (paramIndex >= boundParams.size()) {
            throw std::runtime_error("Not enough parameters bound for query");
        }

        const auto &param = boundParams[paramIndex];
        std::string replacement;

        // Convert parameter to string representation
        switch (param.getType()) {
            case JniParam::Type::String:
                replacement = "'" + escapeString(param.getString()) + "'";
                break;

            case JniParam::Type::StringArray: {
                replacement = "(";
                bool first = true;
                for (const auto &str: param.getStringArray()) {
                    if (!first) replacement += ",";
                    replacement += "'" + escapeString(str) + "'";
                    first = false;
                }
                replacement += ")";
                break;
            }

            case JniParam::Type::Integer:
                replacement = std::to_string(param.getInt());
                break;

            case JniParam::Type::Float:
                replacement = std::to_string(param.getFloat());
                break;

            case JniParam::Type::Double:
                replacement = std::to_string(param.getDouble());
                break;

            case JniParam::Type::Boolean:
                replacement = param.getBool() ? "1" : "0";
                break;
        }

        result.replace(pos, 1, replacement);
        pos += replacement.length();
        paramIndex++;
    }

    LOGF("Interpolated query: %s", result.c_str());
    return result;
}
