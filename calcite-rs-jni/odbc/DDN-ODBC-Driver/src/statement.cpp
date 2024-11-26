// First, include Windows headers
#include <windows.h>

// Then SQL headers in this specific order
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>

// Standard C++ includes
#include <vector>
#include <string>

// Your project headers
#include "../include/statement.hpp"

#include "connection.hpp"
#include "../include/logging.hpp"

static const std::vector<ColumnDesc> TABLE_COLUMNS = {
    {"TABLE_CAT", 0, SQL_VARCHAR, 128, 0, SQL_NULLABLE},
    {"TABLE_SCHEM", 0, SQL_VARCHAR, 128, 0, SQL_NULLABLE},
    {"TABLE_NAME", 0, SQL_VARCHAR, 128, 0, SQL_NO_NULLS},
    {"TABLE_TYPE", 0, SQL_VARCHAR, 128, 0, SQL_NO_NULLS},
    {"REMARKS", 0, SQL_VARCHAR, 254, 0, SQL_NULLABLE}
};

static const std::vector<ColumnDesc> COLUMN_COLUMNS = {
    {"TABLE_CAT", 0, SQL_VARCHAR, 128, 0, SQL_NULLABLE},
    {"TABLE_SCHEM", 0, SQL_VARCHAR, 128, 0, SQL_NULLABLE},
    {"TABLE_NAME", 0, SQL_VARCHAR, 128, 0, SQL_NO_NULLS},
    {"COLUMN_NAME", 0, SQL_VARCHAR, 128, 0, SQL_NO_NULLS},
    {"DATA_TYPE", 0, SQL_SMALLINT, 5, 0, SQL_NO_NULLS},
    {"TYPE_NAME", 0, SQL_VARCHAR, 128, 0, SQL_NO_NULLS},
    {"COLUMN_SIZE", 0, SQL_INTEGER, 10, 0, SQL_NULLABLE},
    {"BUFFER_LENGTH", 0, SQL_INTEGER, 10, 0, SQL_NULLABLE},
    {"DECIMAL_DIGITS", 0, SQL_SMALLINT, 5, 0, SQL_NULLABLE},
    {"NUM_PREC_RADIX", 0, SQL_SMALLINT, 5, 0, SQL_NULLABLE},
    {"NULLABLE", 0, SQL_SMALLINT, 5, 0, SQL_NO_NULLS},
    {"REMARKS", 0, SQL_VARCHAR, 254, 0, SQL_NULLABLE},
    {"COLUMN_DEF", 0, SQL_VARCHAR, 254, 0, SQL_NULLABLE},
    {"SQL_DATA_TYPE", 0, SQL_SMALLINT, 5, 0, SQL_NO_NULLS},
    {"SQL_DATETIME_SUB", 0, SQL_SMALLINT, 5, 0, SQL_NULLABLE},
    {"CHAR_OCTET_LENGTH", 0, SQL_INTEGER, 10, 0, SQL_NULLABLE},
    {"ORDINAL_POSITION", 0, SQL_INTEGER, 10, 0, SQL_NO_NULLS},
    {"IS_NULLABLE", 0, SQL_VARCHAR, 3, 0, SQL_NO_NULLS}
};

SQLRETURN Statement::setArrowResult(jobject schemaRoot, const std::vector<ColumnDesc>& columnDescriptors) {
    LOGF("Starting setArrowResult with %zu columns", columnDescriptors.size());

    if (!conn || !conn->env || !schemaRoot) {
        LOG("Invalid parameters");
        return SQL_ERROR;
    }

    try {
        clearResults();
        resultColumns = columnDescriptors;  // Store the descriptors as-is
        LOG("Stored column descriptors");

        // Process the Arrow data
        jclass rootClass = conn->env->GetObjectClass(schemaRoot);
        jmethodID getRowCountMethod = conn->env->GetMethodID(rootClass, "getRowCount", "()I");
        jmethodID getFieldVectorsMethod = conn->env->GetMethodID(rootClass, "getFieldVectors", "()Ljava/util/List;");

        jint rowCount = conn->env->CallIntMethod(schemaRoot, getRowCountMethod);
        LOGF("Row count: %d", rowCount);

        jobject vectorsList = conn->env->CallObjectMethod(schemaRoot, getFieldVectorsMethod);
        jclass listClass = conn->env->GetObjectClass(vectorsList);
        jmethodID getMethod = conn->env->GetMethodID(listClass, "get", "(I)Ljava/lang/Object;");
        jmethodID sizeMethod = conn->env->GetMethodID(listClass, "size", "()I");

        jint vectorCount = conn->env->CallIntMethod(vectorsList, sizeMethod);
        LOGF("Vector count: %d", vectorCount);

        // Initialize result data
        resultData.resize(rowCount);
        for (jint row = 0; row < rowCount; row++) {
            resultData[row].resize(vectorCount);
        }

        // Process all vectors
        for (jint col = 0; col < vectorCount; col++) {
            LOGF("Processing column %d", col);
            jobject fieldVector = conn->env->CallObjectMethod(vectorsList, getMethod, col);
            if (!fieldVector) {
                LOGF("Null field vector for column %d", col);
                continue;
            }

            jclass vectorClass = conn->env->GetObjectClass(fieldVector);
            jmethodID getObjectMethod = conn->env->GetMethodID(vectorClass, "getObject", "(I)Ljava/lang/Object;");
            jmethodID isNullMethod = conn->env->GetMethodID(vectorClass, "isNull", "(I)Z");

            for (jint row = 0; row < rowCount; row++) {
                // Check if the value is null
                jboolean isNull = conn->env->CallBooleanMethod(fieldVector, isNullMethod, row);
                if (isNull || conn->env->ExceptionCheck()) {
                    if (conn->env->ExceptionCheck()) {
                        conn->env->ExceptionClear();
                    }
                    LOGF("Null value at row %d, col %d", row, col);
                    resultData[row][col].isNull = true;
                    resultData[row][col].data.clear();
                    continue;
                }

                // Get the value
                jobject value = conn->env->CallObjectMethod(fieldVector, getObjectMethod, row);
                if (!value) {
                    LOGF("Null value at row %d, col %d", row, col);
                    resultData[row][col].isNull = true;
                    resultData[row][col].data.clear();
                    continue;
                }

                // Convert value to string
                jclass stringClass = conn->env->FindClass("java/lang/String");
                jmethodID toStringMethod = conn->env->GetMethodID(stringClass, "toString", "()Ljava/lang/String;");
                jstring strValue = (jstring)conn->env->CallObjectMethod(value, toStringMethod);

                if (strValue) {
                    const char* chars = conn->env->GetStringUTFChars(strValue, nullptr);
                    if (chars) {
                        resultData[row][col].isNull = false;
                        resultData[row][col].data = chars;
                        conn->env->ReleaseStringUTFChars(strValue, chars);
                        LOGF("Set value at [%d,%d]: %s", row, col, resultData[row][col].data.c_str());
                    } else {
                        resultData[row][col].isNull = true;
                        resultData[row][col].data.clear();
                    }
                    conn->env->DeleteLocalRef(strValue);
                } else {
                    resultData[row][col].isNull = true;
                    resultData[row][col].data.clear();
                }

                conn->env->DeleteLocalRef(value);
                conn->env->DeleteLocalRef(stringClass);
            }

            conn->env->DeleteLocalRef(vectorClass);
            conn->env->DeleteLocalRef(fieldVector);
        }

        conn->env->DeleteLocalRef(listClass);
        conn->env->DeleteLocalRef(vectorsList);
        conn->env->DeleteLocalRef(rootClass);

        hasResult = true;
        currentRow = 0;

        LOGF("Successfully set up result set with %d rows and %d columns", rowCount, vectorCount);
        return SQL_SUCCESS;

    } catch (...) {
        LOG("Exception in setArrowResult");
        if (conn->env->ExceptionCheck()) {
            conn->env->ExceptionDescribe();
            conn->env->ExceptionClear();
        }
        clearResults();
        return SQL_ERROR;
    }
}

SQLRETURN Statement::fetch() {
    if (!hasResult) {
        return SQL_ERROR;
    }

    // Check if we've reached the end of the result set
    if (currentRow >= resultData.size()) {
        return SQL_NO_DATA;
    }

    // Move to next row
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

std::vector<ColumnDesc> Statement::setupColumnResultColumns() {
    // Clear any existing result set
    clearResults();

    // Create column descriptors for the COLUMNS result set
    resultColumns.clear();
    for (const auto& colDef : COLUMN_COLUMNS) {
        ColumnDesc col{};
        col.name = colDef.name;
        col.nameLength = static_cast<SQLSMALLINT>(strlen(colDef.name));
        col.sqlType = colDef.sqlType;
        col.columnSize = colDef.columnSize;
        col.decimalDigits = colDef.decimalDigits;
        col.nullable = colDef.nullable;
        resultColumns.push_back(col);
    }

    LOGF("Set up %zu column result columns", resultColumns.size());
    return COLUMN_COLUMNS;
}

Statement::Statement(Connection* connection) : conn(connection) {
    hasResult = false;
    currentRow = 0;
    resultData.clear();
}

std::vector<ColumnDesc> Statement::setupTableResultColumns() {
    clearResults();
    resultColumns.clear();

    for (const auto& colDef : TABLE_COLUMNS) {
        ColumnDesc col{};
        col.name = colDef.name;
        col.nameLength = static_cast<SQLSMALLINT>(strlen(colDef.name));
        col.sqlType = colDef.sqlType;
        col.columnSize = colDef.columnSize;
        col.decimalDigits = colDef.decimalDigits;
        col.nullable = colDef.nullable;
        LOGF("Setting up column %s with SQL type %d", col.name, col.sqlType);
        resultColumns.push_back(col);
    }
    return TABLE_COLUMNS;
}

// Additional helper method for fetching data from the result set
SQLRETURN Statement::getData(SQLUSMALLINT colNum, SQLSMALLINT targetType,
                           SQLPOINTER targetValue, SQLLEN bufferLength,
                           SQLLEN* strLengthOrIndicator) {
    LOGF("getData called for column %d", colNum);
    
    // Validate state and parameters
    if (!hasResult || currentRow == 0 || currentRow > resultData.size() ||
        colNum == 0 || colNum > resultColumns.size()) {
        LOG("Invalid state or parameters");
        return SQL_ERROR;
    }

    const auto& colData = resultData[currentRow - 1][colNum - 1];
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
                static_cast<WCHAR*>(targetValue)[maxChars - 1] = L'\0';
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
    hasResult = false;
    currentRow = 0;
    resultData.clear();
    resultColumns.clear();
}
