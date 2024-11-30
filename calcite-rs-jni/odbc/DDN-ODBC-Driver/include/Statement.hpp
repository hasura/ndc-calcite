#pragma once

#include <Error.hpp>
#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <vector>
#include <string>
#include <jni.h>  // Add this if you're using JNI types

#include "Connection.hpp"
#include "Globals.hpp"
#include "JniParam.hpp"

// Forward declaration of Connection class
class Connection;
std::wstring StringToWideString(const std::string &str);

struct ColumnDesc {
    const char* name;
    SQLSMALLINT nameLength;
    SQLSMALLINT sqlType;
    SQLULEN columnSize;
    SQLSMALLINT nullable;
    bool autoIncrement;
    bool caseSensitive;
    bool currency;
    bool definitelyWritable;
    bool readOnly;
    bool searchable;
    bool _signed;
    bool writable;
    const char* catalogName;
    SQLSMALLINT catalogNameLength;
    const char* schemaName;
    SQLSMALLINT schemaNameLength;
    const char* tableName;
    SQLSMALLINT tableNameLength;
    const char* baseColumnName;
    SQLSMALLINT baseColumnNameLength;
    const char* baseTableName;
    SQLSMALLINT baseTableNameLength;
    const char* literalPrefix;
    SQLSMALLINT literalPrefixLength;
    const char* literalSuffix;
    SQLSMALLINT literalSuffixLength;
    const char* localTypeName;
    SQLSMALLINT localTypeNameLength;
    SQLSMALLINT unnamed;
    const char* label;
    SQLSMALLINT labelLength;
    SQLULEN displaySize;
    SQLSMALLINT scale;
    SQLSMALLINT precision;
    SQLSMALLINT octetLength;
    const char* typeName;
    SQLSMALLINT typeNameLength;
};

struct ColumnData {
    bool isNull;
    std::string data;
};

class Statement {
private:
    std::vector<JniParam> boundParams;
    std::string originalQuery;
    SQLULEN rowArraySize = 1;
    SQLULEN* rowsFetchedPtr = nullptr;
    SQLUSMALLINT* rowStatusPtr = nullptr;
    bool retrieveData = true;
    SQLULEN maxLength = 0;
    SQLULEN maxRows = 0;
    SQLULEN queryTimeout = 0;
    Error currentError{"00000", "", 0};

public:
    explicit Statement(Connection* connection): rowsFetchedPtr(nullptr), rowStatusPtr(nullptr), conn(connection),
                                                hasResult(false),
                                                currentRow(0) {
        resultData.clear();
    }

    // Delete copy constructor and assignment operator
    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;
    
    void clearResults();

    SQLRETURN bindParameter(SQLUSMALLINT parameterNumber, SQLSMALLINT inputOutputType, SQLSMALLINT valueType,
                            SQLSMALLINT parameterType, SQLULEN columnSize, SQLSMALLINT decimalDigits,
                            SQLPOINTER parameterValuePtr, SQLLEN bufferLength, SQLLEN *strLen_or_IndPtr);

    std::string escapeString(const std::string &str) const;
    std::string buildInterpolatedQuery() const;

    void setError(const std::string& state, const std::string& msg, SQLINTEGER native) {
        currentError = Error(state, msg, native);
        diagMgr->addDiagnostic(StringToWideString(std::string(state)), native, StringToWideString(std::string(msg)));
    }

    const Error& getLastError() const {
        return currentError;
    }

    SQLRETURN setArrowResult(jobject schemaRoot, const std::vector<ColumnDesc> &columnDescriptors);
    SQLRETURN setOriginalQuery(const std::string &query) { originalQuery = query; return SQL_SUCCESS; }
    SQLRETURN getData(SQLUSMALLINT colNum, SQLSMALLINT targetType,
                      SQLPOINTER targetValue, SQLLEN bufferLength,
                      SQLLEN* strLengthOrIndicator);
    SQLRETURN fetch();
    [[nodiscard]] SQLRETURN getFetchStatus() const;
    [[nodiscard]] bool hasData() const;

    Connection* conn;
    bool hasResult;
    size_t currentRow;
    std::vector<ColumnDesc> resultColumns;
    std::vector<std::vector<ColumnData>> resultData;
    void setRowArraySize(SQLULEN size) { rowArraySize = size; }
    void setRowsFetchedPtr(SQLULEN* ptr) { rowsFetchedPtr = ptr; }
    void setRowStatusPtr(SQLUSMALLINT* ptr) { rowStatusPtr = ptr; }
    void setRetrieveData(bool enable) { retrieveData = enable; }
    void setMaxLength(SQLULEN length) { maxLength = length; }
    void setMaxRows(SQLULEN rows) { maxRows = rows; }
    void setQueryTimeout(SQLULEN timeout) { queryTimeout = timeout; }
};