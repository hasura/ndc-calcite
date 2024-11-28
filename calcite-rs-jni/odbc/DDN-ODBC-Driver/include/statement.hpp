#pragma once

#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <vector>
#include <string>
#include <jni.h>  // Add this if you're using JNI types

#include "JniParam.hpp"

// Forward declaration of Connection class
class Connection;

struct ColumnDesc {
    const char* name;
    SQLSMALLINT nameLength;
    SQLSMALLINT sqlType;
    SQLULEN columnSize;
    SQLSMALLINT decimalDigits;
    SQLSMALLINT nullable;
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
};

struct ColumnData {
    bool isNull;
    std::string data;
};

class Statement {
private:
    std::vector<JniParam> boundParams;
    std::string originalQuery;

public:
    explicit Statement(Connection* connection);
    
    // Delete copy constructor and assignment operator
    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;
    
    void clearResults();

    SQLRETURN bindParameter(SQLUSMALLINT parameterNumber, SQLSMALLINT inputOutputType, SQLSMALLINT valueType,
                            SQLSMALLINT parameterType, SQLULEN columnSize, SQLSMALLINT decimalDigits,
                            SQLPOINTER parameterValuePtr, SQLLEN bufferLength, SQLLEN *strLen_or_IndPtr);

    std::string escapeString(const std::string &str) const;
    std::string buildInterpolatedQuery() const;
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
};