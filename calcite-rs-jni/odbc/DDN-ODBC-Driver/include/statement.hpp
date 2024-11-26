#pragma once

#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <vector>
#include <string>
#include <jni.h>  // Add this if you're using JNI types

// Forward declaration of Connection class
class Connection;

struct ColumnDesc {
    const char* name;
    SQLSMALLINT nameLength;
    SQLSMALLINT sqlType;
    SQLULEN columnSize;
    SQLSMALLINT decimalDigits;
    SQLSMALLINT nullable;
};

struct ColumnData {
    bool isNull;
    std::string data;
};

class Statement {
public:
    std::vector<ColumnDesc> setupColumnResultColumns();

    explicit Statement(Connection* connection);
    
    // Delete copy constructor and assignment operator
    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;
    
    void clearResults();

    std::vector<ColumnDesc> setupTableResultColumns();
    SQLRETURN setArrowResult(jobject schemaRoot, const std::vector<ColumnDesc> &columnDescriptors);
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