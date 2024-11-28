#pragma once
#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN

// ODBC includes
#include <sql.h>

// JNI includes
#include <jni.h>

// Standard includes
#include <complex.h>
#include <string>
#include <vector>

#include "JniParam.hpp"
#include "statement.hpp"

// Forward declarations
class Statement;

struct ConnectionParams {
    std::string server;
    std::string port;
    std::string database;
    std::string role;
    std::string auth;
    std::string uid;
    std::string pwd;
    std::string encrypt = "no";
    std::string timeout;

    bool isValid() const {
        return !server.empty() && !port.empty() && !database.empty();
    }
};

// struct ColumnDesc {
//     std::string name;
//     SQLSMALLINT sqlType;
//     SQLULEN columnSize;
//     SQLSMALLINT decimalDigits;
//     SQLSMALLINT nullable;
// };

class Connection {
public:
    Connection() = default;
    ~Connection();

    // Delete copy constructor and assignment operator
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    // Connection string methods
    void setConnectionString(const std::string& dsn, const std::string& uid, const std::string& authStr);
    void setConnectionString(const std::string& dsn);

    // Connection management
    SQLRETURN connect();
    SQLRETURN disconnect();

    // Query methods
    SQLRETURN Query(const std::string& query, Statement* stmt);
    SQLRETURN GetTables(
        const std::string& catalogName,
        const std::string& schemaName,
        const std::string& tableName,
        const std::string& tableType,
        Statement* stmt) const;
    SQLRETURN GetColumns(
        const std::string& catalogName,
        const std::string& schemaName,
        const std::string& tableName,
        const std::string& columnName,
        Statement* stmt) const;

    // Statement management
    bool hasActiveStmts() const;
    void cleanupActiveStmts();

    // Get connection state
    bool isConnected() const { return connected; }
    JNIEnv* env = nullptr;
    [[nodiscard]] const std::string& getConnectionString() const { return connectionString; }

private:
    // Connection state
    bool connected = false;
    std::string connectionString;
    std::vector<Statement*> activeStmts;

    // JVM/JNI state
    JavaVM* jvm = nullptr;
    jclass wrapperClass = nullptr;
    jobject wrapperInstance = nullptr;

    // Initialization helpers
    bool initJVM();
    bool initWrapper(const ConnectionParams& params);
    bool parseConnectionString(const std::string& connStr, ConnectionParams& params);
    std::string buildJdbcUrl(const ConnectionParams& params);

    SQLRETURN populateColumnDescriptors(jobject schemaRoot, Statement *stmt) const;

    // Result set handling
    SQLRETURN executeAndGetArrowResult(
        const char *methodName,
        const std::vector<JniParam> &params,
        Statement *stmt) const;

    // Type mapping helpers
    static SQLSMALLINT mapArrowTypeToSQL(JNIEnv* env, jobject arrowType);
    static SQLULEN getSQLTypeSize(SQLSMALLINT sqlType);
};

// Helper functions declarations
std::string GetModuleDirectory();
std::string WideStringToString(const std::wstring& wstr);