#pragma once
#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN

// ODBC includes
#include <sql.h>

// JNI includes
#include <jni.h>

// Standard includes
#include <Error.hpp>
#include <string>
#include <vector>

#include "JniParam.hpp"
#include "Statement.hpp"
#include "DiagnosticManager.hpp"
#include "Globals.hpp"

// Forward declarations
class Statement;
std::wstring StringToWideString(const std::string &str);

typedef jint (JNICALL *PFN_CreateJavaVM)(JavaVM **, void **, void *);

typedef jint (JNICALL *PFN_GetCreatedJavaVMs)(JavaVM **, jsize, jsize *);

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

    [[nodiscard]] bool isValid() const {
        return !server.empty() && !port.empty() && !database.empty();
    }
};

class Connection {
public:
    Connection() = default;

    ~Connection();

    // Delete copy constructor and assignment operator
    Connection(const Connection &) = delete;

    Connection &operator=(const Connection &) = delete;

    // Connection string methods
    void setConnectionString(const std::string &dsn, const std::string &uid, const std::string &authStr);

    void setConnectionString(const std::string &dsn);

    // Connection management
    SQLRETURN connect();

    SQLRETURN disconnect();

    // Query methods
    static SQLRETURN Query(const std::string &query, Statement *stmt);

    SQLRETURN GetTables(
        const std::string &catalogName,
        const std::string &schemaName,
        const std::string &tableName,
        const std::string &tableType,
        Statement *stmt) const;

    SQLRETURN GetColumns(
        const std::string &catalogName,
        const std::string &schemaName,
        const std::string &tableName,
        const std::string &columnName,
        Statement *stmt) const;

    // Statement management
    [[nodiscard]] bool hasActiveStmts() const;

    void cleanupActiveStmts();

    // Get connection state
    [[nodiscard]] bool isConnected() const { return connected; }
    [[nodiscard]] const std::string &getConnectionString() const { return connectionString; }
    [[nodiscard]] SQLINTEGER getLoginTimeout() const { return loginTimeout; }
    [[nodiscard]] SQLINTEGER getConnectionTimeout() const { return connectionTimeout; }
    void setLoginTimeout(SQLINTEGER value) { loginTimeout = value; }
    void setConnectionTimeout(const SQLINTEGER value) { connectionTimeout = value; }
    void setCurrentCatalog(const std::string &value) { currentCatalog = value; }
    [[nodiscard]] const std::string &getCurrentCatalog() const { return currentCatalog; }
    void setError(const std::string& state, const std::string& msg, SQLINTEGER native) {
        currentError = Error(state, msg, native);
        diagMgr->addDiagnostic(StringToWideString(std::string(state)), native, StringToWideString(std::string(msg)));
    }
    [[nodiscard]] const Error& getLastError() const {
        return currentError;
    }
    void setAutoCommit(const SQLUINTEGER value) { autoCommit = value; }
    [[nodiscard]] SQLUINTEGER getAutoCommit() const { return autoCommit; }

private:
    // Connection state
    bool connected = false;
    std::string connectionString;
    std::vector<Statement *> activeStmts;
    SQLINTEGER loginTimeout = 60;
    SQLINTEGER connectionTimeout = 60;
    SQLUINTEGER autoCommit = true;
    std::string currentCatalog;
    Error currentError{"00000", "", 0};
    static bool parseConnectionString(const std::string &connStr, ConnectionParams &params);
    std::string buildJdbcUrl(const ConnectionParams &params);
};

std::string WideStringToString(const std::wstring &wstr);

