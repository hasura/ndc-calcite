#pragma once

// Windows includes must come first
#define WIN32_LEAN_AND_MEAN
#include <windows.h>

// SQL includes
#include <sql.h>


// Standard includes
#include <string>


// JNI includes
#include <jni.h>

#include "statement.hpp"

// Forward declarations
class Environment;

class Connection {
private:
    JavaVM* jvm{};
    jobject wrapperInstance{};
    jclass wrapperClass{};
    std::string connectionString;
    bool isConnected{};
    
    struct ConnectionParams {
        std::string server;
        std::string port;
        std::string database;
        std::string role;
        std::string auth;
        std::string uid;
        std::string pwd;
        std::string encrypt;
        std::string timeout;
        
        [[nodiscard]] bool isValid() const {
            return !server.empty() && !port.empty() && !database.empty();
        }
    };

    bool initJVM();
    bool initWrapper(const ConnectionParams& params);

    static bool parseConnectionString(const std::string& connStr, ConnectionParams& params);

    static std::string buildJdbcUrl(const ConnectionParams &params);
    std::vector<Statement*> activeStmts;

public:
    bool hasActiveStmts() const;

    void cleanupActiveStmts();

    SQLRETURN GetTables(const std::string &catalogName, const std::string &schemaName, const std::string &tableName,
                        const std::string &tableType, Statement *stmt) const;

    SQLRETURN GetColumns(const std::string &catalogName, const std::string &schemaName, const std::string &tableName,
                         const std::string &columnName, Statement *stmt) const;

    Connection() = default;
    ~Connection();

    SQLRETURN connect();

    void setConnectionString(const std::string &dsn, const std::string &uid, const std::string &authStr);
    void setConnectionString(const std::string &dsn);




    // Delete copy constructor and assignment operator
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    
    SQLRETURN disconnect();
    JNIEnv* env{};

    // Getters
    [[nodiscard]] JavaVM* getJVM() const { return jvm; }
    [[nodiscard]] jobject getWrapperInstance() const { return wrapperInstance; }
    [[nodiscard]] jclass getWrapperClass() const { return wrapperClass; }
    [[nodiscard]] const std::string& getConnectionString() const { return connectionString; }
};

// Helper functions declarations
std::string GetModuleDirectory(HMODULE hModule);
std::string WideStringToString(const std::wstring& wstr);