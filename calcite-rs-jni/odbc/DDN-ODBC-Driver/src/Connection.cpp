#include "../include/Connection.hpp"
#include <codecvt>
#include "../include/Statement.hpp"
#include "../include/Logging.hpp"
#include <sstream>
#include <memory>
#include "../include/JniParam.hpp"
#include <cwchar>
#include <Error.hpp>
#include <future>
#include <JVMSingleton.hpp>
#include <map>
#include <vector>
#include <stdexcept>

Connection::~Connection() {
    if (connected) {
        disconnect();
    }
}

// Function to convert std::wstring to std::string
std::string WideStringToString(const std::wstring& wstr) {
    // Determine the required buffer size for the multibyte string
    size_t len = wcstombs(nullptr, wstr.c_str(), 0);
    if (len == static_cast<size_t>(-1)) {
        throw std::runtime_error("Conversion error");
    }

    // Create a buffer of the appropriate size
    std::vector<char> buffer(len + 1);
    wcstombs(buffer.data(), wstr.c_str(), buffer.size());

    // Convert the buffer to std::string
    return std::string(buffer.data());
}

// Function to convert std::string to std::wstring
std::wstring StringToWideString(const std::string& str) {
    // Determine the required buffer size for the wide character string
    size_t len = mbstowcs(nullptr, str.c_str(), 0);
    if (len == static_cast<size_t>(-1)) {
        throw std::runtime_error("Conversion error");
    }

    // Create a buffer of the appropriate size
    std::vector<wchar_t> buffer(len + 1);
    mbstowcs(buffer.data(), str.c_str(), buffer.size());

    // Convert the buffer to std::wstring
    return std::wstring(buffer.data());
}

void Connection::setConnectionString(const std::string& dsn, const std::string& uid, const std::string& authStr) {
    std::ostringstream ss;
    ss << "DSN=" << dsn << ";UID=" << uid << ";PWD=" << authStr;
    this->connectionString = ss.str();
}

void Connection::setConnectionString(const std::string& dsn) {
    LOG("Setting connection string.");
    this->connectionString = dsn;
}

SQLRETURN Connection::connect() {
    if (connected) {
        LOG("Already connected");
        return SQL_ERROR;
    }

    LOG("Connecting...");
    LOGF("Connection string: %s", connectionString.c_str());

    ConnectionParams params;
    if (!parseConnectionString(connectionString, params)) {
        LOG("Failed to parse connection string");
        return SQL_ERROR;
    }
    const std::string jdbcUrl = buildJdbcUrl(params);
    JVMSingleton::setConnection(jdbcUrl, params.uid, params.pwd);

    connected = true;
    LOG("Connected successfully");
    return SQL_SUCCESS;
}

SQLRETURN Connection::disconnect() {

    if (!connected) {
        LOG("Not connected");
        return SQL_ERROR;
    }

    cleanupActiveStmts();
    connected = false;
    return SQL_SUCCESS;
}

SQLRETURN Connection::Query(const std::string& query, Statement* stmt) {
    stmt->setOriginalQuery(query);
    std::string interpolatedQuery = stmt->buildInterpolatedQuery();
    return JVMSingleton::executeAndGetArrowResult("executeQuery", {JniParam(interpolatedQuery)}, stmt);
}

SQLRETURN Connection::GetTables(
    const std::string& catalogName,
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& tableType,
    Statement* stmt) const {

    std::vector<std::string> types;
    if (!tableType.empty()) {
        std::istringstream ss(tableType);
        std::string type;
        while (std::getline(ss, type, ',')) {
            type.erase(0, type.find_first_not_of(' '));
            type.erase(type.find_last_not_of(' ') + 1);
            if (!type.empty()) {
                types.push_back(type);
            }
        }
    }

    return JVMSingleton::executeAndGetArrowResult("getTables", {
                                        JniParam(catalogName),
                                        JniParam(schemaName),
                                        JniParam(tableName),
                                        JniParam(types)  // This will be converted to String[]
                                    }, stmt);
}

SQLRETURN Connection::GetColumns(
    const std::string& catalogName,
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName,
    Statement* stmt) const {

    return JVMSingleton::executeAndGetArrowResult("getColumns", {
                                        JniParam(catalogName),
                                        JniParam(schemaName),
                                        JniParam(tableName),
                                        JniParam(columnName)
                                    }, stmt);
}

bool Connection::hasActiveStmts() const {
    return !activeStmts.empty();
}

void Connection::cleanupActiveStmts() {
    for (auto stmt : activeStmts) {
        if (stmt) {
            stmt->clearResults();
        }
    }
    activeStmts.clear();
}

bool Connection::parseConnectionString(const std::string& connStr, ConnectionParams& params) {
    LOG("Starting to parse connection string");
    std::map<std::string, std::string> connParams;

    // Parse the connection string into a map first
    std::istringstream ss(connStr);
    std::string token;

    while (std::getline(ss, token, ';')) {
        if (token.empty()) continue;
        LOGF("Processing token: %s", token.c_str());

        size_t pos = token.find('=');
        if (pos == std::string::npos) {
            LOGF("Skipping malformed token (no '='): %s", token.c_str());
            continue;
        }

        std::string key = token.substr(0, pos);
        std::string value = token.substr(pos + 1);

        // Trim whitespace
        key.erase(0, key.find_first_not_of(' '));
        key.erase(key.find_last_not_of(' ') + 1);
        value.erase(0, value.find_first_not_of(' '));
        value.erase(value.find_last_not_of(' ') + 1);

        LOGF("Parsed key='%s' value='%s'", key.c_str(), value.c_str());
        connParams[key] = value;
    }

    // If DSN is present, read registry values
    if (connParams.find("DSN") != connParams.end()) {
        const std::string& dsn = connParams["DSN"];
        LOGF("Found DSN: %s", dsn.c_str());

        HKEY hKey;
        std::string regPath = "SOFTWARE\\ODBC\\ODBC.INI\\" + dsn;
        LOGF("Attempting to open registry key: %s", regPath.c_str());

        LONG regResult = RegOpenKeyExA(HKEY_LOCAL_MACHINE, regPath.c_str(), 0, KEY_READ, &hKey);
        if (regResult == ERROR_SUCCESS) {
            LOG("Successfully opened registry key");

            char value[1024];
            DWORD valueSize = sizeof(value);
            DWORD type;

            const char* regKeys[] = {"Server", "Port", "Database", "Role", "Auth", "UID", "PWD", "Encrypt", "Timeout"};
            for (const char* key : regKeys) {
                valueSize = sizeof(value);
                LONG queryResult = RegQueryValueExA(hKey, key, nullptr, &type, (LPBYTE)value, &valueSize);
                if (queryResult == ERROR_SUCCESS) {
                    if (strcmp(key, "PWD") != 0) {  // Don't log passwords
                        LOGF("Found registry value for %s: %s", key, value);
                    } else {
                        LOGF("Found registry value for %s: ********", key);
                    }

                    if (connParams.find(key) == connParams.end()) {
                        connParams[key] = value;
                        LOGF("Added registry value for %s", key);
                    } else {
                        LOGF("Skipping registry value for %s (already in connection string)", key);
                    }
                } else {
                    LOGF("Failed to read registry value for %s, error code: %d", key, queryResult);
                }
            }
            RegCloseKey(hKey);
            LOG("Closed registry key");
        } else {
            LOGF("Failed to open registry key, error code: %d", regResult);
            // Try HKEY_CURRENT_USER if HKEY_LOCAL_MACHINE failed
            regPath = "SOFTWARE\\ODBC\\ODBC.INI\\" + dsn;
            LOGF("Attempting to open registry key in HKCU: %s", regPath.c_str());

            regResult = RegOpenKeyExA(HKEY_CURRENT_USER, regPath.c_str(), 0, KEY_READ, &hKey);
            if (regResult == ERROR_SUCCESS) {
                LOG("Successfully opened registry key in HKCU");
                // Repeat the registry reading code for HKCU
                char value[1024];
                DWORD valueSize = sizeof(value);
                DWORD type;

                const char* regKeys[] = {"Server", "Port", "Database", "Role", "Auth", "UID", "PWD", "Encrypt", "Timeout"};
                for (const char* key : regKeys) {
                    valueSize = sizeof(value);
                    LONG queryResult = RegQueryValueExA(hKey, key, nullptr, &type, (LPBYTE)value, &valueSize);
                    if (queryResult == ERROR_SUCCESS) {
                        if (strcmp(key, "PWD") != 0) {
                            LOGF("Found registry value for %s: %s", key, value);
                        } else {
                            LOGF("Found registry value for %s: ********", key);
                        }

                        if (connParams.find(key) == connParams.end()) {
                            connParams[key] = value;
                            LOGF("Added registry value for %s", key);
                        } else {
                            LOGF("Skipping registry value for %s (already in connection string)", key);
                        }
                    } else {
                        LOGF("Failed to read registry value for %s, error code: %d", key, queryResult);
                    }
                }
                RegCloseKey(hKey);
                LOG("Closed HKCU registry key");
            } else {
                LOGF("Failed to open registry key in HKCU, error code: %d", regResult);
            }
        }
    }

    // Now populate the params object
    LOG("Populating final parameters");
    if (connParams.find("Server") != connParams.end()) {
        params.server = connParams["Server"];
        LOGF("Set server=%s", params.server.c_str());
    }
    if (connParams.find("Port") != connParams.end()) {
        params.port = connParams["Port"];
        LOGF("Set port=%s", params.port.c_str());
    }
    if (connParams.find("Database") != connParams.end()) {
        params.database = connParams["Database"];
        LOGF("Set database=%s", params.database.c_str());
    }
    if (connParams.find("Role") != connParams.end()) {
        params.role = connParams["Role"];
        LOGF("Set role=%s", params.role.c_str());
    }
    if (connParams.find("Auth") != connParams.end()) {
        params.auth = connParams["Auth"];
        LOGF("Set auth=%s", params.auth.c_str());
    }
    if (connParams.find("UID") != connParams.end()) {
        params.uid = connParams["UID"];
        LOGF("Set uid from UID=%s", params.uid.c_str());
    }
    if (connParams.find("User") != connParams.end() && params.uid.empty()) {
        params.uid = connParams["User"];
        LOGF("Set uid from User=%s", params.uid.c_str());
    }
    if (connParams.find("PWD") != connParams.end()) {
        params.pwd = connParams["PWD"];
        LOG("Set pwd from PWD=********");
    }
    if (connParams.find("Password") != connParams.end() && params.pwd.empty()) {
        params.pwd = connParams["Password"];
        LOG("Set pwd from Password=********");
    }
    if (connParams.find("Encrypt") != connParams.end()) {
        params.encrypt = connParams["Encrypt"];
        LOGF("Set encrypt=%s", params.encrypt.c_str());
    }
    if (connParams.find("Timeout") != connParams.end()) {
        params.timeout = connParams["Timeout"];
        LOGF("Set timeout=%s", params.timeout.c_str());
    }

    bool isValid = params.isValid();
    LOGF("Params validation result: %s", isValid ? "valid" : "invalid");
    return isValid;
}

std::string Connection::buildJdbcUrl(const ConnectionParams& params) {
    std::string protocol = (params.encrypt == "yes") ? "https" : "http";
    
    std::ostringstream url;
    url << "jdbc:graphql:" << protocol << "://"
        << params.server << ":" << params.port << "/"
        << params.database;

    bool hasParam = false;
    if (!params.role.empty()) {
        url << "?role=" << params.role;
        hasParam = true;
    }

    if (!params.pwd.empty()) {
        url << (hasParam ? "&" : "?") << "password=" << params.pwd;
        hasParam = true;
    }

    if (!params.uid.empty()) {
        url << (hasParam ? "&" : "?") << "user=" << params.uid;
    }

    return url.str();
}
