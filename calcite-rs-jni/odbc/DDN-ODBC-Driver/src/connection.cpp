// Windows includes must come first
#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <windows.h>

// ODBC includes
#include <sql.h>

// JNI includes
#include <jni.h>

// Standard includes
#include <string>
#include <memory>
#include <sstream>
#include <cstdio>

// Project includes
#include "../include/connection.hpp"
#include "../include/statement.hpp"
#include "../include/globals.hpp"
#include "../include/logging.hpp"

bool Connection::initJVM() {
    LOG("Initializing JVM");

    JavaVMInitArgs vm_args;
    JavaVMOption options[5];

    // Add the path to your JAR files
    std::string classPath = "-Djava.class.path=";
    classPath += dllPath + "\\jni-arrow-1.0.0-jar-with-dependencies.jar";
    LOGF("Java classpath: %s", classPath.c_str());

    options[0].optionString = const_cast<char*>(classPath.c_str());

    // Add any necessary JVM options
    std::string extraOptions = "-Xmx512m";
    options[1].optionString = const_cast<char*>(extraOptions.c_str());
    options[2].optionString = const_cast<char*>("--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED");
    options[3].optionString = const_cast<char*>("-Dotel.java.global-autoconfigure.enabled=true");
    options[4].optionString = const_cast<char*>("-Dlog_level=debug");

    // Set the JNI version to 1.8 for OpenJDK 11
    vm_args.version = JNI_VERSION_1_8;
    vm_args.nOptions = 2;
    vm_args.options = options;
    vm_args.ignoreUnrecognized = JNI_TRUE;

    if (const jint rc = JNI_CreateJavaVM(&jvm, reinterpret_cast<void **>(&env), &vm_args); rc != JNI_OK) {
        LOGF("Failed to create JVM: %d", rc);
        LOGF("JNI_CreateJavaVM return code: %d", rc);
        return false;
    }

    LOG("JVM created successfully");
    return true;
}

void Connection::setConnectionString(const std::string& dsn, const std::string& uid, const std::string& authStr) {
    // Store connection string
    std::ostringstream ss;
    ss << "DSN=" << dsn << ";UID=" << uid << ";PWD=" << authStr;
    this->connectionString = ss.str();
}

void Connection::setConnectionString(const std::string& dsn) {
    this->connectionString =dsn;
}

bool Connection::initWrapper(const ConnectionParams& params) {
    try {
        // Find the wrapper class
        wrapperClass = env->FindClass("com/hasura/ArrowJdbcWrapper");
        if (!wrapperClass) {
            LOG("Failed to find ArrowJdbcWrapper class");
            return false;
        }
        LOG("Found ArrowJdbcWrapper class");

        // Get the method ID for the constructor
        jmethodID constructor = env->GetMethodID(wrapperClass, "<init>", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");
        if (constructor == nullptr) {
            LOG("Failed to find ArrowJdbcWrapper constructor");
            return false;
        }
        LOG("Found ArrowJdbcWrapper constructor");

        // Get the method ID for healthCheck
        jmethodID healthCheckMethod = env->GetMethodID(wrapperClass, "healthCheck", "()Z");
        if (healthCheckMethod == nullptr) {
            LOG("Failed to find healthCheck method");
            return false;
        }
        LOG("Found healthCheck method");

        // Create Java strings for the constructor arguments
        const std::string jdbcUrl = buildJdbcUrl(params);
        jstring jvm_jdbcUrl = env->NewStringUTF(jdbcUrl.c_str());  // Replace JDBC_URL with your actual URL
        jstring username = env->NewStringUTF(params.uid.c_str()); // Replace USERNAME with your actual username
        jstring password = env->NewStringUTF(params.pwd.c_str()); // Replace PASSWORD with your actual password

        // Create a new instance of ArrowJdbcWrapper
        LOGF("jdbcUrl %s, username %s, password, %s", jdbcUrl.c_str(), params.uid.c_str(), params.pwd.c_str());
        wrapperInstance = env->NewObject(wrapperClass, constructor, jvm_jdbcUrl, username, password);
        if (wrapperInstance == nullptr) {
            LOG("Failed to construct ArrowJdbcWrapper");
            // Check if an exception was thrown during the object creation
            if (env->ExceptionCheck()) {
                env->ExceptionDescribe(); // Print the exception details to stderr
                env->ExceptionClear();    // Clear the exception so that we can continue
                LOG("Failed to construct ArrowJdbcWrapper due to an exception");
                return false;
            }
            return false;
        }
        LOG("Initialized ArrowJdbcWrapper");

        // Call the healthCheck method
        const jboolean healthStatus = env->CallBooleanMethod(wrapperInstance, healthCheckMethod);
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ExceptionClear();
            LOG("Exception occurred while calling healthCheck method");
            return false;
        }
        bool isHealthy = (healthStatus == JNI_TRUE);
        LOGF("Health check status: %d", isHealthy);

        return isHealthy;
    }
    catch (const std::exception& e) {
        LOGF("Exception in initWrapper: %s", e.what());
        return false;
    }
}

SQLRETURN Connection::connect() {
    if (isConnected) {
        LOG("Already connected");
        return SQL_ERROR;
    }

    // Parse connection parameters
    ConnectionParams params;
    if (!parseConnectionString(connectionString, params)) {
        LOG("Failed to parse connection string");
        return SQL_ERROR;
    }

    // Initialize JVM if not already initialized
    if (!jvm && !initJVM()) {
        LOG("Failed to initialize JVM");
        return SQL_ERROR;
    }

    // Initialize wrapper with connection parameters
    if (!initWrapper(params)) {
        LOG("Failed to initialize wrapper");
        return SQL_ERROR;
    }

    isConnected = true;
    LOG("Connected!");
    return SQL_SUCCESS;
}

SQLRETURN Connection::disconnect() {
    if (!isConnected) {
        LOG("Not connected");
        return SQL_ERROR;
    }

    if (env && wrapperInstance) {
        // Call close on the wrapper instance
        jmethodID closeMethod = env->GetMethodID(wrapperClass, "close", "()V");
        env->CallVoidMethod(wrapperInstance, closeMethod);
        
        // Delete global references
        env->DeleteGlobalRef(wrapperInstance);
        env->DeleteGlobalRef(wrapperClass);
        
        wrapperInstance = nullptr;
        wrapperClass = nullptr;
    }

    if (jvm) {
        jvm->DestroyJavaVM();
        jvm = nullptr;
        env = nullptr;
    }

    isConnected = false;
    return SQL_SUCCESS;
}

Connection::~Connection() {
    if (isConnected) {
        disconnect();
    }
}

// Helper functions
std::string GetModuleDirectory(HMODULE hModule) {
    char path[MAX_PATH];
    GetModuleFileNameA(hModule, path, MAX_PATH);
    std::string modulePath(path);
    size_t pos = modulePath.find_last_of("\\/");
    return (std::string::npos == pos) ? "" : modulePath.substr(0, pos);
}

std::string WideStringToString(const std::wstring& wstr) {
    LOG("WideStringToString: Input length: " + std::to_string(wstr.length()));
    
    if (wstr.empty()) {
        LOG("WideStringToString: Empty input string");
        return {};
    }

    // Print first few wide chars as numbers for debugging
    char intbuf[100] = {0};
    char* intptr = intbuf;
    size_t maxChars = wstr.length() > 5 ? 5 : wstr.length();
    for (size_t i = 0; i < maxChars; ++i) {
        intptr += sprintf_s(intptr, 20, "%d ", static_cast<int>(wstr[i]));
    }
    LOG("WideStringToString: First 5 chars (as ints): " + std::string(intbuf));

    // Get required buffer size
    const int size_needed = WideCharToMultiByte(CP_UTF8, 0,
                                        wstr.data(), static_cast<int>(wstr.length()),
                                        nullptr, 0, 
                                        nullptr, nullptr);
                                        
    if (size_needed <= 0) {
        const DWORD error = GetLastError();
        LOG("WideStringToString: Error calculating buffer size. Error code: " + std::to_string(error));
        return std::string();
    }
    
    LOG("WideStringToString: Allocating buffer of size: " + std::to_string(size_needed));
    std::string strTo(size_needed, 0);
    
    int result = WideCharToMultiByte(CP_UTF8, 0,
                                   wstr.data(), static_cast<int>(wstr.length()),
                                   &strTo[0], size_needed,
                                   nullptr, nullptr);
                                   
    if (result <= 0) {
        DWORD error = GetLastError();
        LOG("WideStringToString: Conversion failed. Error code: " + std::to_string(error));
        return std::string();
    }
    
    LOG("WideStringToString: Converted string length: " + std::to_string(strTo.length()));
    LOG("WideStringToString: Converted string: '" + strTo + "'");
    
    return strTo;
}

bool Connection::parseConnectionString(const std::string& connStr, ConnectionParams& params) {
    if (connStr.empty()) {
        LOG("Empty connection string provided");
        return false;
    }

    LOGF("Parsing connection string: %s", connStr.c_str());
    std::istringstream ss(connStr);
    std::string token;

    while (std::getline(ss, token, ';')) {
        auto pos = token.find('=');
        if (pos == std::string::npos) {
            LOGF("Invalid token: %s", token.c_str());
            continue;
        }

        std::string key = token.substr(0, pos);
        std::string value = token.substr(pos + 1);

        while (!key.empty() && key[0] == ' ') key.erase(0, 1);
        while (!key.empty() && key.back() == ' ') key.pop_back();
        while (!value.empty() && value[0] == ' ') value.erase(0, 1);
        while (!value.empty() && value.back() == ' ') value.pop_back();

        LOGF("Parsed key-value pair: %s=%s", key.c_str(), value.c_str());

        if (key == "Server") params.server = value;
        else if (key == "Port") params.port = value;
        else if (key == "Database") params.database = value;
        else if (key == "Role") params.role = value;
        else if (key == "Auth") params.auth = value;
        else if (key == "UID" || key == "User") params.uid = value;
        else if (key == "PWD" || key == "Password") params.pwd = value;
        else if (key == "Encrypt") params.encrypt = value;
        else if (key == "Timeout") params.timeout = value;
        else if (key == "DRIVER") {
            LOGF("Ignoring key: %s", key.c_str());
        }
        else {
            LOGF("Unknown key: %s", key.c_str());
        }
    }

    LOGF("Server: %s, Port: %s, Database: %s",
         params.server.c_str(), params.port.c_str(), params.database.c_str());

    if (!params.isValid()) {
        LOG("Connection parameters are not valid");
        return false;
    }

    return true;
}

bool Connection::hasActiveStmts() const {
    // Return true if there are any active statements
    return !activeStmts.empty();
}

void Connection::cleanupActiveStmts() {
    // Clean up any active statements
    for (auto stmt : activeStmts) {
        if (stmt) {
            stmt->clearResults();
            // You might want to also free the statement here
            // depending on your memory management strategy
        }
    }
    activeStmts.clear();
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

SQLRETURN Connection::GetTables(
    const std::string& catalogName,
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& tableType,
    Statement* stmt) const {

    LOGF("GetTables called with params - catalog: '%s', schema: '%s', table: '%s', types: '%s'",
         catalogName.c_str(), schemaName.c_str(), tableName.c_str(), tableType.c_str());

    if (!isConnected || !env || !wrapperInstance || !wrapperClass) {
        LOG("Connection validation failed:");
        LOGF(" - isConnected: %d", isConnected);
        LOGF(" - env: %p", (void*)env);
        LOGF(" - wrapperInstance: %p", (void*)wrapperInstance);
        LOGF(" - wrapperClass: %p", (void*)wrapperClass);
        return SQL_ERROR;
    }

    try {
        // Helper lambda to convert std::string to jstring
        auto createJavaString = [this](const std::string& str) -> jstring {
            if (str.empty()) {
                LOG("Creating null jstring for empty input");
                return nullptr;
            }
            LOGF("Creating jstring for input: '%s'", str.c_str());
            return env->NewStringUTF(str.c_str());
        };

        // Convert input strings to Java strings
        LOG("Converting input parameters to Java strings");
        jstring jCatalog = createJavaString(catalogName);
        LOGF("Created jCatalog: %p", (void*)jCatalog);

        jstring jSchema = createJavaString(schemaName);
        LOGF("Created jSchema: %p", (void*)jSchema);

        jstring jTable = createJavaString(tableName);
        LOGF("Created jTable: %p", (void*)jTable);

        // Handle table types
        jobjectArray jTypes = nullptr;
        if (!tableType.empty()) {
            LOG("Processing table types string");
            std::vector<std::string> typeList;
            std::istringstream typeStream(tableType);
            std::string type;

            // Parse comma-separated types
            while (std::getline(typeStream, type, ',')) {
                // Trim whitespace
                type.erase(0, type.find_first_not_of(' '));
                type.erase(type.find_last_not_of(' ') + 1);
                if (!type.empty()) {
                    LOGF("Found table type: '%s'", type.c_str());
                    typeList.push_back(type);
                }
            }

            if (!typeList.empty()) {
                LOGF("Creating Java string array with %zu types", typeList.size());
                jclass stringClass = env->FindClass("java/lang/String");
                if (!stringClass) {
                    LOG("Failed to find java.lang.String class");
                    return SQL_ERROR;
                }

                jTypes = env->NewObjectArray(typeList.size(), stringClass, env->NewStringUTF(""));
                if (!jTypes) {
                    LOG("Failed to create Java string array for types");
                    return SQL_ERROR;
                }

                for (size_t i = 0; i < typeList.size(); i++) {
                    LOGF("Setting array element %zu to '%s'", i, typeList[i].c_str());
                    jstring typeStr = env->NewStringUTF(typeList[i].c_str());
                    env->SetObjectArrayElement(jTypes, i, typeStr);
                    env->DeleteLocalRef(typeStr);
                }
                LOGF("Successfully created jTypes array: %p", (void*)jTypes);
            }
        } else {
            LOG("No table types specified");
        }

        // Find getTables method
        LOG("Looking up getTables method");
        jmethodID getTablesMethod = env->GetMethodID(wrapperClass, "getTables",
            "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;[Ljava/lang/String;)Lorg/apache/arrow/vector/VectorSchemaRoot;");

        if (!getTablesMethod) {
            LOG("Failed to find getTables method");
            return SQL_ERROR;
        }
        LOGF("Found getTables method: %p", (void*)getTablesMethod);

        // Call getTables
        LOG("Calling getTables method");
        jobject schemaRoot = env->CallObjectMethod(wrapperInstance, getTablesMethod,
            jCatalog, jSchema, jTable, jTypes);

        // Clean up local references
        LOG("Cleaning up local references");
        if (jCatalog) {
            env->DeleteLocalRef(jCatalog);
            LOG("Deleted jCatalog reference");
        }
        if (jSchema) {
            env->DeleteLocalRef(jSchema);
            LOG("Deleted jSchema reference");
        }
        if (jTable) {
            env->DeleteLocalRef(jTable);
            LOG("Deleted jTable reference");
        }
        if (jTypes) {
            env->DeleteLocalRef(jTypes);
            LOG("Deleted jTypes reference");
        }

        if (env->ExceptionCheck()) {
            LOG("Java exception detected during getTables call");
            env->ExceptionDescribe();
            env->ExceptionClear();
            LOG("Exception cleared");
            return SQL_ERROR;
        }

        if (!schemaRoot) {
            LOG("getTables returned null schemaRoot");
            return SQL_ERROR;
        }
        LOGF("Successfully got schemaRoot: %p", (void*)schemaRoot);

        // Set up the ODBC result set columns in the statement
        LOG("Setting up table result columns in Statement");
        auto tableDefs = stmt->setupTableResultColumns();
        LOG("Successfully set up table result columns");

        // Convert Arrow VectorSchemaRoot to ODBC result set
        LOG("Converting Arrow VectorSchemaRoot to ODBC result set");
        SQLRETURN result = stmt->setArrowResult(schemaRoot, tableDefs);
        LOGF("setArrowResult returned: %d", result);

        // Clean up schema root
        env->DeleteLocalRef(schemaRoot);
        LOG("Cleaned up schemaRoot reference");

        if (result == SQL_SUCCESS) {
            LOG("GetTables completed successfully");
        } else {
            LOG("GetTables completed with errors");
        }
        return result;

    } catch (const std::exception& e) {
        LOGF("Exception in GetTables: %s", e.what());
        LOG("Stack trace (if available):");
        LOG(e.what());
        return SQL_ERROR;
    } catch (...) {
        LOG("Unknown exception in GetTables");
        return SQL_ERROR;
    }
}

SQLRETURN Connection::GetColumns(
    const std::string& catalogName,
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName,
    Statement* stmt) const {

    LOGF("GetColumns called with params - catalog: '%s', schema: '%s', table: '%s', column: '%s'",
         catalogName.c_str(), schemaName.c_str(), tableName.c_str(), columnName.c_str());

    if (!isConnected || !env || !wrapperInstance || !wrapperClass) {
        LOG("Connection validation failed:");
        LOGF(" - isConnected: %d", isConnected);
        LOGF(" - env: %p", (void*)env);
        LOGF(" - wrapperInstance: %p", (void*)wrapperInstance);
        LOGF(" - wrapperClass: %p", (void*)wrapperClass);
        return SQL_ERROR;
    }

    try {
        // Helper lambda to convert std::string to jstring
        auto createJavaString = [this](const std::string& str) -> jstring {
            if (str.empty()) {
                LOG("Creating null jstring for empty input");
                return nullptr;
            }
            LOGF("Creating jstring for input: '%s'", str.c_str());
            return env->NewStringUTF(str.c_str());
        };

        // Convert input strings to Java strings
        LOG("Converting input parameters to Java strings");
        jstring jCatalog = createJavaString(catalogName);
        jstring jSchema = createJavaString(schemaName);
        jstring jTable = createJavaString(tableName);
        jstring jColumn = createJavaString(columnName);

        // Find getColumns method
        LOG("Looking up getColumns method");
        jmethodID getColumnsMethod = env->GetMethodID(wrapperClass, "getColumns",
            "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)Lorg/apache/arrow/vector/VectorSchemaRoot;");

        if (!getColumnsMethod) {
            LOG("Failed to find getColumns method");
            return SQL_ERROR;
        }

        // Call getColumns
        LOG("Calling getColumns method");
        jobject schemaRoot = env->CallObjectMethod(wrapperInstance, getColumnsMethod,
            jCatalog, jSchema, jTable, jColumn);

        // Clean up local references
        LOG("Cleaning up local references");
        if (jCatalog) env->DeleteLocalRef(jCatalog);
        if (jSchema) env->DeleteLocalRef(jSchema);
        if (jTable) env->DeleteLocalRef(jTable);
        if (jColumn) env->DeleteLocalRef(jColumn);

        if (env->ExceptionCheck()) {
            LOG("Java exception detected during getColumns call");
            env->ExceptionDescribe();
            env->ExceptionClear();
            return SQL_ERROR;
        }

        if (!schemaRoot) {
            LOG("getColumns returned null schemaRoot");
            return SQL_ERROR;
        }

        // Set up the ODBC result set columns in the statement
        LOG("Setting up column result columns in Statement");
        auto columnDefs = stmt->setupColumnResultColumns();

        // Convert Arrow VectorSchemaRoot to ODBC result set
        LOG("Converting Arrow VectorSchemaRoot to ODBC result set");
        SQLRETURN result = stmt->setArrowResult(schemaRoot, columnDefs);

        // Clean up schema root
        env->DeleteLocalRef(schemaRoot);

        return result;

    } catch (const std::exception& e) {
        LOGF("Exception in GetColumns: %s", e.what());
        return SQL_ERROR;
    } catch (...) {
        LOG("Unknown exception in GetColumns");
        return SQL_ERROR;
    }
}

