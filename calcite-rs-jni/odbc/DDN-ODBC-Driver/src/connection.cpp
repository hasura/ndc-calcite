#include "../include/connection.hpp"

#include <codecvt>

#include "../include/statement.hpp"
#include "../include/logging.hpp"
#include <sstream>
#include <memory>

#include "JniParam.hpp"


HMODULE GetCurrentModule() {
    HMODULE hModule = nullptr;
    GetModuleHandleEx(
        GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS |
        GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
        reinterpret_cast<LPCTSTR>(GetCurrentModule),
        &hModule);
    return hModule;
}

std::string GetModuleDirectory() {
    char path[MAX_PATH];
    HMODULE hModule = GetCurrentModule();
    GetModuleFileNameA(hModule, path, MAX_PATH);
    std::string modulePath(path);
    size_t pos = modulePath.find_last_of("\\/");
    return (std::string::npos == pos) ? "" : modulePath.substr(0, pos);
}

Connection::~Connection() {
    if (connected) {
        disconnect();
    }
}

// std::string WideStringToString(const std::wstring& wstr) {
//     LOG("WideStringToString: Input length: " + std::to_string(wstr.length()));
//
//     if (wstr.empty()) {
//         LOG("WideStringToString: Empty input string");
//         return {};
//     }
//
//     // Print first few wide chars as numbers for debugging
//     char intbuf[100] = {0};
//     char* intptr = intbuf;
//     size_t maxChars = wstr.length() > 5 ? 5 : wstr.length();
//     for (size_t i = 0; i < maxChars; ++i) {
//         intptr += sprintf_s(intptr, 20, "%d ", static_cast<int>(wstr[i]));
//     }
//     LOG("WideStringToString: First 5 chars (as ints): " + std::string(intbuf));
//
//     // Get required buffer size
//     const int size_needed = WideCharToMultiByte(CP_UTF8, 0,
//                                         wstr.data(), static_cast<int>(wstr.length()),
//                                         nullptr, 0,
//                                         nullptr, nullptr);
//
//     if (size_needed <= 0) {
//         const DWORD error = GetLastError();
//         LOG("WideStringToString: Error calculating buffer size. Error code: " + std::to_string(error));
//         return std::string();
//     }
//
//     LOG("WideStringToString: Allocating buffer of size: " + std::to_string(size_needed));
//     std::string strTo(size_needed, 0);
//
//     int result = WideCharToMultiByte(CP_UTF8, 0,
//                                    wstr.data(), static_cast<int>(wstr.length()),
//                                    &strTo[0], size_needed,
//                                    nullptr, nullptr);
//
//     if (result <= 0) {
//         DWORD error = GetLastError();
//         LOG("WideStringToString: Conversion failed. Error code: " + std::to_string(error));
//         return std::string();
//     }
//
//     LOG("WideStringToString: Converted string length: " + std::to_string(strTo.length()));
//     LOG("WideStringToString: Converted string: '" + strTo + "'");
//
//     return strTo;
// }

#include <cwchar> // For wcstombs
#include <vector>
#include <stdexcept>

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
    this->connectionString = dsn;
}

SQLRETURN Connection::connect() {
    if (connected) {
        LOG("Already connected");
        return SQL_ERROR;
    }

    ConnectionParams params;
    if (!parseConnectionString(connectionString, params)) {
        LOG("Failed to parse connection string");
        return SQL_ERROR;
    }

    if (!jvm && !initJVM()) {
        LOG("Failed to initialize JVM");
        return SQL_ERROR;
    }

    if (!initWrapper(params)) {
        LOG("Failed to initialize wrapper");
        return SQL_ERROR;
    }

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

    if (env && wrapperInstance) {
        jmethodID closeMethod = env->GetMethodID(wrapperClass, "close", "()V");
        env->CallVoidMethod(wrapperInstance, closeMethod);
        
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

    connected = false;
    return SQL_SUCCESS;
}

SQLRETURN Connection::Query(const std::string& query, Statement* stmt) {
    stmt->setOriginalQuery(query);
    std::string interpolatedQuery = stmt->buildInterpolatedQuery();
    return executeAndGetArrowResult("executeQuery", {JniParam(interpolatedQuery)}, stmt);
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

    return executeAndGetArrowResult("getTables", {
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

    return executeAndGetArrowResult("getColumns", {
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

bool Connection::initJVM() {
    LOG("Initializing JVM");

    JavaVMInitArgs vm_args;
    JavaVMOption options[5];

    std::string classPath = "-Djava.class.path=";
    classPath += GetModuleDirectory() + "\\jni-arrow-1.0.0-jar-with-dependencies.jar";
    LOGF("Java classpath: %s", classPath.c_str());

    options[0].optionString = const_cast<char*>(classPath.c_str());
    options[1].optionString = const_cast<char*>("-Xmx512m");
    options[2].optionString = const_cast<char*>("--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED");
    options[3].optionString = const_cast<char*>("-Dotel.java.global-autoconfigure.enabled=true");
    options[4].optionString = const_cast<char*>("-Dlog_level=warn");

    vm_args.version = JNI_VERSION_1_8;
    vm_args.nOptions = 5;
    vm_args.options = options;
    vm_args.ignoreUnrecognized = JNI_TRUE;

    jint rc = JNI_CreateJavaVM(&jvm, reinterpret_cast<void**>(&env), &vm_args);
    if (rc != JNI_OK) {
        LOGF("Failed to create JVM: %d", rc);
        return false;
    }

    LOG("JVM created successfully");
    return true;
}

bool Connection::initWrapper(const ConnectionParams& params) {
    try {
        wrapperClass = env->FindClass("com/hasura/ArrowJdbcWrapper");
        if (!wrapperClass) {
            LOG("Failed to find ArrowJdbcWrapper class");
            return false;
        }

        wrapperClass = reinterpret_cast<jclass>(env->NewGlobalRef(wrapperClass));

        jmethodID constructor = env->GetMethodID(wrapperClass, "<init>", 
            "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");
        if (!constructor) {
            LOG("Failed to find constructor");
            return false;
        }

        const std::string jdbcUrl = buildJdbcUrl(params);
        jstring jvm_jdbcUrl = env->NewStringUTF(jdbcUrl.c_str());
        jstring username = env->NewStringUTF(params.uid.c_str());
        jstring password = env->NewStringUTF(params.pwd.c_str());

        jobject localInstance = env->NewObject(wrapperClass, constructor, jvm_jdbcUrl, username, password);
        if (!localInstance) {
            LOG("Failed to create wrapper instance");
            return false;
        }

        wrapperInstance = env->NewGlobalRef(localInstance);
        env->DeleteLocalRef(localInstance);

        env->DeleteLocalRef(jvm_jdbcUrl);
        env->DeleteLocalRef(username);
        env->DeleteLocalRef(password);

        jmethodID healthCheckMethod = env->GetMethodID(wrapperClass, "healthCheck", "()Z");
        if (!healthCheckMethod) {
            LOG("Failed to find healthCheck method");
            return false;
        }

        jboolean health = env->CallBooleanMethod(wrapperInstance, healthCheckMethod);
        return health == JNI_TRUE;
    }
    catch (const std::exception& e) {
        LOGF("Exception in initWrapper: %s", e.what());
        return false;
    }
}

bool Connection::parseConnectionString(const std::string& connStr, ConnectionParams& params) {
    std::istringstream ss(connStr);
    std::string token;

    while (std::getline(ss, token, ';')) {
        size_t pos = token.find('=');
        if (pos == std::string::npos) continue;

        std::string key = token.substr(0, pos);
        std::string value = token.substr(pos + 1);

        // Trim whitespace
        key.erase(0, key.find_first_not_of(' '));
        key.erase(key.find_last_not_of(' ') + 1);
        value.erase(0, value.find_first_not_of(' '));
        value.erase(value.find_last_not_of(' ') + 1);

        if (key == "Server") params.server = value;
        else if (key == "Port") params.port = value;
        else if (key == "Database") params.database = value;
        else if (key == "Role") params.role = value;
        else if (key == "Auth") params.auth = value;
        else if (key == "UID" || key == "User") params.uid = value;
        else if (key == "PWD" || key == "Password") params.pwd = value;
        else if (key == "Encrypt") params.encrypt = value;
        else if (key == "Timeout") params.timeout = value;
    }

    return params.isValid();
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

SQLRETURN Connection::populateColumnDescriptors(jobject schemaRoot, Statement* stmt) const {
    try {
        // Extract schema and get the number of fields
        jclass rootClass = env->GetObjectClass(schemaRoot);
        jmethodID getSchemaMethod = env->GetMethodID(rootClass, "getSchema",
            "()Lorg/apache/arrow/vector/types/pojo/Schema;");
        jobject schema = env->CallObjectMethod(schemaRoot, getSchemaMethod);

        jclass schemaClass = env->GetObjectClass(schema);
        jmethodID getFieldsMethod = env->GetMethodID(schemaClass, "getFields", "()Ljava/util/List;");
        jobject fieldsList = env->CallObjectMethod(schema, getFieldsMethod);

        jclass listClass = env->GetObjectClass(fieldsList);
        jmethodID sizeMethod = env->GetMethodID(listClass, "size", "()I");
        jint numFields = env->CallIntMethod(fieldsList, sizeMethod);
        LOGF("Schema contains %d fields", numFields);

        // Get the IRD handle
        SQLHDESC hIRD = nullptr;
        SQLRETURN ret = SQLGetStmtAttr(stmt, SQL_ATTR_IMP_ROW_DESC, &hIRD, 0, nullptr);
        if (!SQL_SUCCEEDED(ret) || !hIRD) {
            LOG("Failed to get IRD handle");
            return SQL_ERROR;
        }

        // Set up descriptors for each column
        stmt->resultColumns.resize(numFields);
        for (jint i = 0; i < numFields; i++) {
            jobject field = env->CallObjectMethod(fieldsList, env->GetMethodID(listClass, "get", "(I)Ljava/lang/Object;"), i);
            jclass fieldClass = env->GetObjectClass(field);

            // Get field name
            jmethodID getNameMethod = env->GetMethodID(fieldClass, "getName", "()Ljava/lang/String;");
            jstring fieldName = (jstring)env->CallObjectMethod(field, getNameMethod);
            const char* nameChars = env->GetStringUTFChars(fieldName, nullptr);

            // Get field type
            jmethodID getTypeMethod = env->GetMethodID(fieldClass, "getType",
                "()Lorg/apache/arrow/vector/types/pojo/ArrowType;");
            jobject arrowType = env->CallObjectMethod(field, getTypeMethod);

            SQLSMALLINT sqlType = mapArrowTypeToSQL(env, arrowType);
            SQLULEN columnSize = getSQLTypeSize(sqlType);

            stmt->resultColumns[i].name = _strdup(nameChars);
            stmt->resultColumns[i].nameLength = (SQLSMALLINT)strlen(nameChars);
            stmt->resultColumns[i].nullable = SQL_NULLABLE;
            stmt->resultColumns[i].columnSize = columnSize;
            stmt->resultColumns[i].sqlType = sqlType;

            env->ReleaseStringUTFChars(fieldName, nameChars);
            env->DeleteLocalRef(fieldName);
            env->DeleteLocalRef(arrowType);
            env->DeleteLocalRef(field);
            env->DeleteLocalRef(fieldClass);
        }

        env->DeleteLocalRef(listClass);
        env->DeleteLocalRef(fieldsList);
        env->DeleteLocalRef(schemaClass);
        env->DeleteLocalRef(schema);
        env->DeleteLocalRef(rootClass);

        return SQL_SUCCESS;
    } catch (const std::exception& e) {
        LOGF("Exception in populateColumnDescriptors: %s", e.what());
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ExceptionClear();
        }
        return SQL_ERROR;
    } catch (...) {
        LOG("Unknown exception in populateColumnDescriptors");
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ExceptionClear();
        }
        return SQL_ERROR;
    }
}

SQLRETURN Connection::executeAndGetArrowResult(
    const char* methodName,
    const std::vector<JniParam>& params,
    Statement* stmt) const {

    if (!connected || !env || !wrapperInstance || !wrapperClass) {
        LOG("Connection not properly initialized");
        return SQL_ERROR;
    }

    try {
        // Build method signature
        std::string signature = "(";
        for (const auto& param : params) {
            signature += param.getSignature();
        }
        signature += ")Lorg/apache/arrow/vector/VectorSchemaRoot;";

        LOGF("Looking for method %s with signature %s", methodName, signature.c_str());

        jmethodID method = env->GetMethodID(wrapperClass, methodName, signature.c_str());
        if (!method) {
            LOGF("Failed to find method: %s with signature: %s", methodName, signature.c_str());
            return SQL_ERROR;
        }

        // Convert parameters to JNI values
        std::vector<jvalue> jniValues;
        for (const auto& param : params) {
            jniValues.push_back(param.toJValue(env));
        }

        // Call the method
        jobject schemaRoot;
        if (params.empty()) {
            schemaRoot = env->CallObjectMethod(wrapperInstance, method);
        } else {
            schemaRoot = env->CallObjectMethodA(wrapperInstance, method, jniValues.data());
        }

        // Clean up parameters
        for (size_t i = 0; i < params.size(); i++) {
            params[i].cleanup(env, jniValues[i]);
        }

        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ExceptionClear();
            return SQL_ERROR;
        }

        if (!schemaRoot) {
            LOG("Method returned null result");
            return SQL_ERROR;
        }

        // Extract schema and create column descriptors
        SQLRETURN ret = populateColumnDescriptors(schemaRoot, stmt);
        if (!SQL_SUCCEEDED(ret)) {
            return ret;
        }

        // Process the Arrow data
        ret = stmt->setArrowResult(schemaRoot, stmt->resultColumns);
        jclass schemaRootClass = env->GetObjectClass(schemaRoot);
        jmethodID closeMethod = env->GetMethodID(schemaRootClass, "close", "()V");
        if (closeMethod != nullptr) {
            env->CallVoidMethod(schemaRoot, closeMethod);
        }
        env->DeleteLocalRef(schemaRoot);
        return ret;
    } catch (const std::exception& e) {
        LOGF("Exception in %s: %s", methodName, e.what());
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ExceptionClear();
        }
        return SQL_ERROR;
    }
}

SQLSMALLINT Connection::mapArrowTypeToSQL(JNIEnv* env, jobject arrowType) {
    jclass typeClass = env->GetObjectClass(arrowType);
    jmethodID getTypeIDMethod = env->GetMethodID(typeClass, "getTypeID",
        "()Lorg/apache/arrow/vector/types/pojo/ArrowType$ArrowTypeID;");
    jobject typeId = env->CallObjectMethod(arrowType, getTypeIDMethod);

    jclass enumClass = env->GetObjectClass(typeId);
    jmethodID nameMethod = env->GetMethodID(enumClass, "name", "()Ljava/lang/String;");
    auto typeName = reinterpret_cast<jstring>(env->CallObjectMethod(typeId, nameMethod));

    const char* typeNameStr = env->GetStringUTFChars(typeName, nullptr);
    SQLSMALLINT sqlType;

    // Map Arrow types to SQL types
    if (strcmp(typeNameStr, "Int") == 0) sqlType = SQL_INTEGER;
    else if (strcmp(typeNameStr, "FloatingPoint") == 0) sqlType = SQL_DOUBLE;
    // else if (strcmp(typeNameStr, "Utf8") == 0) sqlType = SQL_VARCHAR;
    else if (strcmp(typeNameStr, "Bool") == 0) sqlType = SQL_BIT;
    else if (strcmp(typeNameStr, "Date") == 0) sqlType = SQL_TYPE_DATE;
    else if (strcmp(typeNameStr, "Time") == 0) sqlType = SQL_TYPE_TIME;
    else if (strcmp(typeNameStr, "Timestamp") == 0) sqlType = SQL_TYPE_TIMESTAMP;
    else if (strcmp(typeNameStr, "Decimal") == 0) sqlType = SQL_DECIMAL;
    else if (strcmp(typeNameStr, "Binary") == 0) sqlType = SQL_BINARY;
    else sqlType = SQL_VARCHAR; // Default fallback

    env->ReleaseStringUTFChars(typeName, typeNameStr);
    env->DeleteLocalRef(typeName);
    env->DeleteLocalRef(typeId);
    env->DeleteLocalRef(enumClass);
    env->DeleteLocalRef(typeClass);

    return sqlType;
}

SQLULEN Connection::getSQLTypeSize(SQLSMALLINT sqlType) {
    switch (sqlType) {
        case SQL_INTEGER: return sizeof(SQLINTEGER);
        case SQL_SMALLINT: return sizeof(SQLSMALLINT);
        case SQL_BIGINT: return sizeof(SQLBIGINT);
        case SQL_DOUBLE: return sizeof(SQLDOUBLE);
        case SQL_REAL: return sizeof(SQLREAL);
        case SQL_DECIMAL: return 38; // Max precision
        case SQL_BIT: return 1;
        case SQL_TINYINT: return sizeof(SQLSCHAR);
        case SQL_TYPE_DATE: return SQL_DATE_LEN;
        case SQL_TYPE_TIME: return SQL_TIME_LEN;
        case SQL_TYPE_TIMESTAMP: return SQL_TIMESTAMP_LEN;
        case SQL_BINARY:
        case SQL_VARBINARY: return 8000; // Max binary length
        case SQL_VARCHAR:
        case SQL_CHAR: return 8000; // Max string length
        case SQL_WVARCHAR:
        case SQL_WCHAR: return 4000; // Max Unicode string length (in characters)
        default: return 8000; // Default to max string length
    }
}