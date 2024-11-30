#pragma once
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
#include <vector>
#include <stdexcept>

class JVMSingleton {
private:
    static JavaVM* jvm;
    static JNIEnv* env;
    static jobject wrapper;
    static jclass wrapperClass;
    static bool initialized;
    static std::mutex initMutex;

    JVMSingleton();
    static void initializeJVM();
    static SQLRETURN populateColumnDescriptors(jobject schemaRoot, Statement* stmt);
    static SQLSMALLINT mapArrowTypeToSQL(jobject arrowType);
    static SQLULEN getSQLTypeSize(SQLSMALLINT sqlType);
    static std::string getStringMetadata(jobject metadata, jmethodID getMethod, const char* key);
    static SQLSMALLINT getBoolMetadata(jobject metadata, jmethodID getMethod, const char* key);
    static SQLINTEGER getIntMetadata(jobject metadata, jmethodID getMethod, const char* key);

public:
    // Base access methods
    static JNIEnv* getEnv();
    static jobject getWrapper();

    static const char *getTypeNameFromSQLType(SQLSMALLINT sqlType);

    static jclass getWrapperClass();

    // Proxied wrapper methods
    static void executeQuery(const std::string& query);
    static void getTables();
    static void getColumns(const std::string& tableName);
    static void close();
    static void setConnection(const std::string& jdbcUrl, const std::string& username, const std::string& password);
    static SQLRETURN executeAndGetArrowResult(
        const char* methodName,
        const std::vector<JniParam>& params,
        Statement* stmt);
};

typedef jint (JNICALL *JNI_CreateJavaVM_t)(JavaVM **pvm, void **penv, void *args);
typedef jint (JNICALL *JNI_GetCreatedJavaVMs_t)(JavaVM **vmBuf, jsize bufLen, jsize *nVMs);
std::string GetModuleDirectory();
