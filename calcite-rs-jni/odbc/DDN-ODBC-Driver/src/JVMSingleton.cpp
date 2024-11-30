#include "JVMSingleton.hpp"

#include <map>

#include "Logging.hpp"

JavaVM* JVMSingleton::jvm = nullptr;
JNIEnv* JVMSingleton::env = nullptr;
jobject JVMSingleton::wrapper = nullptr;
jclass JVMSingleton::wrapperClass = nullptr;
bool JVMSingleton::initialized = false;
std::mutex JVMSingleton::initMutex;

JNIEnv* JVMSingleton::getEnv() {
    std::lock_guard<std::mutex> lock(initMutex);
    if (!initialized) initializeJVM();
    return env;
}

jobject JVMSingleton::getWrapper() {
    std::lock_guard<std::mutex> lock(initMutex);
    if (!initialized) initializeJVM();
    return wrapper;
}

std::string JVMSingleton::getStringMetadata(jobject metadata, jmethodID getMethod, const char* key) {
    jstring keyString = getEnv()->NewStringUTF(key);
    auto value = (jstring)getEnv()->CallObjectMethod(metadata, getMethod, keyString);
    std::string result;

    if (value) {
        const char* chars = getEnv()->GetStringUTFChars(value, nullptr);
        result = chars;
        getEnv()->ReleaseStringUTFChars(value, chars);
        getEnv()->DeleteLocalRef(value);
    }

    getEnv()->DeleteLocalRef(keyString);
    return result;
}

SQLSMALLINT JVMSingleton::getBoolMetadata(jobject metadata, jmethodID getMethod, const char* key) {
    return getStringMetadata(metadata, getMethod, key) == "true" ? SQL_TRUE : SQL_FALSE;
}

SQLINTEGER JVMSingleton::getIntMetadata(jobject metadata, jmethodID getMethod, const char* key) {
    std::string value = getStringMetadata(metadata, getMethod, key);
    return value.empty() ? 0 : std::stoi(value);
}

SQLSMALLINT mapSQLType(SQLINTEGER sourceType) {
    LOGF("Mapping SQL type: %d", sourceType);

    switch (sourceType) {
        // Numeric types
        case -7:      // BIT
        case 16:      // BOOLEAN (Java SQL type)
            return SQL_BIT;
        case 2:       // NUMERIC
        case 3:       // DECIMAL
            return SQL_DECIMAL;
        case 4:       // INTEGER
            return SQL_INTEGER;
        case 5:       // SMALLINT
            return SQL_SMALLINT;
        case 6:       // FLOAT
            return SQL_FLOAT;
        case 7:       // REAL
            return SQL_REAL;
        case 8:       // DOUBLE
            return SQL_DOUBLE;
        case -5:      // BIGINT
            return SQL_BIGINT;
        case -6:      // TINYINT
            return SQL_TINYINT;

        // Character types
        case 1:       // CHAR
            return SQL_CHAR;
        case 12:      // VARCHAR
        case 2000:    // SQL Server VARCHAR
            return SQL_VARCHAR;
        case -1:      // LONGVARCHAR
            return SQL_LONGVARCHAR;
        case -9:      // NVARCHAR
        case 2001:    // SQL Server NVARCHAR
            return SQL_WVARCHAR;
        case -8:      // NCHAR
            return SQL_WCHAR;
        case -10:     // NTEXT/LONGNVARCHAR
            return SQL_WLONGVARCHAR;
        case 2005:    // CLOB
            return SQL_LONGVARCHAR;
        case 2011:    // NCLOB
            return SQL_WLONGVARCHAR;

        // Binary types
        case -2:      // BINARY
        case -3:      // VARBINARY
        case -4:      // LONGVARBINARY
            return SQL_LONGVARBINARY;
        case 2004:    // BLOB
            return SQL_LONGVARBINARY;

        // Date/Time types
        case 91:      // DATE
            return SQL_TYPE_DATE;
        case 92:      // TIME
            return SQL_TYPE_TIME;
        case 2013:    // TIME_WITH_TIMEZONE
            return SQL_TYPE_TIME;
        case 93:      // TIMESTAMP
            return SQL_TYPE_TIMESTAMP;
        case 2014:    // TIMESTAMP_WITH_TIMEZONE
            return SQL_TYPE_TIMESTAMP;

        // Special types
        case 0:       // NULL
            return SQL_NULL_DATA;
        case -11:     // GUID
            return SQL_GUID;
        case 2009:    // SQL_XML
            return SQL_WLONGVARCHAR;  // XML as wide character string

        // Array/Structured types
        case 2002:    // SQL Server TABLE
        case 2003:    // ARRAY
            LOGF("Converting structured type %d to VARCHAR", sourceType);
            return SQL_VARCHAR;  // Convert structured types to string representation

        default:
            LOGF("Unmapped SQL type: %d - treating as VARCHAR. Please report if this type is needed.", sourceType);
            return SQL_VARCHAR;
    }
}

void setDisplayAndColumnSizes(ColumnDesc& column, const std::map<std::string, std::string>& metadataMap) {
    // Set display size from metadata if available
    auto it = metadataMap.find("ColumnDisplaySize");
    if (it != metadataMap.end() && !it->second.empty() && it->second != "-1" && it->second != "null") {
        column.displaySize = std::stoi(it->second);
        LOGF("Using metadata display size: %d", column.displaySize);
    } else {
        // Calculate display size based on SQL type
        switch (column.sqlType) {
            case SQL_CHAR:
            case SQL_VARCHAR:
            case SQL_WCHAR:
            case SQL_WVARCHAR:
                column.displaySize = 255;  // Default for string types
                break;

            case SQL_INTEGER:
                column.displaySize = 11;   // -2147483648
                break;

            case SQL_SMALLINT:
                column.displaySize = 6;    // -32768
                break;

            case SQL_BIGINT:
                column.displaySize = 20;   // -9223372036854775808
                break;

            case SQL_DECIMAL:
            case SQL_NUMERIC:
                column.displaySize = column.precision + 2;  // Add sign and decimal point
                break;

            case SQL_REAL:
                column.displaySize = 14;   // -3.4E+38
                break;

            case SQL_FLOAT:
            case SQL_DOUBLE:
                column.displaySize = 24;   // -1.79E+308
                break;

            case SQL_BIT:
                column.displaySize = 1;    // 0 or 1
                break;

            case SQL_TINYINT:
                column.displaySize = 4;    // -128
                break;

            case SQL_BINARY:
            case SQL_VARBINARY:
                column.displaySize = column.columnSize * 2;  // Hex display needs 2 chars per byte
                break;

            case SQL_TYPE_DATE:
                column.displaySize = 10;   // YYYY-MM-DD
                break;

            case SQL_TYPE_TIME:
                column.displaySize = 8;    // HH:MM:SS
                break;

            case SQL_TYPE_TIMESTAMP:
                column.displaySize = 23;   // YYYY-MM-DD HH:MM:SS.SSS
                break;

            default:
                column.displaySize = 255;  // Default fallback
                break;
        }
        LOGF("Using calculated display size: %d for SQL type %d", column.displaySize, column.sqlType);
    }

    // Set column size based on SQL type
    switch (column.sqlType) {
        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_WCHAR:
        case SQL_WVARCHAR: {
            auto pair = metadataMap.find("ColumnSize");
            if (pair != metadataMap.end() && !pair->second.empty()) {
                column.columnSize = std::stoi(pair->second);
            } else {
                column.columnSize = 255;
            }

        }
            break;

        case SQL_INTEGER:
            column.columnSize = 4;
            break;

        case SQL_SMALLINT:
            column.columnSize = 2;
            break;

        case SQL_BIGINT:
            column.columnSize = 8;
            break;

        case SQL_DECIMAL:
        case SQL_NUMERIC:
            column.columnSize = column.precision;
            break;

        case SQL_REAL:
            column.columnSize = 4;
            break;

        case SQL_FLOAT:
        case SQL_DOUBLE:
            column.columnSize = 8;
            break;

        case SQL_BIT:
            column.columnSize = 1;
            break;

        case SQL_TINYINT:
            column.columnSize = 1;
            break;

        case SQL_BINARY:
        case SQL_VARBINARY: {
            auto pair = metadataMap.find("ColumnSize");
            if (pair != metadataMap.end() && !pair->second.empty()) {
                column.columnSize = std::stoi(pair->second);
            } else {
                column.columnSize = 255;
            }
        }
            break;

        case SQL_TYPE_DATE:
            column.columnSize = sizeof(SQL_DATE_STRUCT);
            break;

        case SQL_TYPE_TIME:
            column.columnSize = sizeof(SQL_TIME_STRUCT);
            break;

        case SQL_TYPE_TIMESTAMP:
            column.columnSize = sizeof(SQL_TIMESTAMP_STRUCT);
            break;

        default:
            column.columnSize = 255;  // Default fallback
            break;
    }

    LOGF("Final sizes for SQL type %d - Display: %d, Column: %d",
         column.sqlType, column.displaySize, column.columnSize);
}

void logClassDetails(JNIEnv* env, jobject classObject) {
    // Get class object to use for reflection
    jclass classClass = env->FindClass("java/lang/Class");

    // Get class name
    jmethodID getNameMethod = env->GetMethodID(classClass, "getName", "()Ljava/lang/String;");
    if (getNameMethod) {
        auto className = (jstring)env->CallObjectMethod(classObject, getNameMethod);
        const char* nameStr = env->GetStringUTFChars(className, nullptr);
        LOGF("Class name: %s", nameStr);
        env->ReleaseStringUTFChars(className, nameStr);
        env->DeleteLocalRef(className);
    } else {
        LOG("ERROR: Could not get class name");
    }

    // Get all methods
    jmethodID getMethodsMethod = env->GetMethodID(classClass, "getMethods", "()[Ljava/lang/reflect/Method;");
    if (getMethodsMethod) {
        auto methods = (jobjectArray)env->CallObjectMethod(classObject, getMethodsMethod);
        if (methods) {
            jsize methodCount = env->GetArrayLength(methods);
            LOGF("Number of methods: %d", methodCount);

            // Get Method class for reflection
            jclass methodClass = env->FindClass("java/lang/reflect/Method");
            jmethodID getMethodNameMethod = env->GetMethodID(methodClass, "getName", "()Ljava/lang/String;");
            jmethodID getMethodReturnTypeMethod = env->GetMethodID(methodClass, "getReturnType", "()Ljava/lang/Class;");

            // Log each method
            for (jsize i = 0; i < methodCount; i++) {
                jobject method = env->GetObjectArrayElement(methods, i);

                // Get method name
                auto methodName = (jstring)env->CallObjectMethod(method, getMethodNameMethod);
                const char* methodNameStr = env->GetStringUTFChars(methodName, nullptr);

                // Get return type
                jobject returnTypeClass = env->CallObjectMethod(method, getMethodReturnTypeMethod);
                auto returnTypeName = (jstring)env->CallObjectMethod(returnTypeClass, getNameMethod);
                const char* returnTypeStr = env->GetStringUTFChars(returnTypeName, nullptr);

                LOGF("Method %d: %s returns %s", i, methodNameStr, returnTypeStr);

                env->ReleaseStringUTFChars(methodName, methodNameStr);
                env->ReleaseStringUTFChars(returnTypeName, returnTypeStr);
                env->DeleteLocalRef(methodName);
                env->DeleteLocalRef(returnTypeName);
                env->DeleteLocalRef(returnTypeClass);
                env->DeleteLocalRef(method);
            }

            env->DeleteLocalRef(methodClass);
        }
        env->DeleteLocalRef(methods);
    } else {
        LOG("ERROR: Could not get methods");
    }

    // Clean up
    env->DeleteLocalRef(classClass);
}

void logMapContents(JNIEnv* env, jobject map) {
    LOG("Inspecting Map contents:");
    if (!map) {
        LOG("ERROR: Map object is null");
        return;
    }

    // Get the Map's entrySet
    jclass mapClass = env->GetObjectClass(map);
    jmethodID entrySetMethod = env->GetMethodID(mapClass, "entrySet", "()Ljava/util/Set;");
    jobject entrySet = env->CallObjectMethod(map, entrySetMethod);

    // Convert entrySet to Array for easier iteration
    jclass setClass = env->GetObjectClass(entrySet);
    jmethodID toArrayMethod = env->GetMethodID(setClass, "toArray", "()[Ljava/lang/Object;");
    auto entries = (jobjectArray)env->CallObjectMethod(entrySet, toArrayMethod);

    if (entries) {
        jsize size = env->GetArrayLength(entries);
        LOGF("Map contains %d entries", size);

        // Get Map.Entry methods
        jclass entryClass = env->FindClass("java/util/Map$Entry");
        jmethodID getKeyMethod = env->GetMethodID(entryClass, "getKey", "()Ljava/lang/Object;");
        jmethodID getValueMethod = env->GetMethodID(entryClass, "getValue", "()Ljava/lang/Object;");

        // Get toString method for Object class
        jclass objectClass = env->FindClass("java/lang/Object");
        jmethodID toStringMethod = env->GetMethodID(objectClass, "toString", "()Ljava/lang/String;");

        for (jsize i = 0; i < size; i++) {
            jobject entry = env->GetObjectArrayElement(entries, i);

            // Get key and convert to string
            jobject key = env->CallObjectMethod(entry, getKeyMethod);
            auto keyString = (jstring)env->CallObjectMethod(key, toStringMethod);
            const char* keyChars = env->GetStringUTFChars(keyString, nullptr);

            // Get value and convert to string
            jobject value = env->CallObjectMethod(entry, getValueMethod);
            auto valueString = (jstring)env->CallObjectMethod(value, toStringMethod);
            const char* valueChars = env->GetStringUTFChars(valueString, nullptr);

            LOGF("Key: '%s' = Value: '%s'", keyChars, valueChars);

            // Cleanup
            env->ReleaseStringUTFChars(keyString, keyChars);
            env->ReleaseStringUTFChars(valueString, valueChars);
            env->DeleteLocalRef(keyString);
            env->DeleteLocalRef(valueString);
            env->DeleteLocalRef(key);
            env->DeleteLocalRef(value);
            env->DeleteLocalRef(entry);
        }

        // Cleanup classes
        env->DeleteLocalRef(objectClass);
        env->DeleteLocalRef(entryClass);
    } else {
        LOG("Map entries array is null");
    }

    // Cleanup
    env->DeleteLocalRef(entries);
    env->DeleteLocalRef(setClass);
    env->DeleteLocalRef(entrySet);
    env->DeleteLocalRef(mapClass);
}

SQLRETURN JVMSingleton::populateColumnDescriptors(jobject schemaRoot, Statement* stmt) {
    LOG("Entering populateColumnDescriptors");
    try {
        if (!schemaRoot || !stmt) {
            LOG("ERROR: Invalid input parameters");
            return SQL_ERROR;
        }

        // Get Schema from root
        auto rootClass = getEnv()->GetObjectClass(schemaRoot);
        auto getSchemaMethod = getEnv()->GetMethodID(rootClass, "getSchema",
            "()Lorg/apache/arrow/vector/types/pojo/Schema;");
        auto schema = getEnv()->CallObjectMethod(schemaRoot, getSchemaMethod);
        if (!schema) {
            LOG("ERROR: Failed to get schema from root");
            return SQL_ERROR;
        }

        // Get fields list
        auto schemaClass = getEnv()->GetObjectClass(schema);
        auto getFieldsMethod = getEnv()->GetMethodID(schemaClass, "getFields", "()Ljava/util/List;");
        auto fieldsList = getEnv()->CallObjectMethod(schema, getFieldsMethod);
        if (!fieldsList) {
            LOG("ERROR: Failed to get fields list");
            return SQL_ERROR;
        }

        // Get field count
        auto listClass = getEnv()->GetObjectClass(fieldsList);
        auto sizeMethod = getEnv()->GetMethodID(listClass, "size", "()I");
        auto numFields = getEnv()->CallIntMethod(fieldsList, sizeMethod);
        LOGF("Processing %d fields", numFields);

        // Get IRD handle and resize result columns
        SQLHDESC hIRD = nullptr;
        SQLRETURN ret = SQLGetStmtAttr(stmt, SQL_ATTR_IMP_ROW_DESC, &hIRD, 0, nullptr);
        if (!SQL_SUCCEEDED(ret) || !hIRD) {
            LOG("ERROR: Failed to get IRD handle");
            return SQL_ERROR;
        }
        stmt->resultColumns.resize(numFields);

        // Find Field class and get the metadata helper method
        auto fieldClass = getEnv()->FindClass("org/apache/arrow/vector/types/pojo/Field");
        auto getMetadataArrayMethod = getEnv()->GetStaticMethodID(wrapperClass, "getMetadataFromField",
            "(Lorg/apache/arrow/vector/types/pojo/Field;)[[Ljava/lang/String;");
        if (!getMetadataArrayMethod) {
            LOG("ERROR: Failed to get getMetadataFromField method");
            return SQL_ERROR;
        }

        // Cache commonly used methods
        auto listGetMethod = getEnv()->GetMethodID(listClass, "get", "(I)Ljava/lang/Object;");
        auto getNameMethod = getEnv()->GetMethodID(fieldClass, "getName", "()Ljava/lang/String;");

        // Define helper for string assignment
        auto assignColumnString = [](const std::string& value, const char*& target, SQLSMALLINT& length, const char* fieldName) {
            LOGF("Processing %s: '%s'", fieldName, value.c_str());
            if (value.empty() || value == "null") {
                target = nullptr;
                length = 0;
                LOGF("%s set to null", fieldName);
            } else {
                target = _strdup(value.c_str());
                length = target ? static_cast<SQLSMALLINT>(strlen(target)) : 0;
                LOGF("%s set to '%s' with length %d", fieldName, target, length);
            }
        };

        for (jint i = 0; i < numFields; i++) {
            LOGF("Processing field %d/%d", i + 1, numFields);

            // Get field object
            auto field = getEnv()->CallObjectMethod(fieldsList, listGetMethod, i);
            if (!field) {
                LOGF("ERROR: Failed to get field at index %d", i);
                return SQL_ERROR;
            }

            // Get field name for logging
            auto fieldName = (jstring)getEnv()->CallObjectMethod(field, getNameMethod);
            const char* nameStr = getEnv()->GetStringUTFChars(fieldName, nullptr);
            LOGF("Processing field: %s", nameStr);

            // Get metadata array using helper method
            auto metadataArray = (jobjectArray)getEnv()->CallStaticObjectMethod(
                wrapperClass, getMetadataArrayMethod, field);
            if (!metadataArray) {
                LOGF("ERROR: Failed to get metadata array for field %d", i);
                return SQL_ERROR;
            }

            // Store metadata values in map
            std::map<std::string, std::string> metadataMap;
            auto outerLength = getEnv()->GetArrayLength(metadataArray);
            LOGF("Metadata array contains %d key-value pairs", outerLength);

            for (jsize j = 0; j < outerLength; j++) {
                auto innerArray = (jobjectArray)getEnv()->GetObjectArrayElement(metadataArray, j);
                if (!innerArray) continue;

                auto key = (jstring)getEnv()->GetObjectArrayElement(innerArray, 0);
                auto value = (jstring)getEnv()->GetObjectArrayElement(innerArray, 1);

                if (key) {
                    const char* keyStr = getEnv()->GetStringUTFChars(key, nullptr);
                    const char* valueStr = value ? getEnv()->GetStringUTFChars(value, nullptr) : "";
                    metadataMap[keyStr] = valueStr;

                    getEnv()->ReleaseStringUTFChars(key, keyStr);
                    if (value) getEnv()->ReleaseStringUTFChars(value, valueStr);
                }

                if (key) getEnv()->DeleteLocalRef(key);
                if (value) getEnv()->DeleteLocalRef(value);
                getEnv()->DeleteLocalRef(innerArray);
            }

            // Populate column descriptor
            ColumnDesc& column = stmt->resultColumns[i];

            // Set boolean attributes directly from metadata
            column.autoIncrement = metadataMap["AutoIncrement"] == "true" ? SQL_TRUE : SQL_FALSE;
            column.caseSensitive = metadataMap["CaseSensitive"] == "true" ? SQL_TRUE : SQL_FALSE;
            column.currency = metadataMap["Currency"] == "true" ? SQL_TRUE : SQL_FALSE;
            column.definitelyWritable = metadataMap["DefinitelyWritable"] == "true" ? SQL_TRUE : SQL_FALSE;
            column.readOnly = metadataMap["ReadOnly"] == "true" ? SQL_TRUE : SQL_FALSE;
            column.searchable = metadataMap["Searchable"] == "true" ? SQL_TRUE : SQL_FALSE;
            column._signed = metadataMap["Signed"] == "true" ? SQL_TRUE : SQL_FALSE;
            column.writable = metadataMap["Writable"] == "true" ? SQL_TRUE : SQL_FALSE;

            // Set numeric attributes
            column.nullable = !metadataMap["Nullable"].empty() ? std::stoi(metadataMap["Nullable"]) : SQL_NULLABLE_UNKNOWN;
            column.columnSize = !metadataMap["ColumnSize"].empty() ? std::stoi(metadataMap["ColumnSize"]) : 0;
            column.displaySize = !metadataMap["ColumnDisplaySize"].empty() ? std::stoi(metadataMap["ColumnDisplaySize"]) : 0;
            column.octetLength = !metadataMap["OctetLength"].empty() ? std::stoi(metadataMap["OctetLength"]) : 0;
            column.scale = !metadataMap["Scale"].empty() ? std::stoi(metadataMap["Scale"]) : 0;
            column.precision = !metadataMap["Precision"].empty() ? std::stoi(metadataMap["Precision"]) : 0;

            if (!metadataMap["ColumnType"].empty()) {
                column.sqlType = mapSQLType(std::stoi(metadataMap["ColumnType"]));
                LOGF("Set SQL type to %d", column.sqlType);
            }

            // Get string values with column name fallbacks
            auto columnName = metadataMap["ColumnName"].empty() ? std::string(nameStr) : metadataMap["ColumnName"];
            auto columnLabel = metadataMap["ColumnLabel"].empty() ? columnName : metadataMap["ColumnLabel"];

            // Cast pointers for string assignment
            const char*& catalogNameRef = reinterpret_cast<const char*&>(column.catalogName);
            const char*& labelRef = reinterpret_cast<const char*&>(column.label);
            const char*& nameRef = reinterpret_cast<const char*&>(column.name);
            const char*& schemaNameRef = reinterpret_cast<const char*&>(column.schemaName);
            const char*& tableNameRef = reinterpret_cast<const char*&>(column.tableName);
            const char*& typeNameRef = reinterpret_cast<const char*&>(column.typeName);
            const char*& baseColumnNameRef = reinterpret_cast<const char*&>(column.baseColumnName);
            const char*& baseTableNameRef = reinterpret_cast<const char*&>(column.baseTableName);

            // Assign string attributes
            assignColumnString(columnName, nameRef, column.nameLength, "Name");
            assignColumnString(columnLabel, labelRef, column.labelLength, "Label");
            assignColumnString(metadataMap["CatalogName"], catalogNameRef, column.catalogNameLength, "CatalogName");
            assignColumnString(metadataMap["SchemaName"], schemaNameRef, column.schemaNameLength, "SchemaName");
            assignColumnString(metadataMap["TableName"], tableNameRef, column.tableNameLength, "TableName");
            assignColumnString(metadataMap["ColumnTypeName"], typeNameRef, column.typeNameLength, "TypeName");
            assignColumnString(columnName, baseColumnNameRef, column.baseColumnNameLength, "BaseColumnName");
            assignColumnString(metadataMap["TableName"], baseTableNameRef, column.baseTableNameLength, "BaseTableName");

            // Verify column name assignments
            LOGF("Column %d name assignments:", i + 1);
            LOGF("  Name: '%s' (length: %d)", column.name ? column.name : "null", column.nameLength);
            LOGF("  SQLType: '%d'", column.sqlType);
            LOGF("  Column Size '%d'", column.columnSize);
            LOGF("  Type Name '%s'", column.typeName);
            LOGF("  Label: '%s' (length: %d)", column.label ? column.label : "null", column.labelLength);
            LOGF("  BaseColumnName: '%s' (length: %d)", column.baseColumnName ? column.baseColumnName : "null",
                column.baseColumnNameLength);

            // Clean up field-specific references
            getEnv()->ReleaseStringUTFChars(fieldName, nameStr);
            getEnv()->DeleteLocalRef(fieldName);
            getEnv()->DeleteLocalRef(metadataArray);
            getEnv()->DeleteLocalRef(field);
        }

        // Clean up global references
        LOG("Cleaning up global references");
        getEnv()->DeleteLocalRef(fieldClass);
        getEnv()->DeleteLocalRef(listClass);
        getEnv()->DeleteLocalRef(fieldsList);
        getEnv()->DeleteLocalRef(schemaClass);
        getEnv()->DeleteLocalRef(schema);
        getEnv()->DeleteLocalRef(rootClass);

        LOG("Successfully completed populateColumnDescriptors");
        return SQL_SUCCESS;

    } catch (const std::exception& e) {
        LOGF("ERROR: Exception in populateColumnDescriptors: %s", e.what());
        if (getEnv()->ExceptionCheck()) {
            getEnv()->ExceptionDescribe();
            getEnv()->ExceptionClear();
        }
        return SQL_ERROR;
    } catch (...) {
        LOG("ERROR: Unknown exception in populateColumnDescriptors");
        if (getEnv()->ExceptionCheck()) {
            getEnv()->ExceptionDescribe();
            getEnv()->ExceptionClear();
        }
        return SQL_ERROR;
    }
}

const char* JVMSingleton::getTypeNameFromSQLType(SQLSMALLINT sqlType) {
    switch (sqlType) {
        case SQL_INTEGER: return "INTEGER";
        case SQL_SMALLINT: return "SMALLINT";
        case SQL_BIGINT: return "BIGINT";
        case SQL_DOUBLE: return "DOUBLE";
        case SQL_REAL: return "REAL";
        case SQL_DECIMAL: return "DECIMAL";
        case SQL_BIT: return "BIT";
        case SQL_TINYINT: return "TINYINT";
        case SQL_TYPE_DATE: return "DATE";
        case SQL_TYPE_TIME: return "TIME";
        case SQL_TYPE_TIMESTAMP: return "TIMESTAMP";
        case SQL_BINARY: return "BINARY";
        case SQL_VARBINARY: return "VARBINARY";
        case SQL_VARCHAR: return "VARCHAR";
        case SQL_CHAR: return "CHAR";
        case SQL_WVARCHAR: return "WVARCHAR";
        case SQL_WCHAR: return "WCHAR";
        default: return "UNKNOWN";
    }
}

SQLSMALLINT JVMSingleton::mapArrowTypeToSQL(jobject arrowType) {
    LOG("Mapping Arrow type to SQL type");
    jclass typeClass = getEnv()->GetObjectClass(arrowType);
    jmethodID getTypeIDMethod = getEnv()->GetMethodID(typeClass, "getTypeID",
        "()Lorg/apache/arrow/vector/types/pojo/ArrowType$ArrowTypeID;");
    jobject typeId = getEnv()->CallObjectMethod(arrowType, getTypeIDMethod);

    jclass enumClass = getEnv()->GetObjectClass(typeId);
    jmethodID nameMethod = getEnv()->GetMethodID(enumClass, "name", "()Ljava/lang/String;");
    auto typeName = reinterpret_cast<jstring>(getEnv()->CallObjectMethod(typeId, nameMethod));

    const char* typeNameStr = getEnv()->GetStringUTFChars(typeName, nullptr);
    SQLSMALLINT sqlType;

    // Map Arrow types to SQL types.
    if (strcmp(typeNameStr, "Int") == 0) {
        LOG("Mapping Arrow Int to SQL INTEGER");
        sqlType = SQL_INTEGER;
    } else if (strcmp(typeNameStr, "FloatingPoint") == 0) {
        LOG("Mapping Arrow FloatingPoint to SQL DOUBLE");
        sqlType = SQL_DOUBLE;
    } else if (strcmp(typeNameStr, "Bool") == 0) {
        LOG("Mapping Arrow Bool to SQL BIT");
        sqlType = SQL_BIT;
    } else if (strcmp(typeNameStr, "Date") == 0) {
        LOG("Mapping Arrow Date to SQL TYPE_DATE");
        sqlType = SQL_TYPE_DATE;
    } else if (strcmp(typeNameStr, "Time") == 0) {
        LOG("Mapping Arrow Time to SQL TYPE_TIME");
        sqlType = SQL_TYPE_TIME;
    } else if (strcmp(typeNameStr, "Timestamp") == 0) {
        LOG("Mapping Arrow Timestamp to SQL TYPE_TIMESTAMP");
        sqlType = SQL_TYPE_TIMESTAMP;
    } else if (strcmp(typeNameStr, "Decimal") == 0) {
        LOG("Mapping Arrow Decimal to SQL DECIMAL");
        sqlType = SQL_DECIMAL;
    } else if (strcmp(typeNameStr, "Binary") == 0) {
        LOG("Mapping Arrow Binary to SQL BINARY");
        sqlType = SQL_BINARY;
    } else if (strcmp(typeNameStr, "Utf8") == 0) {
        LOG("Mapping Arrow Utf8 to SQL VARCHAR");
        sqlType = SQL_VARCHAR;
    } else {
        LOG("Defaulting to SQL VARCHAR for unknown Arrow type");
        sqlType = SQL_VARCHAR;
    }

    getEnv()->ReleaseStringUTFChars(typeName, typeNameStr);
    getEnv()->DeleteLocalRef(typeName);
    getEnv()->DeleteLocalRef(typeId);
    getEnv()->DeleteLocalRef(enumClass);
    getEnv()->DeleteLocalRef(typeClass);

    return sqlType;
}

SQLULEN JVMSingleton::getSQLTypeSize(SQLSMALLINT sqlType) {
    LOG("Determining column size for SQL type");
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

jclass JVMSingleton::getWrapperClass() {
    std::lock_guard<std::mutex> lock(initMutex);
    if (!initialized) initializeJVM();
    return wrapperClass;
}

void JVMSingleton::initializeJVM() {
    if (initialized) return;

    LOG("Initializing JVM singleton");

    // Get system info
    SYSTEM_INFO si;
    GetNativeSystemInfo(&si);
    LOGF("System Architecture: %d", si.wProcessorArchitecture);

    const char* javaHome;
    #ifdef _M_X64
        LOG("Running x64 process on ARM64 Windows");
        javaHome = getenv("JAVA_HOME_X64");
        LOGF("JAVA_HOME_X64: %s", javaHome ? javaHome : "not set");
    #else
        LOG("Running native ARM64");
        javaHome = getenv("JAVA_HOME");
        LOGF("JAVA_HOME: %s", javaHome ? javaHome : "not set");
    #endif

    if (!javaHome) {
        LOG("ERROR: Required JAVA_HOME environment variable not set");
        return;
    }

    LOG("Updating PATH");
    std::string javaPath = std::string(javaHome) + "\\bin;" + std::string(javaHome) + "\\bin\\server;";
    std::string currentPath = getenv("PATH");
    SetEnvironmentVariableA("PATH", (javaPath + currentPath).c_str());
    LOGF("New PATH: %s", (javaPath + currentPath).c_str());

    LOG("Getting module directory");
    std::string moduleDir = GetModuleDirectory();
    LOGF("Module directory: %s", moduleDir.c_str());
    std::string jarPath = moduleDir + "\\jni-arrow-1.0.0-jar-with-dependencies.jar";
    LOGF("JAR path: %s", jarPath.c_str());

    LOG("Checking JAR file");
    DWORD jarAttrs = GetFileAttributesA(jarPath.c_str());
    if (jarAttrs == INVALID_FILE_ATTRIBUTES || (jarAttrs & FILE_ATTRIBUTE_DIRECTORY)) {
        LOGF("ERROR: JAR file not found. GetLastError: %lu", GetLastError());
        return;
    }
    LOG("JAR file found");

    LOG("Loading JVM DLL");
    std::string jvmPath = std::string(javaHome) + "\\bin\\server\\jvm.dll";
    LOGF("JVM DLL path: %s", jvmPath.c_str());

    HMODULE jvmDll = LoadLibraryA(jvmPath.c_str());
    if (!jvmDll) {
        LOGF("ERROR: Failed to load JVM DLL. GetLastError: %lu", GetLastError());
        return;
    }
    LOG("JVM DLL loaded successfully");

    LOG("Getting JNI_CreateJavaVM function");
    auto JNI_CreateJavaVM_fn = (JNI_CreateJavaVM_t)GetProcAddress(jvmDll, "JNI_CreateJavaVM");
    if (!JNI_CreateJavaVM_fn) {
        LOGF("ERROR: Failed to get JNI_CreateJavaVM. GetLastError: %lu", GetLastError());
        FreeLibrary(jvmDll);
        return;
    }
    LOG("Got JNI_CreateJavaVM function");

    LOG("Setting up JVM options");
    std::string isolatedClassPathStr = "-Djava.class.path=" + jarPath;
    std::string isolatedLibraryPathStr = "-Djava.library.path=" + std::string(javaHome) + "\\bin;" +
                                        std::string(javaHome) + "\\bin\\server";
    LOGF("Classpath: %s", isolatedClassPathStr.c_str());
    LOGF("Library path: %s", isolatedLibraryPathStr.c_str());

    JavaVMOption options[2];
    options[0].optionString = const_cast<char*>(isolatedClassPathStr.c_str());
    options[1].optionString = const_cast<char*>(isolatedLibraryPathStr.c_str());

    JavaVMInitArgs vm_args;
    vm_args.version = JNI_VERSION_1_8;
    vm_args.nOptions = 2;
    vm_args.options = options;
    vm_args.ignoreUnrecognized = JNI_FALSE;
    LOG("JVM options set up");

    LOG("Creating JVM");
    jint rc = JNI_CreateJavaVM_fn(&jvm, reinterpret_cast<void**>(&env), &vm_args);
    if (rc != JNI_OK) {
        LOGF("ERROR: Failed to create JVM. Error code: %d", rc);
        FreeLibrary(jvmDll);
        return;
    }
    LOG("JVM created successfully");

    LOG("Finding ArrowJdbcWrapper class");
    wrapperClass = env->FindClass("com/hasura/ArrowJdbcWrapper");
    if (!wrapperClass) {
        LOG("ERROR: Failed to find ArrowJdbcWrapper class");
        if (env->ExceptionCheck()) {
            LOG("Exception details:");
            env->ExceptionDescribe();
            env->ExceptionClear();
        }
        return;
    }
    LOG("Found wrapper class");

    LOG("Creating global reference for wrapper class");
    wrapperClass = (jclass)env->NewGlobalRef(wrapperClass);

    LOG("Getting constructor");
    jmethodID constructor = env->GetMethodID(wrapperClass, "<init>", "()V");
    if (!constructor) {
        LOG("ERROR: Failed to find constructor");
        if (env->ExceptionCheck()) {
            LOG("Exception details:");
            env->ExceptionDescribe();
            env->ExceptionClear();
        }
        return;
    }
    LOG("Found constructor");

    LOG("Creating wrapper instance");
    jobject localWrapper = env->NewObject(wrapperClass, constructor);
    if (!localWrapper) {
        LOG("ERROR: Failed to create wrapper instance");
        if (env->ExceptionCheck()) {
            LOG("Exception details:");
            env->ExceptionDescribe();
            env->ExceptionClear();
        }
        return;
    }
    LOG("Created wrapper instance");

    LOG("Creating global reference for wrapper instance");
    wrapper = env->NewGlobalRef(localWrapper);
    env->DeleteLocalRef(localWrapper);

    initialized = true;
    LOG("JVM singleton initialization complete");
}

void JVMSingleton::setConnection(const std::string& jdbcUrl, const std::string& username, const std::string& password) {
    jmethodID setConnMethod = getEnv()->GetMethodID(wrapperClass, "setConnection",
        "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");

    jstring jUrl = getEnv()->NewStringUTF(jdbcUrl.c_str());
    jstring jUser = getEnv()->NewStringUTF(username.c_str());
    jstring jPass = getEnv()->NewStringUTF(password.c_str());

    getEnv()->CallVoidMethod(wrapper, setConnMethod, jUrl, jUser, jPass);

    getEnv()->DeleteLocalRef(jUrl);
    getEnv()->DeleteLocalRef(jUser);
    getEnv()->DeleteLocalRef(jPass);
}

void JVMSingleton::executeQuery(const std::string& query) {
    LOG("Executing query through wrapper");
    jmethodID method = getEnv()->GetMethodID(wrapperClass, "executeQuery", "(Ljava/lang/String;)V");
    jstring jQuery = getEnv()->NewStringUTF(query.c_str());
    getEnv()->CallVoidMethod(wrapper, method, jQuery);
    getEnv()->DeleteLocalRef(jQuery);
}

void JVMSingleton::getTables() {
    LOG("Getting tables through wrapper");
    jmethodID method = getEnv()->GetMethodID(wrapperClass, "getTables", "()V");
    getEnv()->CallVoidMethod(wrapper, method);
}

void JVMSingleton::getColumns(const std::string& tableName) {
    LOG("Getting columns through wrapper");
    jmethodID method = getEnv()->GetMethodID(wrapperClass, "getColumns", "(Ljava/lang/String;)V");
    jstring jTableName = getEnv()->NewStringUTF(tableName.c_str());
    getEnv()->CallVoidMethod(wrapper, method, jTableName);
    getEnv()->DeleteLocalRef(jTableName);
}

SQLRETURN JVMSingleton::executeAndGetArrowResult(
    const char* methodName,
    const std::vector<JniParam>& params,
    Statement* stmt) {

    try {
        LOG("Building method signature");
        std::string signature = "(";
        for (const auto& param : params) {
            signature += param.getSignature();
        }
        signature += ")Lorg/apache/arrow/vector/VectorSchemaRoot;";
        LOGF("Method signature: %s", signature.c_str());

        LOG("Getting method ID");
        jmethodID method = getEnv()->GetMethodID(wrapperClass, methodName, signature.c_str());
        if (!method) {
            LOG("Failed to get method ID");
            if (getEnv()->ExceptionCheck()) {
                LOG("Exception during method lookup:");
                getEnv()->ExceptionDescribe();
                getEnv()->ExceptionClear();
            }
            return SQL_ERROR;
        }
        LOGF("Found method: %s", methodName);

        LOG("Converting parameters to JNI values");
        std::vector<jvalue> jniValues;
        for (size_t i = 0; i < params.size(); i++) {
            LOGF("Converting param %zu", i);
            jniValues.push_back(params[i].toJValue(env));
        }

        LOG("Calling Java method");
        jobject schemaRoot;
        if (params.empty()) {
            LOG("Calling with no params");
            schemaRoot = getEnv()->CallObjectMethod(wrapper, method);
        } else {
            LOGF("Calling with %zu params", params.size());
            schemaRoot = getEnv()->CallObjectMethodA(wrapper, method, jniValues.data());
        }

        LOG("Cleaning up parameters");
        for (size_t i = 0; i < params.size(); i++) {
            params[i].cleanup(env, jniValues[i]);
        }

        if (getEnv()->ExceptionCheck()) {
            LOG("Exception during method execution:");
            getEnv()->ExceptionDescribe();
            getEnv()->ExceptionClear();
            return SQL_ERROR;
        }

        if (!schemaRoot) {
            LOG("Method returned null schema root");
            return SQL_ERROR;
        }
        LOG("Got schema root");

        LOG("Populating column descriptors");
        SQLRETURN ret = populateColumnDescriptors(schemaRoot, stmt);
        if (!SQL_SUCCEEDED(ret)) {
            LOG("Failed to populate column descriptors");
            return ret;
        }

        LOG("Setting Arrow result");
        ret = stmt->setArrowResult(schemaRoot, stmt->resultColumns);

        LOG("Closing schema root");
        jclass schemaRootClass = getEnv()->GetObjectClass(schemaRoot);
        jmethodID closeMethod = getEnv()->GetMethodID(schemaRootClass, "close", "()V");
        if (closeMethod) {
            getEnv()->CallVoidMethod(schemaRoot, closeMethod);
        } else {
            LOG("No close method found");
        }

        getEnv()->DeleteLocalRef(schemaRoot);
        LOG("Method execution complete");
        return ret;
    } catch (...) {
        LOG("Caught exception in executeAndGetArrowResult");
        if (getEnv()->ExceptionCheck()) {
            getEnv()->ExceptionDescribe();
            getEnv()->ExceptionClear();
        }
        return SQL_ERROR;
    }
}

void JVMSingleton::close() {
    LOG("Closing wrapper");
    jmethodID method = getEnv()->GetMethodID(wrapperClass, "close", "()V");
    getEnv()->CallVoidMethod(wrapper, method);
}

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

