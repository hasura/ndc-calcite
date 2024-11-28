#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include "../include/connection.hpp"
#include "../include/logging.hpp"
//#include "../include/httplib.h"
#include "../include/globals.hpp"
#include "../include/environment.hpp"
#include "../include/statement.hpp"

extern "C" {

SQLRETURN SQL_API SQLGetTypeInfo(
    SQLHSTMT       StatementHandle,
    SQLSMALLINT    DataType) {

    LOGF("SQLGetTypeInfo called for type: %d", DataType);
    auto* stmt = static_cast<Statement*>(StatementHandle);
    if (!stmt) {
        LOG("Invalid statement handle");
        return SQL_INVALID_HANDLE;
    }

    // Define our supported types and their properties
    struct TypeInfo {
        SQLSMALLINT      dataType;
        const char*      typeName;
        SQLINTEGER      columnSize;
        const char*      literalPrefix;
        const char*      literalSuffix;
        const char*      createParams;
        SQLSMALLINT     nullable;
        SQLSMALLINT     caseSensitive;
        SQLSMALLINT     searchable;
        SQLSMALLINT     unsignedAttr;
        SQLSMALLINT     fixedPrecScale;
        SQLSMALLINT     autoUniqueValue;
        const char*      localTypeName;
        SQLSMALLINT     minimumScale;
        SQLSMALLINT     maximumScale;
        SQLSMALLINT     sqlDataType;
        SQLSMALLINT     dateTimeSub;
        SQLINTEGER      numPrecRadix;
        SQLSMALLINT     intervalPrecision;
    };

    // Define supported types
    static const std::vector<TypeInfo> supportedTypes = {
    {   // VARCHAR
        SQL_VARCHAR,         // dataType
        "VARCHAR",           // typeName
        32767,              // columnSize
        "'",                // literalPrefix
        "'",                // literalSuffix
        "max length",       // createParams
        SQL_NULLABLE,       // nullable
        SQL_TRUE,           // caseSensitive
        SQL_SEARCHABLE,     // searchable
        SQL_FALSE,          // unsignedAttr
        SQL_FALSE,          // fixedPrecScale
        SQL_FALSE,          // autoUniqueValue
        "VARCHAR",          // localTypeName
        0,                  // minimumScale
        0,                  // maximumScale
        SQL_VARCHAR,        // sqlDataType
        0,                  // dateTimeSub
        0,                  // numPrecRadix
        0                   // intervalPrecision
    },
    {   // CHAR
        SQL_CHAR,           // dataType
        "CHAR",             // typeName
        255,                // columnSize
        "'",                // literalPrefix
        "'",                // literalSuffix
        "length",           // createParams
        SQL_NULLABLE,       // nullable
        SQL_TRUE,           // caseSensitive
        SQL_SEARCHABLE,     // searchable
        SQL_FALSE,          // unsignedAttr
        SQL_FALSE,          // fixedPrecScale
        SQL_FALSE,          // autoUniqueValue
        "CHAR",             // localTypeName
        0,                  // minimumScale
        0,                  // maximumScale
        SQL_CHAR,           // sqlDataType
        0,                  // dateTimeSub
        0,                  // numPrecRadix
        0                   // intervalPrecision
    },
    {   // INTEGER
        SQL_INTEGER,        // dataType
        "INTEGER",          // typeName
        10,                 // columnSize
        nullptr,            // literalPrefix
        nullptr,            // literalSuffix
        nullptr,            // createParams
        SQL_NULLABLE,       // nullable
        SQL_FALSE,          // caseSensitive
        SQL_SEARCHABLE,     // searchable
        SQL_FALSE,          // unsignedAttr
        SQL_FALSE,          // fixedPrecScale
        SQL_FALSE,          // autoUniqueValue
        "INTEGER",          // localTypeName
        0,                  // minimumScale
        0,                  // maximumScale
        SQL_INTEGER,        // sqlDataType
        0,                  // dateTimeSub
        10,                 // numPrecRadix
        0                   // intervalPrecision
    },
    {   // SMALLINT
        SQL_SMALLINT,       // dataType
        "SMALLINT",         // typeName
        5,                  // columnSize
        nullptr,            // literalPrefix
        nullptr,            // literalSuffix
        nullptr,            // createParams
        SQL_NULLABLE,       // nullable
        SQL_FALSE,          // caseSensitive
        SQL_SEARCHABLE,     // searchable
        SQL_FALSE,          // unsignedAttr
        SQL_FALSE,          // fixedPrecScale
        SQL_FALSE,          // autoUniqueValue
        "SMALLINT",         // localTypeName
        0,                  // minimumScale
        0,                  // maximumScale
        SQL_SMALLINT,       // sqlDataType
        0,                  // dateTimeSub
        10,                 // numPrecRadix
        0                   // intervalPrecision
    },
    {   // DECIMAL
        SQL_DECIMAL,        // dataType
        "DECIMAL",          // typeName
        38,                 // columnSize
        nullptr,            // literalPrefix
        nullptr,            // literalSuffix
        "precision,scale",  // createParams
        SQL_NULLABLE,       // nullable
        SQL_FALSE,          // caseSensitive
        SQL_SEARCHABLE,     // searchable
        SQL_FALSE,          // unsignedAttr
        SQL_FALSE,          // fixedPrecScale
        SQL_FALSE,          // autoUniqueValue
        "DECIMAL",          // localTypeName
        0,                  // minimumScale
        38,                 // maximumScale
        SQL_DECIMAL,        // sqlDataType
        0,                  // dateTimeSub
        10,                 // numPrecRadix
        0                   // intervalPrecision
    },
    {   // DOUBLE
        SQL_DOUBLE,         // dataType
        "DOUBLE",           // typeName
        15,                 // columnSize
        nullptr,            // literalPrefix
        nullptr,            // literalSuffix
        nullptr,            // createParams
        SQL_NULLABLE,       // nullable
        SQL_FALSE,          // caseSensitive
        SQL_SEARCHABLE,     // searchable
        SQL_FALSE,          // unsignedAttr
        SQL_FALSE,          // fixedPrecScale
        SQL_FALSE,          // autoUniqueValue
        "DOUBLE",           // localTypeName
        0,                  // minimumScale
        0,                  // maximumScale
        SQL_DOUBLE,         // sqlDataType
        0,                  // dateTimeSub
        2,                  // numPrecRadix
        0                   // intervalPrecision
    },
    {   // TIMESTAMP
        SQL_TYPE_TIMESTAMP, // dataType
        "TIMESTAMP",        // typeName
        23,                 // columnSize (YYYY-MM-DD HH:MM:SS.NNN)
        "'",                // literalPrefix
        "'",                // literalSuffix
        "precision",        // createParams
        SQL_NULLABLE,       // nullable
        SQL_FALSE,          // caseSensitive
        SQL_SEARCHABLE,     // searchable
        SQL_FALSE,          // unsignedAttr
        SQL_FALSE,          // fixedPrecScale
        SQL_FALSE,          // autoUniqueValue
        "TIMESTAMP",        // localTypeName
        0,                  // minimumScale
        6,                  // maximumScale
        SQL_TYPE_TIMESTAMP, // sqlDataType
        3,                  // dateTimeSub (SQL_CODE_TIMESTAMP)
        0,                  // numPrecRadix
        0                   // intervalPrecision
    },
    {   // DATE
        SQL_TYPE_DATE,      // dataType
        "DATE",             // typeName
        10,                 // columnSize (YYYY-MM-DD)
        "'",                // literalPrefix
        "'",                // literalSuffix
        nullptr,            // createParams
        SQL_NULLABLE,       // nullable
        SQL_FALSE,          // caseSensitive
        SQL_SEARCHABLE,     // searchable
        SQL_FALSE,          // unsignedAttr
        SQL_FALSE,          // fixedPrecScale
        SQL_FALSE,          // autoUniqueValue
        "DATE",             // localTypeName
        0,                  // minimumScale
        0,                  // maximumScale
        SQL_TYPE_DATE,      // sqlDataType
        1,                  // dateTimeSub (SQL_CODE_DATE)
        0,                  // numPrecRadix
        0                   // intervalPrecision
    },
    {   // BOOLEAN
        SQL_BIT,           // dataType
        "BOOLEAN",         // typeName
        1,                 // columnSize
        nullptr,           // literalPrefix
        nullptr,           // literalSuffix
        nullptr,           // createParams
        SQL_NULLABLE,      // nullable
        SQL_FALSE,         // caseSensitive
        SQL_SEARCHABLE,    // searchable
        SQL_FALSE,         // unsignedAttr
        SQL_FALSE,         // fixedPrecScale
        SQL_FALSE,         // autoUniqueValue
        "BOOLEAN",         // localTypeName
        0,                 // minimumScale
        0,                 // maximumScale
        SQL_BIT,          // sqlDataType
        0,                 // dateTimeSub
        0,                 // numPrecRadix
        0                  // intervalPrecision
    }
};
    // Clear any existing result set
    stmt->clearResults();

    // Set up the column descriptors for the result set
    std::vector<ColumnDesc> columns = {
        {"TYPE_NAME",           0,SQL_VARCHAR, 128, 0, SQL_NO_NULLS},
        {"DATA_TYPE",          0,SQL_SMALLINT, 5, 0, SQL_NO_NULLS},
        {"COLUMN_SIZE",        0,SQL_INTEGER, 10, 0, SQL_NULLABLE},
        {"LITERAL_PREFIX",     0,SQL_VARCHAR, 128, 0, SQL_NULLABLE},
        {"LITERAL_SUFFIX",     0,SQL_VARCHAR, 128, 0, SQL_NULLABLE},
        {"CREATE_PARAMS",      0,SQL_VARCHAR, 128, 0, SQL_NULLABLE},
        {"NULLABLE",           0,SQL_SMALLINT, 5, 0, SQL_NO_NULLS},
        {"CASE_SENSITIVE",     0,SQL_SMALLINT, 5, 0, SQL_NO_NULLS},
        {"SEARCHABLE",         0,SQL_SMALLINT, 5, 0, SQL_NO_NULLS},
        {"UNSIGNED_ATTRIBUTE", 0,SQL_SMALLINT, 5, 0, SQL_NULLABLE},
        {"FIXED_PREC_SCALE",   0,SQL_SMALLINT, 5, 0, SQL_NO_NULLS},
        {"AUTO_UNIQUE_VALUE",  0,SQL_SMALLINT, 5, 0, SQL_NULLABLE},
        {"LOCAL_TYPE_NAME",    0,SQL_VARCHAR, 128, 0, SQL_NULLABLE},
        {"MINIMUM_SCALE",      0,SQL_SMALLINT, 5, 0, SQL_NULLABLE},
        {"MAXIMUM_SCALE",      0,SQL_SMALLINT, 5, 0, SQL_NULLABLE},
        {"SQL_DATA_TYPE",      0,SQL_SMALLINT, 5, 0, SQL_NO_NULLS},
        {"SQL_DATETIME_SUB",   0,SQL_SMALLINT, 5, 0, SQL_NULLABLE},
        {"NUM_PREC_RADIX",     0,SQL_INTEGER, 10, 0, SQL_NULLABLE},
        {"INTERVAL_PRECISION", 0,SQL_SMALLINT, 5, 0, SQL_NULLABLE}
    };

    stmt->resultColumns = columns;

    // Filter types based on DataType parameter
    std::vector<const TypeInfo*> filteredTypes;
    for (const auto& type : supportedTypes) {
        if (DataType == SQL_ALL_TYPES || DataType == type.dataType) {
            filteredTypes.push_back(&type);
        }
    }

    // Set up the result data
    stmt->resultData.resize(filteredTypes.size());
    for (size_t i = 0; i < filteredTypes.size(); i++) {
        const auto& type = filteredTypes[i];
        auto& row = stmt->resultData[i];
        row.resize(columns.size());

        // TYPE_NAME
        row[0].isNull = false;
        row[0].data = type->typeName;

        // DATA_TYPE
        row[1].isNull = false;
        row[1].data = std::to_string(type->dataType);

        // COLUMN_SIZE
        row[2].isNull = false;
        row[2].data = std::to_string(type->columnSize);

        // LITERAL_PREFIX
        row[3].isNull = (type->literalPrefix == nullptr);
        row[3].data = type->literalPrefix ? type->literalPrefix : "";

        // LITERAL_SUFFIX
        row[4].isNull = (type->literalSuffix == nullptr);
        row[4].data = type->literalSuffix ? type->literalSuffix : "";

        // CREATE_PARAMS
        row[5].isNull = (type->createParams == nullptr);
        row[5].data = type->createParams ? type->createParams : "";

        // NULLABLE
        row[6].isNull = false;
        row[6].data = std::to_string(type->nullable);

        // CASE_SENSITIVE
        row[7].isNull = false;
        row[7].data = std::to_string(type->caseSensitive);

        // SEARCHABLE
        row[8].isNull = false;
        row[8].data = std::to_string(type->searchable);

        // UNSIGNED_ATTRIBUTE
        row[9].isNull = false;
        row[9].data = std::to_string(type->unsignedAttr);

        // FIXED_PREC_SCALE
        row[10].isNull = false;
        row[10].data = std::to_string(type->fixedPrecScale);

        // AUTO_UNIQUE_VALUE
        row[11].isNull = false;
        row[11].data = std::to_string(type->autoUniqueValue);

        // LOCAL_TYPE_NAME
        row[12].isNull = (type->localTypeName == nullptr);
        row[12].data = type->localTypeName ? type->localTypeName : "";

        // MINIMUM_SCALE
        row[13].isNull = false;
        row[13].data = std::to_string(type->minimumScale);

        // MAXIMUM_SCALE
        row[14].isNull = false;
        row[14].data = std::to_string(type->maximumScale);

        // SQL_DATA_TYPE
        row[15].isNull = false;
        row[15].data = std::to_string(type->sqlDataType);

        // SQL_DATETIME_SUB
        row[16].isNull = false;
        row[16].data = std::to_string(type->dateTimeSub);

        // NUM_PREC_RADIX
        row[17].isNull = false;
        row[17].data = std::to_string(type->numPrecRadix);

        // INTERVAL_PRECISION
        row[18].isNull = false;
        row[18].data = std::to_string(type->intervalPrecision);
    }

    stmt->hasResult = true;
    stmt->currentRow = 0;

    LOGF("SQLGetTypeInfo returning %zu type(s)", filteredTypes.size());
    return SQL_SUCCESS;
}

    SQLRETURN SQL_API SQLGetTypeInfoW(
        SQLHSTMT       StatementHandle,
        SQLSMALLINT    DataType) {
    return SQLGetTypeInfo(StatementHandle, DataType);
}

}