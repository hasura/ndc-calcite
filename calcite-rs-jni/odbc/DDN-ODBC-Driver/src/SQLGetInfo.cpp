#define NOMINMAX
#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include <algorithm>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <sqlucode.h>
#include "../include/Connection.hpp"
#include "../include/Logging.hpp"
#include "../include/Environment.hpp"
#include "../include/Statement.hpp"

// Helper function to handle string information types
static SQLRETURN HandleStringInfo(
    bool isUnicode,
    SQLPOINTER infoValue,
    SQLSMALLINT bufferLength,
    SQLSMALLINT *stringLength,
    const char *narrowStr,
    const wchar_t *wideStr) {
    if (isUnicode) {
        // For Unicode, we need to get the number of characters
        size_t charCount = wcslen(wideStr);
        // Set the length in bytes (not including null terminator)
        if (stringLength) {
            *stringLength = static_cast<SQLSMALLINT>(charCount * sizeof(WCHAR));
            LOGF("Setting Unicode string length: %d bytes for %zu chars",
                 *stringLength, charCount);
        }
        if (infoValue && bufferLength > 0) {
            // Calculate how many characters we can copy including null terminator
            size_t maxChars = bufferLength / sizeof(WCHAR);
            size_t copyChars = std::min(charCount, maxChars - 1);
            wcsncpy(static_cast<WCHAR *>(infoValue), wideStr, copyChars);
            static_cast<WCHAR *>(infoValue)[copyChars] = L'\0';
            LOGF("Copied %zu Unicode chars to buffer", copyChars);
        }
    } else {
        size_t strLen = strlen(narrowStr);
        if (stringLength) {
            *stringLength = static_cast<SQLSMALLINT>(strLen);
            LOGF("Setting ANSI string length: %d", *stringLength);
        }
        if (infoValue && bufferLength > 0) {
            size_t copyLen = std::min<size_t>(strLen, static_cast<size_t>(bufferLength - 1));
            strncpy(static_cast<char *>(infoValue), narrowStr, copyLen);
            static_cast<char *>(infoValue)[copyLen] = '\0';
            LOGF("Copied %zu ANSI chars to buffer", copyLen);
        }
    }
    return SQL_SUCCESS;
}

// Helper function that handles the common logic
static SQLRETURN SQL_API SQLGetInfo_Internal(
    SQLHDBC hdbc,
    SQLUSMALLINT infoType,
    SQLPOINTER infoValue,
    SQLSMALLINT bufferLength,
    SQLSMALLINT *stringLength,
    bool isUnicode) {
    LOGF("=== SQLGetInfo_Internal Entry ===");
    LOGF("InfoType: %d (0x%04X), BufferLength: %d, isUnicode: %d",
         infoType, infoType, bufferLength, isUnicode);
    LOGF("InfoValue ptr: %p, StringLength ptr: %p", (void*)infoValue, static_cast<void *>(stringLength));

    auto *conn = static_cast<Connection *>(hdbc);
    if (!conn) {
        LOG("Invalid connection handle");
        return SQL_INVALID_HANDLE;
    }

    try {
        switch (infoType) {

            case SQL_DESCRIBE_PARAMETER: {
                if (infoValue) {
                    *static_cast<SQLUSMALLINT*>(infoValue) = SQL_TRUE;
                }
                return SQL_SUCCESS;
            }

            case SQL_NEED_LONG_DATA_LEN: {
                if (infoValue) {
                    *static_cast<SQLUSMALLINT*>(infoValue) = SQL_FALSE;
                }
                return SQL_SUCCESS;
            }

            case SQL_MAX_COLUMNS_IN_TABLE: {
                if (infoValue) {
                    *static_cast<SQLUSMALLINT*>(infoValue) = 1024;  // Or another appropriate value
                }
                return SQL_SUCCESS;
            }

            case SQL_DATA_SOURCE_READ_ONLY: {
                if (infoValue && bufferLength > 0) {
                    if (isUnicode) {
                        *static_cast<wchar_t *>(infoValue) = L'Y';  // Changed to Y since we're read-only
                        if (stringLength) *stringLength = sizeof(wchar_t);
                    } else {
                        *static_cast<char *>(infoValue) = 'Y';      // Changed to Y since we're read-only
                        if (stringLength) *stringLength = 1;
                    }
                }
                LOGF("SQL_DATA_SOURCE_READ_ONLY: Reporting Y");
                return SQL_SUCCESS;
            }

            case SQL_ACCESSIBLE_TABLES: {
                if (infoValue && bufferLength > 0) {
                    if (isUnicode) {
                        *static_cast<wchar_t *>(infoValue) = L'Y';  // Changed to Y to indicate tables are accessible
                        if (stringLength) *stringLength = sizeof(wchar_t);
                    } else {
                        *static_cast<char *>(infoValue) = 'Y';      // Changed to Y to indicate tables are accessible
                        if (stringLength) *stringLength = 1;
                    }
                }
                LOGF("SQL_ACCESSIBLE_TABLES: Reporting Y");
                return SQL_SUCCESS;
            }

            case SQL_CATALOG_NAME: {
                if (infoValue) {
                    *static_cast<SQLSMALLINT*>(infoValue) = SQL_FALSE;
                }
                return SQL_SUCCESS;
            }

            case SQL_CATALOG_NAME_SEPARATOR: {
                if (infoValue && bufferLength > 0) {
                    if (isUnicode) {
                        *static_cast<wchar_t*>(infoValue) = L'\0';
                    } else {
                        *static_cast<char*>(infoValue) = '\0';
                    }
                }
                if (stringLength) {
                    *stringLength = 0;
                }
                return SQL_SUCCESS;
            }

            case SQL_CATALOG_TERM: {
                if (infoValue && bufferLength > 0) {
                    if (isUnicode) {
                        *static_cast<wchar_t*>(infoValue) = L'\0';
                    } else {
                        *static_cast<char*>(infoValue) = '\0';
                    }
                }
                if (stringLength) {
                    *stringLength = 0;
                }
                return SQL_SUCCESS;
            }

            case SQL_CATALOG_USAGE: {
                if (infoValue) {
                    *static_cast<SQLUINTEGER*>(infoValue) = 0;
                }
                return SQL_SUCCESS;
            }

            case SQL_MAX_CATALOG_NAME_LEN: {
                if (infoValue) {
                    *static_cast<SQLSMALLINT*>(infoValue) = 0;
                }
                return SQL_SUCCESS;
            }

            case SQL_DRIVER_ODBC_VER: {
                const char *narrowName = "03.80";
                const wchar_t *wideName = L"03.80";
                LOGF("SQL_DRIVER_ODBC_VER: Reporting %s", narrowName);
                return HandleStringInfo(isUnicode, infoValue, bufferLength, stringLength,
                                        narrowName, wideName);
            }

            case SQL_DRIVER_NAME: {
                const char *narrowName = "DDN-ODBC-Driver";
                const wchar_t *wideName = L"DDN-ODBC-Driver";
                LOGF("SQL_DRIVER_NAME: Reporting %s", narrowName);
                return HandleStringInfo(isUnicode, infoValue, bufferLength, stringLength,
                                        narrowName, wideName);
            }

            case SQL_DRIVER_VER: {
                const char *narrowVer = "01.00.0000";
                const wchar_t *wideVer = L"01.00.0000";
                LOGF("SQL_DRIVER_VER: Reporting %s", narrowVer);
                return HandleStringInfo(isUnicode, infoValue, bufferLength, stringLength,
                                        narrowVer, wideVer);
            }

            case SQL_DBMS_NAME: {
                const char *narrowName = "Hasura DDN";
                const wchar_t *wideName = L"Hasura DDN";
                LOGF("SQL_DBMS_NAME: Reporting %s", narrowName);
                return HandleStringInfo(isUnicode, infoValue, bufferLength, stringLength,
                                        narrowName, wideName);
            }

            case SQL_DBMS_VER: {
                const char *narrowVer = "1.0";
                const wchar_t *wideVer = L"1.0";
                LOGF("SQL_DBMS_VER: Reporting %s", narrowVer);
                return HandleStringInfo(isUnicode, infoValue, bufferLength, stringLength,
                                        narrowVer, wideVer);
            }

            case SQL_UNICODE: {
                if (infoValue) {
                    SQLUINTEGER unicodeSupport = SQL_TRUE;
                    *static_cast<SQLUINTEGER *>(infoValue) = unicodeSupport;
                    LOGF("SQL_UNICODE: Reporting support = %u", unicodeSupport);
                }
                return SQL_SUCCESS;
            }

            case SQL_GETDATA_EXTENSIONS: {
                SQLUINTEGER extensions = SQL_GD_ANY_COLUMN |
                                         SQL_GD_ANY_ORDER |
                                         SQL_GD_BLOCK |
                                         SQL_GD_BOUND;
                if (infoValue) {
                    *static_cast<SQLUINTEGER *>(infoValue) = extensions;
                }
                LOGF("SQL_GETDATA_EXTENSIONS: Reporting extensions = 0x%X", extensions);
                return SQL_SUCCESS;
            }

            case SQL_CONVERT_FUNCTIONS: {
                SQLUINTEGER functions = SQL_FN_CVT_CAST | SQL_FN_CVT_CONVERT;
                if (infoValue) {
                    *static_cast<SQLUINTEGER *>(infoValue) = functions;
                }
                LOGF("SQL_CONVERT_FUNCTIONS: Reporting functions = 0x%X", functions);
                return SQL_SUCCESS;
            }

            case SQL_STRING_FUNCTIONS: {
                SQLUINTEGER functions = SQL_FN_STR_CONCAT |
                                        SQL_FN_STR_LENGTH |
                                        SQL_FN_STR_CHAR_LENGTH |
                                        SQL_FN_STR_ASCII |
                                        SQL_FN_STR_SPACE;
                if (infoValue) {
                    *static_cast<SQLUINTEGER *>(infoValue) = functions;
                }
                LOGF("SQL_STRING_FUNCTIONS: Reporting functions = 0x%X", functions);
                return SQL_SUCCESS;
            }

            case SQL_API_SQLFETCH: {
                if (infoValue) {
                    *static_cast<SQLUINTEGER*>(infoValue) = SQL_TRUE;
                }
                return SQL_SUCCESS;
            }


            case SQL_ACCESSIBLE_PROCEDURES: {
                if (infoValue && bufferLength > 0) {
                    if (isUnicode) {
                        *static_cast<wchar_t *>(infoValue) = L'N';
                        if (stringLength) *stringLength = sizeof(wchar_t);
                    } else {
                        *static_cast<char *>(infoValue) = 'N';
                        if (stringLength) *stringLength = 1;
                    }
                }
                LOGF("Boolean property %d: Reporting N", infoType);
                return SQL_SUCCESS;
            }

            case SQL_IDENTIFIER_QUOTE_CHAR: {
                const char* narrowQuote = "\"";
                const wchar_t* wideQuote = L"\"";
                LOGF("SQL_IDENTIFIER_QUOTE_CHAR: Reporting %s", narrowQuote);
                return HandleStringInfo(isUnicode, infoValue, bufferLength, stringLength,
                                      narrowQuote, wideQuote);
            }

            // Adding SQL-2003 compliant responses for previously unsupported infotypes
            case SQL_OWNER_USAGE: {
                if (infoValue) {
                    SQLUINTEGER usage = SQL_OU_DML_STATEMENTS |
                                      SQL_OU_TABLE_DEFINITION |
                                      SQL_OU_INDEX_DEFINITION |
                                      SQL_OU_PRIVILEGE_DEFINITION;
                    *static_cast<SQLUINTEGER*>(infoValue) = usage;
                }
                return SQL_SUCCESS;
            }

            case SQL_SQL_CONFORMANCE: {
                if (infoValue) {
                    *static_cast<SQLUINTEGER*>(infoValue) = SQL_SC_SQL92_FULL;
                }
                return SQL_SUCCESS;
            }

            case SQL_MAX_COLUMNS_IN_ORDER_BY: {
                if (infoValue) {
                    *static_cast<SQLUSMALLINT*>(infoValue) = 1024;
                }
                return SQL_SUCCESS;
            }

            case SQL_MAX_IDENTIFIER_LEN: {
                if (infoValue) {
                    *static_cast<SQLUSMALLINT*>(infoValue) = 128;
                }
                return SQL_SUCCESS;
            }

            case SQL_MAX_COLUMNS_IN_GROUP_BY: {
                if (infoValue) {
                    *static_cast<SQLUSMALLINT*>(infoValue) = 1024;
                }
                return SQL_SUCCESS;
            }

            case SQL_MAX_COLUMNS_IN_SELECT: {
                if (infoValue) {
                    *static_cast<SQLUSMALLINT*>(infoValue) = 4096;
                }
                return SQL_SUCCESS;
            }

            case SQL_ORDER_BY_COLUMNS_IN_SELECT: {
                if (infoValue && bufferLength > 0) {
                    if (isUnicode) {
                        *static_cast<wchar_t*>(infoValue) = L'Y';
                        if (stringLength) *stringLength = sizeof(wchar_t);
                    } else {
                        *static_cast<char*>(infoValue) = 'Y';
                        if (stringLength) *stringLength = 1;
                    }
                }
                return SQL_SUCCESS;
            }

            case SQL_NUMERIC_FUNCTIONS: {
                if (infoValue) {
                    SQLUINTEGER functions = SQL_FN_NUM_ABS | SQL_FN_NUM_ACOS |
                                          SQL_FN_NUM_ASIN | SQL_FN_NUM_ATAN |
                                          SQL_FN_NUM_CEILING | SQL_FN_NUM_COS |
                                          SQL_FN_NUM_COT | SQL_FN_NUM_EXP |
                                          SQL_FN_NUM_FLOOR | SQL_FN_NUM_LOG |
                                          SQL_FN_NUM_MOD | SQL_FN_NUM_SIGN |
                                          SQL_FN_NUM_SIN | SQL_FN_NUM_SQRT |
                                          SQL_FN_NUM_TAN | SQL_FN_NUM_PI |
                                          SQL_FN_NUM_RAND | SQL_FN_NUM_ROUND |
                                          SQL_FN_NUM_TRUNCATE;
                    *static_cast<SQLUINTEGER*>(infoValue) = functions;
                }
                return SQL_SUCCESS;
            }

            case SQL_TIMEDATE_FUNCTIONS: {
                if (infoValue) {
                    SQLUINTEGER functions = SQL_FN_TD_NOW | SQL_FN_TD_CURDATE |
                                          SQL_FN_TD_DAYOFMONTH | SQL_FN_TD_DAYOFWEEK |
                                          SQL_FN_TD_DAYOFYEAR | SQL_FN_TD_MONTH |
                                          SQL_FN_TD_QUARTER | SQL_FN_TD_WEEK |
                                          SQL_FN_TD_YEAR | SQL_FN_TD_CURTIME |
                                          SQL_FN_TD_HOUR | SQL_FN_TD_MINUTE |
                                          SQL_FN_TD_SECOND | SQL_FN_TD_TIMESTAMPADD |
                                          SQL_FN_TD_TIMESTAMPDIFF | SQL_FN_TD_DAYNAME |
                                          SQL_FN_TD_MONTHNAME;
                    *static_cast<SQLUINTEGER*>(infoValue) = functions;
                }
                return SQL_SUCCESS;
            }

            case SQL_SYSTEM_FUNCTIONS: {
                if (infoValue) {
                    SQLUINTEGER functions = SQL_FN_SYS_USERNAME |
                                          SQL_FN_SYS_DBNAME |
                                          SQL_FN_SYS_IFNULL;
                    *static_cast<SQLUINTEGER*>(infoValue) = functions;
                }
                return SQL_SUCCESS;
            }

            case SQL_TIMEDATE_ADD_INTERVALS:
            case SQL_TIMEDATE_DIFF_INTERVALS: {
                if (infoValue) {
                    SQLUINTEGER intervals = SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND |
                                          SQL_FN_TSI_MINUTE | SQL_FN_TSI_HOUR |
                                          SQL_FN_TSI_DAY | SQL_FN_TSI_WEEK |
                                          SQL_FN_TSI_MONTH | SQL_FN_TSI_QUARTER |
                                          SQL_FN_TSI_YEAR;
                    *static_cast<SQLUINTEGER*>(infoValue) = intervals;
                }
                return SQL_SUCCESS;
            }

            case SQL_CONCAT_NULL_BEHAVIOR: {
                if (infoValue) {
                    *static_cast<SQLUSMALLINT*>(infoValue) = SQL_CB_NULL;
                }
                return SQL_SUCCESS;
            }

            case SQL_OWNER_TERM: {
                const char* narrowTerm = "schema";
                const wchar_t* wideTerm = L"schema";
                return HandleStringInfo(isUnicode, infoValue, bufferLength, stringLength,
                                      narrowTerm, wideTerm);
            }

            case SQL_ODBC_INTERFACE_CONFORMANCE: {
                if (infoValue) {
                    *static_cast<SQLUINTEGER*>(infoValue) = SQL_OIC_CORE;
                }
                return SQL_SUCCESS;
            }

            case SQL_SEARCH_PATTERN_ESCAPE: {
                const char* narrowEscape = "\\";
                const wchar_t* wideEscape = L"\\";
                return HandleStringInfo(isUnicode, infoValue, bufferLength, stringLength,
                                      narrowEscape, wideEscape);
            }

            case SQL_SQL92_PREDICATES: {
                if (infoValue) {
                    SQLUINTEGER predicates = SQL_SP_EXISTS | SQL_SP_ISNOTNULL |
                                           SQL_SP_ISNULL | SQL_SP_MATCH_FULL |
                                           SQL_SP_MATCH_PARTIAL | SQL_SP_MATCH_UNIQUE_FULL |
                                           SQL_SP_MATCH_UNIQUE_PARTIAL | SQL_SP_OVERLAPS |
                                           SQL_SP_UNIQUE | SQL_SP_LIKE | SQL_SP_IN |
                                           SQL_SP_BETWEEN | SQL_SP_COMPARISON |
                                           SQL_SP_QUANTIFIED_COMPARISON;
                    *static_cast<SQLUINTEGER*>(infoValue) = predicates;
                }
                return SQL_SUCCESS;
            }

            case SQL_SQL92_RELATIONAL_JOIN_OPERATORS: {
                if (infoValue) {
                    SQLUINTEGER operators = SQL_SRJO_CROSS_JOIN | SQL_SRJO_EXCEPT_JOIN |
                                          SQL_SRJO_FULL_OUTER_JOIN | SQL_SRJO_INNER_JOIN |
                                          SQL_SRJO_INTERSECT_JOIN | SQL_SRJO_LEFT_OUTER_JOIN |
                                          SQL_SRJO_NATURAL_JOIN | SQL_SRJO_RIGHT_OUTER_JOIN |
                                          SQL_SRJO_UNION_JOIN;
                    *static_cast<SQLUINTEGER*>(infoValue) = operators;
                }
                return SQL_SUCCESS;
            }

            case SQL_SQL92_VALUE_EXPRESSIONS: {
                if (infoValue) {
                    SQLUINTEGER expressions = SQL_SVE_CASE | SQL_SVE_CAST |
                                            SQL_SVE_COALESCE | SQL_SVE_NULLIF;
                    *static_cast<SQLUINTEGER*>(infoValue) = expressions;
                }
                return SQL_SUCCESS;
            }

            case SQL_COLUMN_ALIAS: {
                if (infoValue && bufferLength > 0) {
                    if (isUnicode) {
                        *static_cast<wchar_t*>(infoValue) = L'Y';
                        if (stringLength) *stringLength = sizeof(wchar_t);
                    } else {
                        *static_cast<char*>(infoValue) = 'Y';
                        if (stringLength) *stringLength = 1;
                    }
                }
                return SQL_SUCCESS;
            }

            case SQL_GROUP_BY: {
                if (infoValue) {
                    *static_cast<SQLUSMALLINT*>(infoValue) = SQL_GB_GROUP_BY_EQUALS_SELECT;
                }
                return SQL_SUCCESS;
            }

            case SQL_CONVERT_BIGINT:
            case SQL_CONVERT_BINARY:
            case SQL_CONVERT_BIT:
            case SQL_CONVERT_CHAR:
            case SQL_CONVERT_DECIMAL:
            case SQL_CONVERT_DOUBLE:
            case SQL_CONVERT_FLOAT:
            case SQL_CONVERT_INTEGER:
            case SQL_CONVERT_LONGVARBINARY:
            case SQL_CONVERT_LONGVARCHAR:
            case SQL_CONVERT_NUMERIC:
            case SQL_CONVERT_REAL:
            case SQL_CONVERT_SMALLINT:
            case SQL_CONVERT_TIMESTAMP:
            case SQL_CONVERT_TINYINT:
            case SQL_CONVERT_DATE:
            case SQL_CONVERT_TIME:
            case SQL_CONVERT_VARBINARY:
            case SQL_CONVERT_VARCHAR: {
                if (infoValue) {
                    SQLUINTEGER converts = SQL_CVT_CHAR | SQL_CVT_NUMERIC |
                                         SQL_CVT_DECIMAL | SQL_CVT_INTEGER |
                                         SQL_CVT_SMALLINT | SQL_CVT_FLOAT |
                                         SQL_CVT_REAL | SQL_CVT_DOUBLE |
                                         SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                                         SQL_CVT_BIT | SQL_CVT_TINYINT |
                                         SQL_CVT_BIGINT | SQL_CVT_DATE |
                                         SQL_CVT_TIME | SQL_CVT_TIMESTAMP |
                                         SQL_CVT_BINARY | SQL_CVT_VARBINARY |
                                         SQL_CVT_LONGVARBINARY;
                    *static_cast<SQLUINTEGER*>(infoValue) = converts;
                }
                return SQL_SUCCESS;
            }

            case SQL_CONVERT_WCHAR:
            case SQL_CONVERT_WLONGVARCHAR:
            case SQL_CONVERT_WVARCHAR: {
                if (infoValue) {
                    SQLUINTEGER converts = SQL_CVT_WCHAR | SQL_CVT_WVARCHAR |
                                         SQL_CVT_WLONGVARCHAR;
                    *static_cast<SQLUINTEGER*>(infoValue) = converts;
                }
                return SQL_SUCCESS;
            }

            case SQL_SPECIAL_CHARACTERS: {
                const char* specialChars = "_#$@";
                const wchar_t* wideSpecialChars = L"_#$@";
                LOGF("SQL_SPECIAL_CHARACTERS: Reporting %s", specialChars);
                return HandleStringInfo(isUnicode, infoValue, bufferLength, stringLength,
                                      specialChars, wideSpecialChars);
            }

            default: {
                if (infoValue && bufferLength > 0) {
                    memset(infoValue, 0, bufferLength);
                }
                if (stringLength) {
                    *stringLength = 0;
                }
                return SQL_NO_DATA;
            }
        }
    } catch (const std::exception &e) {
        LOGF("Exception in SQLGetInfo_Internal: %s", e.what());
        return SQL_ERROR;
    }
}

extern "C" {
SQLRETURN SQL_API SQLGetInfo_W(
    SQLHDBC hdbc,
    SQLUSMALLINT infoType,
    SQLPOINTER infoValue,
    SQLSMALLINT bufferLength,
    SQLSMALLINT *stringLength) {
    LOGF("SQLGetInfo_W Entry - InfoType: %d (0x%04X)", infoType, infoType);
    SQLRETURN ret = SQLGetInfo_Internal(hdbc, infoType, infoValue, bufferLength, stringLength, true);
    LOGF("SQLGetInfo_W Exit - Return: %d", ret);
    return ret;
}

SQLRETURN SQL_API SQLGetInfo_A(
    SQLHDBC hdbc,
    SQLUSMALLINT infoType,
    SQLPOINTER infoValue,
    SQLSMALLINT bufferLength,
    SQLSMALLINT *stringLength) {
    LOGF("SQLGetInfo_A Entry - InfoType: %d (0x%04X)", infoType, infoType);
    SQLRETURN ret = SQLGetInfo_Internal(hdbc, infoType, infoValue, bufferLength, stringLength, false);
    LOGF("SQLGetInfo_A Exit - Return: %d", ret);
    return ret;
}

SQLRETURN SQL_API SQLGetFunctions(
    SQLHDBC hdbc,
    SQLUSMALLINT FunctionId,
    SQLUSMALLINT *Supported) {
    LOGF("SQLGetFunctions Entry - FunctionId: %d (0x%04X)", FunctionId, FunctionId);

    if (!hdbc || !Supported) {
        return SQL_ERROR;
    }

    // Handle SQL_API_ALL_FUNCTIONS (0)
    if (FunctionId == SQL_API_ALL_FUNCTIONS) {
        // Fill in the 100-element array
        memset(Supported, SQL_FALSE, sizeof(SQLUSMALLINT) * 100);

        // Essential core functions
        Supported[SQL_API_SQLALLOCHANDLE] = SQL_TRUE;
        Supported[SQL_API_SQLFREEHANDLE] = SQL_TRUE;

        // Connection functions
        Supported[SQL_API_SQLCONNECT] = SQL_TRUE;
        Supported[SQL_API_SQLDISCONNECT] = SQL_TRUE;
        Supported[SQL_API_SQLGETCONNECTATTR] = SQL_TRUE;

        // Statement functions
        Supported[SQL_API_SQLPREPARE] = SQL_TRUE;
        Supported[SQL_API_SQLEXECDIRECT] = SQL_TRUE;
        Supported[SQL_API_SQLEXECUTE] = SQL_TRUE;

        // Result set functions
        Supported[SQL_API_SQLFETCH] = SQL_TRUE;
        Supported[SQL_API_SQLGETDATA] = SQL_TRUE;
        Supported[SQL_API_SQLNUMRESULTCOLS] = SQL_TRUE;
        Supported[SQL_API_SQLROWCOUNT] = SQL_TRUE;
        Supported[SQL_API_SQLDESCRIBECOL] = SQL_TRUE;
        Supported[SQL_API_SQLCOLATTRIBUTE] = SQL_TRUE;

        // Metadata functions
        Supported[SQL_API_SQLGETINFO] = SQL_TRUE;
        Supported[SQL_API_SQLGETSTMTATTR] = SQL_TRUE;
        Supported[SQL_API_SQLGETDESCFIELD] = SQL_TRUE;
        Supported[SQL_API_SQLGETTYPEINFO] = SQL_TRUE;

        // Column binding
        Supported[SQL_API_SQLBINDCOL] = SQL_TRUE;

        // Transaction management
        Supported[SQL_API_SQLENDTRAN] = SQL_TRUE;

        LOGF("Reported all function support status");
        return SQL_SUCCESS;
    }

    // Handle individual function checks
    *Supported = SQL_FALSE;
    switch (FunctionId) {
        // Essential core functions
        case SQL_API_SQLALLOCHANDLE:
        case SQL_API_SQLFREEHANDLE:

        // Connection functions
        case SQL_API_SQLCONNECT:
        case SQL_API_SQLDISCONNECT:
        case SQL_API_SQLGETCONNECTATTR:

        // Statement functions
        case SQL_API_SQLPREPARE:
        case SQL_API_SQLEXECDIRECT:
        case SQL_API_SQLEXECUTE:

        // Result set functions
        case SQL_API_SQLFETCH:
        case SQL_API_SQLGETDATA:
        case SQL_API_SQLNUMRESULTCOLS:
        case SQL_API_SQLROWCOUNT:
        case SQL_API_SQLDESCRIBECOL:
        case SQL_API_SQLCOLATTRIBUTE:

        // Metadata functions
        case SQL_API_SQLGETINFO:
        case SQL_API_SQLGETSTMTATTR:
        case SQL_API_SQLGETDESCFIELD:
        case SQL_API_SQLGETTYPEINFO:

        // Column binding
        case SQL_API_SQLBINDCOL:

        // Transaction management
        case SQL_API_SQLENDTRAN:
            *Supported = SQL_TRUE;
            break;

        default:
            *Supported = SQL_FALSE;
    }

    LOGF("Function %d support status: %d", FunctionId, *Supported);
    return SQL_SUCCESS;
}
} // extern "C"
