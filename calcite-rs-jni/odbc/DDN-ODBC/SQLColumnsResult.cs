using DDN_ODBC;
using System.Text.Json.Serialization;

public class SqlColumnsResult
{
    // ODBC SQL type constants from sql.h
    public const short SQL_UNKNOWN_TYPE = 0;
    public const short SQL_CHAR = 1;
    public const short SQL_NUMERIC = 2;
    public const short SQL_DECIMAL = 3;
    public const short SQL_INTEGER = 4;
    public const short SQL_SMALLINT = 5;
    public const short SQL_FLOAT = 6;
    public const short SQL_REAL = 7;
    public const short SQL_DOUBLE = 8;
    public const short SQL_DATETIME = 9;
    public const short SQL_VARCHAR = 12;
    public const short SQL_TYPE_DATE = 91;
    public const short SQL_TYPE_TIME = 92;
    public const short SQL_TYPE_TIMESTAMP = 93;
    public const short SQL_TYPE_TIMESTAMP_WITH_TIMEZONE = 95;
    public const short SQL_BIT = -7;
    public const short SQL_TINYINT = -6;
    public const short SQL_BIGINT = -5;
    public const short SQL_LONGVARCHAR = -1;
    public const short SQL_BINARY = -2;
    public const short SQL_VARBINARY = -3;
    public const short SQL_LONGVARBINARY = -4;
    public const short SQL_GUID = -11;
    public const short SQL_WCHAR = -8;
    public const short SQL_WVARCHAR = -9;
    public const short SQL_WLONGVARCHAR = -10;
    public const short SQL_ARRAY = 2003;

    // SQL datetime sub-type codes
    public const short SQL_CODE_DATE = 1;
    public const short SQL_CODE_TIME = 2;
    public const short SQL_CODE_TIMESTAMP = 3;
    public const short SQL_CODE_TIME_WITH_TIMEZONE = 4;
    public const short SQL_CODE_TIMESTAMP_WITH_TIMEZONE = 5;

    // Nullable constants
    public const int SQL_NO_NULLS = 0;
    public const int SQL_NULLABLE = 1;
    public const int SQL_NULLABLE_UNKNOWN = 2;

    // Properties matching exact ODBC SQLColumns result set
    public string? TABLE_CAT { get; set; }
    public string? TABLE_SCHEM { get; set; }
    public string? TABLE_NAME { get; set; }
    public string? COLUMN_NAME { get; set; }
    public short DATA_TYPE { get; set; }
    public string TYPE_NAME { get; set; } = string.Empty;
    public int COLUMN_SIZE { get; set; }
    public int BUFFER_LENGTH { get; set; }
    public short DECIMAL_DIGITS { get; set; }
    public short NUM_PREC_RADIX { get; set; }
    public int NULLABLE { get; set; }
    public string? REMARKS { get; set; }
    public string? COLUMN_DEF { get; set; }
    public short SQL_DATA_TYPE { get; set; }
    public short SQL_DATETIME_SUB { get; set; }
    public int CHAR_OCTET_LENGTH { get; set; }
    public int ORDINAL_POSITION { get; set; }
    public string IS_NULLABLE { get; set; } = string.Empty;
    public string? SCOPE_CATALOG { get; set; }
    public string? SCOPE_SCHEMA { get; set; }
    public string? SCOPE_TABLE { get; set; }
    public short? SOURCE_DATA_TYPE { get; set; }
    public string IS_AUTOINCREMENT { get; set; } = "NO";
    public string IS_GENERATEDCOLUMN { get; set; } = "NO";

    public SqlColumnsResult(DDNColumnMetadata metadata)
    {
        // First map the input type to ODBC type
        DATA_TYPE = MapDataType(metadata.DataType);
        
        // Then derive other type information from DATA_TYPE
        TYPE_NAME = GetTypeNameFromDataType(DATA_TYPE);
        
        // Basic metadata
        TABLE_CAT = metadata.TableCatalog;
        TABLE_SCHEM = metadata.TableSchema;
        TABLE_NAME = metadata.TableName;
        COLUMN_NAME = metadata.ColumnName;
        
        // Size and buffer information
        COLUMN_SIZE = metadata.ColumnSize == -1 ? GetDefaultColumnSize(TYPE_NAME) : metadata.ColumnSize;
        BUFFER_LENGTH = CalculateBufferLength(TYPE_NAME, COLUMN_SIZE);
        CHAR_OCTET_LENGTH = metadata.CharOctetLength;
        
        // Numeric information
        DECIMAL_DIGITS = (short)(metadata.DecimalDigits ?? 0);
        NUM_PREC_RADIX = (short)(metadata.NumPrecRadix);
        
        // Nullability
        NULLABLE = metadata.Nullable;
        IS_NULLABLE = MapIsNullable(metadata.IsNullable, metadata.Nullable);
        
        // Additional metadata
        REMARKS = metadata.Remarks;
        COLUMN_DEF = metadata.ColumnDefault;
        ORDINAL_POSITION = metadata.OrdinalPosition;
        
        // Type information
        (SQL_DATA_TYPE, SQL_DATETIME_SUB) = MapSqlDataType(DATA_TYPE);
        
        // Scope information
        SCOPE_CATALOG = metadata.ScopeCatalog;
        SCOPE_SCHEMA = metadata.ScopeSchema;
        SCOPE_TABLE = metadata.ScopeTable;
        SOURCE_DATA_TYPE = metadata.SourceDataType;
        
        // Generation information
        IS_AUTOINCREMENT = metadata.IsAutoincrement ?? "NO";
        IS_GENERATEDCOLUMN = metadata.IsGeneratedColumn ?? "NO";
    }

    private short MapDataType(int calciteType)
    {
        return calciteType switch
        {
            1 => SQL_BIT,              // BOOLEAN
            2 => SQL_TINYINT,          // TINYINT
            3 => SQL_SMALLINT,         // SMALLINT
            4 => SQL_INTEGER,          // INTEGER
            5 => SQL_BIGINT,           // BIGINT
            6 => SQL_FLOAT,            // FLOAT
            7 => SQL_REAL,             // REAL
            8 => SQL_DOUBLE,           // DOUBLE
            9 => SQL_DOUBLE,           // DECIMAL
            10 => SQL_TYPE_DATE,       // DATE
            11 => SQL_TYPE_TIME,       // TIME
            12 => SQL_VARCHAR,         // VARCHAR
            13 => SQL_CHAR,            // CHAR
            2000 => SQL_VARCHAR,       // ANY mapped to VARCHAR
            2003 => SQL_ARRAY,         // ARRAY
            _ => SQL_UNKNOWN_TYPE
        };
    }

    private (short sqlDataType, short sqlDatetimeSub) MapSqlDataType(short odbcType)
    {
        return odbcType switch
        {
            // Date/Time types
            SQL_TYPE_DATE => (SQL_DATETIME, SQL_CODE_DATE),
            SQL_TYPE_TIME => (SQL_DATETIME, SQL_CODE_TIME),
            SQL_TYPE_TIMESTAMP => (SQL_DATETIME, SQL_CODE_TIMESTAMP),
            SQL_TYPE_TIMESTAMP_WITH_TIMEZONE => (SQL_DATETIME, SQL_CODE_TIMESTAMP_WITH_TIMEZONE),

            // For non-datetime types, SQL_DATA_TYPE equals DATA_TYPE and no datetime subcode
            _ => (odbcType, 0)
        };
    }

    private string GetTypeNameFromDataType(short dataType)
    {
        return dataType switch
        {
            SQL_CHAR => "CHAR",
            SQL_VARCHAR => "VARCHAR",
            SQL_LONGVARCHAR => "LONGVARCHAR",
            SQL_WCHAR => "WCHAR",
            SQL_WVARCHAR => "WVARCHAR",
            SQL_WLONGVARCHAR => "WLONGVARCHAR",
            SQL_DECIMAL => "DECIMAL",
            SQL_NUMERIC => "NUMERIC",
            SQL_SMALLINT => "SMALLINT",
            SQL_INTEGER => "INTEGER",
            SQL_REAL => "REAL",
            SQL_FLOAT => "FLOAT",
            SQL_DOUBLE => "DOUBLE",
            SQL_BIT => "BIT",
            SQL_TINYINT => "TINYINT",
            SQL_BIGINT => "BIGINT",
            SQL_BINARY => "BINARY",
            SQL_VARBINARY => "VARBINARY",
            SQL_LONGVARBINARY => "LONGVARBINARY",
            SQL_TYPE_DATE => "DATE",
            SQL_TYPE_TIME => "TIME",
            SQL_TYPE_TIMESTAMP => "TIMESTAMP",
            SQL_TYPE_TIMESTAMP_WITH_TIMEZONE => "TIMESTAMP WITH TIME ZONE",
            SQL_GUID => "GUID",
            SQL_ARRAY => "ARRAY",
            _ => "VARCHAR"  // Default to VARCHAR for unknown types
        };
    }

    private int GetDefaultColumnSize(string sqlTypeName)
    {
        return sqlTypeName switch
        {
            "CHAR" => 1,
            "VARCHAR" => 255,
            "INTEGER" => 10,
            "BIGINT" => 19,
            "SMALLINT" => 5,
            "TINYINT" => 3,
            "DOUBLE" => 15,
            "FLOAT" => 7,
            "DECIMAL" => 18,
            "DATE" => 10,
            "TIME" => 8,
            "TIMESTAMP" => 23,
            "BIT" => 1,
            _ => 0
        };
    }

    private int CalculateBufferLength(string sqlTypeName, int columnSize)
    {
        return sqlTypeName switch
        {
            "CHAR" or "VARCHAR" => columnSize,
            "BINARY" or "VARBINARY" => columnSize,
            "INTEGER" => 4,
            "BIGINT" => 8,
            "SMALLINT" => 2,
            "TINYINT" => 1,
            "DOUBLE" => 8,
            "FLOAT" => 4,
            "DATE" => 6,
            "TIME" => 6,
            "TIMESTAMP" => 16,
            "BIT" => 1,
            _ => 0
        };
    }

    private string MapIsNullable(string? isNullable, int nullable)
    {
        if (isNullable == "NO" || nullable == SQL_NO_NULLS)
            return "NO";
        if (isNullable == "YES" || nullable == SQL_NULLABLE)
            return "YES";
        return "";  // For SQL_NULLABLE_UNKNOWN
    }

    // Helper method to create a list from array of ColumnMetadata
    public static List<SqlColumnsResult> CreateFromMetadataArray(List<DDNColumnMetadata> metadata)
    {
        return metadata.Select(m => new SqlColumnsResult(m)).ToList();
    }
}