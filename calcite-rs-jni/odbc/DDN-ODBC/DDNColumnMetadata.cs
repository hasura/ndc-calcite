namespace DDN_ODBC;
using Newtonsoft.Json;
public class DDNColumnMetadata
{
    [JsonProperty("tableCat")]
    public string? TableCatalog { get; set; }

    [JsonProperty("tableSchem")]
    public string? TableSchema { get; set; }

    [JsonProperty("tableName")]
    public string? TableName { get; set; }

    [JsonProperty("columnName")]
    public string? ColumnName { get; set; }

    [JsonProperty("dataType")]
    public int DataType { get; set; }

    [JsonProperty("typeName")]
    public string? TypeName { get; set; }

    [JsonProperty("columnSize")]
    public int ColumnSize { get; set; }

    [JsonProperty("bufferLength")]
    public int? BufferLength { get; set; }

    [JsonProperty("decimalDigits")]
    public int? DecimalDigits { get; set; }

    [JsonProperty("numPrecRadix")]
    public int NumPrecRadix { get; set; }

    [JsonProperty("nullable")]
    public int Nullable { get; set; }

    [JsonProperty("remarks")]
    public string? Remarks { get; set; }

    [JsonProperty("columnDef")]
    public string? ColumnDefault { get; set; }

    [JsonProperty("sqlDataType")]
    public int? SqlDataType { get; set; }

    [JsonProperty("sqlDatetimeSub")]
    public int? SqlDatetimeSub { get; set; }

    [JsonProperty("charOctetLength")]
    public int CharOctetLength { get; set; }

    [JsonProperty("ordinalPosition")]
    public int OrdinalPosition { get; set; }

    [JsonProperty("isNullable")]
    public string? IsNullable { get; set; }

    [JsonProperty("scopeCatalog")]
    public string? ScopeCatalog { get; set; }

    [JsonProperty("scopeSchema")]
    public string? ScopeSchema { get; set; }

    [JsonProperty("scopeTable")]
    public string? ScopeTable { get; set; }

    [JsonProperty("sourceDataType")]
    public short? SourceDataType { get; set; }

    [JsonProperty("isAutoincrement")]
    public string? IsAutoincrement { get; set; }

    [JsonProperty("isGeneratedcolumn")]
    public string? IsGeneratedColumn { get; set; }
}

// Usage example:
/*
using System.Text.Json;

// Deserialize JSON
var columnMetadata = JsonSerializer.Deserialize<ColumnMetadata>(jsonString);

// Get ODBC type information
short odbcType = columnMetadata.GetOdbcType();
string odbcTypeName = columnMetadata.GetOdbcTypeName();

// Check type categories
bool isString = columnMetadata.IsStringType();
bool isNumeric = columnMetadata.IsNumericType();
bool isTemporal = columnMetadata.IsTemporalType();

// Work with nullability
bool isNullable = columnMetadata.Nullable == ColumnMetadata.SQL_NULLABLE;
*/