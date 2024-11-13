using System.Data;
using System.Data.Common;
using Newtonsoft.Json;

namespace DDN_ODBC;

public class DDN_OdbcCommand : DbCommand
{
    private readonly DDN_ODBC _connection;
    private readonly DDNParameterCollection _parameters;
    private string _commandText;
    private int _commandTimeout = 30; // Default 30 seconds
    private CommandType _commandType = CommandType.Text;
    private bool _isDisposed;
    private UpdateRowSource _updatedRowSource = UpdateRowSource.None;
    private int _parametersCount;
    private CommandParser parser = new CommandParser();

    public DDN_OdbcCommand(DDN_ODBC connection)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _parameters = new DDNParameterCollection();
    }

    #region IDisposable Implementation

    protected override void Dispose(bool disposing)
    {
        if (!_isDisposed)
        {
            if (disposing)
                // Clean up managed resources
                _commandText = null;
            _isDisposed = true;
        }

        base.Dispose(disposing);
    }

    #endregion

    #region DbCommand Property Implementations

    public override string CommandText
    {
        get => _commandText;
        set
        {
            ValidateNotDisposed();
            _commandText = value;
        }
    }

    public override int CommandTimeout
    {
        get => _commandTimeout;
        set
        {
            ValidateNotDisposed();
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), "Timeout must be non-negative.");
            _commandTimeout = value;
        }
    }

    public override CommandType CommandType
    {
        get => _commandType;
        set
        {
            ValidateNotDisposed();
            if (!Enum.IsDefined(typeof(CommandType), value))
                throw new ArgumentOutOfRangeException(nameof(value));
            _commandType = value;
        }
    }

    public override UpdateRowSource UpdatedRowSource
    {
        get => _updatedRowSource;
        set
        {
            ValidateNotDisposed();
            if (!Enum.IsDefined(typeof(UpdateRowSource), value))
                throw new ArgumentOutOfRangeException(nameof(value));
            _updatedRowSource = value;
        }
    }

    protected override DbConnection DbConnection
    {
        get => _connection;
        set => throw new NotSupportedException("Changing the connection is not supported.");
    }

    protected override DbParameterCollection DbParameterCollection => _parameters;

    protected override DbTransaction DbTransaction
    {
        get => null;
        set => throw new NotSupportedException("Transactions are not supported in read-only mode.");
    }

    public override bool DesignTimeVisible { get; set; }

    #endregion

    #region Command Execution Methods

    public override void Cancel()
    {
        ValidateNotDisposed();
        throw new NotSupportedException("Command cancellation is not supported.");
    }

    protected override DbParameter CreateDbParameter()
    {
        throw new NotImplementedException();
    }

    public override int ExecuteNonQuery()
    {
        ValidateNotDisposed();
        throw new NotSupportedException("This is a read-only driver.");
    }

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        ValidateNotDisposed();
        throw new NotSupportedException("This is a read-only driver.");
    }

    public override object ExecuteScalar()
    {
        ValidateNotDisposed();
        using var reader = ExecuteDbDataReader(CommandBehavior.SingleRow);
        return reader.Read() ? reader.GetValue(0) : null;
    }

    public override async Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        ValidateNotDisposed();
        await using var reader = await ExecuteDbDataReaderAsync(CommandBehavior.SingleRow, cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? reader.GetValue(0) : null;
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        ValidateNotDisposed();
        ValidateConnection();

        try
        {
            return ExecuteReaderInternal(behavior);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("Failed to execute reader.", ex);
        }
    }

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior,
        CancellationToken cancellationToken)
    {
        ValidateNotDisposed();
        ValidateConnection();

        try
        {
            return await ExecuteReaderInternalAsync(behavior);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new InvalidOperationException("Failed to execute reader.", ex);
        }
    }
    
    public string GetTypeInfo(DDNParameterCollection parameters)
{
    var types = new[]
    {
        new {
            TYPE_NAME = "VARCHAR",
            DATA_TYPE = (short)12,        // SQL_VARCHAR
            COLUMN_SIZE = 2147483647,     
            LITERAL_PREFIX = "'",
            LITERAL_SUFFIX = "'",
            CREATE_PARAMS = (string)"max length",  // Explicitly typed as string
            NULLABLE = (short)1,          
            CASE_SENSITIVE = true,
            SEARCHABLE = (short)3,        
            UNSIGNED_ATTRIBUTE = false,
            FIXED_PREC_SCALE = false,
            AUTO_UNIQUE_VALUE = false,
            LOCAL_TYPE_NAME = (string)"",   // Empty string instead of null
            MINIMUM_SCALE = (short)0,
            MAXIMUM_SCALE = (short)0,
            SQL_DATA_TYPE = (short)12,    
            SQL_DATETIME_SUB = (short)0,
            NUM_PREC_RADIX = 0
        },
        new {
            TYPE_NAME = "INTEGER",
            DATA_TYPE = (short)4,         
            COLUMN_SIZE = 10,             
            LITERAL_PREFIX = "",
            LITERAL_SUFFIX = "",
            CREATE_PARAMS = (string)"",     // Empty string instead of null
            NULLABLE = (short)1,
            CASE_SENSITIVE = false,
            SEARCHABLE = (short)3,
            UNSIGNED_ATTRIBUTE = false,
            FIXED_PREC_SCALE = false,
            AUTO_UNIQUE_VALUE = false,
            LOCAL_TYPE_NAME = (string)"",   // Empty string instead of null
            MINIMUM_SCALE = (short)0,
            MAXIMUM_SCALE = (short)0,
            SQL_DATA_TYPE = (short)4,
            SQL_DATETIME_SUB = (short)0,
            NUM_PREC_RADIX = 10
        },
        new {
            TYPE_NAME = "REAL",
            DATA_TYPE = (short)7,         
            COLUMN_SIZE = 15,             
            LITERAL_PREFIX = "",
            LITERAL_SUFFIX = "",
            CREATE_PARAMS = (string)"",     // Empty string instead of null
            NULLABLE = (short)1,
            CASE_SENSITIVE = false,
            SEARCHABLE = (short)3,
            UNSIGNED_ATTRIBUTE = false,
            FIXED_PREC_SCALE = false,
            AUTO_UNIQUE_VALUE = false,
            LOCAL_TYPE_NAME = (string)"",   // Empty string instead of null
            MINIMUM_SCALE = (short)0,
            MAXIMUM_SCALE = (short)0,
            SQL_DATA_TYPE = (short)7,
            SQL_DATETIME_SUB = (short)0,
            NUM_PREC_RADIX = 2
        },
        new {
            TYPE_NAME = "TIMESTAMP",
            DATA_TYPE = (short)93,        
            COLUMN_SIZE = 23,             
            LITERAL_PREFIX = "'",
            LITERAL_SUFFIX = "'",
            CREATE_PARAMS = (string)"",     // Empty string instead of null
            NULLABLE = (short)1,
            CASE_SENSITIVE = false,
            SEARCHABLE = (short)3,
            UNSIGNED_ATTRIBUTE = false,
            FIXED_PREC_SCALE = false,
            AUTO_UNIQUE_VALUE = false,
            LOCAL_TYPE_NAME = (string)"",   // Empty string instead of null
            MINIMUM_SCALE = (short)0,
            MAXIMUM_SCALE = (short)3,
            SQL_DATA_TYPE = (short)9,
            SQL_DATETIME_SUB = (short)3,
            NUM_PREC_RADIX = 0
        }
    };

    return JsonConvert.SerializeObject(types);
}

    public override void Prepare()
    {
        ValidateNotDisposed();
        // No preparation needed for this implementation
    }

    #endregion

    #region Private Helper Methods

    private async Task<DbDataReader> ExecuteReaderInternalAsync(CommandBehavior behavior)
    {
        parser.TryParseCommand(_commandText, out var command, out _parametersCount);
        // Handle special commands
        switch (command?.ToUpperInvariant())
        {
            case "SQLTABLES":
                var tables = JsonConvert.DeserializeObject<List<DDNTableMetadata>>(_connection.GetTables(_parameters));
                return CreateTableReader(tables);

            case "SQLCOLUMNS":
                var columns =
                    JsonConvert.DeserializeObject<List<DDNColumnMetadata>>(_connection.GetColumns(_parameters));
                var sqlColumns = SqlColumnsResult.CreateFromMetadataArray(columns);
                return CreateTableReader(sqlColumns);
            
            case "SQLFOREIGNKEY":
            case "SQLSTATISTICS":
            case "SQLSPECIALCOLUMNS":
            case "SQLTABLEPRIVILEGES":
            case "SQLCOLUMNPRIVILEGES":
            case "SQLGETFUNCTIONS":
            case "SQLPRIMARYKEYS":
                return CreateDataReader(GetPrimaryKeys(_parameters), behavior);

            case "SQLGETTYPEINFO":
                return CreateDataReader(GetTypeInfo(_parameters), behavior);

            default:
                if (_commandText?.ToUpperInvariant().StartsWith("SQL") ?? false)
                {
                    throw new InvalidOperationException($"{_commandText} is not a supported SQL command.");
                }
                // Handle regular SQL queries
                if (string.IsNullOrWhiteSpace(_commandText))
                    throw new InvalidOperationException("CommandText cannot be null or empty.");

                var result = ExecuteSqlQuery(_commandText);
                return CreateDataReader(result, behavior);
        }
    }

    private DbDataReader ExecuteReaderInternal(CommandBehavior behavior)
    {
        return ExecuteReaderInternalAsync(behavior).Result;
    }

    private string ExecuteSqlQuery(string sql)
    {
        return ExecuteSqlQueryAsync(sql).Result;
    }

    private string GetPrimaryKeys(DDNParameterCollection parameters)
    {
        // Create an empty JSON array with the correct schema
        var emptyResult = new[]
        {
            new
            {
                TABLE_CAT = (string)null, // Catalog name
                TABLE_SCHEM = (string)null, // Schema name
                TABLE_NAME = "", // Table name
                COLUMN_NAME = "", // Column name
                KEY_SEQ = 0, // Sequence number in primary key
                PK_NAME = (string)null // Primary key name
            }
        }.Where(x => false); // Empty result set

        return JsonConvert.SerializeObject(emptyResult);
    }

    private async Task<string> ExecuteSqlQueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        ValidateReadOnlyQuery(sql);

        try
        {
            var response = await _connection.PostRequestAsync("/v1/sql", sql, cancellationToken);
            return response;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new InvalidOperationException($"Failed to execute query: {sql}", ex);
        }
    }

    private DbDataReader CreateDataReader(string jsonResult, CommandBehavior behavior)
    {
        // Convert JSON result to appropriate DataReader implementation
        // This is just an example - implement according to your needs
        try
        {
            var data = JsonConvert.DeserializeObject<DataTable>(jsonResult);
            return new DDN_OdbcDataReader(data, behavior);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("Failed to create data reader from result.", ex);
        }
    }

    private DbDataReader CreateTableReader<T>(List<T> items)
    {
        return new DDN_OdbcDataReader(ConvertToDataTable(items), CommandBehavior.Default);
    }

    private static DataTable ConvertToDataTable<T>(IEnumerable<T> data)
    {
        var table = new DataTable();
        var properties = typeof(T).GetProperties();

        foreach (var prop in properties)
            table.Columns.Add(prop.Name, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);

        foreach (var item in data)
        {
            var row = table.NewRow();
            foreach (var prop in properties) row[prop.Name] = prop.GetValue(item) ?? DBNull.Value;
            table.Rows.Add(row);
        }

        return table;
    }

    private void ValidateConnection()
    {
        if (_connection == null)
            throw new InvalidOperationException("Connection not set.");

        if (_connection.State != ConnectionState.Open)
            throw new InvalidOperationException("Connection must be open before executing a command.");
    }

    private void ValidateNotDisposed()
    {
        if (_isDisposed)
            throw new ObjectDisposedException(GetType().Name);
    }

    private void ValidateReadOnlyQuery(string sql)
    {
        // Basic SQL validation to ensure read-only operations
        var normalizedSql = sql.Trim().ToUpperInvariant();

        // Check for write operations
        var writeOperations = new[] { "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE", "MERGE" };
        if (writeOperations.Any(op => normalizedSql.StartsWith(op + " ")))
            throw new NotSupportedException("This is a read-only driver. Write operations are not supported.");
    }

    #endregion
}