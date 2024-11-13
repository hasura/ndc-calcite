using System.Collections;
using System.Data;
using System.Data.Common;

namespace DDN_ODBC;

public class DDN_OdbcDataReader : DbDataReader
{
    private readonly CommandBehavior _behavior;
    private readonly DataTable _dataTable;
    private readonly DataTableReader _internalReader;
    private bool _isClosed;
    private bool _isDisposed;

    public DDN_OdbcDataReader(DataTable dataTable, CommandBehavior behavior)
    {
        _dataTable = dataTable ?? throw new ArgumentNullException(nameof(dataTable));
        _internalReader = dataTable.CreateDataReader();
        _behavior = behavior;
    }

    #region DbDataReader Property Implementations

    public override int Depth => _internalReader.Depth;

    public override int FieldCount => _internalReader.FieldCount;

    public override bool HasRows => _dataTable.Rows.Count > 0;

    public override bool IsClosed => _isClosed;

    public override int RecordsAffected => -1; // Always -1 for read-only driver

    public override int VisibleFieldCount => FieldCount;

    #endregion

    #region Data Access Methods

    public override bool GetBoolean(int ordinal)
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _internalReader.GetBoolean(ordinal);
    }

    public override byte GetByte(int ordinal)
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _internalReader.GetByte(ordinal);
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _internalReader.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
    }

    public override char GetChar(int ordinal)
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _internalReader.GetChar(ordinal);
    }

    public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _internalReader.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
    }

    public override string GetDataTypeName(int ordinal)
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _dataTable.Columns[ordinal].DataType.Name;
    }

    public override DateTime GetDateTime(int ordinal)
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _internalReader.GetDateTime(ordinal);
    }

    public override decimal GetDecimal(int ordinal)
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _internalReader.GetDecimal(ordinal);
    }

    public override double GetDouble(int ordinal)
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _internalReader.GetDouble(ordinal);
    }

    public override IEnumerator GetEnumerator()
    {
        throw new NotImplementedException();
    }

    public override Type GetFieldType(int ordinal)
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _dataTable.Columns[ordinal].DataType;
    }

    public override float GetFloat(int ordinal)
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _internalReader.GetFloat(ordinal);
    }

    public override Guid GetGuid(int ordinal)
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _internalReader.GetGuid(ordinal);
    }

    public override short GetInt16(int ordinal)
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _internalReader.GetInt16(ordinal);
    }

    public override int GetInt32(int ordinal)
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _internalReader.GetInt32(ordinal);
    }

    public override long GetInt64(int ordinal)
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _internalReader.GetInt64(ordinal);
    }

    public override string GetName(int ordinal)
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _dataTable.Columns[ordinal].ColumnName;
    }

    public override int GetOrdinal(string name)
    {
        ValidateNotDisposed();
        ValidateNotClosed();

        // Case-insensitive column search
        for (var i = 0; i < _dataTable.Columns.Count; i++)
            if (string.Equals(_dataTable.Columns[i].ColumnName, name, StringComparison.OrdinalIgnoreCase))
                return i;

        throw new IndexOutOfRangeException($"Column '{name}' not found.");
    }

    public override string GetString(int ordinal)
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _internalReader.GetString(ordinal);
    }

    public override object GetValue(int ordinal)
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _internalReader.GetValue(ordinal);
    }

    public override int GetValues(object[] values)
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _internalReader.GetValues(values);
    }

    public override bool IsDBNull(int ordinal)
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _internalReader.IsDBNull(ordinal);
    }

    #endregion

    #region Navigation Methods

    public override bool Read()
    {
        ValidateNotDisposed();
        ValidateNotClosed();

        if (_behavior.HasFlag(CommandBehavior.SingleRow) && _internalReader.RecordsAffected > 0)
            return false;

        return _internalReader.Read();
    }

    public override bool NextResult()
    {
        ValidateNotDisposed();
        ValidateNotClosed();
        return _internalReader.NextResult();
    }

    public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        ValidateNotDisposed();
        ValidateNotClosed();

        cancellationToken.ThrowIfCancellationRequested();

        if (_behavior.HasFlag(CommandBehavior.SingleRow) && _internalReader.RecordsAffected > 0)
            return false;

        return await Task.Run(() => _internalReader.Read(), cancellationToken);
    }

    public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
    {
        ValidateNotDisposed();
        ValidateNotClosed();

        return Task.FromResult(false); // We only support single result sets
    }

    #endregion

    #region Schema Methods

    public override DataTable GetSchemaTable()
    {
        ValidateNotDisposed();
        ValidateNotClosed();

        var schemaTable = new DataTable("SchemaTable");

        // Add schema columns
        schemaTable.Columns.Add("ColumnName", typeof(string));
        schemaTable.Columns.Add("ColumnOrdinal", typeof(int));
        schemaTable.Columns.Add("ColumnSize", typeof(int));
        schemaTable.Columns.Add("DataType", typeof(Type));
        schemaTable.Columns.Add("IsLong", typeof(bool));
        schemaTable.Columns.Add("IsReadOnly", typeof(bool));
        schemaTable.Columns.Add("IsUnique", typeof(bool));
        schemaTable.Columns.Add("IsKey", typeof(bool));
        schemaTable.Columns.Add("IsAutoIncrement", typeof(bool));
        schemaTable.Columns.Add("IsNullable", typeof(bool));
        schemaTable.Columns.Add("NumericPrecision", typeof(int));
        schemaTable.Columns.Add("NumericScale", typeof(int));

        // Populate schema information
        for (var i = 0; i < _dataTable.Columns.Count; i++)
        {
            var column = _dataTable.Columns[i];
            var row = schemaTable.NewRow();

            row["ColumnName"] = column.ColumnName;
            row["ColumnOrdinal"] = i;
            row["ColumnSize"] = GetColumnSize(column);
            row["DataType"] = column.DataType;
            row["IsLong"] = column.DataType == typeof(byte[]) || column.DataType == typeof(string);
            row["IsReadOnly"] = true; // Since this is a read-only driver
            row["IsUnique"] = column.Unique;
            row["IsKey"] = _dataTable.PrimaryKey.Contains(column);
            row["IsAutoIncrement"] = column.AutoIncrement;
            row["IsNullable"] = column.AllowDBNull;
            row["NumericPrecision"] = GetNumericPrecision(column);
            row["NumericScale"] = GetNumericScale(column);

            schemaTable.Rows.Add(row);
        }

        return schemaTable;
    }

    private int GetColumnSize(DataColumn column)
    {
        if (column.MaxLength > 0)
            return column.MaxLength;

        // Default sizes for common types
        return column.DataType.Name switch
        {
            "Boolean" => 1,
            "Byte" => 1,
            "Int16" => 2,
            "Int32" => 4,
            "Int64" => 8,
            "Single" => 4,
            "Double" => 8,
            "Decimal" => 16,
            "DateTime" => 8,
            "Guid" => 16,
            _ => 0
        };
    }

    private int GetNumericPrecision(DataColumn column)
    {
        return column.DataType.Name switch
        {
            "Byte" => 3,
            "Int16" => 5,
            "Int32" => 10,
            "Int64" => 19,
            "Single" => 7,
            "Double" => 15,
            "Decimal" => 28,
            _ => 0
        };
    }

    private int GetNumericScale(DataColumn column)
    {
        return column.DataType.Name switch
        {
            "Single" => 7,
            "Double" => 15,
            "Decimal" => 28,
            _ => 0
        };
    }

    #endregion

    #region IDisposable Implementation

    public override void Close()
    {
        if (!_isClosed)
        {
            _internalReader.Close();
            _isClosed = true;
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (!_isDisposed)
        {
            if (disposing)
            {
                Close();
                _internalReader.Dispose();
            }

            _isDisposed = true;
        }

        base.Dispose(disposing);
    }

    #endregion

    #region Validation Methods

    private void ValidateNotClosed()
    {
        if (_isClosed)
            throw new InvalidOperationException("DataReader is closed.");
    }

    private void ValidateNotDisposed()
    {
        if (_isDisposed)
            throw new ObjectDisposedException(GetType().Name);
    }

    private void ValidateOrdinal(int ordinal)
    {
        if (ordinal < 0 || ordinal >= FieldCount)
            throw new IndexOutOfRangeException($"Invalid ordinal {ordinal}. Must be between 0 and {FieldCount - 1}.");
    }

    #endregion

    #region Indexer Implementation

    public override object this[int ordinal]
    {
        get
        {
            ValidateNotDisposed();
            ValidateNotClosed();
            ValidateOrdinal(ordinal);
            return GetValue(ordinal);
        }
    }

    public override object this[string name]
    {
        get
        {
            ValidateNotDisposed();
            ValidateNotClosed();
            return GetValue(GetOrdinal(name));
        }
    }

    #endregion
}