using System.Data;
using System.Data.Common;

namespace DDN_ODBC;

public class DDNDataParameter : DbParameter
{
    private string _parameterName = string.Empty;
    private string _sourceColumn = string.Empty;
    private object? _value;
    private DbType _dbType = DbType.String;
    private ParameterDirection _direction = ParameterDirection.Input;  // Since read-only
    private int _size;

    public DDNDataParameter(string parameterName, DbType dbType)
    {
        ParameterName = parameterName;
        DbType = dbType;
    }

    public override DbType DbType
    {
        get => _dbType;
        set => _dbType = value;
    }

    public override ParameterDirection Direction
    {
        get => _direction;
        set
        {
            if (value != ParameterDirection.Input)
                throw new NotSupportedException("Only input parameters are supported in read-only mode.");
            _direction = value;
        }
    }

    public override string ParameterName
    {
        get => _parameterName;
        set => _parameterName = value ?? string.Empty;
    }

    public override string SourceColumn
    {
        get => _sourceColumn;
        set => _sourceColumn = value ?? string.Empty;
    }

    public override bool SourceColumnNullMapping { get; set; }
    
    public override DataRowVersion SourceVersion { get; set; } = DataRowVersion.Current;

    public override object? Value
    {
        get => _value;
        set => _value = value;
    }

    public override int Size
    {
        get => _size;
        set
        {
            if (value < 0)
                throw new ArgumentException("Size must be non-negative.", nameof(value));
            _size = value;
        }
    }

    public override bool IsNullable { get; set; }

    public override void ResetDbType() => _dbType = DbType.String;

    public override string ToString() => ParameterName;
}