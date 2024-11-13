using System.Collections;
using System.Data;
using System.Data.Common;
using Newtonsoft.Json.Linq;

namespace DDN_ODBC;

public class JsonDataReader : DbDataReader
{
    private readonly JArray _data;
    private readonly JObject[] _rows;
    private int _currentRow = -1;
    private readonly Dictionary<string, int> _columnMap;
    private bool _isClosed;
    private readonly Dictionary<string, Type> _columnTypes;

    public JsonDataReader(string jsonArrayString)
    {
        _data = JArray.Parse(jsonArrayString);
        _rows = []; //_data.ToArray();
        _columnMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        _columnTypes = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);
        
        if (_rows.Length > 0)
        {
            int index = 0;
            foreach (var prop in _rows[0].Properties())
            {
                _columnMap[prop.Name] = index++;
                var type = InferColumnType(prop.Name);
                _columnTypes[prop.Name] = type ?? typeof(object);
            }
        }
    }

    // Add the missing abstract members:
    public override object this[int ordinal] => GetValue(ordinal);
    
    public override object this[string name] => GetValue(GetOrdinal(name));

    public override string GetDataTypeName(int ordinal)
    {
        return GetFieldType(ordinal).Name;
    }

    public override IEnumerator GetEnumerator()
    {
        return new DbEnumerator(this);
    }

    // Rest of the implementation remains the same...
    private Type InferColumnType(string columnName)
    {
        foreach (var row in _rows)
        {
            if (row[columnName] != null && row[columnName].Type != JTokenType.Null)
            {
                switch (row[columnName].Type)
                {
                    case JTokenType.Integer:
                        return typeof(long);
                    case JTokenType.Float:
                        return typeof(double);
                    case JTokenType.String:
                        return typeof(string);
                    case JTokenType.Boolean:
                        return typeof(bool);
                    case JTokenType.Date:
                        return typeof(DateTime);
                    default:
                        return typeof(object);
                }
            }
        }
        return typeof(object);
    }

    public override bool Read()
    {
        if (_isClosed)
            throw new InvalidOperationException("DataReader is closed");
            
        _currentRow++;
        return _currentRow < _rows.Length;
    }

    public override int FieldCount => _columnMap.Count;

    public override string GetName(int ordinal)
    {
        if (ordinal < 0 || ordinal >= FieldCount)
            throw new IndexOutOfRangeException($"Invalid ordinal {ordinal}");
            
        return _columnMap.First(x => x.Value == ordinal).Key;
    }

    public override Type GetFieldType(int ordinal)
    {
        var name = GetName(ordinal);
        return _columnTypes[name];
    }

    public override object GetValue(int ordinal)
    {
        if (_currentRow == -1)
            throw new InvalidOperationException("Call Read() first");
        if (_currentRow >= _rows.Length)
            throw new InvalidOperationException("No more rows");
        if (ordinal < 0 || ordinal >= FieldCount)
            throw new IndexOutOfRangeException($"Invalid ordinal {ordinal}");

        var propertyName = GetName(ordinal);
        var token = _rows[_currentRow][propertyName];

        if (token == null || token.Type == JTokenType.Null)
            return DBNull.Value;

        var fieldType = GetFieldType(ordinal);
        try
        {
            return token.ToObject(fieldType);
        }
        catch
        {
            return DBNull.Value;
        }
    }

    public override int GetOrdinal(string name)
    {
        if (_columnMap.TryGetValue(name, out int ordinal))
            return ordinal;
        throw new IndexOutOfRangeException($"Column '{name}' not found");
    }

    public override bool IsDBNull(int ordinal)
    {
        var propertyName = GetName(ordinal);
        var token = _rows[_currentRow][propertyName];
        return token == null || token.Type == JTokenType.Null;
    }

    public override bool GetBoolean(int ordinal) => (bool)GetValue(ordinal);
    public override byte GetByte(int ordinal) => (byte)GetValue(ordinal);
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) 
        => throw new NotImplementedException();
    public override char GetChar(int ordinal) => (char)GetValue(ordinal);
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) 
        => throw new NotImplementedException();
    public override DateTime GetDateTime(int ordinal) => (DateTime)GetValue(ordinal);
    public override decimal GetDecimal(int ordinal) => (decimal)GetValue(ordinal);
    public override double GetDouble(int ordinal) => (double)GetValue(ordinal);
    public override float GetFloat(int ordinal) => (float)GetValue(ordinal);
    public override Guid GetGuid(int ordinal) => (Guid)GetValue(ordinal);
    public override short GetInt16(int ordinal) => (short)GetValue(ordinal);
    public override int GetInt32(int ordinal) => (int)GetValue(ordinal);
    public override long GetInt64(int ordinal) => (long)GetValue(ordinal);
    public override string GetString(int ordinal) => (string)GetValue(ordinal);

    public override int GetValues(object[] values)
    {
        var count = Math.Min(values.Length, FieldCount);
        for (int i = 0; i < count; i++)
        {
            values[i] = GetValue(i);
        }
        return count;
    }

    public override bool IsClosed => _isClosed;
    public override bool HasRows => _rows.Length > 0;
    public override int Depth => 0;
    public override int RecordsAffected => -1;

    public override void Close()
    {
        _isClosed = true;
    }

    public override bool NextResult()
    {
        return false;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Close();
        }
        base.Dispose(disposing);
    }
}