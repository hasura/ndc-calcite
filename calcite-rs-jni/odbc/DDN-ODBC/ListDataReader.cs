using System.Collections;
using System.ComponentModel;
using System.Data;

namespace DDN_ODBC;

public class ListDataReader<T> : IDataReader
{
    private readonly IEnumerator<T> _enumerator;
    private readonly IList<T> _list;
    private readonly Dictionary<string, int> _nameToIndexMap;
    private readonly PropertyDescriptorCollection _properties;
    private int _currentIndex = -1;

    public ListDataReader(IList<T> list)
    {
        _list = list ?? throw new ArgumentNullException(nameof(list));

        _enumerator = _list.GetEnumerator();
        _properties = TypeDescriptor.GetProperties(typeof(T));
        _nameToIndexMap = new Dictionary<string, int>();

        for (var i = 0; i < _properties.Count; i++) _nameToIndexMap[_properties[i].Name] = i;
    }

    public int FieldCount => _properties.Count;

    public bool Read()
    {
        return _enumerator.MoveNext();
    }

    public object GetValue(int i)
    {
        return _properties[i].GetValue(_enumerator.Current) ?? DBNull.Value;
    }

    public int GetOrdinal(string name)
    {
        return _nameToIndexMap[name];
    }

    public string GetName(int i)
    {
        return _properties[i].Name;
    }

    public Type GetFieldType(int i)
    {
        return _properties[i].PropertyType;
    }

    // Other methods in IDataReader can throw NotImplementedException or be properly implemented if needed
    public bool IsDBNull(int i)
    {
        return GetValue(i) == DBNull.Value;
    }

    public object this[int i] => GetValue(i);
    public object this[string name] => GetValue(GetOrdinal(name));
    public int Depth => throw new NotImplementedException();
    public bool IsClosed => throw new NotImplementedException();
    public int RecordsAffected => throw new NotImplementedException();

    public void Close()
    {
    }

    public DataTable GetSchemaTable()
    {
        throw new NotImplementedException();
    }

    public bool NextResult()
    {
        throw new NotImplementedException();
    }

    public int GetValues(object[] values)
    {
        throw new NotImplementedException();
    }

    public bool GetBoolean(int i)
    {
        return (bool)GetValue(i);
    }

    public byte GetByte(int i)
    {
        return (byte)GetValue(i);
    }

    public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
    {
        throw new NotImplementedException();
    }

    public char GetChar(int i)
    {
        return (char)GetValue(i);
    }

    public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
    {
        throw new NotImplementedException();
    }

    public IDataReader GetData(int i)
    {
        throw new NotImplementedException();
    }

    public string GetDataTypeName(int i)
    {
        throw new NotImplementedException();
    }

    public DateTime GetDateTime(int i)
    {
        return (DateTime)GetValue(i);
    }

    public decimal GetDecimal(int i)
    {
        return (decimal)GetValue(i);
    }

    public double GetDouble(int i)
    {
        return (double)GetValue(i);
    }

    public float GetFloat(int i)
    {
        return (float)GetValue(i);
    }

    public Guid GetGuid(int i)
    {
        return (Guid)GetValue(i);
    }

    public short GetInt16(int i)
    {
        return (short)GetValue(i);
    }

    public int GetInt32(int i)
    {
        return (int)GetValue(i);
    }

    public long GetInt64(int i)
    {
        return (long)GetValue(i);
    }

    public string GetString(int i)
    {
        return (string)GetValue(i);
    }

    public void Dispose()
    {
        _enumerator.Dispose();
    }

    public IDataReader GetDataTypeReader()
    {
        throw new NotImplementedException();
    }

    public IEnumerator GetEnumerator()
    {
        throw new NotImplementedException();
    }
}