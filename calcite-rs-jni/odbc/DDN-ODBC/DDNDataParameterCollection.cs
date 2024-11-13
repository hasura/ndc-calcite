using System.Collections;
using System.Data;

namespace DDN_ODBC;

public class DDNDataParameterCollection : IDataParameterCollection
{
    private readonly List<DDNDataParameter> _parameters = new();

    public object this[string parameterName]
    {
        get => _parameters.Find(p => p.ParameterName == parameterName);
        set
        {
            var index = _parameters.FindIndex(p => p.ParameterName == parameterName);
            if (index != -1)
                _parameters[index] = (DDNDataParameter)value;
            else
                _parameters.Add((DDNDataParameter)value);
        }
    }

    public object this[int index]
    {
        get => _parameters[index];
        set => _parameters[index] = (DDNDataParameter)value;
    }

    public bool Contains(string parameterName)
    {
        return _parameters.Exists(p => p.ParameterName == parameterName);
    }

    public int IndexOf(string parameterName)
    {
        return _parameters.FindIndex(p => p.ParameterName == parameterName);
    }

    public void RemoveAt(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index != -1)
            _parameters.RemoveAt(index);
    }

    public int Count => _parameters.Count;
    public bool IsReadOnly => false;
    public bool IsFixedSize => false;
    public bool IsSynchronized => false;
    public object SyncRoot => null;

    public int Add(object value)
    {
        _parameters.Add((DDNDataParameter)value);
        return _parameters.Count - 1;
    }

    public void Clear()
    {
        _parameters.Clear();
    }

    public bool Contains(object value)
    {
        return _parameters.Contains((DDNDataParameter)value);
    }

    public int IndexOf(object value)
    {
        return _parameters.IndexOf((DDNDataParameter)value);
    }

    public void Insert(int index, object value)
    {
        _parameters.Insert(index, (DDNDataParameter)value);
    }

    public void Remove(object value)
    {
        _parameters.Remove((DDNDataParameter)value);
    }

    public void RemoveAt(int index)
    {
        _parameters.RemoveAt(index);
    }

    public void CopyTo(Array array, int index)
    {
        _parameters.CopyTo((DDNDataParameter[])array, index);
    }

    public IEnumerator GetEnumerator()
    {
        return _parameters.GetEnumerator();
    }
}