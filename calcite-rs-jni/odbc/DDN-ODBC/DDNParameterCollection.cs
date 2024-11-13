using System.Collections;
using System.Data;
using System.Data.Common;

namespace DDN_ODBC;

public class DDNParameterCollection : DbParameterCollection
{
    private readonly List<DbParameter> _parameters = new();

    public override int Count => _parameters.Count;
    
    public override object SyncRoot => ((ICollection)_parameters).SyncRoot;

    public override int Add(object value)
    {
        if (value is not DbParameter parameter)
            throw new ArgumentException("Value must be a DbParameter");
        
        _parameters.Add(parameter);
        return _parameters.Count - 1;
    }

    public override void AddRange(Array values)
    {
        foreach (var value in values)
            Add(value);
    }

    public override void Clear() => _parameters.Clear();

    public override bool Contains(object value) => 
        value is DbParameter parameter && _parameters.Contains(parameter);

    public override bool Contains(string value) => 
        _parameters.Any(p => string.Equals(p.ParameterName, value, StringComparison.OrdinalIgnoreCase));

    public override void CopyTo(Array array, int index) => 
        ((ICollection)_parameters).CopyTo(array, index);

    public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();

    public override int IndexOf(object value) => 
        value is DbParameter parameter ? _parameters.IndexOf(parameter) : -1;

    public override int IndexOf(string parameterName) => 
        _parameters.FindIndex(p => string.Equals(p.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase));

    public override void Insert(int index, object value)
    {
        if (value is not DbParameter parameter)
            throw new ArgumentException("Value must be a DbParameter");
        _parameters.Insert(index, parameter);
    }

    public override void Remove(object value)
    {
        if (value is DbParameter parameter)
            _parameters.Remove(parameter);
    }

    public override void RemoveAt(int index) => _parameters.RemoveAt(index);

    public override void RemoveAt(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index >= 0)
            RemoveAt(index);
    }

    protected override DbParameter GetParameter(int index) => _parameters[index];

    protected override DbParameter GetParameter(string parameterName) =>
        _parameters.FirstOrDefault(p => string.Equals(p.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase))
        ?? throw new ArgumentException($"Parameter '{parameterName}' not found.", nameof(parameterName));

    protected override void SetParameter(int index, DbParameter value) => 
        _parameters[index] = value ?? throw new ArgumentNullException(nameof(value));

    protected override void SetParameter(string parameterName, DbParameter value)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));
        var index = IndexOf(parameterName);
        if (index >= 0)
            _parameters[index] = value;
        else
            Add(value);
    }
}