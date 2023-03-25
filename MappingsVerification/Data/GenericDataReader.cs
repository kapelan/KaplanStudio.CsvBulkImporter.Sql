using System.Data;
using System.Reflection;

namespace BulkInsert.MappingsVerification.Data;

public class GenericDataReader<T> : IDataReader where T : class
{
    private readonly IList<SchemaFieldDef> _schema;
    private readonly IDictionary<string, int> _schemaMapping;
    private readonly IAsyncEnumerator<T> _asyncEnumerator;
    private readonly List<PropertyInfo> _fields = new List<PropertyInfo>();

    public GenericDataReader(IAsyncEnumerable<T> asyncEnumerable, IList<SchemaFieldDef> schema)
    {
        _schema = schema;
        _schemaMapping = _schema.Select((x, i) => new { x.FieldName, Index = i }).ToDictionary(x => x.FieldName, x => x.Index);
        _asyncEnumerator = asyncEnumerable.GetAsyncEnumerator();

        foreach (PropertyInfo fieldinfo in typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            _fields.Add(fieldinfo);
        }
    }

    public bool IsDBNull(int i)
    {
        throw new NotImplementedException();
    }

    public int FieldCount => _schema.Count;

    public int GetOrdinal(string name)
    {
        int ordinal;
        if (!_schemaMapping.TryGetValue(name, out ordinal))
            throw new InvalidOperationException("Unknown parameter name: " + name);
        return ordinal;
    }

    // public object GetValue(int i)
    // {
    //     if (_asyncEnumerator == null)
    //         throw new ObjectDisposedException(GetType().Name);
    //
    //     var value = _selector(_asyncEnumerator.Current, _schema[i].FieldName);
    //
    //     if (value == null)
    //         return DBNull.Value;
    //
    //     var strValue = value as string;
    //     if (strValue != null)
    //     {
    //         if (strValue.Length > _schema[i].Size && _schema[i].Size > 0)
    //             strValue = strValue.Substring(0, _schema[i].Size);
    //         if (_schema[i].DataType == DbType.String)
    //             return strValue;
    //         return SchemaFieldDef.StringToTypedValue(strValue, _schema[i].DataType) ?? DBNull.Value;
    //     }
    //
    //     return value;
    // }

    public object this[int i] => throw new NotImplementedException();

    public object this[string name] => throw new NotImplementedException();

    public void Dispose() { Close(); }
    

    public bool Read()
    {
        return _asyncEnumerator.MoveNextAsync().AsTask().Result;
    }

    public  int Depth => 1;

    public  bool IsClosed => _asyncEnumerator is null;
    public int RecordsAffected { get; }

    public bool NextResult()
    {
        return false;
    }

    public async void Close(){ await _asyncEnumerator.DisposeAsync(); }
    public DataTable? GetSchemaTable()
    {
        throw new NotImplementedException();
    }

    public bool GetBoolean(int i)
    {
        throw new NotImplementedException();
    }

    public byte GetByte(int i)
    {
        throw new NotImplementedException();
    }

    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
    {
        throw new NotImplementedException();
    }

    public char GetChar(int i)
    {
        throw new NotImplementedException();
    }

    public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
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
        throw new NotImplementedException();
    }

    public decimal GetDecimal(int i)
    {
        throw new NotImplementedException();
    }

    public double GetDouble(int i)
    {
        throw new NotImplementedException();
    }

    public Type GetFieldType(int i){ return _fields[i].PropertyType; }
    public float GetFloat(int i)
    {
        throw new NotImplementedException();
    }

    public Guid GetGuid(int i)
    {
        throw new NotImplementedException();
    }

    public short GetInt16(int i)
    {
        throw new NotImplementedException();
    }

    public int GetInt32(int i)
    {
        throw new NotImplementedException();
    }

    public long GetInt64(int i)
    {
        throw new NotImplementedException();
    }

    public string GetName(int i) { return _fields[i].Name; }

    public string GetString(int i)
    {
        throw new NotImplementedException();
    }

    public object GetValue(int i){ return _fields[i].GetValue(_asyncEnumerator.Current); }
    public int GetValues(object[] values)
    {
        throw new NotImplementedException();
    }
}   