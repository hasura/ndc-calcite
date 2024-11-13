using Newtonsoft.Json;

namespace DDN_ODBC;

public class SqlQuery
{
    [JsonProperty("sql")] public string Sql { get; set; }

    [JsonProperty("disallowMutations")] public bool DisallowMutations { get; set; }
}