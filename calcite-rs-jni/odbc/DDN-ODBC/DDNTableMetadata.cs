using Newtonsoft.Json;

namespace DDN_ODBC;

public class DDNTableMetadata
{
    [JsonProperty("tableSchem")] public string TABLE_SCHEM { get; set; }

    [JsonProperty("tableType")] public string TABLE_TYPE { get; set; }

    [JsonProperty("tableName")] public string TABLE_NAME { get; set; }

    [JsonProperty("tableCat")] public string TABLE_CAT { get; set; }

    [JsonProperty("remarks")] public string REMARKS { get; set; }
}