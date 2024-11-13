using System.Text.RegularExpressions;

namespace DDN_ODBC;

public class CommandParser
{
    private static readonly Regex CallCommandRegex = new(@"{\s*CALL\s+(?<command>\w+)\s*\((?<params>[^)]*)\)\s*}", RegexOptions.Compiled | RegexOptions.IgnoreCase);
    private static readonly Regex SimpleCommandRegex = new(@"^(?<command>\w+)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);
    private static readonly Regex ParameterRegex = new(@"\?", RegexOptions.Compiled);

    private readonly HashSet<string> _validCommands;

    public CommandParser(IEnumerable<string> validCommands)
    {
        _validCommands = new HashSet<string>(validCommands, StringComparer.OrdinalIgnoreCase);
    }
    
    public CommandParser()
    {
        var validCommands = new List<string>
        {
            "SQLTables",
            "SQLColumns",
            "SQLProcedures",
            "SQLProcedureColumns",
            "SQLPrimaryKeys",
            "SQLForeignKeys",
            "SQLStatistics",
            "SQLSpecialColumns",
            "SQLGetTypeInfo",
            "SQLTablePrivileges",
            "SQLColumnPrivileges",
            "SQLGetFunctions"
        };
        _validCommands = new HashSet<string>(validCommands, StringComparer.OrdinalIgnoreCase);
    }

    public bool TryParseCommand(string commandText, out string command, out int parameterCount)
    {
        command = null;
        parameterCount = 0;

        // Try to match the CALL command format
        var match = CallCommandRegex.Match(commandText);
        if (match.Success)
        {
            command = match.Groups["command"].Value;
            if (!_validCommands.Contains(command))
            {
                return false;
            }

            string paramsGroup = match.Groups["params"].Value;
            parameterCount = 0;
            if (!string.IsNullOrEmpty(paramsGroup))
            {
                parameterCount = ParameterRegex.Matches(paramsGroup).Count;
            }

            return true;
        }

        // Try to match the simple command format
        match = SimpleCommandRegex.Match(commandText);
        if (match.Success)
        {
            command = match.Groups["command"].Value;
            return _validCommands.Contains(command);
        }

        // No match
        return false;
    }
}