using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Reflection;
using System.Text;
using Newtonsoft.Json;

namespace DDN_ODBC;

using System;
public class DDN_ODBC : DbConnection
{
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private readonly HttpClient _httpClient;
    private readonly int _javaAppPort;
    private readonly string _javaDDNSQLEngine;
    private string _connectionString;
    private Process _javaProcess;
    private ConnectionState _state = ConnectionState.Closed;

    public DDN_ODBC(string connectionString)
    {
        _connectionString = connectionString;
        _javaDDNSQLEngine = ExtractEmbeddedJar();
        _javaAppPort = GetRandomPort();
        _httpClient = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(30)
        };
    }

    private string ExtractEmbeddedJar()
    {
        var assembly = Assembly.GetExecutingAssembly();
        var resourceName = assembly.GetManifestResourceNames()
            .Single(str => str.EndsWith("sqlengine-1.0.0-jar-with-dependencies.jar"));

        // Create temp directory if it doesn't exist
        var tempPath = Path.Combine(Path.GetTempPath(), "DDN_ODBC");
        Directory.CreateDirectory(tempPath);

        var jarPath = Path.Combine(tempPath, "sqlengine-1.0.0-jar-with-dependencies.jar");
        
        // Only extract if it doesn't exist
        if (!File.Exists(jarPath))
        {
            using (var stream = assembly.GetManifestResourceStream(resourceName))
            using (var file = new FileStream(jarPath, FileMode.Create, FileAccess.Write))
            {
                stream.CopyTo(file);
            }
        }

        return jarPath;
    }
    
    #region IDisposable Implementation

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            try
            {
                // Cancel any pending operations
                _cancellationTokenSource.Cancel();

                // Close the connection if it's open
                if (_state != ConnectionState.Closed)
                    Close();

                // Dispose of managed resources
                _cancellationTokenSource.Dispose();
                _httpClient.Dispose();
                _javaProcess?.Dispose();
            }
            catch
            {
                // Log disposal errors if you have logging
            }

        base.Dispose(disposing);
    }

    #endregion

    #region Required DbConnection Implementation

    public override string ConnectionString
    {
        get => _connectionString;
        set
        {
            if (_state != ConnectionState.Closed)
                throw new InvalidOperationException("Cannot change connection string while connection is open.");
            _connectionString = value;
        }
    }

    public override string Database => string.Empty; // Or return actual database name if applicable

    public override string DataSource => $"http://localhost:{_javaAppPort}";

    public override string ServerVersion => "1.0.0"; // Return your actual server version

    public override ConnectionState State => _state;

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        throw new NotSupportedException("Transactions are not supported in read-only mode.");
    }

    public override void ChangeDatabase(string databaseName)
    {
        throw new NotSupportedException("Changing database is not supported.");
    }

    protected override DbCommand CreateDbCommand()
    {
        if (_state != ConnectionState.Open)
            throw new InvalidOperationException("Cannot create command on closed connection.");
        return new DDN_OdbcCommand(this);
    }

    #endregion

    #region Open/Close Implementation

    public override void Open()
    {
        if (_state == ConnectionState.Open)
            return;

        try
        {
            StartJavaApplication();

            // Wait for the server to start (with timeout)
            var startTime = DateTime.Now;
            var timeout = TimeSpan.FromSeconds(30);
            var success = false;

            while (DateTime.Now - startTime < timeout)
                try
                {
                    var response = GetRequest("/health");
                    if (response.Contains("OK")) // Adjust based on your health check response
                    {
                        success = true;
                        break;
                    }
                }
                catch
                {
                    // Server might not be ready yet
                    Thread.Sleep(100);
                }

            if (!success)
                throw new InvalidOperationException("Failed to start Java application server.");

            _state = ConnectionState.Open;
        }
        catch (Exception ex)
        {
            _state = ConnectionState.Closed;
            throw new InvalidOperationException("Failed to open connection.", ex);
        }
    }

    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        if (_state == ConnectionState.Open)
            return;

        try
        {
            await StartJavaApplicationAsync(cancellationToken);

            // Wait for the server to start (with timeout)
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken,
                _cancellationTokenSource.Token);

            var startTime = DateTime.Now;
            var timeout = TimeSpan.FromSeconds(30);
            var success = false;

            while (DateTime.Now - startTime < timeout)
                try
                {
                    linkedCts.Token.ThrowIfCancellationRequested();
                    var response = await GetRequestAsync("/health", linkedCts.Token);
                    if (response.Contains("OK")) // Adjust based on your health check response
                    {
                        success = true;
                        break;
                    }
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch
                {
                    // Server might not be ready yet
                    await Task.Delay(100, linkedCts.Token);
                }

            if (!success)
                throw new InvalidOperationException("Failed to start Java application server.");

            _state = ConnectionState.Open;
        }
        catch (Exception ex)
        {
            _state = ConnectionState.Closed;
            if (ex is OperationCanceledException)
                throw;
            throw new InvalidOperationException("Failed to open connection.", ex);
        }
    }

    public override void Close()
    {
        if (_state == ConnectionState.Closed)
            return;

        try
        {
            _cancellationTokenSource.Cancel();
            TerminateJavaApplication();
        }
        finally
        {
            _state = ConnectionState.Closed;
        }
    }

    #endregion

    #region HTTP Request Helpers

    private async Task<string> GetRequestAsync(string action, CancellationToken cancellationToken)
    {
        try
        {
            var uri = new Uri($"http://localhost:{_javaAppPort}{action}");
            var response = await _httpClient.GetAsync(uri, cancellationToken);
            response.EnsureSuccessStatusCode();
            return await response.Content.ReadAsStringAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to execute GET request to {action}", ex);
        }
    }

    private string GetRequest(string action)
    {
        try
        {
            var uri = new Uri($"http://localhost:{_javaAppPort}{action}");
            var response = _httpClient.GetAsync(uri).Result;
            response.EnsureSuccessStatusCode();
            return response.Content.ReadAsStringAsync().Result;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to execute GET request to {action}", ex);
        }
    }

    public async Task<string> PostRequestAsync(string action, string postData, CancellationToken cancellationToken)
    {
        try
        {
            var query = new SqlQuery
            {
                Sql = postData,
                DisallowMutations = true // Force read-only mode
            };

            var jsonString = JsonConvert.SerializeObject(query);
            var content = new StringContent(jsonString, Encoding.UTF8, "application/json");
            var uri = new Uri($"http://localhost:{_javaAppPort}{action}");

            var response = await _httpClient.PostAsync(uri, content, cancellationToken);
            response.EnsureSuccessStatusCode();
            return await response.Content.ReadAsStringAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to execute POST request to {action}", ex);
        }
    }

    public async Task<string> GetTablesAsync(string wherePhrase = "*", CancellationToken cancellationToken = default)
    {
        return await PostRequestAsync("/v1/sql", $"SELECT * FROM \"metadata\".TABLES {wherePhrase}", cancellationToken);
    }
    public async Task<string> GetColumnsAsync(string wherePhrase = "*", CancellationToken cancellationToken = default)
    {
        return await PostRequestAsync("/v1/sql", $"SELECT * FROM \"metadata\".COLUMNS {wherePhrase}", cancellationToken);
    }

    public string GetTables(DDNParameterCollection parameters)
    {
        string result = "";
        if (parameters.Count == 0)
        {
            result = "";
        }
        else
        {
            List<string> columns = new List<string>();
            foreach (DDNDataParameter parameter in parameters)
            {
                switch (parameter.ParameterName.Replace("@", "").ToUpper())
                {
                    case "CATALOG":
                        if (parameter.Value != DBNull.Value)
                        {
                            columns.Add($"tableCat = '{parameter.Value}'");
                        }
                        break;
                    case "SCHEMA":
                        if (parameter.Value != DBNull.Value)
                        {
                            columns.Add($"tableSchem LIKE '{parameter.Value}'");
                        }
                        break;
                    case "TABLE":
                        if (parameter.Value != DBNull.Value)
                        {
                            columns.Add($"tableName LIKE '{parameter.Value}'");
                        }

                        break;
                    case "TABLETYPE":
                        if (parameter.Value != DBNull.Value)
                        {
                            columns.Add($"tableType LIKE '{parameter.Value}'");
                        }

                        break;
                    default:
                        throw new InvalidOperationException($"Invalid parameter name: {parameter.ParameterName}");
                }
            }
            result = $"WHERE {string.Join(" AND ", columns)}";
        }

        var task = GetTablesAsync(result, _cancellationTokenSource.Token);
        try
        {
            return task.Result;
        }
        catch (AggregateException ae)
        {
            throw ae.InnerException ?? ae;
        }
    }

    public string GetColumns(DDNParameterCollection parameters)
    {
        string result = "";
        if (parameters.Count == 0)
        {
            result = "";
        }
        else
        {
            List<string> columns = new List<string>();
            foreach (DDNDataParameter parameter in parameters)
            {
                switch (parameter.ParameterName.Replace("@", "").ToUpper())
                {
                    case "CATALOG":
                        if (parameter.Value != DBNull.Value)
                        {
                            columns.Add($"tableCat = '{parameter.Value}'");
                        }
                        break;
                    case "SCHEMA":
                        if (parameter.Value != DBNull.Value)
                        {
                            columns.Add($"tableSchem LIKE '{parameter.Value}'");
                        }
                        break;
                    case "TABLE":
                        if (parameter.Value != DBNull.Value)
                        {
                            columns.Add($"tableName LIKE '{parameter.Value}'");
                        }

                        break;
                    case "COLUMN":
                        if (parameter.Value != DBNull.Value)
                        {
                            columns.Add($"columnName LIKE '{parameter.Value}'");
                        }

                        break;
                    default:
                        throw new InvalidOperationException($"Invalid parameter name: {parameter.ParameterName}");
                }
            }
            result = $"WHERE {string.Join(" AND ", columns)}";
        }
        var task = GetColumnsAsync(result, _cancellationTokenSource.Token);
        try
        {
            return task.Result;
        }
        catch (AggregateException ae)
        {
            throw ae.InnerException ?? ae;
        }
    }
    #endregion

    #region Java Process Management

    private void StartJavaApplication()
    {
        var javaHomePath = Environment.GetEnvironmentVariable("JAVA_HOME")
                           ?? throw new InvalidOperationException("JAVA_HOME environment variable is not set.");

        var javaExecutablePath = Path.Combine(javaHomePath, "bin", "java");

        var startInfo = new ProcessStartInfo
        {
            FileName = javaExecutablePath,
            Arguments = $"-classpath {_javaDDNSQLEngine} com.hasura.SQLHttpServer",
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };

        startInfo.EnvironmentVariables["PORT"] = _javaAppPort.ToString();
        startInfo.EnvironmentVariables["JDBC_URL"] = _connectionString;

        _javaProcess = new Process { StartInfo = startInfo };

        _javaProcess.OutputDataReceived += (sender, args) =>
        {
            if (args.Data != null)
                Debug.WriteLine($"[Java Process Output]: {args.Data}");
        };

        _javaProcess.ErrorDataReceived += (sender, args) =>
        {
            if (args.Data != null)
                Debug.WriteLine($"[Java Process Error]: {args.Data}");
        };

        _javaProcess.Start();
        _javaProcess.BeginOutputReadLine();
        _javaProcess.BeginErrorReadLine();
    }

    private async Task StartJavaApplicationAsync(CancellationToken cancellationToken)
    {
        await Task.Run(() => StartJavaApplication(), cancellationToken);
    }

    private void TerminateJavaApplication()
    {
        if (_javaProcess != null && !_javaProcess.HasExited)
            try
            {
                _javaProcess.Kill(true);
                _javaProcess.WaitForExit(5000);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error terminating Java process: {ex.Message}");
            }
    }

    private int GetRandomPort()
    {
        var random = new Random();
        return random.Next(8081, 65535);
    }

    #endregion
}

// Parameter Collection Implementation