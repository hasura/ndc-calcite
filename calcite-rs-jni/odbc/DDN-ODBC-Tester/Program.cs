using System.Data;
using System.Data.Odbc;

namespace DDN_ODBC_Tester;

internal class Program
{
    private const string DSN_STRING = @"DSN=test5x64;";

    private const string SQL_CONNECTION_STRING =
        @"Driver={DDN-ODBC-Driver-x64};Server=192.168.86.47;Port=3280;Database=graphql;Role=admin;Timeout=120";

    private static void Main()
    {
        try
        {
            using var conn = new OdbcConnection(SQL_CONNECTION_STRING);
            Console.WriteLine("Testing connection...");
            conn.Open();
            Console.WriteLine("Connected successfully.\n");

            // Test 1: Driver Information
            // TestDriverInfo(conn);
            
            OdbcDirectTester.TestColumnAttributes(conn);

            // Test 2: Schema Information
            // TestSchemaInfo(conn);

            // Test 3: Table Information
            // TestTableInfo(conn);

            // Test 4: Column Information
            // TestColumnInfo(conn);

            // Test 5: Data Type Information
            // TestDataTypeInfo(conn);

            // Test 6: Basic Query Execution
            // TestBasicQueries(conn);

            // Test 7: Parameterized Queries
            // TestParameterizedQueries(conn);

            // Test 8: Metadata Retrieval
            // TestMetadataRetrieval(conn);

            // Test 9: Transaction Support
            // TestTransactionSupport(conn);

            // Test 10: Cursor Behavior
            // TestCursorBehavior(conn);

            Console.WriteLine("\nAll tests completed successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
            if (ex.InnerException != null)
                Console.WriteLine($"Inner: {ex.InnerException.Message}");
        }
    }

    private static void TestDriverInfo(OdbcConnection conn)
    {
        Console.WriteLine("\n=== Driver Information ===");
        var info = new Dictionary<string, string>
        {
            ["DriverName"] = conn.Driver,
            ["DriverVersion"] = conn.ServerVersion,
            ["DataSource"] = conn.DataSource,
            ["Database"] = conn.Database
        };

        foreach (var item in info) Console.WriteLine($"{item.Key}: {item.Value}");
    }

    private static void TestSchemaInfo(OdbcConnection conn)
    {
        Console.WriteLine("\n=== Schema Collections ===");
        var collections = conn.GetSchema();
        foreach (DataRow row in collections.Rows) Console.WriteLine($"Schema collection: {row["CollectionName"]}");
    }

    private static void TestTableInfo(OdbcConnection conn)
    {
        Console.WriteLine("\n=== Tables Information ===");
        var tables = conn.GetSchema("Tables");
        foreach (DataRow row in tables.Rows)
            Console.WriteLine($"Table: {row["TABLE_SCHEM"]}.{row["TABLE_NAME"]} ({row["TABLE_TYPE"]})");
    }

    private static void TestColumnInfo(OdbcConnection conn)
    {
        Console.WriteLine("\n=== Column Information ===");
        try
        {
            var tables = conn.GetSchema("Tables");
            if (tables.Rows.Count > 0)
                foreach (DataRow tableRow in tables.Rows)
                {
                    var schemaName = tableRow["TABLE_SCHEM"].ToString();
                    var tableName = tableRow["TABLE_NAME"].ToString();

                    Console.WriteLine($"\nDetailed column analysis for table: {schemaName}.{tableName}");

                    // Create a query to get one row of data
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = $"SELECT * FROM \"{schemaName}\".\"{tableName}\" LIMIT 1";

                    using var reader = cmd.ExecuteReader();
                    var schemaTable = reader.GetSchemaTable();

                    if (schemaTable != null)
                        foreach (DataRow row in schemaTable.Rows)
                        {
                            Console.WriteLine($"\nColumn: {row["ColumnName"]}");
                            Console.WriteLine($"  SQL Type Name: {row["DataTypeName"]}"); // SQL_DESC_TYPE_NAME
                            Console.WriteLine($"  Column Size: {row["ColumnSize"]}"); // SQL_DESC_LENGTH
                            Console.WriteLine($"  Numeric Precision: {row["NumericPrecision"]}");
                            Console.WriteLine($"  Numeric Scale: {row["NumericScale"]}");
                            Console.WriteLine($"  Is Nullable: {row["AllowDBNull"]}"); // SQL_DESC_NULLABLE
                            Console.WriteLine($"  Is Read Only: {row["IsReadOnly"]}");
                            Console.WriteLine($"  Is Key: {row["IsKey"]}");
                            Console.WriteLine($"  Base Schema Name: {row["BaseSchemaName"]}");
                            Console.WriteLine($"  Base Table Name: {row["BaseTableName"]}");
                            Console.WriteLine($"  Base Column Name: {row["BaseColumnName"]}");

                            // Print all available schema information
                            Console.WriteLine("\n  All available schema information:");
                            foreach (DataColumn col in schemaTable.Columns)
                                if (row[col] != DBNull.Value)
                                    Console.WriteLine($"    {col.ColumnName}: {row[col]}");
                        }
                }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error while retrieving column information: {ex.Message}");
            if (ex.InnerException != null)
                Console.WriteLine($"Inner Exception: {ex.InnerException.Message}");
        }
    }

    private static void TestDataTypeInfo(OdbcConnection conn)
    {
        Console.WriteLine("\n=== Data Type Information ===");
        var typeInfo = conn.GetSchema("DataTypes");
        foreach (DataRow row in typeInfo.Rows)
            Console.WriteLine(
                $"Type: {row["TypeName"]}, Provider DbType: {row["ProviderDbType"]}, Framework Type: {row["DataType"]}");
    }

    private static void TestBasicQueries(OdbcConnection conn)
    {
        Console.WriteLine("\n=== Basic Query Tests ===");
        // Get first table from previous test
        var tables = conn.GetSchema("Tables");
        if (tables.Rows.Count > 0)
        {
            var tableName = tables.Rows[0]["TABLE_NAME"].ToString();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT * FROM \"{tableName}\" LIMIT 5";
            try
            {
                using var reader = cmd.ExecuteReader();
                var columns = Enumerable.Range(0, reader.FieldCount)
                    .Select(i => reader.GetName(i));
                Console.WriteLine(string.Join("\t", columns));

                while (reader.Read())
                {
                    var values = Enumerable.Range(0, reader.FieldCount)
                        .Select(i => reader[i]?.ToString() ?? "NULL");
                    Console.WriteLine(string.Join("\t", values));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Parameterized query not supported: {ex.Message}");
            }
        }
    }

    private static void TestParameterizedQueries(OdbcConnection conn)
    {
        Console.WriteLine("\n=== Parameterized Query Tests ===");
        // Get first table from previous test
        var tables = conn.GetSchema("Tables");
        if (tables.Rows.Count > 0)
        {
            var tableName = tables.Rows[0]["TABLE_NAME"].ToString();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT * FROM {tableName} WHERE 1=?";
            cmd.Parameters.Add("@p1", OdbcType.Int).Value = 1;

            try
            {
                using var reader = cmd.ExecuteReader();
                var columns = Enumerable.Range(0, reader.FieldCount)
                    .Select(i => reader.GetName(i));
                Console.WriteLine(string.Join("\t", columns));

                while (reader.Read())
                {
                    var values = Enumerable.Range(0, reader.FieldCount)
                        .Select(i => reader[i]?.ToString() ?? "NULL");
                    Console.WriteLine(string.Join("\t", values));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Parameterized query not supported: {ex.Message}");
            }
        }
    }

    private static void TestMetadataRetrieval(OdbcConnection conn)
    {
        Console.WriteLine("\n=== Testing Metadata Retrieval ===");
        var tables = conn.GetSchema("Tables");
        if (tables.Rows.Count > 0)
        {
            var tableName = tables.Rows[0]["TABLE_NAME"].ToString();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT * FROM {tableName} LIMIT 1";

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    Console.WriteLine($"Column {i}:");
                    Console.WriteLine($"  Name: {reader.GetName(i)}");
                    Console.WriteLine($"  Type: {reader.GetFieldType(i)}");
                    Console.WriteLine($"  Precision: {reader.GetSchemaTable().Rows[i]["NumericPrecision"]}");
                    Console.WriteLine($"  Scale: {reader.GetSchemaTable().Rows[i]["NumericScale"]}");
                    Console.WriteLine($"  IsNullable: {reader.GetSchemaTable().Rows[i]["AllowDBNull"]}");
                }
        }
    }

    private static void TestTransactionSupport(OdbcConnection conn)
    {
        Console.WriteLine("\n=== Testing Transaction Support ===");
        try
        {
            using var transaction = conn.BeginTransaction();
            Console.WriteLine("Transaction started successfully");
            transaction.Rollback();
            Console.WriteLine("Transaction rolled back successfully");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Transaction not supported: {ex.Message}");
        }
    }

    private static void TestCursorBehavior(OdbcConnection conn)
    {
        Console.WriteLine("\n=== Testing Cursor Behavior ===");
        var tables = conn.GetSchema("Tables");
        if (tables.Rows.Count > 0)
        {
            var tableName = tables.Rows[0]["TABLE_NAME"].ToString();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT * FROM {tableName} LIMIT 10";

            using var reader = cmd.ExecuteReader();
            Console.WriteLine("Forward-only cursor test:");
            var rowCount = 0;
            while (reader.Read()) rowCount++;

            Console.WriteLine($"Read {rowCount} rows successfully");
        }
    }
}