using System.Data;
using System.Data.Odbc;

namespace DDN_ODBC_Tester
{
    class Program
    {
        private const string SQL_CONNECTION_STRING = 
            @"Driver={DDN-ODBC-Driver};Server=192.168.86.47;Port=3280;Database=graphql;Role=admin;Timeout=120";

        static void Main()
        {
            try
            {
                using var conn = new OdbcConnection(SQL_CONNECTION_STRING);
                Console.WriteLine("Testing connection...");
                conn.Open();
                Console.WriteLine("Connected successfully.\n");

                // Test 1: Driver Information
                TestDriverInfo(conn);

                // Test 2: Schema Information
                TestSchemaInfo(conn);

                // Test 3: Table Information
                TestTableInfo(conn);

                // Test 4: Column Information
                TestColumnInfo(conn);

                // Test 5: Data Type Information
                TestDataTypeInfo(conn);

                // Test 6: Basic Query Execution
                TestBasicQueries(conn);

                // Test 7: Parameterized Queries
                TestParameterizedQueries(conn);

                // Test 8: Metadata Retrieval
                TestMetadataRetrieval(conn);

                // Test 9: Transaction Support
                TestTransactionSupport(conn);

                // Test 10: Cursor Behavior
                TestCursorBehavior(conn);

                Console.WriteLine("\nAll tests completed successfully!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Inner: {ex.InnerException.Message}");
            }
        }

        static void TestDriverInfo(OdbcConnection conn)
        {
            Console.WriteLine("\n=== Driver Information ===");
            var info = new Dictionary<string, string>
            {
                ["DriverName"] = conn.Driver,
                ["DriverVersion"] = conn.ServerVersion,
                ["DataSource"] = conn.DataSource,
                ["Database"] = conn.Database
            };

            foreach (var item in info)
            {
                Console.WriteLine($"{item.Key}: {item.Value}");
            }
        }

        static void TestSchemaInfo(OdbcConnection conn)
        {
            Console.WriteLine("\n=== Schema Collections ===");
            DataTable collections = conn.GetSchema();
            foreach (DataRow row in collections.Rows)
            {
                Console.WriteLine($"Schema collection: {row["CollectionName"]}");
            }
        }

        static void TestTableInfo(OdbcConnection conn)
        {
            Console.WriteLine("\n=== Tables Information ===");
            DataTable tables = conn.GetSchema("Tables");
            foreach (DataRow row in tables.Rows)
            {
                Console.WriteLine($"Table: {row["TABLE_SCHEM"]}.{row["TABLE_NAME"]} ({row["TABLE_TYPE"]})");
            }
        }

        static void TestColumnInfo(OdbcConnection conn)
        {
            Console.WriteLine("\n=== Column Information ===");
            // Get first table from previous test
            DataTable tables = conn.GetSchema("Tables");
            if (tables.Rows.Count > 0)
            {
                string tableName = tables.Rows[0]["TABLE_NAME"].ToString();
                DataTable columns = conn.GetSchema("Columns", new[] { null, null, tableName, null });
                Console.WriteLine($"\nColumns for table {tableName}:");
                foreach (DataRow row in columns.Rows)
                {
                    Console.WriteLine($"Column: {row["COLUMN_NAME"]}, Type: {row["DATA_TYPE"]}, Size: {row["COLUMN_SIZE"]}, Nullable: {row["IS_NULLABLE"]}");
                }
            }
        }

        static void TestDataTypeInfo(OdbcConnection conn)
        {
            Console.WriteLine("\n=== Data Type Information ===");
            DataTable typeInfo = conn.GetSchema("DataTypes");
            foreach (DataRow row in typeInfo.Rows)
            {
                Console.WriteLine($"Type: {row["TypeName"]}, Provider DbType: {row["ProviderDbType"]}, Framework Type: {row["DataType"]}");
            }
        }

        static void TestBasicQueries(OdbcConnection conn)
        {
            Console.WriteLine("\n=== Basic Query Tests ===");
            // Get first table from previous test
            DataTable tables = conn.GetSchema("Tables");
            if (tables.Rows.Count > 0)
            {
                string tableName = tables.Rows[0]["TABLE_NAME"].ToString();
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

        static void TestParameterizedQueries(OdbcConnection conn)
        {
            Console.WriteLine("\n=== Parameterized Query Tests ===");
            // Get first table from previous test
            DataTable tables = conn.GetSchema("Tables");
            if (tables.Rows.Count > 0)
            {
                string tableName = tables.Rows[0]["TABLE_NAME"].ToString();
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

        static void TestMetadataRetrieval(OdbcConnection conn)
        {
            Console.WriteLine("\n=== Testing Metadata Retrieval ===");
            DataTable tables = conn.GetSchema("Tables");
            if (tables.Rows.Count > 0)
            {
                string tableName = tables.Rows[0]["TABLE_NAME"].ToString();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT * FROM {tableName} LIMIT 1";
                
                using var reader = cmd.ExecuteReader();
                while(reader.Read()) {
                    for (int i = 0; i < reader.FieldCount; i++)
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
        }

        static void TestTransactionSupport(OdbcConnection conn)
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

        static void TestCursorBehavior(OdbcConnection conn)
        {
            Console.WriteLine("\n=== Testing Cursor Behavior ===");
            DataTable tables = conn.GetSchema("Tables");
            if (tables.Rows.Count > 0)
            {
                string tableName = tables.Rows[0]["TABLE_NAME"].ToString();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT * FROM {tableName} LIMIT 10";
                
                using var reader = cmd.ExecuteReader();
                Console.WriteLine("Forward-only cursor test:");
                int rowCount = 0;
                while (reader.Read())
                {
                    rowCount++;
                }
                Console.WriteLine($"Read {rowCount} rows successfully");
            }
        }

        static void TestMetadata(OdbcConnection conn, string testName, string sql)
        {
            Console.WriteLine($"\n=== Testing {testName} ===");
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            
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
                Console.WriteLine($"Error executing query: {ex.Message}");
            }
        }
    }
}