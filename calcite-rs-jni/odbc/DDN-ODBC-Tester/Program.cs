using System;
using System.Data;
using DDN_ODBC;

namespace DDN_ODBC_Tester
{
    class OdbcMetadataExample
    {
        static void Main()
        {
            string connectionString = "jdbc:graphql:http://localhost:3280/graphql";

            try
            {
                using (DDN_ODBC.DDN_ODBC connection = new DDN_ODBC.DDN_ODBC(connectionString))
                {
                    connection.Open();
                    Console.WriteLine("Connected successfully.\n");

                    // Get Tables
                    Console.WriteLine("=== Tables ===");
                    using (IDbCommand command = connection.CreateCommand())
                    {
                        command.CommandText = "SQLTables";
                        
                        using (IDataReader reader = command.ExecuteReader())
                        {
                            // Print column names
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                Console.Write($"{reader.GetName(i)}\t");
                            }
                            Console.WriteLine();

                            // Print data
                            while (reader.Read())
                            {
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    Console.Write($"{reader[i]}\t");
                                }
                                Console.WriteLine();
                            }
                        }
                    }
                    
                    // Get Tables Parameterized
                    Console.WriteLine("=== Tables ===");
                    using (IDbCommand command = connection.CreateCommand())
                    {
                        command.CommandText = "{ CALL SQLTables(?, ?, ?, ?) }";
                
                        // Adding parameters (catalog, schema, table, table type)
                        // Create parameters
                        var param1 = new DDNDataParameter("@catalog", DbType.String) { Value = DBNull.Value };
                        var param2 = new DDNDataParameter("@schema", DbType.String) { Value = DBNull.Value };
                        var param3 = new DDNDataParameter("@table", DbType.String) { Value = DBNull.Value };
                        var param4 = new DDNDataParameter("@tableType", DbType.String) { Value = "TABLE" };

                        // Add parameters
                        command.Parameters.Add(param1);
                        command.Parameters.Add(param2);
                        command.Parameters.Add(param3);
                        command.Parameters.Add(param4);

                        using (IDataReader reader = command.ExecuteReader())
                        {
                            // Print column names
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                Console.Write($"{reader.GetName(i)}\t");
                            }
                            Console.WriteLine();

                            // Print data
                            while (reader.Read())
                            {
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    Console.Write($"{reader[i]}\t");
                                }
                                Console.WriteLine();
                            }
                        }
                    }
                    
                    // Get Columns
                    Console.WriteLine("\n=== Columns for YourTable ===");
                    using (IDbCommand command = connection.CreateCommand())
                    {
                        command.CommandText = "SQLColumns";
                        
                        using (IDataReader reader = command.ExecuteReader())
                        {
                            // Print column names
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                Console.Write($"{reader.GetName(i)}\t");
                            }
                            Console.WriteLine();

                            // Print data
                            while (reader.Read())
                            {
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    Console.Write($"{reader[i]}\t");
                                }
                                Console.WriteLine();
                            }
                        }
                    }

                    // Get Columns
                    Console.WriteLine("\n=== Columns for YourTable ===");
                    using (IDbCommand command = connection.CreateCommand())
                    {
                        command.CommandText = "{ CALL SQLColumns(?, ?, ?, ?) }";
                
                        // Adding parameters (catalog, schema, table, table type)
                        // Create parameters
                        var param1 = new DDNDataParameter("@catalog", DbType.String) { Value = DBNull.Value };
                        var param2 = new DDNDataParameter("@schema", DbType.String) { Value = DBNull.Value };
                        var param3 = new DDNDataParameter("@table", DbType.String) { Value = DBNull.Value };
                        var param4 = new DDNDataParameter("@column", DbType.String) { Value = "fi%" };

                        // Add parameters
                        command.Parameters.Add(param1);
                        command.Parameters.Add(param2);
                        command.Parameters.Add(param3);
                        command.Parameters.Add(param4);
                        
                        using (IDataReader reader = command.ExecuteReader())
                        {
                            // Print column names
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                Console.Write($"{reader.GetName(i)}\t");
                            }
                            Console.WriteLine();

                            // Print data
                            while (reader.Read())
                            {
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    Console.Write($"{reader[i]}\t");
                                }
                                Console.WriteLine();
                            }
                        }
                    }
                    
                    // Get Columns
                    Console.WriteLine("\n=== Plain old SQL for Customer Table ===");
                    using (IDbCommand command = connection.CreateCommand())
                    {
                        command.CommandText = "SELECT * FROM Customer";
                        
                        using (IDataReader reader = command.ExecuteReader())
                        {
                            // Print column names
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                Console.Write($"{reader.GetName(i)}\t");
                            }
                            Console.WriteLine();

                            // Print data
                            while (reader.Read())
                            {
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    Console.Write($"{reader[i]}\t");
                                }
                                Console.WriteLine();
                            }
                        }
                    }
                    
                    // Get Primary Keys
                    Console.WriteLine("\n=== Primary Keys ===");
                    using (IDbCommand command = connection.CreateCommand())
                    {
                        command.CommandText = "SQLPrimaryKeys";
                        
                        using (IDataReader reader = command.ExecuteReader())
                        {
                            // Print column names
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                Console.Write($"{reader.GetName(i)}\t");
                            }
                            Console.WriteLine();

                            // Print data
                            while (reader.Read())
                            {
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    Console.Write($"{reader[i]}\t");
                                }
                                Console.WriteLine();
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\nError: {ex.Message}");
            }
        }
    }
}