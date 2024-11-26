# DDN ODBC Driver

The DDN ODBC Driver is a .NET Standard 2.0 library that provides an ODBC-compliant interface to interact with the Hasura DDN (Data Delivery Network) platform. This driver allows you to execute SQL queries and retrieve data from your Hasura DDN instance using a familiar ODBC-based API.

## Features

- Supports ODBC API for executing SQL queries and retrieving data
- Provides a simple and intuitive connection management interface
- Automatically starts and manages a Java application server in the background
- Supports reading metadata (tables, columns) from the Hasura DDN instance
- Operates in a read-only mode to ensure data integrity

## Usage

To use the DDN ODBC Driver, follow these steps:

1. Add a reference to the `DDN-ODBC.dll` assembly in your project.
2. Create an instance of the `DDN_ODBC` class and set the connection string:

   ```csharp
   var connectionString = "jdbc:ddn://your-hasura-instance.com:8080";
   var connection = new DDN_ODBC(connectionString);
   ```

3. Open the connection:

   ```csharp
   connection.Open();
   ```

4. Create a command and execute SQL queries:

   ```csharp
   using (var command = connection.CreateDbCommand())
   {
       command.CommandText = "SELECT * FROM users";
       using (var reader = command.ExecuteReader())
       {
           while (reader.Read())
           {
               // Process the data
           }
       }
   }
   ```

5. Close the connection when you're done:

   ```csharp
   connection.Close();
   ```

## Architecture

The DDN ODBC Driver is built on top of the .NET Standard 2.0 framework and uses the following key components:

1. `DDN_ODBC` class: The main entry point for the driver, responsible for managing the connection, starting the Java application server, and executing SQL queries.
2. `DDN_OdbcCommand` class: Implements the ODBC command interface, allowing you to execute SQL queries and retrieve data.
3. `DDN_OdbcDataReader` class: Implements the ODBC data reader interface, providing a way to iterate over the result set.
4. `DDNDataParameter` and `DDNDataParameterCollection` classes: Provide a way to manage input parameters for SQL queries.
5. `SqlQuery` class: Represents a SQL query to be executed on the Hasura DDN instance.

The driver uses an embedded Java application server to communicate with the Hasura DDN instance. The Java application server is started and managed by the `DDN_ODBC` class, and all SQL queries are executed through this server.

## Limitations

- The driver currently operates in a read-only mode, meaning that it does not support any write operations (insert, update, delete) on the Hasura DDN instance.
- Transactions are not supported, as the Hasura DDN instance is designed for read-only access.
- The driver does not provide any advanced features like connection pooling or asynchronous operations. It focuses on providing a basic, ODBC-compliant interface to interact with the Hasura DDN platform.

## Contributing

If you encounter any issues or have suggestions for improvements, please feel free to create a new issue or submit a pull request on the project's GitHub repository.