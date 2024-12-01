# DDN ODBC Driver

The DDN ODBC Driver provides an ODBC-compliant interface to interact with the Hasura DDN (Data Delivery Network) platform. This driver allows you to execute SQL queries and retrieve data from your Hasura DDN instance using standard ODBC interfaces in .NET.

## Features

- Standard ODBC API support for executing SQL queries and retrieving data
- Schema and metadata discovery capabilities
- Read-only access to ensure data integrity
- Full support for standard ODBC connection string parameters
- Compatible with standard System.Data.Odbc interfaces

## Installation

1. After building, Run the `install-driver.ps1` script in the ./bin/<Platform> directory to install the DDN ODBC driver on your system.
2. Both the ARM64 and X64 driver will be registered in your system's ODBC drivers list.

## Usage

To use the DDN ODBC Driver, follow these steps:

1. Add a reference to `System.Data.Odbc` in your project.
2. Create a connection using the standard `OdbcConnection` class:

   ```csharp
   var connectionString = @"Driver={DDN-ODBC-Driver-x64};Server=your-server;Port=3280;Database=your-database;Role=admin;Timeout=120";
   using var connection = new OdbcConnection(connectionString);
   ```

3. Open the connection:

   ```csharp
   connection.Open();
   ```

4. Create a command and execute SQL queries:

   ```csharp
   using var command = connection.CreateCommand();
   command.CommandText = "SELECT * FROM users";
   using var reader = command.ExecuteReader();
   while (reader.Read())
   {
       // Process the data
   }
   ```

## Connection String Parameters

The following connection string parameters are supported:

- `Driver`: The name of the DDN ODBC driver (DDN-ODBC-Driver-x64)
- `Server`: The hostname or IP address of your DDN server
- `Port`: The port number for your DDN server
- `Database`: The name of the database to connect to
- `Role`: The role to use when connecting (optional - defaults to `admin`)
- `Timeout`: Connection timeout in seconds (optional),
- `UID`: The user id (optional)
- `PWD`: The user id's password (optional)
- `Auth`: The authorization header required for access

Example connection string:
```
Driver={DDN-ODBC-Driver-x64};Server=192.168.86.47;Port=3280;Database=graphql;Role=admin;Timeout=120
```

## Features Support

### Supported Features
- Basic query execution
- Schema information retrieval
- Table and column metadata
- Parameter binding
- Multiple result sets
- Connection string configuration
- Standard ODBC error handling

### Limitations
- Read-only access (no INSERT, UPDATE, or DELETE operations)
- No transaction support
- No batch operations
- No asynchronous operations

## Error Handling

The driver uses standard ODBC error handling mechanisms. Errors can be caught using standard try-catch blocks:

```csharp
try
{
    connection.Open();
}
catch (OdbcException ex)
{
    Console.WriteLine($"Error: {ex.Message}");
    if (ex.InnerException != null)
        Console.WriteLine($"Inner: {ex.InnerException.Message}");
}
```

## Metadata Retrieval

The driver supports standard ODBC metadata retrieval methods:

```csharp
// Get schema information
var schemaTable = connection.GetSchema();

// Get table information
var tables = connection.GetSchema("Tables");

// Get column information
var columns = connection.GetSchema("Columns");
```

## Contributing to DDN ODBC Driver Development

### Development Prerequisites

1. Core Development Tools:
   - Visual Studio 2022 or later
   - .NET 8.0 SDK or later
   - Git
   - Java Development Kit (JDK) 11 or later (for Java application server components). There is a companion Java project - `jni-arrow` required for this project to build.
   - Maven 3.8 or later (for building Java components)
   - CMake 3.20 or later (for native ODBC components)
   - C++ Build Tools (VS2022 build tools with Windows SDK)
   - PowerShell 7.0 or later

2. Optional Tools:
   - Visual Studio Code with extensions:
      - C# Dev Kit
      - C/C++ Extension Pack
      - Java Extension Pack
   - JetBrains Rider
   - Docker Desktop (for testing)

### Setting Up Development Environment

1. Clone the Repository:
   ```bash
   git clone https://github.com/ddn/odbc-driver.git
   cd odbc-driver
   git submodule update --init --recursive
   ```

2. Build Environment Setup:
   ```powershell
   # Install required build tools
   ./scripts/setup-dev-environment.ps1

   # Verify environment
   ./scripts/verify-prerequisites.ps1
   ```

3. Build the Driver:
   ```powershell
   # Build all components
   ./build.ps1 

   # Build specific components
   ./build.ps1 -Component native  # For native ODBC components
   ./build.ps1 -Component managed # For .NET components
   ./build.ps1 -Component java    # For Java components
   ```

### Project Structure

```
ddn-odbc-driver/
├── include/             # C++ ODBC driver header files
└── src/                 # C++ ODBC driver implementation
```
The root has cmake files and .def files.

### Development Workflow

1. Create a New Feature Branch:
   **This is a sub-project within the ndc-calcite repo**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Development Guidelines:
   - Follow the existing code style
   - Add unit tests for new features
   - Update documentation as needed
   - Keep commits focused and well-documented


3. Testing Your Changes:
   **TBD - Not completed**
   ```powershell
   # Run all tests
   ./scripts/run-tests.ps1

   # Run specific test categories
   ./scripts/run-tests.ps1 -Category unit
   ./scripts/run-tests.ps1 -Category integration
   ```

4. Code Review Process:
   - Create a pull request against the `develop` branch
   - Ensure CI pipeline passes
   - Address reviewer feedback
   - Update the changelog

### Building and Testing

1. Build Configurations:
   **TBD - Not completed - DEV only**
   ```powershell
   # Debug build
   ./build.ps1 -Configuration Debug

   # Release build
   ./build.ps1 -Configuration Release
   ```

2. Testing Matrix:
   - Windows x64
   - Different ODBC driver manager versions
   - Various .NET Framework versions
   - Multiple Java versions

3. Integration Testing:
   ```powershell
   # Start test environment
   ./scripts/start-test-environment.ps1

   # Run integration tests
   ./scripts/run-integration-tests.ps1
   ```

### Debugging

1. Native Debugging:
   - Use Visual Studio's native debugger
   - Enable ODBC tracing
   - Use WinDbg for crash analysis

2. Java Component Debugging:
   - Use remote debugging with IDE
   - Enable JVM debugging options
   - Use JFR for performance analysis

### Release Process

1. Version Bump:
**TBD - Not completed**
   ```powershell
   ./scripts/bump-version.ps1 -Version "x.y.z"
   ```

2. Release Checklist:
   - Update changelog
   - Run full test suite
   - Update documentation
   - Create release notes
   - Build release artifacts

3. Release Verification:
   **TBD - Not completed**
   ```powershell
   # Verify release artifacts
   ./scripts/verify-release.ps1 -Version "x.y.z"
   ```

## Troubleshooting

Common issues and solutions:

1. Driver not found
   - Ensure the install-driver.ps1 script was run successfully
   - Verify the driver is listed in ODBC Data Source Administrator

2. Connection failures
   - Verify server address and port
   - Check network connectivity
   - Ensure database name and role are correct

3. Query timeout
   - Adjust the Timeout parameter in the connection string
   - Optimize your queries if possible

## Best Practices

1. Always use the `using` statement with connections and commands to ensure proper resource cleanup
2. Set appropriate timeout values in the connection string
3. Handle ODBC exceptions appropriately
4. Close connections explicitly when done
5. Use parameter binding for queries when possible

## Support

For issues, questions, or contributions:
1. Check the troubleshooting guide above
2. Search existing GitHub issues
3. Create a new GitHub issue with:
   - Driver version
   - Full error message
   - Steps to reproduce
   - Environment details

## License

[Add appropriate license information here]