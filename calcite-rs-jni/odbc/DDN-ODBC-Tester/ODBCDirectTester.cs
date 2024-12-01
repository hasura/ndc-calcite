using System.Runtime.InteropServices;
using System.Text;
using System.Data.Odbc;
using System.Reflection;
namespace DDN_ODBC_Tester;
public class OdbcDirectTester
{
    // ODBC API Constants
    private const short SQL_SUCCESS = 0;
    private const short SQL_SUCCESS_WITH_INFO = 1;
    private const short SQL_ERROR = -1;
    
    // Column Attribute Constants
    private const short SQL_DESC_COUNT = 0;
    private const short SQL_DESC_NAME = 1011;
    private const short SQL_DESC_TYPE_NAME = 14;
    private const short SQL_DESC_LENGTH = 1003;
    private const short SQL_DESC_NULLABLE = 1008;
    private const short SQL_DESC_OCTET_LENGTH = 1013;
    private const short SQL_DESC_PRECISION = 1005;
    private const short SQL_DESC_SCALE = 1006;

    // ODBC Function Imports
    [DllImport("odbc32.dll")]
    private static extern short SQLAllocHandle(short HandleType, IntPtr InputHandle, out IntPtr OutputHandle);

    [DllImport("odbc32.dll", CharSet = CharSet.Unicode)]
    private static extern short SQLExecDirectW(IntPtr StatementHandle, [MarshalAs(UnmanagedType.LPWStr)] string StatementText, int TextLength);

    [DllImport("odbc32.dll")]
    private static extern short SQLFreeHandle(short HandleType, IntPtr Handle);

    [DllImport("odbc32.dll", CharSet = CharSet.Unicode)]
    private static extern short SQLColAttributeW(
        IntPtr StatementHandle,
        short ColumnNumber,
        short FieldIdentifier,
        IntPtr CharacterAttribute,
        short BufferLength,
        out short StringLength,
        IntPtr NumericAttribute);

    // Handle Type Constants
    private const short SQL_HANDLE_ENV = 1;
    private const short SQL_HANDLE_DBC = 2;
    private const short SQL_HANDLE_STMT = 3;

    public static void TestColumnAttributes(OdbcConnection connection)
    {
        Console.WriteLine("\n=== Testing Direct ODBC Column Attributes ===");

        // Get the connection handle using reflection
        var connectionHandle = GetConnectionHandle(connection);
        if (connectionHandle == IntPtr.Zero)
        {
            Console.WriteLine("Failed to get connection handle");
            return;
        }

        // Allocate statement handle
        IntPtr stmtHandle;
        short ret = SQLAllocHandle(SQL_HANDLE_STMT, connectionHandle, out stmtHandle);
        if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
        {
            Console.WriteLine("Failed to allocate statement handle");
            return;
        }

        try
        {
            // Execute query for anomaly analyses
            const string sql = "SELECT \"analysisMode\", \"anomaliesDetected\", \"timestamp\", \"anomalousRecordsCount\", \"historicalDataSize\", \"id\", \"status\" FROM \"GRAPHQL\".\"AnomalyAnalyses\"";
            ret = SQLExecDirectW(stmtHandle, sql, -3); // -3 for null-terminated string
            if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
            {
                Console.WriteLine("Failed to execute query");
                return;
            }

            // Test attributes for each column
            for (short colNum = 1; colNum <= 7; colNum++)
            {
                Console.WriteLine($"\nTesting Column {colNum}:");
                TestColumnAttribute(stmtHandle, colNum, SQL_DESC_NAME, "Name");
                TestColumnAttribute(stmtHandle, colNum, SQL_DESC_TYPE_NAME, "Type Name");
                TestColumnAttribute(stmtHandle, colNum, SQL_DESC_LENGTH, "Length");
                TestColumnAttribute(stmtHandle, colNum, SQL_DESC_NULLABLE, "Nullable");
                TestColumnAttribute(stmtHandle, colNum, SQL_DESC_OCTET_LENGTH, "Octet Length");
                TestColumnAttribute(stmtHandle, colNum, SQL_DESC_PRECISION, "Precision");
                TestColumnAttribute(stmtHandle, colNum, SQL_DESC_SCALE, "Scale");
            }
        }
        finally
        {
            SQLFreeHandle(SQL_HANDLE_STMT, stmtHandle);
        }
    }

    private static void TestColumnAttribute(IntPtr stmtHandle, short columnNum, short fieldId, string fieldName)
    {
        Console.WriteLine($"  Testing {fieldName} (Field ID: {fieldId}):");

        // First try to get the required buffer size for string attributes
        short stringLength;
        var numericValue = Marshal.AllocCoTaskMem(sizeof(long));
        try
        {
            short ret = SQLColAttributeW(stmtHandle, columnNum, fieldId, IntPtr.Zero, 0, out stringLength, numericValue);
            
            if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO)
            {
                // For string attributes
                if (stringLength > 0)
                {
                    // Allocate buffer for the string
                    IntPtr buffer = Marshal.AllocCoTaskMem(stringLength + 2); // +2 for null terminator
                    try
                    {
                        ret = SQLColAttributeW(stmtHandle, columnNum, fieldId, buffer, (short)(stringLength + 2), out stringLength, IntPtr.Zero);
                        if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO)
                        {
                            string value = Marshal.PtrToStringUni(buffer, stringLength/2);
                            Console.WriteLine($"    String Value: '{value}'");
                            Console.WriteLine($"    String Length: {stringLength}");
                        }
                        else
                        {
                            Console.WriteLine($"    Failed to get string value. Return code: {ret}");
                        }
                    }
                    finally
                    {
                        Marshal.FreeCoTaskMem(buffer);
                    }
                }
                // For numeric attributes
                else
                {
                    long value = Marshal.ReadInt64(numericValue);
                    Console.WriteLine($"    Numeric Value: {value}");
                }
            }
            else
            {
                Console.WriteLine($"    Failed to get attribute. Return code: {ret}");
            }
        }
        finally
        {
            Marshal.FreeCoTaskMem(numericValue);
        }
    }

    private static IntPtr GetConnectionHandle(OdbcConnection connection)
    {
        var connectionHandleField = connection.GetType()
            .GetField("_connectionHandle", BindingFlags.NonPublic | BindingFlags.Instance);
    
        if (connectionHandleField != null)
        {
            var connectionHandle = connectionHandleField.GetValue(connection);
            if (connectionHandle != null)
            {
                var handleField = connectionHandle.GetType()
                    .GetField("handle", BindingFlags.NonPublic | BindingFlags.Instance);
                
                if (handleField != null)
                {
                    return (IntPtr)handleField.GetValue(connectionHandle);
                }
            }
        }
    
        Console.WriteLine("Could not find handle field in connection handle");
        return IntPtr.Zero;
    }
}