import java.sql.*;
import java.util.Properties;

public class SimpleXLSXTest {
    public static void main(String[] args) throws Exception {
        Class.forName("com.hasura.file.FileDriver");
        
        String url = "jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/xlsx/company_data.xlsx";
        System.out.println("Testing XLSX with URL: " + url);
        
        try (Connection conn = DriverManager.getConnection(url)) {
            System.out.println("✅ Connection successful");
            
            DatabaseMetaData meta = conn.getMetaData();
            
            System.out.println("\nDiscovered tables:");
            try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    String tableType = tables.getString("TABLE_TYPE");
                    System.out.println("  - " + tableName + " (" + tableType + ")");
                }
            }
            
            // Try different query approaches
            String[] queries = {
                "SELECT * FROM files.\"company_data\" LIMIT 1",
                "SELECT * FROM files.\"company_data\".\"employees\" LIMIT 1", 
                "SELECT * FROM files.\"employees\" LIMIT 1",
                "SELECT * FROM files.\"company_data__employees\" LIMIT 1"
            };
            
            for (String query : queries) {
                System.out.println("\nTrying query: " + query);
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(query)) {
                    
                    if (rs.next()) {
                        System.out.println("✅ Query succeeded!");
                        ResultSetMetaData rsmd = rs.getMetaData();
                        int cols = rsmd.getColumnCount();
                        System.out.print("Columns: ");
                        for (int i = 1; i <= cols; i++) {
                            if (i > 1) System.out.print(", ");
                            System.out.print(rsmd.getColumnName(i));
                        }
                        System.out.println();
                        break;
                    }
                } catch (SQLException e) {
                    System.out.println("❌ Query failed: " + e.getMessage());
                }
            }
        }
    }
}