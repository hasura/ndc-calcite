import java.sql.*;

public class TestFileDriverDebug {
    public static void main(String[] args) {
        try {
            System.out.println("=== Testing File JDBC Driver ===");
            String url = "jdbc:file://" + System.getProperty("user.dir") + "/test-data.csv";
            System.out.println("URL: " + url);
            
            try (Connection conn = DriverManager.getConnection(url)) {
                DatabaseMetaData meta = conn.getMetaData();
                
                // List all schemas
                System.out.println("\nSchemas:");
                try (ResultSet schemas = meta.getSchemas()) {
                    while (schemas.next()) {
                        String schemaName = schemas.getString("TABLE_SCHEM");
                        System.out.println("  Schema: " + schemaName);
                        
                        // List tables in this schema
                        try (ResultSet tables = meta.getTables(null, schemaName, "%", null)) {
                            while (tables.next()) {
                                System.out.println("    Table: " + tables.getString("TABLE_NAME"));
                            }
                        }
                    }
                }
                
                // Try a simple query with the default schema
                System.out.println("\nTrying query with default schema...");
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT * FROM \"test-data.csv\"")) {
                    
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    
                    // Print column names
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(metaData.getColumnName(i) + "\t");
                    }
                    System.out.println();
                    
                    // Print first few rows
                    int rowCount = 0;
                    while (rs.next() && rowCount < 3) {
                        for (int i = 1; i <= columnCount; i++) {
                            System.out.print(rs.getString(i) + "\t");
                        }
                        System.out.println();
                        rowCount++;
                    }
                } catch (SQLException e) {
                    System.out.println("Failed with table name 'test-data.csv': " + e.getMessage());
                    
                    // Try without extension
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery("SELECT * FROM files.\"test-data\"")) {
                        System.out.println("\nSucceeded with 'files.test-data'");
                    } catch (SQLException e2) {
                        System.out.println("Failed with 'files.test-data': " + e2.getMessage());
                    }
                }
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}