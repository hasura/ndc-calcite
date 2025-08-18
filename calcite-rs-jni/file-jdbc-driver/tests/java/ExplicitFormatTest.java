import java.sql.*;
import java.util.Properties;

public class ExplicitFormatTest {
    public static void main(String[] args) {
        System.out.println("=== Explicit Format Test ===");
        
        // Register the driver
        try {
            Class.forName("com.hasura.file.FileDriver");
            System.out.println("✅ Driver registered");
        } catch (ClassNotFoundException e) {
            System.err.println("❌ Failed to register driver: " + e.getMessage());
            return;
        }
        
        // Test directory with explicit arrow format
        testDirectoryWithArrowFormat();
        
        // Test directory without format (defaults to CSV)
        testDirectoryWithoutFormat();
    }
    
    private static void testDirectoryWithArrowFormat() {
        System.out.println("\n--- Testing Directory with ?format=arrow ---");
        try {
            // Explicitly specify arrow format for directory
            String url = "jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/arrow?format=arrow";
            System.out.println("URL: " + url);
            
            Properties props = new Properties();
            props.setProperty("caseSensitive", "false");
            
            try (Connection connection = DriverManager.getConnection(url, props)) {
                System.out.println("✅ Connection succeeded");
                
                DatabaseMetaData metaData = connection.getMetaData();
                try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                    System.out.println("Tables found:");
                    boolean foundTables = false;
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        System.out.println("  - " + tableName);
                        foundTables = true;
                        
                        // Try to query the table
                        try (Statement stmt = connection.createStatement();
                             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM files.\"" + tableName + "\"")) {
                            if (rs.next()) {
                                System.out.println("    ✅ Query succeeded: " + rs.getInt(1) + " rows");
                            }
                        } catch (Exception e) {
                            System.out.println("    ❌ Query failed: " + e.getMessage());
                            if (e.getMessage().contains("z3") || e.getMessage().contains("gandiva")) {
                                System.out.println("      ⚠️  This shows ArrowSchemaFactory (with Z3 dependency) was used!");
                            }
                        }
                    }
                    
                    if (!foundTables) {
                        System.out.println("  (No tables found)");
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("❌ Connection failed: " + e.getMessage());
            if (e.getMessage().contains("z3") || e.getMessage().contains("gandiva")) {
                System.out.println("  ⚠️  This confirms ArrowSchemaFactory requires Z3!");
            }
        }
    }
    
    private static void testDirectoryWithoutFormat() {
        System.out.println("\n--- Testing Directory without format (defaults to CSV) ---");
        try {
            String url = "jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/arrow";
            System.out.println("URL: " + url);
            
            Properties props = new Properties();
            props.setProperty("caseSensitive", "false");
            
            try (Connection connection = DriverManager.getConnection(url, props)) {
                System.out.println("✅ Connection succeeded (uses CSV format, not Arrow)");
                
                DatabaseMetaData metaData = connection.getMetaData();
                try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                    System.out.println("Tables found:");
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        System.out.println("  - " + tableName + " (treated as CSV, not Arrow)");
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("❌ Connection failed: " + e.getMessage());
        }
    }
}