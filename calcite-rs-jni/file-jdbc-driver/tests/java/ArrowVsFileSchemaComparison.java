import java.sql.*;
import java.util.Properties;

public class ArrowVsFileSchemaComparison {
    public static void main(String[] args) {
        System.out.println("=== ArrowSchemaFactory vs FileSchemaFactory Comparison ===");
        
        // Test directory-based Arrow access (this is what ArrowSchemaFactory expects)
        testDirectoryBasedArrow();
        
        // Test single file Arrow access (this is what we've been testing with FileSchemaFactory)
        testSingleFileArrow();
    }
    
    private static void testDirectoryBasedArrow() {
        System.out.println("\n--- Testing Directory-Based Arrow (ArrowSchemaFactory style) ---");
        try {
            // ArrowSchemaFactory expects a directory containing arrow files
            String url = "jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/arrow";
            System.out.println("URL: " + url);
            
            Properties props = new Properties();
            props.setProperty("caseSensitive", "false");
            
            try (Connection connection = DriverManager.getConnection(url, props)) {
                System.out.println("✅ Connection succeeded");
                
                DatabaseMetaData metaData = connection.getMetaData();
                try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                    System.out.println("Tables found:");
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        System.out.println("  - " + tableName);
                        
                        // Try to query the table
                        try (Statement stmt = connection.createStatement();
                             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM files.\"" + tableName + "\"")) {
                            if (rs.next()) {
                                System.out.println("    ✅ Query succeeded: " + rs.getInt(1) + " rows");
                            }
                        } catch (Exception e) {
                            System.out.println("    ❌ Query failed: " + e.getMessage());
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("❌ Directory-based Arrow failed: " + e.getMessage());
            if (e.getMessage().contains("z3") || e.getMessage().contains("gandiva")) {
                System.out.println("    ⚠️  Z3/Gandiva dependency issue");
            }
        }
    }
    
    private static void testSingleFileArrow() {
        System.out.println("\n--- Testing Single File Arrow (FileSchemaFactory style) ---");
        try {
            // Single file approach that FileSchemaFactory uses
            String url = "jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/arrow/sample_compressed.arrow";
            System.out.println("URL: " + url);
            
            Properties props = new Properties();
            props.setProperty("caseSensitive", "false");
            
            try (Connection connection = DriverManager.getConnection(url, props)) {
                System.out.println("✅ Connection succeeded");
                
                DatabaseMetaData metaData = connection.getMetaData();
                try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                    System.out.println("Tables found:");
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        System.out.println("  - " + tableName);
                        
                        // Try to query the table
                        try (Statement stmt = connection.createStatement();
                             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM files.\"" + tableName + "\"")) {
                            if (rs.next()) {
                                System.out.println("    ✅ Query succeeded: " + rs.getInt(1) + " rows");
                            }
                        } catch (Exception e) {
                            System.out.println("    ❌ Query failed: " + e.getMessage());
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("❌ Single file Arrow failed: " + e.getMessage());
        }
    }
}