import java.sql.*;
import java.util.Properties;

public class SingleArrowTest {
    public static void main(String[] args) {
        System.out.println("Testing single Arrow file compression with Arrow 17.0.0...");
        
        // Test the compressed Arrow file directly
        String compressedUrl = "jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/arrow/sample_compressed.arrow";
        testArrowFile(compressedUrl, "sample_compressed");
        
        // Test the uncompressed Arrow file
        String uncompressedUrl = "jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/arrow/sample_uncompressed.arrow";
        testArrowFile(uncompressedUrl, "sample_uncompressed");
    }
    
    private static void testArrowFile(String url, String name) {
        System.out.println("\n=== Testing " + name + " ===");
        System.out.println("URL: " + url);
        
        try {
            Properties props = new Properties();
            props.setProperty("caseSensitive", "false");
            
            try (Connection connection = DriverManager.getConnection(url, props)) {
                System.out.println("✅ Connection succeeded");
                
                // List available tables
                DatabaseMetaData metaData = connection.getMetaData();
                try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                    System.out.println("Available tables:");
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        System.out.println("  - " + tableName);
                        
                        // Try to query this table
                        String query = "SELECT COUNT(*) FROM files.\"" + tableName + "\"";
                        System.out.println("Query: " + query);
                        
                        try (Statement stmt = connection.createStatement();
                             ResultSet rs = stmt.executeQuery(query)) {
                            if (rs.next()) {
                                System.out.println("✅ Query succeeded - Count: " + rs.getInt(1));
                            }
                        } catch (Exception e) {
                            System.out.println("❌ Query failed: " + e.getMessage());
                            if (e.getMessage().contains("LZ4_FRAME")) {
                                System.out.println("⚠️  LZ4_FRAME compression issue detected with Arrow 17.0.0");
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("❌ Connection failed: " + e.getMessage());
            if (e.getMessage().contains("LZ4_FRAME")) {
                System.out.println("⚠️  LZ4_FRAME compression issue detected with Arrow 17.0.0");
            }
        }
    }
}