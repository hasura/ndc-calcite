import java.sql.*;
import java.util.Properties;

public class FileSchemaFactoryArrowTest {
    public static void main(String[] args) {
        System.out.println("=== Testing FileSchemaFactory with Arrow/Parquet Files ===");
        
        // Test if FileSchemaFactory can actually handle Arrow files
        testArrowWithFileSchemaFactory();
        testParquetWithFileSchemaFactory();
    }
    
    private static void testArrowWithFileSchemaFactory() {
        System.out.println("\n--- Testing Arrow file with FileSchemaFactory ---");
        try {
            // Force use of FileSchemaFactory by using a different URL pattern
            // or by testing our SafeArrowSchemaFactory fallback behavior
            String url = "jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/arrow/sample_compressed.arrow";
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
                                System.out.println("✅ Query succeeded: " + rs.getInt(1) + " rows");
                            }
                        } catch (Exception e) {
                            System.out.println("❌ Query failed: " + e.getMessage());
                            if (e.getMessage().contains("compress") || e.getMessage().contains("arrow") || e.getMessage().contains("format")) {
                                System.out.println("⚠️  This indicates FileSchemaFactory cannot handle Arrow files!");
                            }
                        }
                    }
                    
                    if (!foundTables) {
                        System.out.println("❌ No tables found - FileSchemaFactory cannot discover Arrow files");
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("❌ Connection/Discovery failed: " + e.getMessage());
            if (e.getMessage().contains("compress") || e.getMessage().contains("arrow") || e.getMessage().contains("format")) {
                System.out.println("⚠️  This confirms FileSchemaFactory cannot handle Arrow files!");
            }
        }
    }
    
    private static void testParquetWithFileSchemaFactory() {
        System.out.println("\n--- Testing Parquet file with FileSchemaFactory ---");
        try {
            String url = "jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/parquet/sample.parquet";
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
                                System.out.println("✅ Query succeeded: " + rs.getInt(1) + " rows");
                            }
                        } catch (Exception e) {
                            System.out.println("❌ Query failed: " + e.getMessage());
                            if (e.getMessage().contains("parquet") || e.getMessage().contains("format")) {
                                System.out.println("⚠️  This indicates FileSchemaFactory cannot handle Parquet files!");
                            }
                        }
                    }
                    
                    if (!foundTables) {
                        System.out.println("❌ No tables found - FileSchemaFactory cannot discover Parquet files");
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("❌ Connection/Discovery failed: " + e.getMessage());
            if (e.getMessage().contains("parquet") || e.getMessage().contains("format")) {
                System.out.println("⚠️  This confirms FileSchemaFactory cannot handle Parquet files!");
            }
        }
    }
}