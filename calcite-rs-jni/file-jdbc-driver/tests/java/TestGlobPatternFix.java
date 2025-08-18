import java.sql.*;
import java.util.Properties;

public class TestGlobPatternFix {
    public static void main(String[] args) {
        System.out.println("=== Testing Glob Pattern Fix for Arrow/Parquet ===");
        
        // Register the driver
        try {
            Class.forName("com.hasura.file.FileDriver");
            System.out.println("✅ Driver registered");
        } catch (ClassNotFoundException e) {
            System.err.println("❌ Failed to register driver: " + e.getMessage());
            return;
        }
        
        // Test glob patterns for Arrow files
        testArrowGlob();
        
        // Test glob patterns for Parquet files
        testParquetGlob();
    }
    
    private static void testArrowGlob() {
        System.out.println("\n--- Testing Arrow Files via Glob Pattern ---");
        String url = "jdbc:file://multi?path=/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data&glob=**/*.arrow";
        
        try {
            Properties props = new Properties();
            props.setProperty("caseSensitive", "false");
            
            try (Connection connection = DriverManager.getConnection(url, props)) {
                System.out.println("✅ Connection succeeded");
                
                DatabaseMetaData metaData = connection.getMetaData();
                try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                    int count = 0;
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        System.out.println("  Found table: " + tableName);
                        count++;
                        
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
                    System.out.println("Total Arrow tables found: " + count);
                }
            }
        } catch (Exception e) {
            System.out.println("❌ Failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void testParquetGlob() {
        System.out.println("\n--- Testing Parquet Files via Glob Pattern ---");
        String url = "jdbc:file://multi?path=/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data&glob=**/*.parquet";
        
        try {
            Properties props = new Properties();
            props.setProperty("caseSensitive", "false");
            
            try (Connection connection = DriverManager.getConnection(url, props)) {
                System.out.println("✅ Connection succeeded");
                
                DatabaseMetaData metaData = connection.getMetaData();
                try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                    int count = 0;
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        System.out.println("  Found table: " + tableName);
                        count++;
                        
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
                    System.out.println("Total Parquet tables found: " + count);
                }
            }
        } catch (Exception e) {
            System.out.println("❌ Failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}