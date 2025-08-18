import java.sql.*;
import java.util.Properties;

public class FormatSpecificationTest {
    public static void main(String[] args) {
        System.out.println("=== Format Specification Analysis ===");
        
        // Register the driver
        try {
            Class.forName("com.hasura.file.FileDriver");
            System.out.println("✅ Driver registered");
        } catch (ClassNotFoundException e) {
            System.err.println("❌ Failed to register driver: " + e.getMessage());
            return;
        }
        
        // Test 1: Directory with explicit arrow format (uses ArrowSchemaFactory/SafeArrowSchemaFactory)
        testExplicitArrowFormat();
        
        // Test 2: Single Arrow file (uses FileSchemaFactory with Arrow fallback)
        testSingleArrowFile();
        
        // Test 3: Directory without format (uses CSV/FileSchemaFactory)
        testDefaultCsvFormat();
        
        System.out.println("\n=== CONCLUSION ===");
        System.out.println("• ArrowSchemaFactory is used when format=arrow is explicitly specified for directories");
        System.out.println("• FileSchemaFactory is used for single files and defaults, with built-in Arrow fallback");
        System.out.println("• This is why we need both SafeArrowSchemaFactory AND FileSchemaFactory fallback");
    }
    
    private static void testExplicitArrowFormat() {
        System.out.println("\n--- Test 1: Directory with ?format=arrow (ArrowSchemaFactory) ---");
        try {
            String url = "jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/arrow?format=arrow";
            Properties props = new Properties();
            props.setProperty("caseSensitive", "false");
            
            try (Connection connection = DriverManager.getConnection(url, props)) {
                System.out.println("✅ Connection succeeded (ArrowSchemaFactory via SafeArrowSchemaFactory)");
                testTableAccess(connection);
            }
        } catch (Exception e) {
            System.out.println("❌ Failed: " + e.getMessage());
        }
    }
    
    private static void testSingleArrowFile() {
        System.out.println("\n--- Test 2: Single Arrow file (FileSchemaFactory) ---");
        try {
            String url = "jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/arrow/sample_compressed.arrow";
            Properties props = new Properties();
            props.setProperty("caseSensitive", "false");
            
            try (Connection connection = DriverManager.getConnection(url, props)) {
                System.out.println("✅ Connection succeeded (FileSchemaFactory with Arrow fallback)");
                testTableAccess(connection);
            }
        } catch (Exception e) {
            System.out.println("❌ Failed: " + e.getMessage());
        }
    }
    
    private static void testDefaultCsvFormat() {
        System.out.println("\n--- Test 3: Directory without format (FileSchemaFactory for CSV) ---");
        try {
            String url = "jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/arrow";
            Properties props = new Properties();
            props.setProperty("caseSensitive", "false");
            
            try (Connection connection = DriverManager.getConnection(url, props)) {
                System.out.println("✅ Connection succeeded (FileSchemaFactory treats as CSV)");
                testTableAccess(connection);
            }
        } catch (Exception e) {
            System.out.println("❌ Failed: " + e.getMessage());
        }
    }
    
    private static void testTableAccess(Connection connection) {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    System.out.println("  Found table: " + tableName);
                    
                    try (Statement stmt = connection.createStatement();
                         ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM files.\"" + tableName + "\"")) {
                        if (rs.next()) {
                            System.out.println("    ✅ Rows: " + rs.getInt(1));
                        }
                    } catch (Exception e) {
                        System.out.println("    ❌ Query failed: " + e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("  ❌ Table access failed: " + e.getMessage());
        }
    }
}