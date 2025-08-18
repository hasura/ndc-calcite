import java.sql.*;
import java.io.File;

public class TestGlobCaching {
    public static void main(String[] args) throws Exception {
        Class.forName("com.hasura.file.FileDriver");
        
        String testDataDir = System.getenv("TEST_DATA_DIR");
        
        System.out.println("=== Testing Glob Pattern Caching Issue ===\n");
        
        // Test 1: CSV glob pattern
        System.out.println("Test 1: CSV glob pattern");
        String csvGlobUrl = "jdbc:file://multi?path=" + new File(testDataDir).getAbsolutePath() + "&glob=**/*.csv";
        testGlobPattern(csvGlobUrl, "CSV");
        
        // Test 2: TSV glob pattern (should fail due to caching)
        System.out.println("\nTest 2: TSV glob pattern (expected to fail due to schema caching)");
        String tsvGlobUrl = "jdbc:file://multi?path=" + new File(testDataDir).getAbsolutePath() + "&glob=**/*.tsv";
        testGlobPattern(tsvGlobUrl, "TSV");
        
        // Test 3: JSON glob pattern with explicit different schema name
        System.out.println("\nTest 3: JSON glob pattern with different schema name");
        String jsonGlobUrl = "jdbc:file://multi?path=" + new File(testDataDir).getAbsolutePath() + "&glob=**/*.json&schema=json_files";
        testGlobPatternWithSchema(jsonGlobUrl, "JSON", "json_files");
        
        System.out.println("\n=== Conclusion ===");
        System.out.println("The FileDriver caches schemas by name. Once a schema 'files' is created,");
        System.out.println("subsequent glob patterns with the same schema name are ignored.");
        System.out.println("Workaround: Use different schema names for different glob patterns.");
    }
    
    private static void testGlobPattern(String url, String expectedFormat) {
        try (Connection conn = DriverManager.getConnection(url)) {
            DatabaseMetaData meta = conn.getMetaData();
            
            System.out.println("  URL: " + url);
            System.out.println("  Looking in schema: files");
            
            int tableCount = 0;
            try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    System.out.println("  Found table: " + tableName);
                    tableCount++;
                }
            }
            
            if (tableCount == 0) {
                System.out.println("  ❌ No tables found - likely due to schema caching!");
            } else {
                System.out.println("  ✅ Found " + tableCount + " tables");
            }
        } catch (Exception e) {
            System.out.println("  ❌ Error: " + e.getMessage());
        }
    }
    
    private static void testGlobPatternWithSchema(String url, String expectedFormat, String schemaName) {
        try (Connection conn = DriverManager.getConnection(url)) {
            DatabaseMetaData meta = conn.getMetaData();
            
            System.out.println("  URL: " + url);
            System.out.println("  Looking in schema: " + schemaName);
            
            int tableCount = 0;
            try (ResultSet tables = meta.getTables(null, schemaName, "%", null)) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    System.out.println("  Found table: " + tableName);
                    tableCount++;
                }
            }
            
            if (tableCount == 0) {
                System.out.println("  ❌ No tables found");
            } else {
                System.out.println("  ✅ Found " + tableCount + " tables");
            }
        } catch (Exception e) {
            System.out.println("  ❌ Error: " + e.getMessage());
        }
    }
}