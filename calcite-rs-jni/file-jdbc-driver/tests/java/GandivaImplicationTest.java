import java.sql.*;
import java.util.Properties;

public class GandivaImplicationTest {
    public static void main(String[] args) {
        System.out.println("=== Testing Gandiva Implications ===");
        
        // Test 1: Basic Arrow connection and table discovery
        testBasicArrowAccess();
        
        // Test 2: Simple SELECT without complex operations
        testSimpleQuery();
        
        // Test 3: More complex query with projections and filters
        testComplexQuery();
    }
    
    private static void testBasicArrowAccess() {
        System.out.println("\n--- Test 1: Basic Arrow Access ---");
        try {
            String url = "jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/arrow";
            Properties props = new Properties();
            props.setProperty("caseSensitive", "false");
            
            try (Connection connection = DriverManager.getConnection(url, props)) {
                System.out.println("✅ Connection succeeded");
                
                DatabaseMetaData metaData = connection.getMetaData();
                try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                    System.out.println("Available tables:");
                    while (tables.next()) {
                        System.out.println("  - " + tables.getString("TABLE_NAME"));
                    }
                    System.out.println("✅ Table discovery succeeded");
                }
            }
        } catch (Exception e) {
            System.out.println("❌ Basic access failed: " + e.getMessage());
            if (e.getMessage().contains("Gandiva") || e.getMessage().contains("GandivaException")) {
                System.out.println("⚠️  Gandiva-related error in basic access");
            }
        }
    }
    
    private static void testSimpleQuery() {
        System.out.println("\n--- Test 2: Simple Query ---");
        try {
            String url = "jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/arrow";
            Properties props = new Properties();
            props.setProperty("caseSensitive", "false");
            
            try (Connection connection = DriverManager.getConnection(url, props)) {
                String query = "SELECT * FROM files.\"sample\" LIMIT 2";
                System.out.println("Query: " + query);
                
                try (Statement stmt = connection.createStatement();
                     ResultSet rs = stmt.executeQuery(query)) {
                    
                    ResultSetMetaData rsmd = rs.getMetaData();
                    int columnCount = rsmd.getColumnCount();
                    
                    // Print column names
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(rsmd.getColumnName(i) + "\t");
                    }
                    System.out.println();
                    
                    // Print rows
                    int rowCount = 0;
                    while (rs.next() && rowCount < 2) {
                        for (int i = 1; i <= columnCount; i++) {
                            System.out.print(rs.getObject(i) + "\t");
                        }
                        System.out.println();
                        rowCount++;
                    }
                    System.out.println("✅ Simple query succeeded");
                }
            }
        } catch (Exception e) {
            System.out.println("❌ Simple query failed: " + e.getMessage());
            if (e.getMessage().contains("Gandiva") || e.getMessage().contains("GandivaException")) {
                System.out.println("⚠️  Gandiva required for basic queries");
            }
        }
    }
    
    private static void testComplexQuery() {
        System.out.println("\n--- Test 3: Complex Query ---");
        try {
            String url = "jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/arrow";
            Properties props = new Properties();
            props.setProperty("caseSensitive", "false");
            
            try (Connection connection = DriverManager.getConnection(url, props)) {
                String query = "SELECT product, price FROM files.\"sample\" WHERE price > 20 ORDER BY price";
                System.out.println("Query: " + query);
                
                try (Statement stmt = connection.createStatement();
                     ResultSet rs = stmt.executeQuery(query)) {
                    
                    System.out.println("product\tprice");
                    int rowCount = 0;
                    while (rs.next() && rowCount < 5) {
                        System.out.println(rs.getString("product") + "\t" + rs.getDouble("price"));
                        rowCount++;
                    }
                    System.out.println("✅ Complex query succeeded");
                }
            }
        } catch (Exception e) {
            System.out.println("❌ Complex query failed: " + e.getMessage());
            if (e.getMessage().contains("Gandiva") || e.getMessage().contains("GandivaException")) {
                System.out.println("⚠️  Gandiva required for complex operations (projections/filters)");
            } else if (e.getMessage().contains("UnsatisfiedLinkError")) {
                System.out.println("⚠️  Native library dependency issue (Z3 for Gandiva)");
            }
        }
    }
}