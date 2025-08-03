import java.sql.*;
import java.util.Properties;

public class CompressionVerificationTest {
    public static void main(String[] args) {
        System.out.println("=== Testing Compressed Arrow Files with Gandiva 18.3.0 ===");
        
        // Test the compressed Arrow file specifically
        testCompressedArrow();
    }
    
    private static void testCompressedArrow() {
        try {
            String url = "jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/arrow/sample_compressed.arrow";
            System.out.println("Testing: " + url);
            
            Properties props = new Properties();
            props.setProperty("caseSensitive", "false");
            
            try (Connection connection = DriverManager.getConnection(url, props)) {
                System.out.println("✅ Connection to compressed Arrow file succeeded");
                
                // List tables
                DatabaseMetaData metaData = connection.getMetaData();
                try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                    System.out.println("Tables in compressed Arrow file:");
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        System.out.println("  - " + tableName);
                        
                        // Try to query this table
                        String query = "SELECT COUNT(*) as row_count FROM files.\"" + tableName + "\"";
                        System.out.println("Query: " + query);
                        
                        try (Statement stmt = connection.createStatement();
                             ResultSet rs = stmt.executeQuery(query)) {
                            if (rs.next()) {
                                System.out.println("✅ Count query succeeded: " + rs.getInt("row_count") + " rows");
                            }
                        } catch (Exception e) {
                            System.out.println("❌ Query failed: " + e.getMessage());
                            if (e.getMessage().contains("LZ4_FRAME")) {
                                System.out.println("⚠️  LZ4_FRAME compression issue detected!");
                                return;
                            } else if (e.getMessage().contains("UnsatisfiedLinkError") && e.getMessage().contains("gandiva")) {
                                System.out.println("⚠️  Gandiva native library issue");
                                return;
                            }
                        }
                        
                        // Try a more complex query
                        String selectQuery = "SELECT * FROM files.\"" + tableName + "\" LIMIT 2";
                        System.out.println("Query: " + selectQuery);
                        
                        try (Statement stmt = connection.createStatement();
                             ResultSet rs = stmt.executeQuery(selectQuery)) {
                            
                            ResultSetMetaData rsmd = rs.getMetaData();
                            int columnCount = rsmd.getColumnCount();
                            
                            // Print column headers
                            for (int i = 1; i <= columnCount; i++) {
                                System.out.print(rsmd.getColumnName(i) + "\t");
                            }
                            System.out.println();
                            
                            // Print data
                            int rowCount = 0;
                            while (rs.next() && rowCount < 2) {
                                for (int i = 1; i <= columnCount; i++) {
                                    System.out.print(rs.getObject(i) + "\t");
                                }
                                System.out.println();
                                rowCount++;
                            }
                            System.out.println("✅ SELECT query on compressed Arrow succeeded");
                        } catch (Exception e) {
                            System.out.println("❌ SELECT query failed: " + e.getMessage());
                            if (e.getMessage().contains("LZ4_FRAME")) {
                                System.out.println("⚠️  LZ4_FRAME compression issue detected!");
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("❌ Connection failed: " + e.getMessage());
            if (e.getMessage().contains("LZ4_FRAME")) {
                System.out.println("⚠️  LZ4_FRAME compression issue at connection level");
            } else if (e.getMessage().contains("UnsatisfiedLinkError") && e.getMessage().contains("gandiva")) {
                System.out.println("⚠️  Gandiva native library issue (Z3 missing)");
            }
        }
    }
}