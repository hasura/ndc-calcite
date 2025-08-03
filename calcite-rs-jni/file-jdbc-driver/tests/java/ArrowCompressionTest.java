import java.sql.*;
import java.util.Properties;

public class ArrowCompressionTest {
    public static void main(String[] args) {
        try {
            // Test only the compressed Arrow file
            String url = "jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/arrow";
            System.out.println("Testing Arrow 17.0.0 compression support...");
            System.out.println("URL: " + url);
            
            Properties props = new Properties();
            props.setProperty("caseSensitive", "false");
            
            try (Connection connection = DriverManager.getConnection(url, props)) {
                System.out.println("✅ Connection succeeded");
                
                // List available tables
                DatabaseMetaData metaData = connection.getMetaData();
                try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                    System.out.println("Available tables:");
                    while (tables.next()) {
                        System.out.println("  - " + tables.getString("TABLE_NAME"));
                    }
                }
                
                // Try to query a compressed file specifically
                String query = "SELECT COUNT(*) FROM files.\"SAMPLE_COMPRESSED\"";
                System.out.println("Testing compressed Arrow file with query: " + query);
                
                try (Statement stmt = connection.createStatement();
                     ResultSet rs = stmt.executeQuery(query)) {
                    if (rs.next()) {
                        System.out.println("✅ Compressed Arrow query succeeded - Count: " + rs.getInt(1));
                    }
                } catch (Exception e) {
                    System.out.println("❌ Compressed Arrow query failed: " + e.getMessage());
                    // Test the uncompressed version
                    String uncompressedQuery = "SELECT COUNT(*) FROM files.\"SAMPLE_UNCOMPRESSED\"";
                    System.out.println("Testing uncompressed version: " + uncompressedQuery);
                    try (Statement stmt = connection.createStatement();
                         ResultSet rs = stmt.executeQuery(uncompressedQuery)) {
                        if (rs.next()) {
                            System.out.println("✅ Uncompressed Arrow query succeeded - Count: " + rs.getInt(1));
                        }
                    } catch (Exception e2) {
                        System.out.println("❌ Uncompressed Arrow query also failed: " + e2.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("❌ Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}