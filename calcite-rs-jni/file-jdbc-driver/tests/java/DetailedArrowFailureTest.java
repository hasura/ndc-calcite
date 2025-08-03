import java.sql.*;
import java.util.Properties;

public class DetailedArrowFailureTest {
    public static void main(String[] args) {
        System.out.println("=== Detailed Arrow Failure Test ===");
        
        // Test at each step to see exactly where the Z3 dependency hits
        testFileSchemaFactoryDirectly();
    }
    
    private static void testFileSchemaFactoryDirectly() {
        System.out.println("\n--- Testing FileSchemaFactory with Arrow - Step by Step ---");
        
        try {
            // Force FileSchemaFactory by temporarily disabling SafeArrowSchemaFactory
            String url = "jdbc:file:///Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/arrow/sample_compressed.arrow";
            System.out.println("1. Creating connection to: " + url);
            
            Properties props = new Properties();
            props.setProperty("caseSensitive", "false");
            
            try (Connection connection = DriverManager.getConnection(url, props)) {
                System.out.println("2. ✅ Connection created successfully");
                
                try {
                    System.out.println("3. Getting database metadata...");
                    DatabaseMetaData metaData = connection.getMetaData();
                    System.out.println("4. ✅ Database metadata obtained");
                    
                    System.out.println("5. Getting table list...");
                    try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                        System.out.println("6. ✅ Table metadata query executed");
                        
                        while (tables.next()) {
                            String tableName = tables.getString("TABLE_NAME");
                            System.out.println("7. ✅ Found table: " + tableName);
                            
                            System.out.println("8. Attempting to query table...");
                            String query = "SELECT COUNT(*) FROM files.\"" + tableName + "\"";
                            System.out.println("   Query: " + query);
                            
                            try (Statement stmt = connection.createStatement()) {
                                System.out.println("9. ✅ Statement created");
                                
                                try (ResultSet rs = stmt.executeQuery(query)) {
                                    System.out.println("10. ⚠️  This is where Z3 dependency should hit!");
                                    
                                    if (rs.next()) {
                                        System.out.println("11. ✅ Query result: " + rs.getInt(1));
                                    }
                                } catch (Exception e) {
                                    System.out.println("10. ❌ Query execution failed: " + e.getMessage());
                                    if (e.getMessage().contains("z3") || e.getMessage().contains("gandiva") || 
                                        e.getMessage().contains("UnsatisfiedLinkError")) {
                                        System.out.println("    ⚠️  This confirms Z3 dependency in query execution!");
                                    }
                                    return;
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("❌ Table discovery failed: " + e.getMessage());
                    if (e.getMessage().contains("z3") || e.getMessage().contains("gandiva") || 
                        e.getMessage().contains("UnsatisfiedLinkError")) {
                        System.out.println("    ⚠️  This confirms Z3 dependency in table discovery!");
                    }
                    return;
                }
            }
        } catch (Exception e) {
            System.out.println("❌ Connection failed: " + e.getMessage());
            if (e.getMessage().contains("z3") || e.getMessage().contains("gandiva") || 
                e.getMessage().contains("UnsatisfiedLinkError")) {
                System.out.println("    ⚠️  This confirms Z3 dependency at connection level!");
            }
            e.printStackTrace();
        }
    }
}