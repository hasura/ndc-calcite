import java.sql.*;
import java.io.File;
import java.util.Properties;

public class DebugDriverOperand {
    public static void main(String[] args) throws Exception {
        // Load the driver class and add debugging
        Class<?> driverClass = Class.forName("com.hasura.file.FileDriver");
        System.out.println("Driver class loaded: " + driverClass.getName());
        
        String nestedDir = "/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/nested";
        String url = "jdbc:file://" + nestedDir;
        
        System.out.println("Testing driver with nested directory: " + nestedDir);
        System.out.println("JDBC URL: " + url);
        System.out.println("Directory exists: " + new File(nestedDir).exists());
        System.out.println("Directory is directory: " + new File(nestedDir).isDirectory());
        
        // Test with debug properties
        Properties props = new Properties();
        // Add any debug properties that might help
        
        try (Connection conn = DriverManager.getConnection(url, props)) {
            System.out.println("✅ Connection successful through driver");
            
            DatabaseMetaData meta = conn.getMetaData();
            
            // Check tables
            try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                System.out.println("Tables found through driver:");
                int count = 0;
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    System.out.println("  - " + tableName);
                    count++;
                }
                System.out.println("Total tables found through driver: " + count);
                
                if (count == 0) {
                    System.out.println("❌ Driver is not finding tables - need to debug operand creation");
                } else {
                    System.out.println("✅ Driver found " + count + " tables");
                }
            }
            
        } catch (SQLException e) {
            System.out.println("❌ Driver connection failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}