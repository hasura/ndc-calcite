import java.sql.*;
import java.io.File;

public class DebugRecursiveDirectory {
    public static void main(String[] args) throws Exception {
        Class.forName("com.hasura.file.FileDriver");
        
        String nestedDir = "/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/nested";
        String url = "jdbc:file://" + nestedDir;
        
        System.out.println("Testing nested directory: " + nestedDir);
        System.out.println("JDBC URL: " + url);
        System.out.println("Directory exists: " + new File(nestedDir).exists());
        System.out.println("Directory is directory: " + new File(nestedDir).isDirectory());
        
        try (Connection conn = DriverManager.getConnection(url)) {
            System.out.println("✅ Connection successful");
            
            DatabaseMetaData meta = conn.getMetaData();
            
            // Check tables
            try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                System.out.println("Tables found:");
                int count = 0;
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    System.out.println("  - " + tableName);
                    count++;
                }
                System.out.println("Total tables found: " + count);
                
                if (count == 0) {
                    System.out.println("❌ No tables found - recursive scanning may not be working");
                } else {
                    System.out.println("✅ Found " + count + " tables - recursive scanning appears to work");
                }
            }
            
        } catch (SQLException e) {
            System.out.println("❌ Connection failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}