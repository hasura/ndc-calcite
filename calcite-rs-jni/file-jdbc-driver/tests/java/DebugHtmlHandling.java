import java.sql.*;
import java.io.File;

public class DebugHtmlHandling {
    public static void main(String[] args) throws Exception {
        Class.forName("com.hasura.file.FileDriver");
        
        String htmlFile = "/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/html/report.html";
        String url = "jdbc:file://" + htmlFile;
        
        System.out.println("Testing HTML file: " + htmlFile);
        System.out.println("JDBC URL: " + url);
        System.out.println("File exists: " + new File(htmlFile).exists());
        System.out.println("File ends with .html: " + htmlFile.toLowerCase().endsWith(".html"));
        System.out.println("Format should be detected as html");
        
        try (Connection conn = DriverManager.getConnection(url)) {
            System.out.println("✅ Connection successful");
            
            DatabaseMetaData meta = conn.getMetaData();
            
            // Check tables
            try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                System.out.println("Tables found:");
                boolean foundAny = false;
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    System.out.println("  - " + tableName);
                    foundAny = true;
                }
                if (!foundAny) {
                    System.out.println("  (no tables found)");
                }
            }
            
        } catch (SQLException e) {
            System.out.println("❌ Connection failed: " + e.getMessage());
        }
    }
}