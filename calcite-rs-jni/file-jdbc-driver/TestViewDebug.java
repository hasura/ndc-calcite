import java.sql.*;
import java.io.*;

public class TestViewDebug {
    public static void main(String[] args) throws Exception {
        // Create test CSV
        try (FileWriter writer = new FileWriter("view-test.csv")) {
            writer.write("id,name,amount\n");
            writer.write("1,Widget,100\n");
            writer.write("2,Gadget,200\n");
        }
        
        String url = "jdbc:file://" + System.getProperty("user.dir") + "/view-test.csv";
        
        try (Connection conn = DriverManager.getConnection(url)) {
            // First, verify the base table name
            DatabaseMetaData meta = conn.getMetaData();
            System.out.println("=== Base Tables ===");
            try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                while (tables.next()) {
                    System.out.println("Table: " + tables.getString("TABLE_NAME"));
                }
            }
            
            // Test basic query
            System.out.println("\n=== Basic Query Test ===");
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM files.\"VIEW-TEST\"")) {
                System.out.println("Basic query works!");
            }
        }
        
        // Now test with a view - try different SQL formats
        String[] viewSqls = {
            "SELECT * FROM files.\\\"VIEW-TEST\\\"",
            "SELECT * FROM \\\"files\\\".\\\"VIEW-TEST\\\"",
            "SELECT * FROM VIEW-TEST",
            "SELECT * FROM \\\"VIEW-TEST\\\""
        };
        
        for (int i = 0; i < viewSqls.length; i++) {
            String viewSql = viewSqls[i];
            String views = "[{\"name\":\"test_view\",\"sql\":\"" + viewSql + "\"}]";
            String viewUrl = url + "?views=" + views;
            
            System.out.println("\n=== View Test " + (i+1) + " ===");
            System.out.println("View SQL: " + viewSql);
            
            try (Connection conn = DriverManager.getConnection(viewUrl)) {
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT * FROM files.\"TEST_VIEW\"")) {
                    System.out.println("SUCCESS!");
                } catch (SQLException e) {
                    System.out.println("FAILED: " + e.getMessage());
                }
            } catch (Exception e) {
                System.out.println("Connection FAILED: " + e.getMessage());
            }
        }
    }
}