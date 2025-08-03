import java.sql.*;
import java.io.*;

public class TestNamingRules {
    public static void main(String[] args) throws Exception {
        // Create test CSV
        try (FileWriter writer = new FileWriter("naming-test.csv")) {
            writer.write("id,name\n");
            writer.write("1,Test\n");
        }
        
        String url = "jdbc:file://" + System.getProperty("user.dir") + "/naming-test.csv";
        
        try (Connection conn = DriverManager.getConnection(url)) {
            DatabaseMetaData meta = conn.getMetaData();
            
            System.out.println("=== Schemas ===");
            try (ResultSet schemas = meta.getSchemas()) {
                while (schemas.next()) {
                    System.out.println("Schema: " + schemas.getString("TABLE_SCHEM"));
                }
            }
            
            System.out.println("\n=== Tables ===");
            try (ResultSet tables = meta.getTables(null, null, "%", null)) {
                while (tables.next()) {
                    System.out.println("Schema: " + tables.getString("TABLE_SCHEM") + 
                                     ", Table: " + tables.getString("TABLE_NAME"));
                }
            }
            
            // Try different query variations
            System.out.println("\n=== Query Tests ===");
            
            String[] queries = {
                "SELECT * FROM NAMING-TEST",
                "SELECT * FROM \"NAMING-TEST\"", 
                "SELECT * FROM files.NAMING-TEST",
                "SELECT * FROM files.\"NAMING-TEST\"",
                "SELECT * FROM \"files\".\"NAMING-TEST\""
            };
            
            for (String query : queries) {
                try (Statement stmt = conn.createStatement()) {
                    System.out.print("Query: " + query + " - ");
                    ResultSet rs = stmt.executeQuery(query);
                    if (rs.next()) {
                        System.out.println("SUCCESS");
                    }
                } catch (SQLException e) {
                    System.out.println("FAILED: " + e.getMessage());
                }
            }
        }
    }
}