import java.sql.*;

public class TestFileDriver {
    public static void main(String[] args) {
        try {
            // Test 1: Direct file access
            System.out.println("=== Test 1: Direct CSV file access ===");
            String url = "jdbc:file://" + System.getProperty("user.dir") + "/test-data.csv";
            
            try (Connection conn = DriverManager.getConnection(url);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM \"test-data\" WHERE age > 28")) {
                
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                
                // Print column names
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(metaData.getColumnName(i) + "\t");
                }
                System.out.println();
                
                // Print results
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(rs.getString(i) + "\t");
                    }
                    System.out.println();
                }
            }
            
            // Test 2: Directory access
            System.out.println("\n=== Test 2: Directory access ===");
            url = "jdbc:file://" + System.getProperty("user.dir") + "?format=csv";
            
            try (Connection conn = DriverManager.getConnection(url);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total FROM \"test-data\"")) {
                
                if (rs.next()) {
                    System.out.println("Total rows: " + rs.getInt("total"));
                }
            }
            
            System.out.println("\nAll tests passed!");
            
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}