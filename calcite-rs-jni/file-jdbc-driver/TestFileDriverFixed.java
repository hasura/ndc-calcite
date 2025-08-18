import java.sql.*;

public class TestFileDriverFixed {
    public static void main(String[] args) {
        try {
            System.out.println("=== File JDBC Driver Test ===");
            String url = "jdbc:file://" + System.getProperty("user.dir") + "/test-data.csv";
            
            try (Connection conn = DriverManager.getConnection(url);
                 Statement stmt = conn.createStatement()) {
                
                // Test 1: SELECT with WHERE clause
                System.out.println("\n1. SELECT with WHERE age > 28:");
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM files.\"TEST-DATA\" WHERE age > 28")) {
                    printResultSet(rs);
                }
                
                // Test 2: Aggregate function
                System.out.println("\n2. COUNT and AVG:");
                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as total, AVG(age) as avg_age FROM files.\"TEST-DATA\"")) {
                    printResultSet(rs);
                }
                
                // Test 3: GROUP BY
                System.out.println("\n3. GROUP BY city:");
                try (ResultSet rs = stmt.executeQuery("SELECT city, COUNT(*) as count FROM files.\"TEST-DATA\" GROUP BY city ORDER BY count DESC")) {
                    printResultSet(rs);
                }
            }
            
            System.out.println("\nAll tests passed!");
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        
        // Print column names
        for (int i = 1; i <= columnCount; i++) {
            System.out.print(metaData.getColumnName(i) + "\t");
        }
        System.out.println();
        System.out.println("-".repeat(40));
        
        // Print results
        while (rs.next()) {
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(rs.getString(i) + "\t");
            }
            System.out.println();
        }
    }
}