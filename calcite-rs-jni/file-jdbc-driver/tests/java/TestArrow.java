import java.sql.*;

public class TestArrow {
    public static void main(String[] args) throws Exception {
        String arrowFile = "/Users/kennethstott/ndc-calcite/calcite-rs-jni/file-jdbc-driver/tests/data/arrow/sample.arrow";
        String url = "jdbc:file://" + arrowFile;
        
        System.out.println("Testing Arrow file: " + arrowFile);
        System.out.println("JDBC URL: " + url);
        
        try (Connection conn = DriverManager.getConnection(url)) {
            System.out.println("✅ Connection successful");
            
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet tables = meta.getTables(null, "files", "%", null);
            
            System.out.println("Tables found:");
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                System.out.println("  - " + tableName);
                
                // Try to query the table
                String sql = "SELECT * FROM files.\"" + tableName + "\" LIMIT 1";
                System.out.println("Query: " + sql);
                
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(sql)) {
                    
                    ResultSetMetaData rsmd = rs.getMetaData();
                    int columnCount = rsmd.getColumnCount();
                    
                    // Print column headers
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(rsmd.getColumnName(i));
                        if (i < columnCount) System.out.print(" | ");
                    }
                    System.out.println();
                    
                    // Print row data
                    if (rs.next()) {
                        for (int i = 1; i <= columnCount; i++) {
                            System.out.print(rs.getString(i));
                            if (i < columnCount) System.out.print(" | ");
                        }
                        System.out.println();
                        System.out.println("✅ Query successful!");
                    }
                } catch (Exception e) {
                    System.out.println("❌ Query failed: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.out.println("❌ Connection failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}