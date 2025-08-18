package com.hasura.file;

import java.sql.*;
import java.util.Properties;
import java.io.File;
import java.io.FileWriter;

public class ComprehensiveCasingTest {
    public static void main(String[] args) throws Exception {
        // Load the driver
        Class.forName("com.hasura.file.FileDriver");
        
        // Create test directory and CSV file
        File testDir = new File("target/comprehensive-casing-test");
        testDir.mkdirs();
        
        File testCsv = new File(testDir, "Product_Data.csv");
        try (FileWriter writer = new FileWriter(testCsv)) {
            writer.write("Product_ID,Product_Name,Unit_Price,In_Stock\n");
            writer.write("1,Widget Pro,99.99,true\n");
            writer.write("2,Gadget Plus,149.99,false\n");
            writer.write("3,Doohickey Max,199.99,true\n");
        }
        
        System.out.println("Created test CSV: " + testCsv.getAbsolutePath());
        System.out.println("CSV has mixed case filename: Product_Data.csv");
        System.out.println("CSV has mixed case columns: Product_ID, Product_Name, Unit_Price, In_Stock\n");
        
        // Test 1: Default behavior
        System.out.println("=== Test 1: Default Behavior (table=UPPER, column=UNCHANGED) ===");
        testConfiguration(testDir.getAbsolutePath(), null, "PRODUCT_DATA", 
            new String[] {"Product_ID", "Product_Name", "Unit_Price", "In_Stock"});
        
        // Test 2: All uppercase
        System.out.println("\n=== Test 2: All Uppercase ===");
        Properties props = new Properties();
        props.setProperty("table_name_casing", "UPPER");
        props.setProperty("column_name_casing", "UPPER");
        testConfiguration(testDir.getAbsolutePath(), props, "PRODUCT_DATA", 
            new String[] {"PRODUCT_ID", "PRODUCT_NAME", "UNIT_PRICE", "IN_STOCK"});
        
        // Test 3: All lowercase
        System.out.println("\n=== Test 3: All Lowercase ===");
        props = new Properties();
        props.setProperty("table_name_casing", "LOWER");
        props.setProperty("column_name_casing", "LOWER");
        testConfiguration(testDir.getAbsolutePath(), props, "product_data", 
            new String[] {"product_id", "product_name", "unit_price", "in_stock"});
        
        // Test 4: Mixed - uppercase tables, lowercase columns
        System.out.println("\n=== Test 4: Mixed (table=UPPER, column=LOWER) ===");
        props = new Properties();
        props.setProperty("table_name_casing", "UPPER");
        props.setProperty("column_name_casing", "LOWER");
        testConfiguration(testDir.getAbsolutePath(), props, "PRODUCT_DATA", 
            new String[] {"product_id", "product_name", "unit_price", "in_stock"});
        
        // Test 5: Unchanged
        System.out.println("\n=== Test 5: All Unchanged ===");
        props = new Properties();
        props.setProperty("table_name_casing", "UNCHANGED");
        props.setProperty("column_name_casing", "UNCHANGED");
        testConfiguration(testDir.getAbsolutePath(), props, "Product_Data", 
            new String[] {"Product_ID", "Product_Name", "Unit_Price", "In_Stock"});
        
        // Clean up
        testCsv.delete();
        testDir.delete();
        
        System.out.println("\n=== All tests completed successfully! ===");
    }
    
    private static void testConfiguration(String directory, Properties props, 
                                         String expectedTableName, String[] expectedColumns) throws SQLException {
        String url = "jdbc:file:" + directory + "?recursive=false";
        
        try (Connection conn = props != null ? 
                DriverManager.getConnection(url, props) : 
                DriverManager.getConnection(url)) {
            
            DatabaseMetaData meta = conn.getMetaData();
            
            // Check table name
            System.out.println("Expected table name: " + expectedTableName);
            boolean tableFound = false;
            try (ResultSet tables = meta.getTables(null, "files", null, null)) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    System.out.println("  Found table: " + tableName);
                    if (tableName.equals(expectedTableName)) {
                        tableFound = true;
                    }
                }
            }
            
            if (!tableFound) {
                throw new RuntimeException("Expected table " + expectedTableName + " not found!");
            }
            
            // Check column names
            System.out.println("Expected columns: " + String.join(", ", expectedColumns));
            try (ResultSet columns = meta.getColumns(null, "files", expectedTableName, null)) {
                int colIndex = 0;
                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME");
                    String expectedColumn = expectedColumns[colIndex++];
                    System.out.println("  Found column: " + columnName + " (expected: " + expectedColumn + ")");
                    if (!columnName.equals(expectedColumn)) {
                        throw new RuntimeException("Column mismatch: expected " + expectedColumn + ", got " + columnName);
                    }
                }
            }
            
            // Try a query
            System.out.println("Executing query...");
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM files.\"" + expectedTableName + "\" LIMIT 1")) {
                ResultSetMetaData rsmd = rs.getMetaData();
                
                // Verify column names in result set
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    String colName = rsmd.getColumnName(i);
                    String expected = expectedColumns[i-1];
                    if (!colName.equals(expected)) {
                        throw new RuntimeException("ResultSet column mismatch: expected " + expected + ", got " + colName);
                    }
                }
                
                if (rs.next()) {
                    System.out.println("  Query successful! First row: " + 
                        rs.getString(1) + ", " + rs.getString(2) + ", " + 
                        rs.getString(3) + ", " + rs.getString(4));
                }
            }
            
            System.out.println("  ✓ Test passed!");
        }
    }
}
