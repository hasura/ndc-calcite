package com.hasura.file;

import java.sql.*;
import java.util.Properties;
import java.io.File;
import java.io.FileWriter;

public class CasingTestProgram {
    public static void main(String[] args) throws Exception {
        // Load the driver
        Class.forName("com.hasura.file.FileDriver");
        
        // Create test directory and CSV file
        File testDir = new File("target/test-data");
        testDir.mkdirs();
        
        File testCsv = new File(testDir, "MixedCase_Table.csv");
        try (FileWriter writer = new FileWriter(testCsv)) {
            writer.write("ProductID,Product_Name,unitPrice,InStock\n");
            writer.write("1,Widget,10.99,true\n");
            writer.write("2,Gadget,25.50,false\n");
            writer.write("3,Doohickey,15.00,true\n");
        }
        
        System.out.println("Created test CSV file: " + testCsv.getAbsolutePath());
        
        // Test 1: Default casing (table=UPPER, column=UNCHANGED)
        System.out.println("\n=== Test 1: Default Casing ===");
        testCasing(testDir.getAbsolutePath(), null);
        
        // Test 2: All lowercase
        System.out.println("\n=== Test 2: All Lowercase ===");
        Properties props = new Properties();
        props.setProperty("table_name_casing", "LOWER");
        props.setProperty("column_name_casing", "LOWER");
        testCasing(testDir.getAbsolutePath(), props);
        
        // Test 3: All uppercase
        System.out.println("\n=== Test 3: All Uppercase ===");
        props = new Properties();
        props.setProperty("table_name_casing", "UPPER");
        props.setProperty("column_name_casing", "UPPER");
        testCasing(testDir.getAbsolutePath(), props);
        
        // Test 4: All unchanged
        System.out.println("\n=== Test 4: All Unchanged ===");
        props = new Properties();
        props.setProperty("table_name_casing", "UNCHANGED");
        props.setProperty("column_name_casing", "UNCHANGED");
        testCasing(testDir.getAbsolutePath(), props);
        
        // Clean up
        // testCsv.delete();
        // testDir.delete();
        System.out.println("\nTest files kept at: " + testDir.getAbsolutePath());
    }
    
    private static void testCasing(String directory, Properties props) throws SQLException {
        String url = "jdbc:file:" + directory + "?recursive=false";
        
        try (Connection conn = props != null ? 
                DriverManager.getConnection(url, props) : 
                DriverManager.getConnection(url)) {
            
            DatabaseMetaData meta = conn.getMetaData();
            
            // List all tables
            System.out.println("Tables:");
            try (ResultSet tables = meta.getTables(null, "files", null, null)) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    System.out.println("  - " + tableName);
                    
                    // List columns for this table
                    System.out.println("    Columns:");
                    try (ResultSet columns = meta.getColumns(null, "files", tableName, null)) {
                        while (columns.next()) {
                            System.out.println("      - " + columns.getString("COLUMN_NAME"));
                        }
                    }
                }
            }
            
            // Try to query with different table names
            Statement stmt = conn.createStatement();
            String[] tableVariants = {"MIXEDCASE_TABLE", "mixedcase_table", "MixedCase_Table"};
            
            for (String tableName : tableVariants) {
                try {
                    ResultSet rs = stmt.executeQuery("SELECT * FROM files." + tableName + " LIMIT 1");
                    if (rs.next()) {
                        System.out.println("Successfully queried table as: " + tableName);
                    }
                    rs.close();
                } catch (SQLException e) {
                    System.out.println("Failed to query table as: " + tableName + " (" + e.getMessage() + ")");
                }
            }
        }
    }
}
