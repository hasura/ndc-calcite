package com.hasura.file;

import java.sql.*;
import java.util.Properties;
import java.io.File;
import java.io.FileWriter;

public class SimpleCasingTest {
    public static void main(String[] args) throws Exception {
        // Load the driver
        Class.forName("com.hasura.file.FileDriver");
        
        // Create test directory and CSV file
        File testDir = new File("target/casing-test");
        testDir.mkdirs();
        
        File testCsv = new File(testDir, "test_data.csv");
        try (FileWriter writer = new FileWriter(testCsv)) {
            writer.write("id,name,amount\n");
            writer.write("1,Product A,100.50\n");
            writer.write("2,Product B,200.75\n");
        }
        
        System.out.println("Created test CSV: " + testCsv.getAbsolutePath());
        
        // Test 1: Connect to directory with default casing
        System.out.println("\n=== Test 1: Default casing (table=UPPER) ===");
        String url = "jdbc:file:" + testDir.getAbsolutePath();
        try (Connection conn = DriverManager.getConnection(url)) {
            DatabaseMetaData meta = conn.getMetaData();
            
            System.out.println("\nTables in schema files:");
            try (ResultSet tables = meta.getTables(null, "files", null, null)) {
                while (tables.next()) {
                    System.out.println("  Table: " + tables.getString("TABLE_NAME"));
                }
            }
            
            // Try to query
            System.out.println("\nQuerying TEST_DATA table:");
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM files.TEST_DATA")) {
                ResultSetMetaData rsmd = rs.getMetaData();
                System.out.println("Columns:");
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    System.out.println("  - " + rsmd.getColumnName(i));
                }
                
                System.out.println("\nData:");
                while (rs.next()) {
                    System.out.println("  " + rs.getInt(1) + ", " + rs.getString(2) + ", " + rs.getDouble(3));
                }
            }
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
        
        // Test 2: Connect with lowercase casing
        System.out.println("\n=== Test 2: Lowercase casing ===");
        Properties props = new Properties();
        props.setProperty("table_name_casing", "LOWER");
        props.setProperty("column_name_casing", "LOWER");
        
        try (Connection conn = DriverManager.getConnection(url, props)) {
            DatabaseMetaData meta = conn.getMetaData();
            
            System.out.println("\nTables in schema files:");
            try (ResultSet tables = meta.getTables(null, "files", null, null)) {
                while (tables.next()) {
                    System.out.println("  Table: " + tables.getString("TABLE_NAME"));
                }
            }
            
            // Try to query
            System.out.println("\nQuerying test_data table:");
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM files.test_data")) {
                ResultSetMetaData rsmd = rs.getMetaData();
                System.out.println("Columns:");
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    System.out.println("  - " + rsmd.getColumnName(i));
                }
                
                System.out.println("\nData:");
                while (rs.next()) {
                    System.out.println("  " + rs.getInt(1) + ", " + rs.getString(2) + ", " + rs.getDouble(3));
                }
            }
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
        
        // Clean up
        testCsv.delete();
        testDir.delete();
    }
}
