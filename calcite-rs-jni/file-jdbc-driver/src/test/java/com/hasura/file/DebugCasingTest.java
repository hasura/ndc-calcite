package com.hasura.file;

import java.sql.*;
import java.util.Properties;
import java.io.File;
import java.io.FileWriter;

public class DebugCasingTest {
    public static void main(String[] args) throws Exception {
        // Load the driver
        Class.forName("com.hasura.file.FileDriver");
        
        // Create test directory and CSV file  
        File testDir = new File("target/debug-test");
        testDir.mkdirs();
        
        // Create a simple CSV file
        File testCsv = new File(testDir, "test.csv");
        try (FileWriter writer = new FileWriter(testCsv)) {
            writer.write("id,name\n");
            writer.write("1,Test1\n");
            writer.write("2,Test2\n");
        }
        
        System.out.println("Created test CSV: " + testCsv.getAbsolutePath());
        
        // Try connecting directly to the CSV file
        System.out.println("\n=== Test 1: Direct file connection ===");
        String fileUrl = "jdbc:file://" + testCsv.getAbsolutePath();
        System.out.println("URL: " + fileUrl);
        
        try (Connection conn = DriverManager.getConnection(fileUrl)) {
            DatabaseMetaData meta = conn.getMetaData();
            System.out.println("Connected successfully!");
            
            // List tables
            System.out.println("\nTables:");
            try (ResultSet tables = meta.getTables(null, "files", null, null)) {
                while (tables.next()) {
                    System.out.println("  - " + tables.getString("TABLE_NAME"));
                }
            }
            
            // Try to query
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM files.TEST")) {
                System.out.println("\nQuery successful!");
                while (rs.next()) {
                    System.out.println("  " + rs.getInt(1) + ", " + rs.getString(2));
                }
            }
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
        
        // Try connecting to the directory
        System.out.println("\n=== Test 2: Directory connection ===");
        String dirUrl = "jdbc:file:" + testDir.getAbsolutePath();
        System.out.println("URL: " + dirUrl);
        
        try (Connection conn = DriverManager.getConnection(dirUrl)) {
            DatabaseMetaData meta = conn.getMetaData();
            System.out.println("Connected successfully!");
            
            // List tables
            System.out.println("\nTables:");
            try (ResultSet tables = meta.getTables(null, "files", null, null)) {
                while (tables.next()) {
                    System.out.println("  - " + tables.getString("TABLE_NAME"));
                }
            }
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
        
        // Try with table_name_casing=LOWER
        System.out.println("\n=== Test 3: Directory with table_name_casing=LOWER ===");
        Properties props = new Properties();
        props.setProperty("table_name_casing", "LOWER");
        
        try (Connection conn = DriverManager.getConnection(dirUrl, props)) {
            DatabaseMetaData meta = conn.getMetaData();
            System.out.println("Connected successfully!");
            
            // List tables
            System.out.println("\nTables:");
            try (ResultSet tables = meta.getTables(null, "files", null, null)) {
                while (tables.next()) {
                    System.out.println("  - " + tables.getString("TABLE_NAME"));
                }
            }
            
            // Try to query with lowercase table name
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM files.test")) {
                System.out.println("\nQuery successful with lowercase table name!");
                while (rs.next()) {
                    System.out.println("  " + rs.getInt(1) + ", " + rs.getString(2));
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
