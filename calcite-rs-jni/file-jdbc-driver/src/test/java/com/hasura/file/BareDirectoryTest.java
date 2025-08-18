package com.hasura.file;

import java.sql.*;
import java.util.Properties;
import java.io.File;
import java.io.FileWriter;

public class BareDirectoryTest {
    public static void main(String[] args) throws Exception {
        // Load the driver
        Class.forName("com.hasura.file.FileDriver");
        
        // Create test directory structure
        File testDir = new File("target/bare-dir-test");
        testDir.mkdirs();
        
        // Create CSV files in root directory
        File rootCsv1 = new File(testDir, "customers.csv");
        try (FileWriter writer = new FileWriter(rootCsv1)) {
            writer.write("id,name,email\n");
            writer.write("1,John Doe,john@example.com\n");
            writer.write("2,Jane Smith,jane@example.com\n");
        }
        
        File rootCsv2 = new File(testDir, "products.csv");
        try (FileWriter writer = new FileWriter(rootCsv2)) {
            writer.write("id,name,price\n");
            writer.write("1,Widget,19.99\n");
            writer.write("2,Gadget,29.99\n");
        }
        
        // Create subdirectory with more CSV files
        File subDir = new File(testDir, "archive");
        subDir.mkdirs();
        
        File subCsv1 = new File(subDir, "orders_2023.csv");
        try (FileWriter writer = new FileWriter(subCsv1)) {
            writer.write("id,customer_id,total\n");
            writer.write("1,1,49.99\n");
            writer.write("2,2,29.99\n");
        }
        
        // Create a deeper subdirectory
        File deepDir = new File(subDir, "old");
        deepDir.mkdirs();
        
        File deepCsv = new File(deepDir, "orders_2022.csv");
        try (FileWriter writer = new FileWriter(deepCsv)) {
            writer.write("id,customer_id,total\n");
            writer.write("1,1,39.99\n");
        }
        
        System.out.println("Created test structure:");
        System.out.println("  " + testDir.getAbsolutePath() + "/");
        System.out.println("    customers.csv");
        System.out.println("    products.csv");
        System.out.println("    archive/");
        System.out.println("      orders_2023.csv");
        System.out.println("      old/");
        System.out.println("        orders_2022.csv");
        
        // Test 1: Bare directory with recursive=false (should find only root files)
        System.out.println("\n=== Test 1: Bare directory with recursive=false ===");
        String url = "jdbc:file:" + testDir.getAbsolutePath() + "?recursive=false";
        System.out.println("URL: " + url);
        testConnection(url, null);
        
        // Test 2: Bare directory with recursive=true (should find all files)
        System.out.println("\n=== Test 2: Bare directory with recursive=true ===");
        url = "jdbc:file:" + testDir.getAbsolutePath() + "?recursive=true";
        System.out.println("URL: " + url);
        testConnection(url, null);
        
        // Test 3: Bare directory with no recursive parameter (should default to true)
        System.out.println("\n=== Test 3: Bare directory with no recursive parameter (default) ===");
        url = "jdbc:file:" + testDir.getAbsolutePath();
        System.out.println("URL: " + url);
        testConnection(url, null);
        
        // Test 4: Using properties instead of URL parameters
        System.out.println("\n=== Test 4: Using properties for recursive=false ===");
        url = "jdbc:file:" + testDir.getAbsolutePath();
        Properties props = new Properties();
        props.setProperty("recursive", "false");
        System.out.println("URL: " + url + " with props: recursive=false");
        testConnection(url, props);
        
        // Clean up
        deepCsv.delete();
        deepDir.delete();
        subCsv1.delete();
        subDir.delete();
        rootCsv1.delete();
        rootCsv2.delete();
        testDir.delete();
        
        System.out.println("\n=== All tests completed ===");
    }
    
    private static void testConnection(String url, Properties props) {
        try (Connection conn = props != null ? 
                DriverManager.getConnection(url, props) : 
                DriverManager.getConnection(url)) {
            
            DatabaseMetaData meta = conn.getMetaData();
            
            System.out.println("\nTables found:");
            try (ResultSet tables = meta.getTables(null, "files", null, null)) {
                int count = 0;
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    System.out.println("  - " + tableName);
                    count++;
                }
                System.out.println("Total tables: " + count);
            }
            
            // Try to query one of the tables
            System.out.println("\nQuerying CUSTOMERS table:");
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM files.CUSTOMERS")) {
                while (rs.next()) {
                    System.out.println("  " + rs.getInt("id") + ": " + 
                        rs.getString("name") + " (" + rs.getString("email") + ")");
                }
            } catch (SQLException e) {
                System.out.println("  Failed to query CUSTOMERS: " + e.getMessage());
            }
            
        } catch (SQLException e) {
            System.err.println("Connection failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
