package com.hasura.file;

import java.sql.*;
import java.io.File;
import java.io.FileWriter;

public class RecursiveTest {
    public static void main(String[] args) throws Exception {
        // Load the driver
        Class.forName("com.hasura.file.FileDriver");
        
        // Create test directory structure
        File testDir = new File("target/recursive-test");
        testDir.mkdirs();
        
        // Create a CSV file in root
        File rootCsv = new File(testDir, "root.csv");
        try (FileWriter writer = new FileWriter(rootCsv)) {
            writer.write("id,name\n");
            writer.write("1,Root Item\n");
        }
        
        // Create a subdirectory with another CSV
        File subDir = new File(testDir, "subdir");
        subDir.mkdirs();
        File subCsv = new File(subDir, "sub.csv");
        try (FileWriter writer = new FileWriter(subCsv)) {
            writer.write("id,name\n");
            writer.write("2,Sub Item\n");
        }
        
        System.out.println("Created test structure:");
        System.out.println("  " + rootCsv.getAbsolutePath());
        System.out.println("  " + subCsv.getAbsolutePath());
        
        // Test 1: With recursive=false (default should now be false)
        System.out.println("\n=== Test 1: recursive=false ===");
        String url = "jdbc:file:" + testDir.getAbsolutePath() + "?recursive=false";
        System.out.println("URL: " + url);
        
        try (Connection conn = DriverManager.getConnection(url)) {
            DatabaseMetaData meta = conn.getMetaData();
            
            System.out.println("\nTables found:");
            try (ResultSet tables = meta.getTables(null, "files", null, null)) {
                int count = 0;
                while (tables.next()) {
                    System.out.println("  - " + tables.getString("TABLE_NAME"));
                    count++;
                }
                System.out.println("Total tables: " + count);
            }
        }
        
        // Test 2: With recursive=true
        System.out.println("\n=== Test 2: recursive=true ===");
        url = "jdbc:file:" + testDir.getAbsolutePath() + "?recursive=true";
        System.out.println("URL: " + url);
        
        try (Connection conn = DriverManager.getConnection(url)) {
            DatabaseMetaData meta = conn.getMetaData();
            
            System.out.println("\nTables found:");
            try (ResultSet tables = meta.getTables(null, "files", null, null)) {
                int count = 0;
                while (tables.next()) {
                    System.out.println("  - " + tables.getString("TABLE_NAME"));
                    count++;
                }
                System.out.println("Total tables: " + count);
            }
        }
        
        // Clean up
        subCsv.delete();
        subDir.delete();
        rootCsv.delete();
        testDir.delete();
    }
}
