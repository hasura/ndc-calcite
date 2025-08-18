package com.hasura.file;

import java.sql.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

public class TestGlobPattern {
    public static void main(String[] args) {
        System.out.println("Testing glob pattern file matching...\n");
        
        try {
            setupTestFiles();
            testLocalGlobPattern();
            testS3GlobPattern();
            
            System.out.println("\n✓ All glob pattern tests completed successfully!");
            
        } catch (Exception e) {
            System.err.println("✗ Glob pattern tests failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            cleanupTestFiles();
        }
    }
    
    private static void setupTestFiles() throws IOException {
        System.out.println("Setting up test directory structure...");
        
        // Create test directory structure
        Files.createDirectories(Paths.get("test-glob-data/2024/01"));
        Files.createDirectories(Paths.get("test-glob-data/2024/02"));
        Files.createDirectories(Paths.get("test-glob-data/2025"));
        Files.createDirectories(Paths.get("test-glob-data/reports"));
        
        // Create CSV files with sample data
        createCsvFile("test-glob-data/2024/01/sales.csv", 
            "id,product,amount,date\n" +
            "1,Widget,100,2024-01-15\n" +
            "2,Gadget,200,2024-01-20");
        
        createCsvFile("test-glob-data/2024/02/sales.csv", 
            "id,product,amount,date\n" +
            "3,Widget,150,2024-02-10\n" +
            "4,Gadget,250,2024-02-15");
        
        createCsvFile("test-glob-data/2025/sales.csv", 
            "id,product,amount,date\n" +
            "5,Widget,300,2025-01-05\n" +
            "6,Gadget,400,2025-01-10");
        
        createCsvFile("test-glob-data/reports/quarterly.csv",
            "quarter,revenue,profit\n" +
            "Q1-2024,1000,200\n" +
            "Q2-2024,1500,350");
        
        // Create JSON files
        createJsonFile("test-glob-data/2024/01/users.json",
            "[{\"id\":1,\"name\":\"Alice\",\"joined\":\"2024-01-01\"}," +
            "{\"id\":2,\"name\":\"Bob\",\"joined\":\"2024-01-15\"}]");
        
        createJsonFile("test-glob-data/2024/02/users.json",
            "[{\"id\":3,\"name\":\"Charlie\",\"joined\":\"2024-02-01\"}," +
            "{\"id\":4,\"name\":\"David\",\"joined\":\"2024-02-20\"}]");
        
        // Create  file
        createFile("test-glob-data/events.",
            "{\"event\":\"login\",\"user\":\"alice\",\"time\":\"2024-01-01T10:00:00\"}\n" +
            "{\"event\":\"logout\",\"user\":\"alice\",\"time\":\"2024-01-01T18:00:00\"}\n" +
            "{\"event\":\"login\",\"user\":\"bob\",\"time\":\"2024-01-02T09:00:00\"}");
        
        System.out.println("✓ Test files created\n");
    }
    
    private static void testLocalGlobPattern() throws SQLException {
        System.out.println("=== Testing Local File Glob Patterns ===\n");
        
        // Test 1: Recursive CSV search
        System.out.println("Test 1: Recursive CSV search (glob=**/*.csv)");
        testGlobWithQuery("jdbc:file://test-glob-data?glob=**/*.csv",
            "SELECT SUM(amount) as total FROM files.sales",
            "Total sales amount across all CSV files");
        
        // Test 2: Year-specific search
        System.out.println("\nTest 2: 2024 sales files only (glob=2024/**/sales.csv)");
        testGlobWithQuery("jdbc:file://test-glob-data?glob=2024/**/sales.csv",
            "SELECT COUNT(*) as count, SUM(amount) as total FROM files.sales",
            "2024 sales statistics");
        
        // Test 3: All JSON files
        System.out.println("\nTest 3: All JSON files (glob=**/*.json)");
        testGlobWithQuery("jdbc:file://test-glob-data?glob=**/*.json",
            "SELECT COUNT(*) as user_count FROM files.users",
            "Total users from JSON files");
        
        // Test 4: Pattern matching
        System.out.println("\nTest 4: Pattern matching (glob=**/sales*.csv)");
        String url = "jdbc:file://test-glob-data?glob=**/sales*.csv";
        try (Connection conn = DriverManager.getConnection(url)) {
            System.out.println("Connecting to: " + url);
            System.out.println("✓ Successfully connected\n");
            
            DatabaseMetaData meta = conn.getMetaData();
            System.out.println("Tables found:");
            try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    System.out.println("  - " + tableName);
                    
                    // Show sample data from each table
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery("SELECT * FROM files." + tableName + " LIMIT 2")) {
                        
                        ResultSetMetaData rsMeta = rs.getMetaData();
                        int columnCount = rsMeta.getColumnCount();
                        
                        System.out.print("    Columns: ");
                        for (int i = 1; i <= columnCount; i++) {
                            if (i > 1) System.out.print(", ");
                            System.out.print(rsMeta.getColumnName(i));
                        }
                        System.out.println();
                        
                        int rowNum = 0;
                        while (rs.next()) {
                            System.out.print("    Row " + (++rowNum) + ": ");
                            for (int i = 1; i <= columnCount; i++) {
                                if (i > 1) System.out.print(", ");
                                System.out.print(rs.getString(i));
                            }
                            System.out.println();
                        }
                    }
                }
            }
        }
        
        // Test 5: Multiple patterns would look like this (future enhancement)
        System.out.println("\nTest 5: Complex patterns");
        System.out.println("Examples of complex glob patterns:");
        System.out.println("  - data_20[0-9][0-9].csv    (matches data_2020.csv through data_2099.csv)");
        System.out.println("  - {sales,revenue}*.csv     (matches sales*.csv OR revenue*.csv)");
        System.out.println("  - !(*test*).csv            (excludes files with 'test' in name)");
    }
    
    private static void testS3GlobPattern() {
        System.out.println("\n\n=== S3 Glob Pattern Examples ===\n");
        
        System.out.println("S3 glob patterns work similarly to local patterns:");
        System.out.println("1. All Parquet files in bucket:");
        System.out.println("   jdbc:file:s3://my-bucket?glob=**/*.parquet\n");
        
        System.out.println("2. Daily log files for January 2024:");
        System.out.println("   jdbc:file:s3://logs-bucket?glob=2024/01/*.json\n");
        
        System.out.println("3. All CSV files in data folder:");
        System.out.println("   jdbc:file:s3://analytics-bucket/data?glob=*.csv\n");
        
        System.out.println("4. Recursive search for sales data:");
        System.out.println("   jdbc:file:s3://company-data?glob=**/sales_*.parquet\n");
        
        System.out.println("Note: S3 glob patterns require AWS credentials to test");
    }
    
    private static void testGlobWithQuery(String jdbcUrl, String query, String description) throws SQLException {
        System.out.println("Connecting to: " + jdbcUrl);
        
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            System.out.println("✓ Successfully connected");
            
            // Execute query
            System.out.println("\nQuery: " + query);
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                
                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();
                
                System.out.println("Result: " + description);
                if (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        String colName = meta.getColumnName(i);
                        Object value = rs.getObject(i);
                        System.out.printf("  %s: %s%n", colName, value);
                    }
                }
            }
        }
    }
    
    private static void createCsvFile(String path, String content) throws IOException {
        Files.write(Paths.get(path), content.getBytes());
    }
    
    private static void createJsonFile(String path, String content) throws IOException {
        Files.write(Paths.get(path), content.getBytes());
    }
    
    private static void createFile(String path, String content) throws IOException {
        Files.write(Paths.get(path), content.getBytes());
    }
    
    private static void cleanupTestFiles() {
        try {
            Path testDir = Paths.get("test-glob-data");
            if (Files.exists(testDir)) {
                Files.walk(testDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // Ignore
                        }
                    });
            }
        } catch (IOException e) {
            System.err.println("Warning: Could not cleanup test files: " + e.getMessage());
        }
    }
}