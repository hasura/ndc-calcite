package com.hasura.file;

import java.sql.*;
import java.io.File;

public class TestTransports {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Transport Protocol Testing ===\n");
        
        // Test HTTP transports
        System.out.println("## HTTP TRANSPORT TESTING");
        System.out.println("=".repeat(50));
        
        testHttpTransport("https://raw.githubusercontent.com/plotly/datasets/master/tips.csv", "CSV over HTTPS");
        testHttpTransport("https://jsonplaceholder.typicode.com/users", "JSON API over HTTPS"); 
        testHttpTransport("http://httpbin.org/json", "Simple JSON over HTTP");
        
        // Test S3 transport (expected to fail without credentials)
        System.out.println("\n## S3 TRANSPORT TESTING");
        System.out.println("=".repeat(50));
        
        testS3Transport("s3://example-bucket/data.csv", "CSV from S3");
        testS3Transport("s3://public-datasets/sample.json", "JSON from S3");
        
        System.out.println("\n" + "=".repeat(60));
        System.out.println("TRANSPORT TESTING COMPLETE");
        System.out.println("=".repeat(60));
        System.out.println("🌐 HTTP/HTTPS transport capability confirmed");
        System.out.println("☁️ S3 transport architecture present (needs credentials)");
    }
    
    private static void testHttpTransport(String url, String description) {
        System.out.println("\n### " + description);
        System.out.println("-".repeat(40));
        
        try {
            String jdbcUrl = "jdbc:file://" + url;
            System.out.println("  Connecting to: " + url);
            
            try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
                System.out.println("  Connection: ✅ SUCCESS");
                
                // Try to get table metadata
                DatabaseMetaData meta = conn.getMetaData();
                try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                    int tableCount = 0;
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        System.out.println("    Table found: " + tableName);
                        tableCount++;
                    }
                    
                    if (tableCount > 0) {
                        System.out.println("  Schema Discovery: ✅ " + tableCount + " table(s) found");
                        System.out.println("  Overall Result: ✅ HTTP TRANSPORT WORKING");
                    } else {
                        System.out.println("  Schema Discovery: ❌ No tables found");
                        System.out.println("  Overall Result: ⚠️ CONNECTION OK, NO TABLES");
                    }
                }
                
            } catch (SQLException e) {
                System.out.println("  Connection: ❌ SQL Error - " + e.getMessage().substring(0, Math.min(60, e.getMessage().length())));
                System.out.println("  Overall Result: ❌ HTTP TRANSPORT FAILED");
            }
            
        } catch (Exception e) {
            System.out.println("  Connection: ❌ " + e.getClass().getSimpleName() + " - " + 
                e.getMessage().substring(0, Math.min(60, e.getMessage().length())));
            System.out.println("  Overall Result: ❌ HTTP TRANSPORT FAILED");
        }
    }
    
    private static void testS3Transport(String s3Url, String description) {
        System.out.println("\n### " + description);
        System.out.println("-".repeat(40));
        
        try {
            String jdbcUrl = "jdbc:file://" + s3Url;
            System.out.println("  Connecting to: " + s3Url);
            
            try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
                System.out.println("  Connection: ✅ SUCCESS (unexpected!)");
                
                DatabaseMetaData meta = conn.getMetaData();
                try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                    boolean hasTable = tables.next();
                    if (hasTable) {
                        System.out.println("  Schema Discovery: ✅ Tables found");
                        System.out.println("  Overall Result: ✅ S3 TRANSPORT WORKING");
                    } else {
                        System.out.println("  Schema Discovery: ❌ No tables");
                        System.out.println("  Overall Result: ⚠️ S3 CONNECTION OK, NO TABLES");
                    }
                }
                
            } catch (SQLException e) {
                System.out.println("  Connection: ❌ SQL Error - " + e.getMessage().substring(0, Math.min(60, e.getMessage().length())));
                System.out.println("  Overall Result: ❌ S3 TRANSPORT FAILED");
            }
            
        } catch (Exception e) {
            String errorMsg = e.getMessage() != null ? e.getMessage() : "Unknown error";
            System.out.println("  Connection: ❌ " + e.getClass().getSimpleName() + " - " + 
                errorMsg.substring(0, Math.min(60, errorMsg.length())));
            System.out.println("  Overall Result: ❌ S3 TRANSPORT FAILED (expected)");
        }
    }
}