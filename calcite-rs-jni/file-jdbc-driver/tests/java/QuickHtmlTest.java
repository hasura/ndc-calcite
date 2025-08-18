package com.hasura.file;

import java.sql.*;
import java.io.File;

public class QuickHtmlTest {
    public static void main(String[] args) throws Exception {
        Class.forName("com.hasura.file.FileDriver");
        
        String url = "jdbc:file://" + new File("tests/data/html/report.html").getAbsolutePath();
        
        try (Connection conn = DriverManager.getConnection(url)) {
            DatabaseMetaData meta = conn.getMetaData();
            
            System.out.println("HTML Tables discovered:");
            try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    System.out.println("- " + tableName);
                }
            }
        }
        
        // Also test TSV
        System.out.println("\nTSV Tables discovered:");
        String tsvUrl = "jdbc:file://" + new File("tests/data/tsv/inventory.tsv").getAbsolutePath();
        
        try (Connection conn = DriverManager.getConnection(tsvUrl)) {
            DatabaseMetaData meta = conn.getMetaData();
            
            try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    System.out.println("- " + tableName);
                }
            }
        }
    }
}