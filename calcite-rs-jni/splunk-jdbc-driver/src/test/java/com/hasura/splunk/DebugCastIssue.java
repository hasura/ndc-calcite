package com.hasura.splunk;

import org.junit.Test;
import java.sql.*;
import java.util.Properties;

public class DebugCastIssue {
    
    @Test
    public void debugCast() throws Exception {
        // Create a simple in-memory database to test CAST behavior
        Properties props = new Properties();
        props.setProperty("model", "inline:"
            + "{\n"
            + "  version: '1.0',\n"
            + "  defaultSchema: 'TEST',\n"
            + "  schemas: [\n"
            + "    {\n"
            + "      name: 'TEST',\n"
            + "      tables: [\n"
            + "        {\n"
            + "          name: 'DATA',\n"
            + "          type: 'custom',\n"
            + "          factory: 'org.apache.calcite.adapter.java.ReflectiveSchema$Factory',\n"
            + "          operand: {\n"
            + "            class: 'com.hasura.splunk.DebugCastIssue$TestData'\n"
            + "          }\n"
            + "        }\n"
            + "      ]\n"
            + "    }\n"
            + "  ]\n"
            + "}");
        
        try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props)) {
            System.out.println("\n========== Testing CAST with in-memory data ==========");
            
            // Test 1: Direct query
            System.out.println("\nTest 1: Direct query");
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT id, bytes FROM TEST.DATA")) {
                while (rs.next()) {
                    System.out.println("id=" + rs.getInt("id") + ", bytes=" + rs.getLong("bytes"));
                }
            }
            
            // Test 2: CAST literal
            System.out.println("\nTest 2: CAST literal");
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT CAST(950 AS VARCHAR) as literal_cast")) {
                if (rs.next()) {
                    System.out.println("CAST(950 AS VARCHAR) = " + rs.getString("literal_cast"));
                }
            }
            
            // Test 3: CAST field
            System.out.println("\nTest 3: CAST field");
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT id, bytes, CAST(bytes AS VARCHAR) as bytes_cast FROM TEST.DATA")) {
                while (rs.next()) {
                    Long bytes = rs.getLong("bytes");
                    String bytesCast = rs.getString("bytes_cast");
                    System.out.println("id=" + rs.getInt("id") + 
                                     ", bytes=" + bytes + 
                                     ", CAST(bytes AS VARCHAR)=" + bytesCast);
                    
                    if (bytes != null && bytes == 950L && bytesCast == null) {
                        System.out.println("❌ CAST FAILURE: bytes=950 but CAST returned null!");
                    }
                }
            }
        }
    }
    
    // Test data class
    public static class TestData {
        public final TestRow[] DATA = {
            new TestRow(1, 950L),
            new TestRow(2, 1200L),
            new TestRow(3, null)
        };
    }
    
    public static class TestRow {
        public final int id;
        public final Long bytes;
        
        public TestRow(int id, Long bytes) {
            this.id = id;
            this.bytes = bytes;
        }
    }
}