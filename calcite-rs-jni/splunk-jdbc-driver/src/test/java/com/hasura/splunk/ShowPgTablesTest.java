package com.hasura.splunk;

import org.junit.Test;
import org.junit.BeforeClass;

import java.sql.*;
import java.util.Properties;

/**
 * Test to show the actual output of querying pg_catalog.pg_tables with all 24 CIM models.
 */
public class ShowPgTablesTest {
    
    private static Properties localProperties;
    
    @BeforeClass
    public static void setUpClass() throws Exception {
        Class.forName("com.hasura.splunk.SplunkDriver");
        
        // Load local properties
        localProperties = new Properties();
        try (java.io.FileInputStream fis = new java.io.FileInputStream("local-properties.settings")) {
            localProperties.load(fis);
        } catch (java.io.IOException e) {
            System.out.println("Could not load local-properties.settings: " + e.getMessage());
            localProperties = new Properties();
        }
    }
    
    @Test
    public void showPgTablesOutput() throws Exception {
        System.out.println("\n=== pg_catalog.pg_tables Output (All 24 CIM Models) ===");
        
        // Connect without specifying cimModels to get all 24
        String splunkUrl = localProperties.getProperty("splunk.url", "https://kentest.xyz:8089");
        java.net.URI uri = new java.net.URI(splunkUrl);
        String jdbcUrl = String.format("jdbc:splunk://%s:%d/main", uri.getHost(), uri.getPort());
        
        Properties props = new Properties();
        props.setProperty("user", localProperties.getProperty("splunk.username", "admin"));
        props.setProperty("password", localProperties.getProperty("splunk.password", ""));
        props.setProperty("ssl", "true");
        props.setProperty("disableSslValidation", "true");
        props.setProperty("schema", "splunk");
        // NOTE: No cimModels specified - will get all 24
        
        try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
            
            // Query pg_catalog.pg_tables
            String query = "SELECT schemaname, tablename, tableowner, hasindexes, hasrules, hastriggers " +
                          "FROM pg_catalog.pg_tables " +
                          "WHERE schemaname = 'splunk' " +
                          "ORDER BY tablename";
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                
                System.out.println();
                System.out.printf("%-20s | %-20s | %-15s | %-10s | %-8s | %-11s%n", 
                    "SCHEMANAME", "TABLENAME", "TABLEOWNER", "HASINDEXES", "HASRULES", "HASTRIGGERS");
                System.out.println("---------------------|----------------------|-----------------|------------|----------|------------");
                
                int count = 0;
                while (rs.next()) {
                    String schemaName = rs.getString("schemaname");
                    String tableName = rs.getString("tablename");
                    String tableOwner = rs.getString("tableowner");
                    boolean hasIndexes = rs.getBoolean("hasindexes");
                    boolean hasRules = rs.getBoolean("hasrules");
                    boolean hasTriggers = rs.getBoolean("hastriggers");
                    
                    System.out.printf("%-20s | %-20s | %-15s | %-10s | %-8s | %-11s%n",
                        schemaName, tableName, tableOwner, hasIndexes, hasRules, hasTriggers);
                    count++;
                }
                
                System.out.println("---------------------|----------------------|-----------------|------------|----------|------------");
                System.out.printf("Total: %d tables%n", count);
            }
        }
    }
    
    @Test
    public void showInformationSchemaTablesOutput() throws Exception {
        System.out.println("\n=== information_schema.tables Output (All 24 CIM Models) ===");
        
        // Connect without specifying cimModels to get all 24
        String splunkUrl = localProperties.getProperty("splunk.url", "https://kentest.xyz:8089");
        java.net.URI uri = new java.net.URI(splunkUrl);
        String jdbcUrl = String.format("jdbc:splunk://%s:%d/main", uri.getHost(), uri.getPort());
        
        Properties props = new Properties();
        props.setProperty("user", localProperties.getProperty("splunk.username", "admin"));
        props.setProperty("password", localProperties.getProperty("splunk.password", ""));
        props.setProperty("ssl", "true");
        props.setProperty("disableSslValidation", "true");
        props.setProperty("schema", "splunk");
        // NOTE: No cimModels specified - will get all 24
        
        try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
            
            // Query information_schema.tables
            String query = "SELECT table_catalog, table_schema, table_name, table_type " +
                          "FROM information_schema.tables " +
                          "WHERE table_schema = 'splunk' " +
                          "ORDER BY table_name";
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                
                System.out.println();
                System.out.printf("%-15s | %-12s | %-20s | %-12s%n", 
                    "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE");
                System.out.println("----------------|--------------|----------------------|-------------");
                
                int count = 0;
                while (rs.next()) {
                    String catalog = rs.getString("table_catalog");
                    String schema = rs.getString("table_schema");
                    String tableName = rs.getString("table_name");
                    String tableType = rs.getString("table_type");
                    
                    System.out.printf("%-15s | %-12s | %-20s | %-12s%n",
                        catalog == null ? "NULL" : catalog, schema, tableName, tableType);
                    count++;
                }
                
                System.out.println("----------------|--------------|----------------------|-------------");
                System.out.printf("Total: %d tables%n", count);
            }
        }
    }
}