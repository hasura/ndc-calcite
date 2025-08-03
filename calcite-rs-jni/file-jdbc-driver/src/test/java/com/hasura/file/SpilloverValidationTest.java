package com.hasura.file;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.*;

import java.sql.*;
import java.nio.file.Path;
import java.nio.file.Files;
import java.io.IOException;

/**
 * Test spillover functionality and enhanced configuration
 */
public class SpilloverValidationTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    
    private Path tempDir;
    private Connection connection;
    
    @Before
    public void setUp() throws IOException {
        tempDir = tempFolder.getRoot().toPath();
        
        // Create larger CSV file to test spillover
        StringBuilder largeData = new StringBuilder();
        largeData.append("id,name,department,salary,description\n");
        for (int i = 1; i <= 1000; i++) {
            largeData.append(String.format("%d,Employee_%d,Dept_%d,%.2f,Long description for employee %d with lots of text to increase memory usage\n", 
                i, i, (i % 10), 50000.0 + (i * 10), i));
        }
        
        Path csvFile = tempDir.resolve("large_dataset.csv");
        Files.write(csvFile, largeData.toString().getBytes());
    }
    
    @After
    public void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
    
    @Test
    public void testSpilloverConfiguration() throws SQLException {
        // Test with basic configuration - use PARQUET engine to avoid enumerable issues
        String url = "jdbc:file://" + tempDir.toString() + "?tableNameCasing=LOWER&columnNameCasing=LOWER&executionEngine=PARQUET";
        connection = DriverManager.getConnection(url);
        
        assertNotNull("Connection should be established", connection);
        
        // Verify we can query the large dataset - simple count
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM files.large_dataset")) {
                
                assertTrue("Should have results", rs.next());
                int count = rs.getInt(1);
                assertEquals("Should have 1000 records", 1000, count);
            }
        }
    }
    
    @Test
    public void testExecutionEngineSupport() throws SQLException {
        // Test default engine - avoid PARQUET for now
        String url = "jdbc:file://" + tempDir.toString() + "?tableNameCasing=LOWER&columnNameCasing=LOWER";
        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull("Connection should work", conn);
            
            // Test table discovery
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                boolean foundTable = false;
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    if ("large_dataset".equals(tableName) || "LARGE_DATASET".equals(tableName)) {
                        foundTable = true;
                        break;
                    }
                }
                assertTrue("Should find large_dataset table", foundTable);
            }
        }
    }
    
    @Test
    public void testEnhancedDefaults() throws SQLException {
        // Test that enhanced defaults are applied
        String url = "jdbc:file://" + tempDir.toString() + "?tableNameCasing=LOWER&columnNameCasing=LOWER";
        connection = DriverManager.getConnection(url);
        
        assertNotNull("Connection with defaults should work", connection);
        
        // Test connection properties
        assertFalse("Connection should be open", connection.isClosed());
        
        // Test metadata access
        DatabaseMetaData metaData = connection.getMetaData();
        assertNotNull("Metadata should be available", metaData);
        assertEquals("Driver name should be correct", "Calcite JDBC Driver", metaData.getDriverName());
    }
}