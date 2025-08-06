package com.hasura.file;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.sql.*;
import java.nio.file.Path;
import java.nio.file.Files;
import java.io.IOException;

public class DebugTest {
    
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    
    private Path tempDir;
    private Connection connection;
    
    @Before
    public void setUp() throws IOException, SQLException {
        tempDir = tempFolder.getRoot().toPath();
        
        // Create test CSV file
        Path csvFile = tempDir.resolve("employees.csv");
        Files.write(csvFile, 
            ("id,name,department,salary\n" +
             "1,Alice Johnson,Engineering,75000.00\n" +
             "2,Bob Smith,Marketing,60000.00\n").getBytes());
    }
    
    @After
    public void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
    
    @Test
    public void debugTableDiscovery() throws SQLException {
        String url = "jdbc:file://" + tempDir.toString();
        System.out.println("Connection URL: " + url);
        
        connection = DriverManager.getConnection(url);
        
        // List all available tables
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
            System.out.println("Available tables:");
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                String tableType = tables.getString("TABLE_TYPE");
                String schema = tables.getString("TABLE_SCHEM");
                System.out.println("  Schema: " + schema + ", Table: " + tableName + ", Type: " + tableType);
            }
        }
        
        // List all schemas
        try (ResultSet schemas = metaData.getSchemas()) {
            System.out.println("Available schemas:");
            while (schemas.next()) {
                String schemaName = schemas.getString("TABLE_SCHEM");
                System.out.println("  Schema: " + schemaName);
            }
        }
    }
}