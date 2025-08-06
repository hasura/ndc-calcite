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

public class CasingDebugTest {
    
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    
    private Path tempDir;
    private Connection connection;
    
    @Before
    public void setUp() throws IOException, SQLException {
        tempDir = tempFolder.getRoot().toPath();
        
        // Create larger CSV file to test spillover
        StringBuilder largeData = new StringBuilder();
        largeData.append("id,name,department,salary,description\n");
        for (int i = 1; i <= 100; i++) {
            largeData.append(String.format("%d,Employee_%d,Dept_%d,%.2f,Description for employee %d\n", 
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
    public void debugCasingWithParameters() throws SQLException {
        String url = "jdbc:file://" + tempDir.toString() + "?tableNameCasing=LOWER&columnNameCasing=LOWER";
        System.out.println("Testing URL: " + url);
        
        connection = DriverManager.getConnection(url);
        
        // List all available tables
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
            System.out.println("Available tables with casing config:");
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                String tableType = tables.getString("TABLE_TYPE");
                String schema = tables.getString("TABLE_SCHEM");
                System.out.println("  Schema: " + schema + ", Table: " + tableName + ", Type: " + tableType);
            }
        }
    }
}