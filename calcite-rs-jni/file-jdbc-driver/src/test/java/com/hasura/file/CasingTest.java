package com.hasura.file;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class CasingTest {
    private File testDir;
    private File testCsv;
    
    static {
        try {
            Class.forName("com.hasura.file.FileDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load FileDriver", e);
        }
    }
    
    @Before
    public void setUp() throws IOException {
        // Create test directory and CSV file
        testDir = new File("target/test-data");
        testDir.mkdirs();
        
        // Create a test CSV file with mixed case column names
        testCsv = new File(testDir, "MixedCase_Table.csv");
        try (FileWriter writer = new FileWriter(testCsv)) {
            writer.write("ProductID,Product_Name,unitPrice,InStock\n");
            writer.write("1,Widget,10.99,true\n");
            writer.write("2,Gadget,25.50,false\n");
            writer.write("3,Doohickey,15.00,true\n");
        }
    }
    
    @After
    public void tearDown() {
        if (testCsv != null && testCsv.exists()) {
            testCsv.delete();
        }
        if (testDir != null && testDir.exists()) {
            testDir.delete();
        }
    }
    
    @Test
    public void testDefaultCasing() throws SQLException {
        // Default: table_name_casing=UPPER, column_name_casing=UNCHANGED
        String url = "jdbc:file:" + testDir.getAbsolutePath() + "?recursive=false";
        
        try (Connection conn = DriverManager.getConnection(url)) {
            DatabaseMetaData meta = conn.getMetaData();
            
            // Check table name is uppercase
            try (ResultSet tables = meta.getTables(null, "files", "MIXEDCASE_TABLE", null)) {
                assertTrue("Table MIXEDCASE_TABLE should exist", tables.next());
            }
            
            // Check column names are unchanged
            try (ResultSet columns = meta.getColumns(null, "files", "MIXEDCASE_TABLE", null)) {
                assertTrue(columns.next());
                assertEquals("ProductID", columns.getString("COLUMN_NAME"));
                assertTrue(columns.next());
                assertEquals("Product_Name", columns.getString("COLUMN_NAME"));
                assertTrue(columns.next());
                assertEquals("unitPrice", columns.getString("COLUMN_NAME"));
                assertTrue(columns.next());
                assertEquals("InStock", columns.getString("COLUMN_NAME"));
            }
            
            // Query the table
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM files.MIXEDCASE_TABLE");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("ProductID"));
            assertEquals("Widget", rs.getString("Product_Name"));
        }
    }
    
    @Test
    public void testLowerCasing() throws SQLException {
        Properties props = new Properties();
        props.setProperty("table_name_casing", "LOWER");
        props.setProperty("column_name_casing", "LOWER");
        
        String url = "jdbc:file:" + testDir.getAbsolutePath() + "?recursive=false";
        
        try (Connection conn = DriverManager.getConnection(url, props)) {
            DatabaseMetaData meta = conn.getMetaData();
            
            // Check table name is lowercase
            try (ResultSet tables = meta.getTables(null, "files", "mixedcase_table", null)) {
                assertTrue("Table mixedcase_table should exist", tables.next());
            }
            
            // Check column names are lowercase
            try (ResultSet columns = meta.getColumns(null, "files", "mixedcase_table", null)) {
                assertTrue(columns.next());
                assertEquals("productid", columns.getString("COLUMN_NAME"));
                assertTrue(columns.next());
                assertEquals("product_name", columns.getString("COLUMN_NAME"));
                assertTrue(columns.next());
                assertEquals("unitprice", columns.getString("COLUMN_NAME"));
                assertTrue(columns.next());
                assertEquals("instock", columns.getString("COLUMN_NAME"));
            }
            
            // Query the table
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM files.mixedcase_table");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("productid"));
            assertEquals("Widget", rs.getString("product_name"));
        }
    }
    
    @Test
    public void testUpperCasing() throws SQLException {
        Properties props = new Properties();
        props.setProperty("table_name_casing", "UPPER");
        props.setProperty("column_name_casing", "UPPER");
        
        String url = "jdbc:file:" + testDir.getAbsolutePath() + "?recursive=false";
        
        try (Connection conn = DriverManager.getConnection(url, props)) {
            DatabaseMetaData meta = conn.getMetaData();
            
            // Check table name is uppercase
            try (ResultSet tables = meta.getTables(null, "files", "MIXEDCASE_TABLE", null)) {
                assertTrue("Table MIXEDCASE_TABLE should exist", tables.next());
            }
            
            // Check column names are uppercase
            try (ResultSet columns = meta.getColumns(null, "files", "MIXEDCASE_TABLE", null)) {
                assertTrue(columns.next());
                assertEquals("PRODUCTID", columns.getString("COLUMN_NAME"));
                assertTrue(columns.next());
                assertEquals("PRODUCT_NAME", columns.getString("COLUMN_NAME"));
                assertTrue(columns.next());
                assertEquals("UNITPRICE", columns.getString("COLUMN_NAME"));
                assertTrue(columns.next());
                assertEquals("INSTOCK", columns.getString("COLUMN_NAME"));
            }
            
            // Query the table
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM files.MIXEDCASE_TABLE");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("PRODUCTID"));
            assertEquals("Widget", rs.getString("PRODUCT_NAME"));
        }
    }
    
    @Test
    public void testUnchangedCasing() throws SQLException {
        Properties props = new Properties();
        props.setProperty("table_name_casing", "UNCHANGED");
        props.setProperty("column_name_casing", "UNCHANGED");
        
        String url = "jdbc:file:" + testDir.getAbsolutePath() + "?recursive=false";
        
        try (Connection conn = DriverManager.getConnection(url, props)) {
            DatabaseMetaData meta = conn.getMetaData();
            
            // Check table name is unchanged
            try (ResultSet tables = meta.getTables(null, "files", "MixedCase_Table", null)) {
                assertTrue("Table MixedCase_Table should exist", tables.next());
            }
            
            // Check column names are unchanged
            try (ResultSet columns = meta.getColumns(null, "files", "MixedCase_Table", null)) {
                assertTrue(columns.next());
                assertEquals("ProductID", columns.getString("COLUMN_NAME"));
                assertTrue(columns.next());
                assertEquals("Product_Name", columns.getString("COLUMN_NAME"));
                assertTrue(columns.next());
                assertEquals("unitPrice", columns.getString("COLUMN_NAME"));
                assertTrue(columns.next());
                assertEquals("InStock", columns.getString("COLUMN_NAME"));
            }
            
            // Query the table with exact casing
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM files.MixedCase_Table");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("ProductID"));
            assertEquals("Widget", rs.getString("Product_Name"));
        }
    }
    
    @Test
    public void testCaseSensitiveQueries() throws SQLException {
        Properties props = new Properties();
        props.setProperty("table_name_casing", "LOWER");
        props.setProperty("column_name_casing", "LOWER");
        props.setProperty("caseSensitive", "false");
        
        String url = "jdbc:file:" + testDir.getAbsolutePath() + "?recursive=false";
        
        try (Connection conn = DriverManager.getConnection(url, props)) {
            Statement stmt = conn.createStatement();
            
            // With caseSensitive=false, these should all work
            ResultSet rs1 = stmt.executeQuery("SELECT * FROM files.MIXEDCASE_TABLE");
            assertTrue(rs1.next());
            rs1.close();
            
            ResultSet rs2 = stmt.executeQuery("SELECT * FROM files.MixedCase_Table");
            assertTrue(rs2.next());
            rs2.close();
            
            ResultSet rs3 = stmt.executeQuery("SELECT * FROM files.mixedcase_table");
            assertTrue(rs3.next());
            rs3.close();
        }
    }
}
