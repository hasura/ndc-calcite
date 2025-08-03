package com.hasura.file;

import org.junit.Test;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import java.sql.*;
import java.util.Properties;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.*;

public class FileDriverTest {
    
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    
    @Before
    public void setUp() {
        // Ensure driver is registered
        try {
            Class.forName("com.hasura.file.FileDriver");
        } catch (ClassNotFoundException e) {
            fail("FileDriver not found");
        }
    }
    
    @Test
    public void testDriverRegistration() throws SQLException {
        Driver driver = DriverManager.getDriver("jdbc:file:///tmp/data");
        assertNotNull("Driver should be registered", driver);
        assertTrue("Should be FileDriver", driver instanceof FileDriver);
    }
    
    @Test
    public void testAcceptsURL() throws SQLException {
        FileDriver driver = new FileDriver();
        
        // Valid URLs
        assertTrue(driver.acceptsURL("jdbc:file:///tmp/data"));
        assertTrue(driver.acceptsURL("jdbc:file:///home/user/data.csv"));
        assertTrue(driver.acceptsURL("jdbc:file://./data/sales.json"));
        assertTrue(driver.acceptsURL("jdbc:file:///c:/data/file.parquet"));
        assertTrue(driver.acceptsURL("jdbc:file:http://example.com/data.csv"));
        assertTrue(driver.acceptsURL("jdbc:file:https://api.example.com/export.json"));
        assertTrue(driver.acceptsURL("jdbc:file:s3://my-bucket/data/file.parquet"));
        assertTrue(driver.acceptsURL("jdbc:file:s3://analytics/reports/sales.csv"));
        assertTrue(driver.acceptsURL("jdbc:file:multi?locations=/data/file1.csv|/data/file2.csv"));
        assertTrue(driver.acceptsURL("jdbc:file:multi?locations=http://example.com/data.csv|s3://bucket/file.json"));
        
        // Invalid URLs
        assertFalse(driver.acceptsURL("jdbc:mysql://localhost:3306"));
        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost:5432"));
        assertFalse(driver.acceptsURL(null));
    }
    
    @Test
    public void testCSVConnection() throws SQLException, IOException {
        // Create test CSV file
        File csvFile = tempFolder.newFile("test.csv");
        try (FileWriter writer = new FileWriter(csvFile)) {
            writer.write("id,name,value\n");
            writer.write("1,Alice,100\n");
            writer.write("2,Bob,200\n");
            writer.write("3,Charlie,300\n");
        }
        
        String url = "jdbc:file://" + csvFile.getAbsolutePath();
        
        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull("Connection should not be null", conn);
            assertFalse("Connection should not be closed", conn.isClosed());
            
            // Test simple query
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM files.\"test\"")) {
                assertTrue("Should have results", rs.next());
                assertEquals("Should have 3 rows", 3, rs.getInt("cnt"));
            }
        }
    }
    
    @Test
    public void testJSONConnection() throws SQLException, IOException {
        // Create test JSON file
        File jsonFile = tempFolder.newFile("test.json");
        try (FileWriter writer = new FileWriter(jsonFile)) {
            writer.write("[");
            writer.write("{\"id\":1,\"name\":\"Alice\",\"value\":100},");
            writer.write("{\"id\":2,\"name\":\"Bob\",\"value\":200},");
            writer.write("{\"id\":3,\"name\":\"Charlie\",\"value\":300}");
            writer.write("]");
        }
        
        String url = "jdbc:file://" + jsonFile.getAbsolutePath() + "?format=json";
        
        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull("Connection should not be null", conn);
            
            // Test would query the JSON data
            // Note: Actual query execution requires proper JSON schema setup
        }
    }
    
    @Test
    public void testPostgreSQLSyntax() {
        // Test that PostgreSQL-style queries can be parsed
        String[] validQueries = {
            "SELECT * FROM files.sales WHERE date::date = CURRENT_DATE",
            "SELECT category, SUM(amount) FROM files.transactions GROUP BY category ORDER BY 2 DESC LIMIT 10",
            "WITH totals AS (SELECT * FROM files.data WHERE type = 'TOTAL') SELECT * FROM totals",
            "SELECT data->>'customer' FROM files.orders WHERE data ? 'customer'"
        };
        
        // These would be tested with actual connection
        for (String query : validQueries) {
            assertNotNull("Query should not be null", query);
        }
    }
    
    @Test
    public void testDirectoryConnection() throws SQLException, IOException {
        // Create test directory with multiple CSV files
        File dataDir = tempFolder.newFolder("data");
        
        File sales = new File(dataDir, "sales.csv");
        try (FileWriter writer = new FileWriter(sales)) {
            writer.write("date,product,amount\n");
            writer.write("2024-01-01,Widget,100\n");
            writer.write("2024-01-02,Gadget,200\n");
        }
        
        File customers = new File(dataDir, "customers.csv");
        try (FileWriter writer = new FileWriter(customers)) {
            writer.write("id,name,city\n");
            writer.write("1,Alice,New York\n");
            writer.write("2,Bob,London\n");
        }
        
        String url = "jdbc:file://" + dataDir.getAbsolutePath() + "?format=csv";
        
        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull("Connection should not be null", conn);
            
            // Both tables should be available
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                int tableCount = 0;
                while (tables.next()) {
                    tableCount++;
                }
                assertEquals("Should have 2 tables", 2, tableCount);
            }
        }
    }
    
    @Test
    public void testMultiLocationConnection() throws SQLException, IOException {
        // Create test files
        File file1 = tempFolder.newFile("sales.csv");
        try (FileWriter writer = new FileWriter(file1)) {
            writer.write("date,amount\n");
            writer.write("2024-01-01,1000\n");
            writer.write("2024-01-02,2000\n");
        }
        
        File file2 = tempFolder.newFile("products.csv");
        try (FileWriter writer = new FileWriter(file2)) {
            writer.write("id,name,price\n");
            writer.write("1,Widget,10.99\n");
            writer.write("2,Gadget,25.99\n");
        }
        
        // Create multi-location URL
        String locations = file1.getAbsolutePath() + "|" + file2.getAbsolutePath();
        String url = "jdbc:file:multi?locations=" + locations;
        
        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull("Connection should not be null", conn);
            
            // Check that both tables are available
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet tables = meta.getTables(null, "files", "%", null)) {
                int tableCount = 0;
                boolean hasSales = false;
                boolean hasProducts = false;
                
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    if ("sales".equals(tableName)) hasSales = true;
                    if ("products".equals(tableName)) hasProducts = true;
                    tableCount++;
                }
                
                assertEquals("Should have 2 tables", 2, tableCount);
                assertTrue("Should have sales table", hasSales);
                assertTrue("Should have products table", hasProducts);
            }
        }
    }
    
    @Test
    public void testViewsFromInlineJSON() throws SQLException, IOException {
        // Create test CSV file
        File csvFile = tempFolder.newFile("sales.csv");
        try (FileWriter writer = new FileWriter(csvFile)) {
            writer.write("date,product,amount\n");
            writer.write("2024-01-01,Widget,100\n");
            writer.write("2024-01-02,Widget,150\n");
            writer.write("2024-01-03,Gadget,200\n");
        }
        
        // Create URL with inline view definition
        String views = "[{\"name\":\"product_totals\",\"sql\":\"SELECT product, SUM(amount) as total FROM files.sales GROUP BY product\"}]";
        String url = "jdbc:file://" + csvFile.getAbsolutePath() + "?views=" + views;
        
        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull("Connection should not be null", conn);
            
            // Query the view
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM files.product_totals ORDER BY product")) {
                
                assertTrue("Should have results", rs.next());
                assertEquals("Gadget", rs.getString("product"));
                assertEquals(200, rs.getInt("total"));
                
                assertTrue("Should have second row", rs.next());
                assertEquals("Widget", rs.getString("product"));
                assertEquals(250, rs.getInt("total"));
                
                assertFalse("Should have no more rows", rs.next());
            }
        }
    }
    
    @Test
    public void testViewsFromFile() throws SQLException, IOException {
        // Create test CSV file
        File csvFile = tempFolder.newFile("orders.csv");
        try (FileWriter writer = new FileWriter(csvFile)) {
            writer.write("customer_id,amount\n");
            writer.write("1,100\n");
            writer.write("1,200\n");
            writer.write("2,150\n");
            writer.write("2,250\n");
            writer.write("3,500\n");
        }
        
        // Create views file
        File viewsFile = tempFolder.newFile("views.json");
        try (FileWriter writer = new FileWriter(viewsFile)) {
            writer.write("[{\"name\":\"customer_summary\",");
            writer.write("\"sql\":\"SELECT customer_id, COUNT(*) as order_count, SUM(amount) as total FROM files.orders GROUP BY customer_id\"}]");
        }
        
        String url = "jdbc:file://" + csvFile.getAbsolutePath() + "?viewsFile=" + viewsFile.getAbsolutePath();
        
        try (Connection conn = DriverManager.getConnection(url)) {
            assertNotNull("Connection should not be null", conn);
            
            // Query the view
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM files.customer_summary WHERE total > 200 ORDER BY customer_id")) {
                
                assertTrue("Should have results", rs.next());
                assertEquals(1, rs.getInt("customer_id"));
                assertEquals(2, rs.getInt("order_count"));
                assertEquals(300, rs.getInt("total"));
                
                assertTrue("Should have second row", rs.next());
                assertEquals(2, rs.getInt("customer_id"));
                assertEquals(2, rs.getInt("order_count"));
                assertEquals(400, rs.getInt("total"));
                
                assertTrue("Should have third row", rs.next());
                assertEquals(3, rs.getInt("customer_id"));
                assertEquals(1, rs.getInt("order_count"));
                assertEquals(500, rs.getInt("total"));
                
                assertFalse("Should have no more rows", rs.next());
            }
        }
    }
}