package com.hasura.splunk;

import org.junit.Test;
import org.junit.Before;
import org.junit.BeforeClass;
import static org.junit.Assert.*;
import java.sql.*;
import java.util.Properties;

public class DirectBytesTest {
    private Connection connection;
    
    @BeforeClass
    public static void setUpClass() {
        System.setProperty("calcite.debug", "true");
    }
    
    @Before
    public void setUp() throws Exception {
        Properties props = new Properties();
        props.setProperty("model", "cim");
        String url = "jdbc:splunk://localhost:8089/test";
        connection = DriverManager.getConnection(url, props);
        System.out.println("Connected to Splunk");
    }
    
    @Test
    public void testDirectBytes() throws SQLException {
        System.out.println("\n🔍 Direct query for bytes field (no CAST):");
        
        String sql = "SELECT web.bytes FROM web LIMIT 5";
        System.out.println("SQL: " + sql);
        
        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            
            int rowCount = 0;
            while (resultSet.next()) {
                Object bytes = resultSet.getObject("bytes");
                System.out.println("Row " + (rowCount + 1) + ": bytes = " + bytes + 
                                 " (type: " + (bytes != null ? bytes.getClass().getName() : "null") + ")");
                rowCount++;
            }
            
            System.out.println("\nTotal rows: " + rowCount);
        }
    }
}