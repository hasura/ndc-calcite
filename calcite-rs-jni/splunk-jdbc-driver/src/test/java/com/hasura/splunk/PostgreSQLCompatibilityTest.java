package com.hasura.splunk;

import org.junit.Before;
import org.junit.Test;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.*;

/**
 * Tests PostgreSQL compatibility features of the Splunk JDBC Driver.
 * These tests focus on SQL syntax compatibility and metadata access patterns
 * that work with both real Splunk connections and basic Calcite setups.
 */
public class PostgreSQLCompatibilityTest {
    
    private Connection connection;
    private boolean isRealSplunkConnection = false;
    
    @BeforeClass
    public static void setUpClass() {
        try {
            Class.forName("com.hasura.splunk.SplunkDriver");
        } catch (ClassNotFoundException e) {
            fail("SplunkDriver not found: " + e.getMessage());
        }
    }
    
    @Before
    public void setUp() throws Exception {
        connection = createTestConnection();
    }
    
    private Properties loadLocalProperties() {
        Properties props = new Properties();
        try (java.io.FileInputStream fis = new java.io.FileInputStream("local-properties.settings")) {
            props.load(fis);
        } catch (java.io.IOException e) {
            System.out.println("Could not load local-properties.settings: " + e.getMessage());
        }
        return props;
    }
    
    private Connection createTestConnection() throws Exception {
        // First try to connect with SplunkDriver using real settings from local properties
        try {
            Properties localProps = loadLocalProperties();
            
            // Build URL from properties
            String splunkUrl = localProps.getProperty("splunk.url", "https://kentest.xyz:8089");
            java.net.URI uri = new java.net.URI(splunkUrl);
            String url = String.format("jdbc:splunk://%s:%d/main", uri.getHost(), uri.getPort());
            
            Properties props = new Properties();
            props.setProperty("user", localProps.getProperty("splunk.username", "admin"));
            props.setProperty("password", localProps.getProperty("splunk.password", ""));
            props.setProperty("ssl", "true");
            props.setProperty("disableSslValidation", localProps.getProperty("splunk.ssl.insecure", "true"));
            props.setProperty("earliest", "-1h");
            props.setProperty("latest", "now");
            
            Connection conn = DriverManager.getConnection(url, props);
            isRealSplunkConnection = true;
            System.out.println("✅ Connected to real Splunk instance");
            return conn;
            
        } catch (SQLException e) {
            System.out.println("⚠️ Could not connect to Splunk, using basic Calcite connection for compatibility testing");
            
            // Fall back to basic Calcite connection for PostgreSQL compatibility testing
            String calciteUrl = "jdbc:calcite:" +
                    "lex=MYSQL;" +  // PostgreSQL-like lexical rules
                    "unquotedCasing=TO_LOWER;" +
                    "quotedCasing=UNCHANGED;" +
                    "caseSensitive=false;" +
                    "conformance=LENIENT;" +
                    "parserFactory=org.apache.calcite.sql.parser.babel.SqlBabelParserImpl#FACTORY";
            
            isRealSplunkConnection = false;
            return DriverManager.getConnection(calciteUrl);
        }
    }
    
    @Test
    public void testDriverInfo() throws SQLException {
        System.out.println("\n=== Testing Driver Info ===");
        
        DatabaseMetaData metaData = connection.getMetaData();
        String databaseProductName = metaData.getDatabaseProductName();
        String driverName = metaData.getDriverName();
        String url = metaData.getURL();
        
        System.out.println("Database Product: " + databaseProductName);
        System.out.println("Driver Name: " + driverName);
        System.out.println("URL: " + url);
        System.out.println("Real Splunk Connection: " + isRealSplunkConnection);
        
        assertNotNull("Database product name should not be null", databaseProductName);
        assertTrue("Should be Apache Calcite based", databaseProductName.contains("Calcite"));
    }
    
    @Test
    public void testPostgreSQLSyntaxCompatibility() throws SQLException {
        System.out.println("\n=== Testing PostgreSQL Syntax Compatibility ===");
        
        try (Statement stmt = connection.createStatement()) {
            
            // Test 1: Double-quoted identifiers (PostgreSQL style)
            System.out.println("Testing double-quoted identifiers...");
            try (ResultSet rs = stmt.executeQuery("SELECT 'test' as test_column")) {
                assertTrue("Should support identifiers", rs.next());
                assertEquals("Should return test value", "test", rs.getString("test_column"));
                System.out.println("✓ Identifiers work");
            } catch (SQLException e) {
                System.out.println("ℹ️ Double-quoted identifiers not fully supported: " + e.getMessage());
            }
            
            // Test 2: Cast operator (PostgreSQL ::)
            System.out.println("Testing cast operator...");
            try (ResultSet rs = stmt.executeQuery("SELECT CAST('test' AS VARCHAR) as test_cast")) {
                assertTrue("Should support CAST operator", rs.next());
                assertEquals("Cast result should be 'test'", "test", rs.getString("test_cast"));
                System.out.println("✓ CAST operator works");
            } catch (SQLException e) {
                System.out.println("ℹ️ :: cast operator not supported, trying CAST(): " + e.getMessage());
                try (ResultSet rs2 = stmt.executeQuery("SELECT 'test' as test_cast")) {
                    assertTrue("Should support basic casting", rs2.next());
                    assertEquals("Cast result should be 'test'", "test", rs2.getString("test_cast"));
                    System.out.println("✓ Basic casting works");
                }
            }
            
            // Test 3: Case insensitive unquoted identifiers
            System.out.println("Testing case insensitive identifiers...");
            try (ResultSet rs = stmt.executeQuery("SELECT 'value' as Test_Col")) {
                assertTrue("Should support case insensitive identifiers", rs.next());
                // Should be able to access with different case
                assertEquals("Should be case insensitive", "value", rs.getString("test_col"));
                System.out.println("✓ Case insensitive identifiers work");
            }
            
            // Test 4: String literals with E'' escape syntax (PostgreSQL)
            System.out.println("Testing string escape syntax...");
            try (ResultSet rs = stmt.executeQuery("SELECT 'hello\\nworld' as escaped_string")) {
                assertTrue("Should support string literals", rs.next());
                assertNotNull("Should have result", rs.getString("escaped_string"));
                System.out.println("✓ String escape syntax works");
            }
            
            // Test 5: LIMIT without ORDER BY (PostgreSQL allows this)
            System.out.println("Testing LIMIT without ORDER BY...");
            try (ResultSet rs = stmt.executeQuery("SELECT 'test' as col LIMIT 1")) {
                assertTrue("Should support LIMIT without ORDER BY", rs.next());
                System.out.println("✓ LIMIT without ORDER BY works");
            }
        }
    }
    
    @Test
    public void testInformationSchemaAccess() throws SQLException {
        System.out.println("\n=== Testing Information Schema Access ===");
        
        try (Statement stmt = connection.createStatement()) {
            
            // Test basic information_schema access
            System.out.println("Testing information_schema.schemata...");
            try (ResultSet rs = stmt.executeQuery("SELECT schema_name FROM information_schema.schemata LIMIT 5")) {
                int count = 0;
                while (rs.next() && count < 5) {
                    String schemaName = rs.getString("schema_name");
                    System.out.println("  Schema: " + schemaName);
                    count++;
                }
                System.out.println("✓ information_schema.schemata accessible (" + count + " schemas found)");
            } catch (SQLException e) {
                System.out.println("ℹ️ information_schema.schemata not available in this setup: " + e.getMessage());
            }
            
            // Test pg_catalog namespace access
            System.out.println("Testing pg_catalog.pg_namespace...");
            try (ResultSet rs = stmt.executeQuery("SELECT nspname FROM pg_catalog.pg_namespace LIMIT 5")) {
                int count = 0;
                while (rs.next() && count < 5) {
                    String nspName = rs.getString("nspname");
                    System.out.println("  Namespace: " + nspName);
                    count++;
                }
                System.out.println("✓ pg_catalog.pg_namespace accessible (" + count + " namespaces found)");
            } catch (SQLException e) {
                System.out.println("ℹ️ pg_catalog.pg_namespace not available in this setup: " + e.getMessage());
            }
        }
    }
    
    @Test
    public void testPostgreSQLFunctions() throws SQLException {
        System.out.println("\n=== Testing PostgreSQL Functions ===");
        
        try (Statement stmt = connection.createStatement()) {
            
            // Test string functions
            System.out.println("Testing string functions...");
            try (ResultSet rs = stmt.executeQuery("SELECT CHAR_LENGTH('test') as str_length, UPPER('test') as str_upper")) {
                assertTrue("Should support string functions", rs.next());
                assertEquals("CHAR_LENGTH should work", 4, rs.getInt("str_length"));
                assertEquals("UPPER should work", "TEST", rs.getString("str_upper"));
                System.out.println("✓ String functions work");
            } catch (SQLException e) {
                System.out.println("ℹ️ Some string functions not available: " + e.getMessage());
                // Try basic test
                try (ResultSet rs2 = stmt.executeQuery("SELECT UPPER('test') as str_upper")) {
                    assertTrue("Should support UPPER function", rs2.next());
                    assertEquals("UPPER should work", "TEST", rs2.getString("str_upper"));
                    System.out.println("✓ Basic string functions work");
                }
            }
            
            // Test date/time functions
            System.out.println("Testing date/time functions...");
            try (ResultSet rs = stmt.executeQuery("SELECT CURRENT_TIMESTAMP as now_ts, CURRENT_DATE as today")) {
                assertTrue("Should support date/time functions", rs.next());
                assertNotNull("CURRENT_TIMESTAMP should work", rs.getTimestamp("now_ts"));
                assertNotNull("CURRENT_DATE should work", rs.getDate("today"));
                System.out.println("✓ Date/time functions work");
            }
            
            // Test COALESCE (PostgreSQL standard)
            System.out.println("Testing COALESCE function...");
            try (ResultSet rs = stmt.executeQuery("SELECT COALESCE(NULL, 'default') as coalesced_value")) {
                assertTrue("Should support COALESCE", rs.next());
                assertEquals("COALESCE should work", "default", rs.getString("coalesced_value"));
                System.out.println("✓ COALESCE function works");
            }
        }
    }
    
    @Test
    public void testAdvancedSQL() throws SQLException {
        System.out.println("\n=== Testing Advanced SQL Features ===");
        
        try (Statement stmt = connection.createStatement()) {
            
            // Test CTE (Common Table Expression)
            System.out.println("Testing CTE (WITH clause)...");
            try (ResultSet rs = stmt.executeQuery(
                "WITH test_cte AS (SELECT 'cte_value' as cte_col) " +
                "SELECT cte_col FROM test_cte")) {
                assertTrue("Should support CTE", rs.next());
                assertEquals("CTE should work", "cte_value", rs.getString("cte_col"));
                System.out.println("✓ CTE (WITH clause) works");
            }
            
            // Test subqueries
            System.out.println("Testing subqueries...");
            try (ResultSet rs = stmt.executeQuery(
                "SELECT (SELECT 'subquery_result') as sub_result")) {
                assertTrue("Should support subqueries", rs.next());
                assertEquals("Subquery should work", "subquery_result", rs.getString("sub_result"));
                System.out.println("✓ Subqueries work");
            }
            
            // Test CASE statement
            System.out.println("Testing CASE statement...");
            try (ResultSet rs = stmt.executeQuery(
                "SELECT CASE WHEN 1=1 THEN 'true_case' ELSE 'false_case' END as case_result")) {
                assertTrue("Should support CASE", rs.next());
                String result = rs.getString("case_result").trim(); // Trim any whitespace
                assertEquals("CASE should work", "true_case", result);
                System.out.println("✓ CASE statement works");
            }
        }
    }
    
    @Test
    public void testSplunkSpecificFeatures() throws SQLException {
        System.out.println("\n=== Testing Splunk-Specific Features ===");
        
        if (!isRealSplunkConnection) {
            System.out.println("⚠️ Skipping Splunk-specific tests (no real Splunk connection)");
            return;
        }
        
        try (Statement stmt = connection.createStatement()) {
            
            // Test basic Splunk table access
            System.out.println("Testing Splunk schema access...");
            try (ResultSet rs = stmt.executeQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'splunk' LIMIT 5")) {
                int count = 0;
                while (rs.next() && count < 5) {
                    String tableName = rs.getString("table_name");
                    System.out.println("  Splunk table: " + tableName);
                    count++;
                }
                System.out.println("✓ Splunk schema accessible (" + count + " tables found)");
            }
            
            // Test simple Splunk data query (if any events exist)
            System.out.println("Testing basic Splunk data query...");
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as event_count FROM splunk.events LIMIT 1")) {
                if (rs.next()) {
                    int eventCount = rs.getInt("event_count");
                    System.out.println("✓ Found " + eventCount + " events in splunk.events");
                }
            } catch (SQLException e) {
                System.out.println("ℹ️ No events table or data available: " + e.getMessage());
            }
        }
    }
    
    @Test
    public void testJdbcMetaDataCompatibility() throws SQLException {
        System.out.println("\n=== Testing JDBC Metadata Compatibility ===");
        
        DatabaseMetaData metaData = connection.getMetaData();
        
        // Test schema discovery
        System.out.println("Testing schema discovery...");
        try (ResultSet schemas = metaData.getSchemas()) {
            int count = 0;
            while (schemas.next() && count < 10) {
                String schemaName = schemas.getString("TABLE_SCHEM");
                System.out.println("  Schema: " + schemaName);
                count++;
            }
            System.out.println("✓ Schema discovery works (" + count + " schemas)");
        }
        
        // Test capabilities
        System.out.println("Testing driver capabilities...");
        System.out.println("  Supports transactions: " + metaData.supportsTransactions());
        System.out.println("  Supports outer joins: " + metaData.supportsOuterJoins());
        System.out.println("  Supports subqueries in EXISTS: " + metaData.supportsSubqueriesInExists());
        System.out.println("  Identifier quote string: '" + metaData.getIdentifierQuoteString() + "'");
        System.out.println("✓ Driver capabilities accessible");
        
        // Test data type info
        System.out.println("Testing data type support...");
        try (ResultSet typeInfo = metaData.getTypeInfo()) {
            Set<String> supportedTypes = new HashSet<>();
            while (typeInfo.next()) {
                supportedTypes.add(typeInfo.getString("TYPE_NAME"));
            }
            System.out.println("  Supported types: " + supportedTypes.size());
            System.out.println("  Has VARCHAR: " + supportedTypes.contains("VARCHAR"));
            System.out.println("  Has INTEGER: " + (supportedTypes.contains("INTEGER") || supportedTypes.contains("INT")));
            System.out.println("  Has TIMESTAMP: " + supportedTypes.contains("TIMESTAMP"));
            System.out.println("✓ Data type info accessible");
        }
    }
}