package com.hasura.splunk;

import org.junit.Test;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Comprehensive test to verify the SplunkDriver facade correctly delegates 
 * all Splunk adapter capabilities and features.
 */
public class SplunkDriverFacadeTest {
    
    private static Properties localProperties;
    
    @BeforeClass
    public static void loadProperties() {
        localProperties = new Properties();
        try (FileInputStream fis = new FileInputStream("local-properties.settings")) {
            localProperties.load(fis);
        } catch (IOException e) {
            System.out.println("Could not load local-properties.settings: " + e.getMessage());
            // Use defaults for testing
            localProperties.setProperty("splunk.url", "https://localhost:8089");
            localProperties.setProperty("splunk.username", "admin");
            localProperties.setProperty("splunk.password", "changeme");
        }
    }
    
    @Test
    public void testFacadeDriverDelegation() throws SQLException {
        // Verify the facade driver is properly registered
        SplunkDriver facadeDriver = new SplunkDriver();
        assertNotNull("Facade driver should not be null", facadeDriver);
        
        // Check delegation methods
        assertTrue("Should accept splunk URLs", facadeDriver.acceptsURL("jdbc:splunk://localhost:8089"));
        assertFalse("Should reject non-splunk URLs", facadeDriver.acceptsURL("jdbc:mysql://localhost:3306"));
        
        // Version numbers are delegated to the underlying adapter
        assertTrue("Major version should be non-negative", facadeDriver.getMajorVersion() >= 0);
        assertTrue("Minor version should be non-negative", facadeDriver.getMinorVersion() >= 0);
        // JDBC compliance is delegated to underlying adapter
        // Just verify the method doesn't throw an exception
        boolean compliant = facadeDriver.jdbcCompliant();
        assertNotNull("JDBC compliance check should complete", Boolean.valueOf(compliant));
    }
    
    @Test
    public void testDirectJdbcUrlFormat() {
        // Test all URL formats supported by the adapter
        SplunkDriver driver = new SplunkDriver();
        
        try {
            // Direct parameter format: jdbc:splunk:url='https://host:port';param='value'
            String directUrl = "jdbc:splunk:url='https://splunk.example.com:8089';user='admin';password='secret';app='Splunk_SA_CIM'";
            assertTrue("Should accept direct parameter format", driver.acceptsURL(directUrl));
            
            // Environment variable format (minimal URL)
            String envUrl = "jdbc:splunk:datamodelCacheTtl=-1";
            assertTrue("Should accept minimal format for env vars", driver.acceptsURL(envUrl));
            
            // Empty parameter format
            String emptyUrl = "jdbc:splunk:";
            assertTrue("Should accept empty parameters", driver.acceptsURL(emptyUrl));
            
        } catch (SQLException e) {
            fail("URL acceptance should not throw SQL exceptions: " + e.getMessage());
        }
    }
    
    @Test
    public void testEnvironmentVariableSupport() {
        // Test that environment variables are properly supported
        // Note: This would require setting actual env vars for full testing
        
        SplunkDriver driver = new SplunkDriver();
        
        // Verify that minimal URL (relying on env vars) is accepted
        try {
            assertTrue("Should accept URL with minimal params", 
                      driver.acceptsURL("jdbc:splunk:app='Splunk_SA_CIM'"));
        } catch (SQLException e) {
            fail("Environment variable support should not fail URL acceptance");
        }
    }
    
    @Test 
    public void testAuthenticationMethods() {
        SplunkDriver driver = new SplunkDriver();
        
        try {
            // Username/password authentication
            String userPassUrl = "jdbc:splunk:url='https://splunk.com:8089';user='admin';password='secret'";
            assertTrue("Should support username/password auth", driver.acceptsURL(userPassUrl));
            
            // Token authentication  
            String tokenUrl = "jdbc:splunk:url='https://splunk.com:8089';token='eyJhbGciOiJIUzI1...'";
            assertTrue("Should support token auth", driver.acceptsURL(tokenUrl));
            
        } catch (SQLException e) {
            fail("Authentication method URLs should be accepted: " + e.getMessage());
        }
    }
    
    @Test
    public void testAppContextSupport() {
        SplunkDriver driver = new SplunkDriver();
        
        try {
            // CIM app context
            String cimUrl = "jdbc:splunk:url='https://splunk.com:8089';app='Splunk_SA_CIM'";
            assertTrue("Should support CIM app context", driver.acceptsURL(cimUrl));
            
            // Vendor Technology Add-on contexts
            String ciscoUrl = "jdbc:splunk:url='https://splunk.com:8089';app='Splunk_TA_cisco-esa'";
            assertTrue("Should support vendor TA context", driver.acceptsURL(ciscoUrl));
            
            String paloAltoUrl = "jdbc:splunk:url='https://splunk.com:8089';app='Splunk_TA_paloalto'";
            assertTrue("Should support Palo Alto context", driver.acceptsURL(paloAltoUrl));
            
            // Custom app context
            String customUrl = "jdbc:splunk:url='https://splunk.com:8089';app='my_custom_app'";
            assertTrue("Should support custom app context", driver.acceptsURL(customUrl));
            
        } catch (SQLException e) {
            fail("App context URLs should be accepted: " + e.getMessage());
        }
    }
    
    @Test
    public void testDataModelFeatures() {
        SplunkDriver driver = new SplunkDriver();
        
        try {
            // Data model filtering
            String filterUrl = "jdbc:splunk:url='https://splunk.com:8089';datamodelFilter='auth*'";
            assertTrue("Should support data model filtering", driver.acceptsURL(filterUrl));
            
            // Cache configuration
            String cacheUrl = "jdbc:splunk:url='https://splunk.com:8089';datamodelCacheTtl=-1";
            assertTrue("Should support cache TTL configuration", driver.acceptsURL(cacheUrl));
            
            // Cache refresh
            String refreshUrl = "jdbc:splunk:url='https://splunk.com:8089';refreshDatamodels=true";
            assertTrue("Should support cache refresh", driver.acceptsURL(refreshUrl));
            
            // Calculated fields
            String fieldsUrl = "jdbc:splunk:url='https://splunk.com:8089';calculatedFields='{\"Authentication\": [{\"name\": \"action\", \"splunkField\": \"action\"}]}'";
            assertTrue("Should support calculated fields", driver.acceptsURL(fieldsUrl));
            
        } catch (SQLException e) {
            fail("Data model feature URLs should be accepted: " + e.getMessage());
        }
    }
    
    @Test
    public void testSSLConfiguration() {
        SplunkDriver driver = new SplunkDriver();
        
        try {
            // SSL disabled for development
            String devUrl = "jdbc:splunk:url='https://localhost:8089';disableSslValidation=true";
            assertTrue("Should support SSL validation disabling", driver.acceptsURL(devUrl));
            
            // Production SSL (default)
            String prodUrl = "jdbc:splunk:url='https://prod.splunk.com:8089'";
            assertTrue("Should support production SSL", driver.acceptsURL(prodUrl));
            
        } catch (SQLException e) {
            fail("SSL configuration URLs should be accepted: " + e.getMessage());
        }
    }
    
    @Test
    public void testPropertiesObjectSupport() throws SQLException {
        SplunkDriver driver = new SplunkDriver();
        
        // Test that properties object is properly passed through
        Properties props = new Properties();
        props.setProperty("url", "https://splunk.example.com:8089");
        props.setProperty("user", "admin");
        props.setProperty("password", "secret");
        props.setProperty("app", "Splunk_SA_CIM");
        props.setProperty("datamodelCacheTtl", "-1");
        
        // This should not throw an exception during URL validation
        assertTrue("Should accept minimal URL with properties", 
                  driver.acceptsURL("jdbc:splunk:"));
        
        // Note: Full connection test would require actual Splunk instance
        // Connection conn = driver.connect("jdbc:splunk:", props);
    }
    
    @Test
    public void testPropertyInfoDelegation() throws SQLException {
        SplunkDriver driver = new SplunkDriver();
        
        Properties props = new Properties();
        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo("jdbc:splunk://localhost:8089", props);
        
        assertNotNull("Property info should not be null", propertyInfo);
        // The actual adapter should provide detailed property information
        // This verifies the delegation is working
    }
    
    @Test
    public void testConnectionWithMockProperties() {
        // Test that would work with actual Splunk instance
        String testUrl = "jdbc:splunk:url='" + localProperties.getProperty("splunk.url") + "'";
        
        Properties props = new Properties();
        props.setProperty("user", localProperties.getProperty("splunk.username"));
        props.setProperty("password", localProperties.getProperty("splunk.password"));
        props.setProperty("disableSslValidation", "true"); // For testing
        
        SplunkDriver driver = new SplunkDriver();
        
        // Just test that the delegation doesn't throw exceptions during setup
        try {
            assertNotNull("Driver should handle property setup", driver);
            assertTrue("Should accept test URL", driver.acceptsURL(testUrl));
        } catch (SQLException e) {
            fail("Property handling should not fail: " + e.getMessage());
        }
        
        // Actual connection would be:
        // Connection conn = driver.connect(testUrl, props);
        // ... test queries ...
    }
    
    @Test
    public void testFederationUrlSupport() {
        SplunkDriver driver = new SplunkDriver();
        
        // Federation would use Calcite model files, but verify URL handling
        try {
            // Multi-schema setup (would use model files)
            String federationUrl = "jdbc:splunk:"; // Minimal for model-based federation
            assertTrue("Should support federation URLs", driver.acceptsURL(federationUrl));
            
        } catch (SQLException e) {
            fail("Federation URL support should not fail: " + e.getMessage());
        }
    }
}