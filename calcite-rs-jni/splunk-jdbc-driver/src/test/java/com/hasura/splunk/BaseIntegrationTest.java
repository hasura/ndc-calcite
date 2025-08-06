package com.hasura.splunk;

import org.junit.Assume;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Base class for integration tests that require a live Splunk connection.
 * Automatically skips tests if proper credentials are not available.
 */
@Category(IntegrationTest.class)
public abstract class BaseIntegrationTest {
    
    protected Properties splunkProperties;
    protected String jdbcUrl;
    protected Properties connectionProps;
    
    @Before
    public void setUp() throws Exception {
        // Load properties and validate configuration
        loadAndValidateConfiguration();
    }
    
    private void loadAndValidateConfiguration() {
        splunkProperties = loadLocalProperties();
        
        // Check if we have the required properties for integration testing
        String splunkUrl = splunkProperties.getProperty("splunk.url");
        String username = splunkProperties.getProperty("splunk.username");
        String password = splunkProperties.getProperty("splunk.password");
        String token = splunkProperties.getProperty("splunk.token");
        
        // Skip test if we don't have basic connection info
        Assume.assumeTrue("Splunk URL not configured - skipping integration test", 
                         splunkUrl != null && !splunkUrl.trim().isEmpty());
        
        // Must have either token or username/password
        boolean hasToken = token != null && !token.trim().isEmpty();
        boolean hasCredentials = username != null && !username.trim().isEmpty() 
                                && password != null && !password.trim().isEmpty();
        
        Assume.assumeTrue("Splunk credentials not configured - skipping integration test", 
                         hasToken || hasCredentials);
        
        // Build JDBC URL in the format expected by the SplunkDriver
        StringBuilder urlBuilder = new StringBuilder("jdbc:splunk:");
        urlBuilder.append("url=").append(splunkUrl);
        
        connectionProps = new Properties();
        
        if (hasToken) {
            urlBuilder.append(";token=").append(token);
        } else {
            urlBuilder.append(";user=").append(username);
            urlBuilder.append(";password=").append(password);
        }
        
        // Add SSL configuration
        String sslInsecure = splunkProperties.getProperty("splunk.ssl.insecure", "false");
        if ("true".equals(sslInsecure)) {
            urlBuilder.append(";disableSslValidation=true");
        }
        
        jdbcUrl = urlBuilder.toString();
        
        // Add additional connection properties
        connectionProps.setProperty("ssl", "true");
        String timeout = splunkProperties.getProperty("splunk.connection.timeout", "30000");
        connectionProps.setProperty("connectTimeout", timeout);
    }
    
    protected Properties loadLocalProperties() {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream("local-properties.settings")) {
            props.load(fis);
        } catch (IOException e) {
            System.out.println("Could not load local-properties.settings: " + e.getMessage());
            // Return empty properties - tests will be skipped
        }
        return props;
    }
    
    /**
     * Creates a test connection using the configured properties.
     * @return Connection to Splunk
     * @throws SQLException if connection fails
     */
    protected Connection createTestConnection() throws SQLException {
        try {
            Class.forName("com.hasura.splunk.SplunkDriver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("SplunkDriver not found", e);
        }
        return DriverManager.getConnection(jdbcUrl, connectionProps);
    }
    
    /**
     * Performs basic connectivity test and assumes the connection works.
     * Call this in test methods that require actual network connectivity.
     */
    protected void assumeConnectionWorks() {
        try (Connection conn = createTestConnection()) {
            // Basic connectivity test
            conn.getMetaData().getDatabaseProductName();
        } catch (Exception e) {
            Assume.assumeNoException("Splunk server not reachable - skipping test: " + e.getMessage(), e);
        }
    }
}