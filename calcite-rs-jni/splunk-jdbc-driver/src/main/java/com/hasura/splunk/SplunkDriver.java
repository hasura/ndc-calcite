package com.hasura.splunk;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Facade JDBC driver for Splunk that delegates to the Calcite Splunk adapter.
 * 
 * This driver provides a clean packaging of the Calcite Splunk adapter with all its features:
 * - Dynamic data model discovery with filtering and caching
 * - Multiple authentication methods (username/password, token)
 * - Environment variable support for production deployments
 * - App context configuration for different Splunk apps (CIM, vendor TAs, custom apps)
 * - Federation support for multi-vendor security analytics
 * - PostgreSQL-compatible metadata schemas
 * - Production-grade caching with configurable TTL
 */
public class SplunkDriver implements java.sql.Driver {
    private static final Logger LOGGER = Logger.getLogger(SplunkDriver.class.getName());
    private final org.apache.calcite.adapter.splunk.SplunkDriver delegate;
    
    static {
        try {
            java.sql.DriverManager.registerDriver(new SplunkDriver());
        } catch (SQLException e) {
            LOGGER.severe("Failed to register Splunk JDBC driver: " + e.getMessage());
        }
    }

    public SplunkDriver() {
        this.delegate = new org.apache.calcite.adapter.splunk.SplunkDriver();
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return delegate.connect(url, info);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return delegate.acceptsURL(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return delegate.getPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
        return delegate.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return delegate.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return delegate.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return delegate.getParentLogger();
    }

}