package com.hasura.sharepoint;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.Driver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Logger;
import java.net.URI;
import java.net.URISyntaxException;
import java.io.IOException;

/**
 * JDBC driver for SharePoint Lists that provides comprehensive SQL access through Apache Calcite.
 * 
 * This driver provides a clean JDBC interface to SharePoint Lists with all adapter features:
 * - Full CRUD Support (SELECT, INSERT, DELETE operations)
 * - DDL Support (CREATE TABLE, DROP TABLE for managing lists)
 * - Multiple authentication methods (Client Credentials, Username/Password, Certificate, Device Code, Managed Identity)
 * - Environment variable support for production deployments
 * - PostgreSQL-compatible metadata schemas
 * - Automatic schema discovery with all accessible lists as tables
 * - Batch operations with Graph API optimization
 * 
 * Connection URL formats:
 * 1. Direct parameter format: jdbc:sharepoint:siteUrl='https://company.sharepoint.com/sites/site';authType='CLIENT_CREDENTIALS';...
 * 2. PostgreSQL-style format: jdbc:sharepoint://company.sharepoint.com/sites/site/schema?param=value
 * 3. Environment variable format: jdbc:sharepoint: (credentials from SHAREPOINT_* env vars)
 * 
 * Environment Variable Support:
 * - SHAREPOINT_SITE_URL: SharePoint site URL
 * - SHAREPOINT_CLIENT_ID: Azure App Client ID
 * - SHAREPOINT_CLIENT_SECRET: Azure App Client Secret
 * - SHAREPOINT_TENANT_ID: Azure Tenant ID
 * - SHAREPOINT_USERNAME: Username (for USERNAME_PASSWORD auth)
 * - SHAREPOINT_PASSWORD: Password (for USERNAME_PASSWORD auth)
 * - SHAREPOINT_AUTH_TYPE: Authentication type (CLIENT_CREDENTIALS, USERNAME_PASSWORD, etc.)
 */
public class SharePointListDriver extends Driver {
    private static final Logger LOGGER = Logger.getLogger(SharePointListDriver.class.getName());
    
    static {
        new SharePointListDriver().register();
    }

    @Override
    protected String getConnectStringPrefix() {
        return "jdbc:sharepoint:";
    }

    @Override
    protected DriverVersion createDriverVersion() {
        return new SharePointDriverVersion();
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        try {
            // Parse the URL and extract SharePoint-specific parameters
            SharePointConnectionParams params = parseConnectionUrl(url, info);
            
            // Create Calcite model configuration for SharePoint
            String model = createCalciteModel(params);
            
            // Set up Calcite connection properties
            Properties calciteProps = new Properties();
            calciteProps.setProperty("model", "inline:" + model);
            calciteProps.setProperty("caseSensitive", "false"); // PostgreSQL compatibility
            calciteProps.setProperty("unquotedCasing", "TO_LOWER"); // PostgreSQL compatibility
            calciteProps.setProperty("quotedCasing", "UNCHANGED");
            
            // Create the Calcite connection
            String calciteUrl = "jdbc:calcite:";
            Connection connection = super.connect(calciteUrl, calciteProps);
            CalciteConnection calciteConnection = (CalciteConnection) connection;
            
            LOGGER.info("Successfully created SharePoint JDBC connection to: " + params.getSiteUrl());
            return calciteConnection;
            
        } catch (Exception e) {
            LOGGER.severe("Failed to create SharePoint connection: " + e.getMessage());
            throw new SQLException("Failed to create SharePoint connection", e);
        }
    }

    /**
     * Parse SharePoint JDBC URL and extract connection parameters.
     * Supports environment variable fallback for production deployments.
     */
    private SharePointConnectionParams parseConnectionUrl(String url, Properties info) throws SQLException {
        try {
            SharePointConnectionParams params = new SharePointConnectionParams();
            
            // Parse URL: jdbc:sharepoint:siteUrl='https://site';param='value';...
            // or jdbc:sharepoint://host/sites/site/schema?param=value
            if (url.startsWith("jdbc:sharepoint://")) {
                // PostgreSQL-style URL format
                parsePostgreSQLStyleUrl(url, params, info);
            } else {
                // Direct parameter format: jdbc:sharepoint:siteUrl='...';param='value'
                parseDirectParameterUrl(url, params, info);
            }
            
            // Apply environment variable fallback
            applyEnvironmentVariables(params);
            
            // Apply properties fallback
            applyPropertiesFallback(params, info);
            
            // Set defaults and validate
            setDefaults(params);
            validateParams(params);
            
            return params;
            
        } catch (Exception e) {
            throw new SQLException("Invalid SharePoint JDBC URL format", e);
        }
    }
    
    private void parsePostgreSQLStyleUrl(String url, SharePointConnectionParams params, Properties info) throws URISyntaxException {
        // Remove jdbc:sharepoint:// prefix
        String cleanUrl = url.substring("jdbc:sharepoint://".length());
        URI uri = new URI("https://" + cleanUrl); // Add scheme for parsing
        
        if (uri.getHost() != null) {
            String siteUrl = "https://" + uri.getHost();
            if (uri.getPath() != null && !uri.getPath().isEmpty()) {
                siteUrl += uri.getPath();
                
                // Extract schema from last path component if it's not 'sites'
                String[] pathParts = uri.getPath().split("/");
                if (pathParts.length > 0) {
                    String lastPart = pathParts[pathParts.length - 1];
                    if (!lastPart.equals("sites") && !lastPart.isEmpty()) {
                        params.setDefaultSchema(lastPart);
                        // Remove schema from site URL
                        siteUrl = siteUrl.substring(0, siteUrl.lastIndexOf("/" + lastPart));
                    }
                }
            }
            params.setSiteUrl(siteUrl);
        }
        
        // Parse query parameters
        if (uri.getQuery() != null) {
            parseQueryParameters(uri.getQuery(), params);
        }
    }
    
    private void parseDirectParameterUrl(String url, SharePointConnectionParams params, Properties info) {
        // Remove jdbc:sharepoint: prefix
        String paramString = url.substring("jdbc:sharepoint:".length());
        
        if (paramString.isEmpty()) {
            // No parameters in URL, will use properties or environment variables
            return;
        }
        
        // Parse parameters: siteUrl='https://site';param='value';...
        String[] parts = paramString.split(";");
        for (String part : parts) {
            if (part.trim().isEmpty()) continue;
            
            String[] keyValue = part.split("=", 2);
            if (keyValue.length == 2) {
                String key = keyValue[0].trim();
                String value = keyValue[1].trim();
                
                // Remove quotes if present
                if (value.startsWith("'") && value.endsWith("'")) {
                    value = value.substring(1, value.length() - 1);
                }
                
                setParameterValue(params, key, value);
            }
        }
    }
    
    private void parseQueryParameters(String query, SharePointConnectionParams params) {
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            String[] keyValue = pair.split("=", 2);
            if (keyValue.length == 2) {
                String key = keyValue[0];
                String value = keyValue[1];
                setParameterValue(params, key, value);
            }
        }
    }
    
    private void setParameterValue(SharePointConnectionParams params, String key, String value) {
        switch (key.toLowerCase()) {
            case "siteurl":
                params.setSiteUrl(value);
                break;
            case "authtype":
                params.setAuthType(value);
                break;
            case "clientid":
                params.setClientId(value);
                break;
            case "clientsecret":
                params.setClientSecret(value);
                break;
            case "tenantid":
                params.setTenantId(value);
                break;
            case "username":
                params.setUsername(value);
                break;
            case "password":
                params.setPassword(value);
                break;
            case "certificatepath":
                params.setCertificatePath(value);
                break;
            case "certificatepassword":
                params.setCertificatePassword(value);
                break;
            case "thumbprint":
                params.setThumbprint(value);
                break;
            case "schema":
                params.setDefaultSchema(value);
                break;
            default:
                LOGGER.info("Unknown parameter: " + key + "=" + value);
        }
    }
    
    private void applyEnvironmentVariables(SharePointConnectionParams params) {
        if (params.getSiteUrl() == null) {
            params.setSiteUrl(System.getenv("SHAREPOINT_SITE_URL"));
        }
        if (params.getAuthType() == null) {
            params.setAuthType(System.getenv("SHAREPOINT_AUTH_TYPE"));
        }
        if (params.getClientId() == null) {
            params.setClientId(System.getenv("SHAREPOINT_CLIENT_ID"));
        }
        if (params.getClientSecret() == null) {
            params.setClientSecret(System.getenv("SHAREPOINT_CLIENT_SECRET"));
        }
        if (params.getTenantId() == null) {
            params.setTenantId(System.getenv("SHAREPOINT_TENANT_ID"));
        }
        if (params.getUsername() == null) {
            params.setUsername(System.getenv("SHAREPOINT_USERNAME"));
        }
        if (params.getPassword() == null) {
            params.setPassword(System.getenv("SHAREPOINT_PASSWORD"));
        }
    }
    
    private void applyPropertiesFallback(SharePointConnectionParams params, Properties info) {
        if (params.getSiteUrl() == null) {
            params.setSiteUrl(info.getProperty("siteUrl"));
        }
        if (params.getAuthType() == null) {
            params.setAuthType(info.getProperty("authType"));
        }
        if (params.getClientId() == null) {
            params.setClientId(info.getProperty("clientId"));
        }
        if (params.getClientSecret() == null) {
            params.setClientSecret(info.getProperty("clientSecret"));
        }
        if (params.getTenantId() == null) {
            params.setTenantId(info.getProperty("tenantId"));
        }
        if (params.getUsername() == null) {
            params.setUsername(info.getProperty("username"));
        }
        if (params.getPassword() == null) {
            params.setPassword(info.getProperty("password"));
        }
        if (params.getDefaultSchema() == null) {
            params.setDefaultSchema(info.getProperty("schema", "sharepoint"));
        }
    }
    
    private void setDefaults(SharePointConnectionParams params) {
        if (params.getDefaultSchema() == null) {
            params.setDefaultSchema("sharepoint");
        }
        if (params.getAuthType() == null) {
            params.setAuthType("CLIENT_CREDENTIALS");
        }
    }
    
    private void validateParams(SharePointConnectionParams params) throws SQLException {
        if (params.getSiteUrl() == null || params.getSiteUrl().trim().isEmpty()) {
            throw new SQLException("SharePoint site URL is required. Set via 'siteUrl' parameter or SHAREPOINT_SITE_URL environment variable.");
        }
        
        String authType = params.getAuthType().toUpperCase();
        
        switch (authType) {
            case "CLIENT_CREDENTIALS":
                if (params.getClientId() == null || params.getClientSecret() == null || params.getTenantId() == null) {
                    throw new SQLException("CLIENT_CREDENTIALS authentication requires clientId, clientSecret, and tenantId");
                }
                break;
            case "USERNAME_PASSWORD":
                if (params.getClientId() == null || params.getTenantId() == null || params.getUsername() == null || params.getPassword() == null) {
                    throw new SQLException("USERNAME_PASSWORD authentication requires clientId, tenantId, username, and password");
                }
                break;
            case "CERTIFICATE":
                if (params.getClientId() == null || params.getTenantId() == null || params.getCertificatePath() == null) {
                    throw new SQLException("CERTIFICATE authentication requires clientId, tenantId, and certificatePath");
                }
                break;
            case "DEVICE_CODE":
                if (params.getClientId() == null || params.getTenantId() == null) {
                    throw new SQLException("DEVICE_CODE authentication requires clientId and tenantId");
                }
                break;
            case "MANAGED_IDENTITY":
                // clientId is optional for managed identity
                break;
            default:
                throw new SQLException("Unsupported authentication type: " + authType + ". Supported types: CLIENT_CREDENTIALS, USERNAME_PASSWORD, CERTIFICATE, DEVICE_CODE, MANAGED_IDENTITY");
        }
    }
    
    /**
     * Create Calcite model configuration for SharePoint connection.
     */
    private String createCalciteModel(SharePointConnectionParams params) throws IOException {
        Map<String, Object> model = new HashMap<>();
        model.put("version", "1.0");
        model.put("defaultSchema", params.getDefaultSchema());
        
        List<Map<String, Object>> schemas = new ArrayList<>();
        
        // Create main SharePoint schema
        Map<String, Object> sharePointSchema = new HashMap<>();
        sharePointSchema.put("name", params.getDefaultSchema());
        sharePointSchema.put("type", "custom");
        sharePointSchema.put("factory", "org.apache.calcite.adapter.sharepoint.SharePointListSchemaFactory");
        
        Map<String, Object> operand = new HashMap<>();
        operand.put("siteUrl", params.getSiteUrl());
        operand.put("authType", params.getAuthType());
        
        if (params.getClientId() != null) {
            operand.put("clientId", params.getClientId());
        }
        if (params.getClientSecret() != null) {
            operand.put("clientSecret", params.getClientSecret());
        }
        if (params.getTenantId() != null) {
            operand.put("tenantId", params.getTenantId());
        }
        if (params.getUsername() != null) {
            operand.put("username", params.getUsername());
        }
        if (params.getPassword() != null) {
            operand.put("password", params.getPassword());
        }
        if (params.getCertificatePath() != null) {
            operand.put("certificatePath", params.getCertificatePath());
        }
        if (params.getCertificatePassword() != null) {
            operand.put("certificatePassword", params.getCertificatePassword());
        }
        if (params.getThumbprint() != null) {
            operand.put("thumbprint", params.getThumbprint());
        }
        
        sharePointSchema.put("operand", operand);
        schemas.add(sharePointSchema);
        
        model.put("schemas", schemas);
        
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(model);
    }

    /**
     * Internal class to hold parsed connection parameters.
     */
    private static class SharePointConnectionParams {
        private String siteUrl;
        private String authType;
        private String clientId;
        private String clientSecret;
        private String tenantId;
        private String username;
        private String password;
        private String certificatePath;
        private String certificatePassword;
        private String thumbprint;
        private String defaultSchema;

        // Getters and setters
        public String getSiteUrl() { return siteUrl; }
        public void setSiteUrl(String siteUrl) { this.siteUrl = siteUrl; }
        
        public String getAuthType() { return authType; }
        public void setAuthType(String authType) { this.authType = authType; }
        
        public String getClientId() { return clientId; }
        public void setClientId(String clientId) { this.clientId = clientId; }
        
        public String getClientSecret() { return clientSecret; }
        public void setClientSecret(String clientSecret) { this.clientSecret = clientSecret; }
        
        public String getTenantId() { return tenantId; }
        public void setTenantId(String tenantId) { this.tenantId = tenantId; }
        
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        
        public String getCertificatePath() { return certificatePath; }
        public void setCertificatePath(String certificatePath) { this.certificatePath = certificatePath; }
        
        public String getCertificatePassword() { return certificatePassword; }
        public void setCertificatePassword(String certificatePassword) { this.certificatePassword = certificatePassword; }
        
        public String getThumbprint() { return thumbprint; }
        public void setThumbprint(String thumbprint) { this.thumbprint = thumbprint; }
        
        public String getDefaultSchema() { return defaultSchema; }
        public void setDefaultSchema(String defaultSchema) { this.defaultSchema = defaultSchema; }
    }

    /**
     * SharePoint driver version.
     */
    static class SharePointDriverVersion extends DriverVersion {
        SharePointDriverVersion() {
            super(
                "SharePoint List JDBC Driver",
                "1.0",
                "SharePoint",
                "SharePoint List Adapter",
                true,
                1,
                0,
                1,
                0
            );
        }
    }
}