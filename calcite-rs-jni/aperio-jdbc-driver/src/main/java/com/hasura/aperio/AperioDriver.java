package com.hasura.aperio;

import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.adapter.file.FileSchemaFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

public class AperioDriver extends Driver {
    
    private static final Logger LOGGER = Logger.getLogger(AperioDriver.class.getName());
    
    static {
        try {
            java.sql.DriverManager.registerDriver(new AperioDriver());
        } catch (SQLException e) {
            LOGGER.severe("Failed to register Aperio JDBC driver: " + e.getMessage());
        }
    }
    
    @Override
    protected String getConnectStringPrefix() {
        return "jdbc:aperio:";
    }
    
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        
        LOGGER.info("Creating Aperio file connection: " + url);
        
        // Parse connection parameters
        FileConnectionParams params = parseConnectionParameters(url, info);
        
        // Create Calcite model
        String model = createCalciteModel(params);
        
        // Connect using Calcite with our model
        String calciteUrl = "jdbc:calcite:model=inline:" + model;
        return super.connect(calciteUrl, info);
    }
    
    private FileConnectionParams parseConnectionParameters(String url, Properties info) throws SQLException {
        FileConnectionParams params = new FileConnectionParams();
        
        // Remove prefix
        String remainder = url.substring(getConnectStringPrefix().length());
        
        // Handle different URL formats:
        // jdbc:aperio:path=/data/files
        // jdbc:aperio://localhost/data/files  
        // jdbc:aperio:dataPath='/data/files';engine='parquet'
        
        if (remainder.startsWith("//")) {
            // PostgreSQL-style: jdbc:aperio://localhost/data/files
            String[] parts = remainder.substring(2).split("/", 2);
            if (parts.length > 1) {
                params.setDataPath("/" + parts[1]);
            }
        } else if (remainder.contains("=")) {
            // Parameter style: jdbc:aperio:dataPath='/data/files';engine='parquet'
            String[] pairs = remainder.split(";");
            for (String pair : pairs) {
                String[] keyValue = pair.split("=", 2);
                if (keyValue.length == 2) {
                    String key = keyValue[0].trim();
                    String value = keyValue[1].trim().replaceAll("^'|'$", ""); // Remove quotes
                    
                    switch (key.toLowerCase()) {
                        case "datapath":
                        case "path":
                            params.setDataPath(value);
                            break;
                        case "engine":
                        case "executionengine":
                            params.setEngine(value);
                            break;
                        case "schema":
                        case "defaultschema":
                            params.setDefaultSchema(value);
                            break;
                        case "batchsize":
                            try {
                                params.setBatchSize(Integer.parseInt(value));
                            } catch (NumberFormatException e) {
                                LOGGER.warning("Invalid batch size: " + value);
                            }
                            break;
                        case "memorythreshold":
                            params.setMemoryThreshold(Long.parseLong(value));
                            break;
                        case "spilldirectory":
                            params.setSpillDirectory(value);
                            break;
                        case "refreshinterval":
                            params.setRefreshInterval(value);
                            break;
                        case "recursive":
                            params.setRecursive(Boolean.parseBoolean(value));
                            break;
                        case "tablenamecasing":
                            params.setTableNameCasing(value);
                            break;
                        case "columnnamecasing":
                            params.setColumnNameCasing(value);
                            break;
                        case "viewsfile":
                            params.setViewsFile(value);
                            break;
                        case "materializationsenabled":
                            params.setMaterializationsEnabled(Boolean.parseBoolean(value));
                            break;
                        case "storagetype":
                            params.setStorageType(value);
                            break;
                        case "awsregion":
                            params.setAwsRegion(value);
                            break;
                        case "tenantid":
                            params.setTenantId(value);
                            break;
                        case "clientid":
                            params.setClientId(value);
                            break;
                        case "clientsecret":
                            params.setClientSecret(value);
                            break;
                        case "username":
                            params.setUsername(value);
                            break;
                        case "password":
                            params.setPassword(value);
                            break;
                        case "privatekeypath":
                            params.setPrivateKeyPath(value);
                            break;
                    }
                }
            }
        } else {
            // Simple path: jdbc:aperio:/data/files
            params.setDataPath(remainder);
        }
        
        // Override with properties if provided
        if (info.getProperty("dataPath") != null) {
            params.setDataPath(info.getProperty("dataPath"));
        }
        if (info.getProperty("engine") != null) {
            params.setEngine(info.getProperty("engine"));
        }
        if (info.getProperty("defaultSchema") != null) {
            params.setDefaultSchema(info.getProperty("defaultSchema"));
        }
        
        // Handle pipe-delimited data paths with optional schema names
        // Format: dataPath="/path1|sales:/data/sales|s3://bucket/analytics|customers:https://api.com/customers.json"
        if (params.getDataPath() != null && params.getDataPath().contains("|")) {
            String[] paths = params.getDataPath().split("\\|");
            java.util.Map<String, java.util.List<TableConfig>> schemaMap = new java.util.HashMap<>();
            
            for (int i = 0; i < paths.length; i++) {
                String pathSpec = paths[i].trim();
                String schemaName;
                String path;
                
                if (pathSpec.contains(":") && !pathSpec.matches("^[a-zA-Z]+://.*")) {
                    // Has schema name (but not a URL protocol)
                    String[] parts = pathSpec.split(":", 2);
                    schemaName = parts[0].trim();
                    path = parts[1].trim();
                } else {
                    // No schema name, use default pattern
                    schemaName = "path" + (i + 1);
                    path = pathSpec;
                }
                
                // Store for multiple schema creation
                if (!schemaMap.containsKey(schemaName)) {
                    schemaMap.put(schemaName, new java.util.ArrayList<>());
                }
                
                // Handle multi-table files (xlsx, md, html) vs single-table files
                if (isMultiTableFile(path) || path.endsWith("/") || (!path.contains(".") && !path.matches("^[a-zA-Z]+://.*"))) {
                    // Multi-table files or directories: use auto-discovery
                    schemaMap.get(schemaName).add(new TableConfig("tables", path));
                } else {
                    // Single-table files: create explicit table definition
                    String tableName = extractTableName(path);
                    schemaMap.get(schemaName).add(new TableConfig(tableName, path));
                }
            }
            
            // Store schemas for model creation
            params.setSchemaMap(schemaMap);
            // Clear dataPath since we're using explicit schemas now
            params.setDataPath(null);
        }
        
        // Set defaults
        if (params.getDataPath() == null && params.getTables().isEmpty()) {
            params.setDataPath(System.getProperty("user.dir") + "/data");
        }
        if (params.getEngine() == null) {
            params.setEngine("parquet");  // Default to parquet as recommended
        }
        if (params.getDefaultSchema() == null) {
            params.setDefaultSchema("files");
        }
        
        return params;
    }
    
    private boolean isMultiTableFile(String path) {
        // Check if this file format creates multiple tables
        String extension = getFileExtension(path).toLowerCase();
        return extension.equals("xlsx") || extension.equals("xls") || 
               extension.equals("md") || extension.equals("html") || 
               extension.equals("docx") || extension.equals("gz");
    }
    
    private String getFileExtension(String path) {
        if (path.contains(".")) {
            return path.substring(path.lastIndexOf(".") + 1);
        }
        return "";
    }
    
    private String extractTableName(String path) {
        // If it's a specific file, extract the filename without extension
        if (path.contains("/") && !path.endsWith("/")) {
            String filename = path.substring(path.lastIndexOf("/") + 1);
            if (filename.contains(".")) {
                return filename.substring(0, filename.lastIndexOf(".")).toUpperCase();
            }
            return filename.toUpperCase();
        }
        // If it's a directory/bucket, use "tables" as default table name for discovery
        return "tables";
    }
    
    private String createCalciteModel(FileConnectionParams params) throws SQLException {
        try {
            Map<String, Object> model = new HashMap<>();
            model.put("version", "1.0");
            
            java.util.List<Map<String, Object>> schemas = new java.util.ArrayList<>();
            
            // Handle multiple schemas from pipe-delimited paths
            if (!params.getSchemaMap().isEmpty()) {
                // Use first schema as default
                String firstSchemaName = params.getSchemaMap().keySet().iterator().next();
                model.put("defaultSchema", firstSchemaName);
                
                // Create a schema for each named path
                for (Map.Entry<String, java.util.List<TableConfig>> entry : params.getSchemaMap().entrySet()) {
                    String schemaName = entry.getKey();
                    java.util.List<TableConfig> schemaTables = entry.getValue();
                    
                    Map<String, Object> schema = new HashMap<>();
                    schema.put("name", schemaName);
                    schema.put("type", "custom");
                    schema.put("factory", "org.apache.calcite.adapter.file.FileSchemaFactory");
                    
                    Map<String, Object> operand = new HashMap<>();
                    operand.put("executionEngine", params.getEngine());
                    operand.put("batchSize", params.getBatchSize());
                    
                    // Pass through advanced adapter features if specified
                    if (params.getMemoryThreshold() != null) {
                        operand.put("memoryThreshold", params.getMemoryThreshold());
                    }
                    if (params.getSpillDirectory() != null) {
                        operand.put("spillDirectory", params.getSpillDirectory());
                    }
                    if (params.getRefreshInterval() != null) {
                        operand.put("refreshInterval", params.getRefreshInterval());
                    }
                    if (params.getRecursive() != null) {
                        operand.put("recursive", params.getRecursive());
                    }
                    
                    // Storage provider configuration
                    if (params.getStorageType() != null) {
                        operand.put("storageType", params.getStorageType());
                    }
                    if (params.getAwsRegion() != null) {
                        // Set AWS region for S3 access
                        System.setProperty("aws.region", params.getAwsRegion());
                    }
                    
                    // Authentication configuration
                    Map<String, Object> authConfig = new HashMap<>();
                    if (params.getTenantId() != null) authConfig.put("tenantId", params.getTenantId());
                    if (params.getClientId() != null) authConfig.put("clientId", params.getClientId());
                    if (params.getClientSecret() != null) authConfig.put("clientSecret", params.getClientSecret());
                    if (params.getUsername() != null) authConfig.put("username", params.getUsername());
                    if (params.getPassword() != null) authConfig.put("password", params.getPassword());
                    if (params.getPrivateKeyPath() != null) authConfig.put("privateKeyPath", params.getPrivateKeyPath());
                    if (!authConfig.isEmpty()) {
                        operand.put("config", authConfig);
                    }
                    
                    // Check if all entries are for auto-discovery (table name = "tables")
                    boolean allAutoDiscovery = schemaTables.stream().allMatch(t -> t.getName().equals("tables"));
                    
                    if (allAutoDiscovery && schemaTables.size() == 1) {
                        // Single directory or multi-table file: use directory-based auto-discovery
                        operand.put("directory", schemaTables.get(0).getUrl());
                    } else if (allAutoDiscovery) {
                        // Multiple directories/multi-table files: create multiple directory entries
                        java.util.List<String> directories = new java.util.ArrayList<>();
                        for (TableConfig table : schemaTables) {
                            directories.add(table.getUrl());
                        }
                        operand.put("directories", directories);
                    } else {
                        // Mix of explicit tables and auto-discovery, or all explicit tables
                        java.util.List<Map<String, Object>> tables = new java.util.ArrayList<>();
                        String baseDirectory = null;
                        
                        for (TableConfig table : schemaTables) {
                            if (table.getName().equals("tables")) {
                                // This is a directory or multi-table file for auto-discovery
                                if (baseDirectory == null) {
                                    baseDirectory = table.getUrl();
                                }
                            } else {
                                // This is an explicit table definition
                                Map<String, Object> tableMap = new HashMap<>();
                                tableMap.put("name", table.getName());
                                tableMap.put("url", table.getUrl());
                                if (table.getFormat() != null) {
                                    tableMap.put("format", table.getFormat());
                                }
                                tables.add(tableMap);
                            }
                        }
                        
                        if (!tables.isEmpty()) {
                            operand.put("tables", tables);
                        }
                        if (baseDirectory != null) {
                            operand.put("directory", baseDirectory);
                        }
                    }
                    
                    schema.put("operand", operand);
                    schemas.add(schema);
                }
            } else {
                // Single schema mode (backwards compatibility)
                model.put("defaultSchema", params.getDefaultSchema());
                
                Map<String, Object> schema = new HashMap<>();
                schema.put("name", params.getDefaultSchema());
                schema.put("type", "custom");
                schema.put("factory", "org.apache.calcite.adapter.file.FileSchemaFactory");
                
                Map<String, Object> operand = new HashMap<>();
                operand.put("executionEngine", params.getEngine());
                operand.put("batchSize", params.getBatchSize());
                
                // Pass through advanced adapter features for single schema mode
                if (params.getMemoryThreshold() != null) {
                    operand.put("memoryThreshold", params.getMemoryThreshold());
                }
                if (params.getSpillDirectory() != null) {
                    operand.put("spillDirectory", params.getSpillDirectory());
                }
                if (params.getRefreshInterval() != null) {
                    operand.put("refreshInterval", params.getRefreshInterval());
                }
                if (params.getRecursive() != null) {
                    operand.put("recursive", params.getRecursive());
                }
                
                // Storage provider configuration
                if (params.getStorageType() != null) {
                    operand.put("storageType", params.getStorageType());
                }
                if (params.getAwsRegion() != null) {
                    System.setProperty("aws.region", params.getAwsRegion());
                }
                
                // Authentication configuration
                Map<String, Object> authConfig = new HashMap<>();
                if (params.getTenantId() != null) authConfig.put("tenantId", params.getTenantId());
                if (params.getClientId() != null) authConfig.put("clientId", params.getClientId());
                if (params.getClientSecret() != null) authConfig.put("clientSecret", params.getClientSecret());
                if (params.getUsername() != null) authConfig.put("username", params.getUsername());
                if (params.getPassword() != null) authConfig.put("password", params.getPassword());
                if (params.getPrivateKeyPath() != null) authConfig.put("privateKeyPath", params.getPrivateKeyPath());
                if (!authConfig.isEmpty()) {
                    operand.put("config", authConfig);
                }
                
                if (!params.getTables().isEmpty()) {
                    // Explicit table definitions
                    java.util.List<Map<String, Object>> tables = new java.util.ArrayList<>();
                    for (TableConfig table : params.getTables()) {
                        Map<String, Object> tableMap = new HashMap<>();
                        tableMap.put("name", table.getName());
                        tableMap.put("url", table.getUrl());
                        if (table.getFormat() != null) {
                            tableMap.put("format", table.getFormat());
                        }
                        tables.add(tableMap);
                    }
                    operand.put("tables", tables);
                    
                    if (params.getDataPath() != null) {
                        operand.put("directory", params.getDataPath());
                    }
                } else {
                    // Directory-based auto-discovery
                    operand.put("directory", params.getDataPath() != null ? 
                        params.getDataPath() : System.getProperty("user.dir") + "/data");
                }
                
                schema.put("operand", operand);
                schemas.add(schema);
            }
            
            model.put("schemas", schemas);
            
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(model);
            
        } catch (IOException e) {
            throw new SQLException("Failed to create Calcite model", e);
        }
    }
    
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith(getConnectStringPrefix());
    }
    
    private static class FileConnectionParams {
        private String dataPath;
        private String engine = "parquet"; // Default to parquet as recommended
        private String defaultSchema = "files";
        private int batchSize = 1000;
        private java.util.List<TableConfig> tables = new java.util.ArrayList<>();
        private java.util.Map<String, java.util.List<TableConfig>> schemaMap = new java.util.HashMap<>();
        
        // Advanced adapter features
        private Long memoryThreshold;
        private String spillDirectory;
        private String refreshInterval;
        private Boolean recursive;
        private String tableNameCasing;
        private String columnNameCasing;
        private String viewsFile;
        private Boolean materializationsEnabled;
        
        // Storage provider authentication
        private String storageType;
        private String awsRegion;
        private String tenantId;
        private String clientId;
        private String clientSecret;
        private String username;
        private String password;
        private String privateKeyPath;
        
        public String getDataPath() { return dataPath; }
        public void setDataPath(String dataPath) { this.dataPath = dataPath; }
        
        public String getEngine() { return engine; }
        public void setEngine(String engine) { this.engine = engine; }
        
        public String getDefaultSchema() { return defaultSchema; }
        public void setDefaultSchema(String defaultSchema) { this.defaultSchema = defaultSchema; }
        
        public int getBatchSize() { return batchSize; }
        public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
        
        public java.util.List<TableConfig> getTables() { return tables; }
        public void addTable(TableConfig table) { this.tables.add(table); }
        
        public java.util.Map<String, java.util.List<TableConfig>> getSchemaMap() { return schemaMap; }
        public void setSchemaMap(java.util.Map<String, java.util.List<TableConfig>> schemaMap) { this.schemaMap = schemaMap; }
        
        // Getters and setters for advanced features
        public Long getMemoryThreshold() { return memoryThreshold; }
        public void setMemoryThreshold(Long memoryThreshold) { this.memoryThreshold = memoryThreshold; }
        
        public String getSpillDirectory() { return spillDirectory; }
        public void setSpillDirectory(String spillDirectory) { this.spillDirectory = spillDirectory; }
        
        public String getRefreshInterval() { return refreshInterval; }
        public void setRefreshInterval(String refreshInterval) { this.refreshInterval = refreshInterval; }
        
        public Boolean getRecursive() { return recursive; }
        public void setRecursive(Boolean recursive) { this.recursive = recursive; }
        
        public String getTableNameCasing() { return tableNameCasing; }
        public void setTableNameCasing(String tableNameCasing) { this.tableNameCasing = tableNameCasing; }
        
        public String getColumnNameCasing() { return columnNameCasing; }
        public void setColumnNameCasing(String columnNameCasing) { this.columnNameCasing = columnNameCasing; }
        
        public String getViewsFile() { return viewsFile; }
        public void setViewsFile(String viewsFile) { this.viewsFile = viewsFile; }
        
        public Boolean getMaterializationsEnabled() { return materializationsEnabled; }
        public void setMaterializationsEnabled(Boolean materializationsEnabled) { this.materializationsEnabled = materializationsEnabled; }
        
        public String getStorageType() { return storageType; }
        public void setStorageType(String storageType) { this.storageType = storageType; }
        
        public String getAwsRegion() { return awsRegion; }
        public void setAwsRegion(String awsRegion) { this.awsRegion = awsRegion; }
        
        public String getTenantId() { return tenantId; }
        public void setTenantId(String tenantId) { this.tenantId = tenantId; }
        
        public String getClientId() { return clientId; }
        public void setClientId(String clientId) { this.clientId = clientId; }
        
        public String getClientSecret() { return clientSecret; }
        public void setClientSecret(String clientSecret) { this.clientSecret = clientSecret; }
        
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        
        public String getPrivateKeyPath() { return privateKeyPath; }
        public void setPrivateKeyPath(String privateKeyPath) { this.privateKeyPath = privateKeyPath; }
    }
    
    private static class TableConfig {
        private String name;
        private String url;
        private String format;
        
        public TableConfig(String name, String url) {
            this.name = name;
            this.url = url;
        }
        
        public String getName() { return name; }
        public String getUrl() { return url; }
        public String getFormat() { return format; }
        public void setFormat(String format) { this.format = format; }
    }
}