package com.hasura.apericio;

import org.apache.calcite.jdbc.Driver;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.logging.Logger;
import java.net.URLDecoder;
import java.io.UnsupportedEncodingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Apericio JDBC Driver - A thin wrapper around Apache Calcite's File Adapter
 * that provides simplified connection and always uses DuckDB as the execution engine.
 */
public class ApericioDriver extends Driver {
    
    private static final Logger LOGGER = Logger.getLogger(ApericioDriver.class.getName());
    
    static {
        try {
            java.sql.DriverManager.registerDriver(new ApericioDriver());
        } catch (SQLException e) {
            LOGGER.severe("Failed to register Apericio JDBC driver: " + e.getMessage());
        }
    }
    
    @Override
    protected String getConnectStringPrefix() {
        return "jdbc:apericio:";
    }
    
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        
        LOGGER.info("Creating Apericio connection: " + url);
        
        // Parse the URL to extract path and query parameters
        String remainder = url.substring(getConnectStringPrefix().length());
        
        // Check for model file specification
        if (remainder.startsWith("model=")) {
            String modelPath = remainder.substring(6);
            String calciteUrl = "jdbc:calcite:model=" + modelPath;
            return super.connect(calciteUrl, info);
        }
        
        // Parse path and query parameters
        String path;
        Properties combinedProps = new Properties(info);
        
        int queryIndex = remainder.indexOf('?');
        if (queryIndex != -1) {
            path = remainder.substring(0, queryIndex);
            String queryString = remainder.substring(queryIndex + 1);
            parseQueryParameters(queryString, combinedProps);
        } else {
            path = remainder;
        }
        
        // Create model with combined properties
        String model = createSimpleModel(path, combinedProps);
        
        // Connect using Calcite with our model
        String calciteUrl = "jdbc:calcite:model=inline:" + model;
        return super.connect(calciteUrl, combinedProps);
    }
    
    private void parseQueryParameters(String queryString, Properties props) {
        if (queryString == null || queryString.isEmpty()) {
            return;
        }
        
        String[] pairs = queryString.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf('=');
            if (idx > 0) {
                try {
                    String key = URLDecoder.decode(pair.substring(0, idx), "UTF-8");
                    String value = idx < pair.length() - 1 
                        ? URLDecoder.decode(pair.substring(idx + 1), "UTF-8") 
                        : "";
                    props.setProperty(key, value);
                } catch (UnsupportedEncodingException e) {
                    LOGGER.warning("Failed to decode query parameter: " + pair);
                }
            }
        }
    }
    
    private String createSimpleModel(String path, Properties info) throws SQLException {
        try {
            Map<String, Object> model = new HashMap<>();
            model.put("version", "1.0");
            
            // Default schema name (lowercase by convention)
            String schemaName = info.getProperty("schema", "files");
            model.put("defaultSchema", schemaName);
            
            // Create schema - automatically set factory
            List<Map<String, Object>> schemas = new ArrayList<>();
            Map<String, Object> schema = new HashMap<>();
            schema.put("name", schemaName);
            schema.put("type", "custom");
            schema.put("factory", "org.apache.calcite.adapter.file.FileSchemaFactory");
            
            // Build operand
            Map<String, Object> operand = new HashMap<>();
            
            // Set directory path
            if (path.startsWith("//")) {
                // URL-style: jdbc:apericio://localhost/data/files
                String[] parts = path.substring(2).split("/", 2);
                if (parts.length > 1) {
                    operand.put("directory", "/" + parts[1]);
                }
            } else if (path.startsWith("/") || path.matches("^[A-Za-z]:.*")) {
                // Absolute path
                operand.put("directory", path);
            } else if (!path.isEmpty()) {
                // Relative path
                operand.put("directory", path);
            } else {
                // Default to current directory
                operand.put("directory", System.getProperty("user.dir"));
            }
            
            // Execution engine configuration (default to DuckDB if available, else Parquet)
            // Note: DuckDB provides best performance but requires duckdb-jdbc in classpath
            String executionEngine = info.getProperty("executionEngine", 
                System.getenv("APERICIO_EXECUTION_ENGINE") != null 
                    ? System.getenv("APERICIO_EXECUTION_ENGINE") 
                    : "duckdb");  // Falls back to parquet if DuckDB driver not available
            operand.put("executionEngine", executionEngine);
            
            // DuckDB configuration (if using DuckDB engine)
            if ("duckdb".equalsIgnoreCase(executionEngine)) {
                Map<String, Object> duckdbConfig = new HashMap<>();
                
                // Memory limit (from property or env var)
                String memoryLimit = info.getProperty("duckdbMemoryLimit", 
                    System.getenv("DUCKDB_MEMORY_LIMIT"));
                if (memoryLimit != null) {
                    duckdbConfig.put("memory_limit", memoryLimit);
                }
                
                // Thread count (from property or env var)
                String threads = info.getProperty("duckdbThreads", 
                    System.getenv("DUCKDB_THREADS"));
                if (threads != null) {
                    try {
                        duckdbConfig.put("threads", Integer.parseInt(threads));
                    } catch (NumberFormatException e) {
                        LOGGER.warning("Invalid duckdbThreads value: " + threads);
                    }
                }
                
                // Temp directory (from property or env var)
                String tempDir = info.getProperty("duckdbTempDirectory",
                    System.getenv("DUCKDB_TEMP_DIRECTORY"));
                if (tempDir != null) {
                    duckdbConfig.put("temp_directory", tempDir);
                }
                
                if (!duckdbConfig.isEmpty()) {
                    operand.put("duckdbConfig", duckdbConfig);
                }
            }
            
            // Core configuration properties with environment variable support
            addPropertyWithEnvFallback(info, operand, "batchSize", "APERICIO_BATCH_SIZE", Integer.class);
            addPropertyWithEnvFallback(info, operand, "memoryThreshold", "APERICIO_MEMORY_THRESHOLD", Long.class);
            addPropertyWithEnvFallback(info, operand, "spillDirectory", "APERICIO_SPILL_DIRECTORY", String.class);
            addPropertyIfPresent(info, operand, "recursive", Boolean.class);
            addPropertyIfPresent(info, operand, "multiTable", Boolean.class);
            addPropertyIfPresent(info, operand, "directoryPattern", String.class);
            
            // SQL configuration with proper defaults
            String lex = info.getProperty("lex", "ORACLE");
            operand.put("lex", lex);
            
            String unquotedCasing = info.getProperty("unquotedCasing", "TO_LOWER");
            operand.put("unquotedCasing", unquotedCasing);
            
            String quotedCasing = info.getProperty("quotedCasing", "UNCHANGED");
            operand.put("quotedCasing", quotedCasing);
            
            addPropertyIfPresent(info, operand, "caseSensitive", Boolean.class);
            addPropertyIfPresent(info, operand, "conformance", String.class);
            
            // Table and column naming configuration
            String tableNameCasing = info.getProperty("tableNameCasing", "SMART_CASING");
            operand.put("tableNameCasing", tableNameCasing);
            
            String columnNameCasing = info.getProperty("columnNameCasing", "SMART_CASING");
            operand.put("columnNameCasing", columnNameCasing);
            addPropertyIfPresent(info, operand, "refreshInterval", String.class);
            addPropertyIfPresent(info, operand, "encoding", String.class);  // Changed from charset to encoding
            
            // CSV Type Inference configuration
            String csvInferTypes = info.getProperty("csvInferTypes",
                System.getenv("APERICIO_CSV_INFER_TYPES"));
            if ("true".equalsIgnoreCase(csvInferTypes)) {
                Map<String, Object> csvTypeInference = new HashMap<>();
                csvTypeInference.put("enabled", true);
                
                // Sampling rate (optional)
                String samplingRate = info.getProperty("csvSamplingRate",
                    System.getenv("CSV_SAMPLING_RATE"));
                if (samplingRate != null) {
                    try {
                        csvTypeInference.put("samplingRate", Double.parseDouble(samplingRate));
                    } catch (NumberFormatException e) {
                        // Use default
                    }
                }
                
                // Max sample rows (optional)
                String maxSampleRows = info.getProperty("csvMaxSampleRows",
                    System.getenv("CSV_MAX_SAMPLE_ROWS"));
                if (maxSampleRows != null) {
                    try {
                        csvTypeInference.put("maxSampleRows", Integer.parseInt(maxSampleRows));
                    } catch (NumberFormatException e) {
                        // Use default
                    }
                }
                
                // Infer dates/times (optional)
                String inferDates = info.getProperty("csvInferDates",
                    System.getenv("CSV_INFER_DATES"));
                if (inferDates != null) {
                    csvTypeInference.put("inferDates", "true".equalsIgnoreCase(inferDates));
                }
                
                operand.put("csvTypeInference", csvTypeInference);
            }
            
            // Statistics and caching
            addPropertyIfPresent(info, operand, "enableStatistics", Boolean.class);
            addPropertyIfPresent(info, operand, "statisticsCache", String.class);
            addPropertyIfPresent(info, operand, "cacheEnabled", Boolean.class);
            addPropertyIfPresent(info, operand, "maxConcurrency", Integer.class);
            
            // Storage provider configuration with environment variable support
            String storageType = info.getProperty("storageType", 
                System.getenv("APERICIO_STORAGE_TYPE"));
            if (storageType != null) {
                operand.put("storageType", storageType);
                
                // Storage-specific configuration based on type
                if ("s3".equalsIgnoreCase(storageType)) {
                    Map<String, Object> s3Config = new HashMap<>();
                    
                    // S3 bucket and region
                    String bucket = info.getProperty("s3Bucket", System.getenv("S3_BUCKET"));
                    if (bucket != null) s3Config.put("bucket", bucket);
                    
                    String region = info.getProperty("s3Region", 
                        System.getenv("AWS_REGION") != null ? System.getenv("AWS_REGION") : 
                        System.getenv("AWS_DEFAULT_REGION"));
                    if (region != null) s3Config.put("region", region);
                    
                    // S3 credentials (optional - can use AWS SDK default chain)
                    String accessKey = info.getProperty("s3AccessKey", System.getenv("AWS_ACCESS_KEY_ID"));
                    if (accessKey != null) s3Config.put("accessKey", accessKey);
                    
                    String secretKey = info.getProperty("s3SecretKey", System.getenv("AWS_SECRET_ACCESS_KEY"));
                    if (secretKey != null) s3Config.put("secretKey", secretKey);
                    
                    if (!s3Config.isEmpty()) {
                        operand.put("storageConfig", s3Config);
                    }
                    
                } else if ("http".equalsIgnoreCase(storageType) || "https".equalsIgnoreCase(storageType)) {
                    Map<String, Object> httpConfig = new HashMap<>();
                    
                    // HTTP base URL and authentication
                    String baseUrl = info.getProperty("httpBaseUrl", System.getenv("APERICIO_HTTP_BASE_URL"));
                    if (baseUrl != null) httpConfig.put("baseUrl", baseUrl);
                    
                    String authType = info.getProperty("httpAuthType", System.getenv("APERICIO_HTTP_AUTH_TYPE"));
                    if (authType != null) httpConfig.put("authType", authType);
                    
                    String authToken = info.getProperty("httpAuthToken", System.getenv("APERICIO_HTTP_AUTH_TOKEN"));
                    if (authToken != null) httpConfig.put("authToken", authToken);
                    
                    if (!httpConfig.isEmpty()) {
                        operand.put("storageConfig", httpConfig);
                    }
                    
                } else if (info.containsKey("storageConfig")) {
                    // Generic storage config parsing for other types
                    String configStr = info.getProperty("storageConfig");
                    if (configStr != null && configStr.startsWith("{")) {
                        ObjectMapper mapper = new ObjectMapper();
                        Map<String, Object> storageConfig = mapper.readValue(configStr, Map.class);
                        operand.put("storageConfig", storageConfig);
                    }
                }
            }
            
            // Apache Iceberg configuration with environment variable support
            String icebergEnabled = info.getProperty("icebergEnabled", 
                System.getenv("APERICIO_ICEBERG_ENABLED"));
            if ("true".equalsIgnoreCase(icebergEnabled)) {
                Map<String, Object> icebergConfig = new HashMap<>();
                icebergConfig.put("enabled", true);
                
                // Iceberg catalog configuration
                String catalogType = info.getProperty("icebergCatalogType", 
                    System.getenv("ICEBERG_CATALOG_TYPE"));
                if (catalogType != null) {
                    icebergConfig.put("catalogType", catalogType);
                }
                
                // Iceberg warehouse location
                String warehouse = info.getProperty("icebergWarehouse", 
                    System.getenv("ICEBERG_WAREHOUSE"));
                if (warehouse != null) {
                    icebergConfig.put("warehouse", warehouse);
                }
                
                operand.put("icebergConfig", icebergConfig);
            }
            
            // File filtering configuration with environment variable support
            String filePattern = info.getProperty("filePattern", 
                System.getenv("APERICIO_FILE_PATTERN"));
            if (filePattern != null) {
                operand.put("filePattern", filePattern);
            }
            
            String excludePattern = info.getProperty("excludePattern", 
                System.getenv("APERICIO_EXCLUDE_PATTERN"));
            if (excludePattern != null) {
                operand.put("excludePattern", excludePattern);
            }
            
            // Parallelism configuration
            addPropertyWithEnvFallback(info, operand, "parallelism", "APERICIO_PARALLELISM", Integer.class);
            addPropertyWithEnvFallback(info, operand, "fetchSize", "APERICIO_FETCH_SIZE", Integer.class);
            
            // Materialized views
            if (info.containsKey("materializedViews")) {
                String mvJson = info.getProperty("materializedViews");
                if (mvJson != null && mvJson.startsWith("[")) {
                    ObjectMapper mapper = new ObjectMapper();
                    List<Map<String, Object>> mvs = mapper.readValue(mvJson, List.class);
                    operand.put("materializedViews", mvs);
                }
            }
            
            // Tables configuration
            if (info.containsKey("tables")) {
                String tablesStr = info.getProperty("tables");
                List<Map<String, String>> tables = new ArrayList<>();
                for (String tableEntry : tablesStr.split(",")) {
                    String[] parts = tableEntry.split(":");
                    if (parts.length >= 2) {
                        Map<String, String> table = new HashMap<>();
                        table.put("name", parts[0].trim());
                        table.put("url", parts[1].trim());
                        tables.add(table);
                    }
                }
                if (!tables.isEmpty()) {
                    operand.put("tables", tables);
                }
            }
            
            // Environment variable expansion
            operand = expandEnvironmentVariables(operand);
            
            schema.put("operand", operand);
            schemas.add(schema);
            model.put("schemas", schemas);
            
            // Convert to JSON
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(model);
            
        } catch (Exception e) {
            throw new SQLException("Failed to create Calcite model: " + e.getMessage(), e);
        }
    }
    
    private void addPropertyIfPresent(Properties info, Map<String, Object> operand, 
                                      String key, Class<?> type) {
        if (info.containsKey(key)) {
            String value = info.getProperty(key);
            if (type == Integer.class) {
                operand.put(key, Integer.parseInt(value));
            } else if (type == Long.class) {
                operand.put(key, Long.parseLong(value));
            } else if (type == Boolean.class) {
                operand.put(key, Boolean.parseBoolean(value));
            } else {
                operand.put(key, value);
            }
        }
    }
    
    private void addPropertyWithEnvFallback(Properties info, Map<String, Object> operand,
                                           String property, String envVar, Class<?> type) {
        String value = info.getProperty(property, System.getenv(envVar));
        if (value != null) {
            try {
                if (type == Integer.class) {
                    operand.put(property, Integer.parseInt(value));
                } else if (type == Long.class) {
                    operand.put(property, Long.parseLong(value));
                } else if (type == Boolean.class) {
                    operand.put(property, Boolean.parseBoolean(value));
                } else {
                    operand.put(property, value);
                }
            } catch (NumberFormatException e) {
                LOGGER.warning("Invalid value for " + property + ": " + value);
            }
        }
    }
    
    private Map<String, Object> expandEnvironmentVariables(Map<String, Object> map) {
        Map<String, Object> expanded = new HashMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof String) {
                expanded.put(entry.getKey(), expandEnvVars((String) value));
            } else if (value instanceof Map) {
                expanded.put(entry.getKey(), expandEnvironmentVariables((Map<String, Object>) value));
            } else {
                expanded.put(entry.getKey(), value);
            }
        }
        return expanded;
    }
    
    private String expandEnvVars(String value) {
        if (value == null) return null;
        
        String result = value;
        int startIdx = 0;
        while ((startIdx = result.indexOf("${", startIdx)) != -1) {
            int endIdx = result.indexOf("}", startIdx);
            if (endIdx != -1) {
                String varName = result.substring(startIdx + 2, endIdx);
                String envValue = System.getenv(varName);
                if (envValue == null) {
                    envValue = System.getProperty(varName, "${" + varName + "}");
                }
                result = result.substring(0, startIdx) + envValue + result.substring(endIdx + 1);
                startIdx += envValue.length();
            } else {
                break;
            }
        }
        return result;
    }
}