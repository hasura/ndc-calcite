package com.hasura.file;

import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.adapter.csv.CsvSchemaFactory;
import org.apache.calcite.adapter.arrow.ArrowSchemaFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.avatica.util.Casing;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.net.URI;
import java.net.URISyntaxException;
import java.io.File;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Statement;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.net.URL;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.AbstractSchema;
import com.hasura.file.CasingSchema;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.FileVisitor;
import java.nio.file.FileVisitResult;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.PathMatcher;
import java.nio.file.FileSystems;
import java.io.FileInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.regions.Region;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.linq4j.tree.Expression;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.materialize.Lattice;
import java.util.Collection;
import java.util.Set;
import java.util.HashSet;
import java.io.IOException;
import org.apache.calcite.avatica.DriverVersion;
import java.nio.file.InvalidPathException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enhanced JDBC driver for querying files using Apache Calcite with PostgreSQL compatibility,
 * large dataset spillover support, and comprehensive format support.
 * 
 * Supported formats: CSV, TSV, JSON, YAML, HTML, Parquet, Arrow, Excel (XLSX)
 * Supported sources: Local files, HTTP/HTTPS URLs, S3 buckets
 * 
 * Connection URL formats:
 * 
 * Basic file access:
 * jdbc:file:///path/to/directory?format=csv
 * jdbc:file:///path/to/file.csv
 * 
 * With spillover configuration (for large datasets):
 * jdbc:file:///path/to/data?format=csv&memory_threshold=67108864&spill_directory=/tmp/calcite_spill
 * 
 * Glob patterns (automatically detected):
 * jdbc:file:///data/star-star/star.csv
 * jdbc:file:///logs/2024star.json
 * 
 * Multi-location queries:
 * jdbc:file:multi?locations=/data/sales.csv|http://api.com/products.json|s3://bucket/customers.parquet
 * 
 * Remote files:
 * jdbc:file:http://example.com/data.csv
 * jdbc:file:s3://my-bucket/data/sales.parquet
 * 
 * With views:
 * jdbc:file:///data/sales.csv?viewsFile=/config/views.yaml
 * 
 * Configuration Parameters:
 * - format: File format (csv|tsv|json|yaml|html|parquet|arrow|excel|markdown|docx) [auto-detected]
 * - executionEngine: Processing engine (parquet|vectorized|arrow|linq4j) [parquet]
 * - memoryThreshold: Memory limit before spillover in bytes [67108864]
 * - spillDirectory: Directory for spillover files [system temp dir]
 * - batchSize: Processing batch size [8192-10000]
 * - refreshInterval: Auto-refresh interval (e.g., "5 minutes", "1 hour")
 * - tableNameCasing: Table name casing (UPPER|LOWER|UNCHANGED) [UPPER]
 * - columnNameCasing: Column name casing (UPPER|LOWER|UNCHANGED) [UNCHANGED]
 * - recursive: Recursively scan directories [true]
 * - partitionedTables: JSON configuration for partitioned tables
 * 
 * Storage Provider Parameters:
 * - storageType: Provider type (local|s3|http|ftp|sftp|sharepoint) [auto-detected]
 * - awsRegion: AWS region for S3 access [us-east-1]
 * - tenantId, clientId, clientSecret: SharePoint OAuth2 credentials
 * - username, password: FTP/SFTP credentials
 * - privateKeyPath: SSH key path for SFTP
 * 
 * File-specific Parameters:
 * - multiline: Support multiline JSON [false]
 * - header: CSV has header row [true]
 * - delimiter: Field separator for CSV/TSV [, or tab]
 * - charset: Character encoding [UTF-8]
 * - flatten: Flatten nested JSON structures [false]
 * - skipLines: Lines to skip at start [0]
 * 
 * Security Features:
 * - Path traversal protection
 * - Configuration validation
 * - Safe numeric parameter parsing
 * 
 * Performance Features:
 * - Automatic spillover for datasets larger than memory
 * - Connection deduplication
 * - Metrics collection
 * - Thread-safe operations
 */
public class FileDriver extends Driver {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileDriver.class);
    
    // Track initialized schemas to avoid duplicate setup
    private static final Set<String> INITIALIZED_SCHEMAS = ConcurrentHashMap.newKeySet();
    
    // Driver version information
    private static final String DRIVER_NAME = "Hasura File JDBC Driver";
    private static final String DRIVER_VERSION = "2.0.0";
    private static final String VENDOR_NAME = "Hasura";
    private static final String CALCITE_VERSION = "1.41.0-SNAPSHOT";
    
    // Metrics collection
    private static final AtomicLong CONNECTION_COUNT = new AtomicLong(0);
    private static final AtomicLong SCHEMA_SETUP_COUNT = new AtomicLong(0);
    private static final AtomicLong ERROR_COUNT = new AtomicLong(0);
    private static final Map<String, AtomicLong> FORMAT_USAGE = new ConcurrentHashMap<>();
    
    static {
        new FileDriver().register();
    }

    @Override
    protected String getConnectStringPrefix() {
        return "jdbc:file:";
    }
    
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null || url.trim().isEmpty()) {
            return false;
        }
        
        if (!url.startsWith(getConnectStringPrefix())) {
            return false;
        }
        
        // Enhanced URL validation
        String urlPart = url.substring(getConnectStringPrefix().length());
        
        // Check for valid URL patterns - enhanced for latest file adapter features
        return urlPart.startsWith("//") ||           // File URLs: jdbc:file://path
               urlPart.startsWith("http://") ||      // HTTP URLs: jdbc:file:http://example.com
               urlPart.startsWith("https://") ||     // HTTPS URLs: jdbc:file:https://example.com
               urlPart.startsWith("s3://") ||        // S3 URLs: jdbc:file:s3://bucket/key
               urlPart.startsWith("ftp://") ||       // FTP URLs: jdbc:file:ftp://server/path
               urlPart.startsWith("ftps://") ||      // FTPS URLs: jdbc:file:ftps://server/path
               urlPart.startsWith("sftp://") ||      // SFTP URLs: jdbc:file:sftp://server/path
               urlPart.startsWith("multi?") ||       // Multi-location: jdbc:file:multi?locations=...
               urlPart.startsWith("config=") ||      // Config: jdbc:file:config=/path/to/config
               urlPart.startsWith("/") ||           // Absolute path: jdbc:file:/path/to/data
               urlPart.matches("^[a-zA-Z]:\\\\.*") || // Windows path: jdbc:file:C:\\data
               urlPart.matches("^[^:/]+.*");         // Relative path: jdbc:file:data/files
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        
        long startTime = System.currentTimeMillis();
        CONNECTION_COUNT.incrementAndGet();
        
        // Check if this is a config file URL
        if (url.contains(":config=") || url.contains("?config=")) {
            return connectWithConfig(url, info);
        }
        
        // Parse connection URL and merge with properties
        Properties mergedProps = parseAndMergeProperties(url, info);
        
        // Validate configuration for security and correctness
        validateConfiguration(mergedProps);
        
        // Set PostgreSQL-compatible SQL dialect
        configurePostgreSQLDialect(mergedProps);
        
        // Create base Calcite connection with proper configuration
        Connection connection = null;
        try {
            // If no model is specified, create a minimal model to ensure proper rules are loaded
            if (!mergedProps.containsKey("model")) {
                // Create a minimal model configuration to ensure Calcite loads the right rules
                Map<String, Object> model = new HashMap<>();
                model.put("version", "1.0");
                model.put("defaultSchema", mergedProps.getProperty("schema", "files"));
                
                // Add empty schemas array to trigger proper initialization
                List<Map<String, Object>> schemas = new ArrayList<>();
                model.put("schemas", schemas);
                
                // Write to temp file
                File tempModelFile = File.createTempFile("file-driver-model-", ".json");
                tempModelFile.deleteOnExit();
                ObjectMapper mapper = new ObjectMapper();
                mapper.writeValue(tempModelFile, model);
                
                mergedProps.setProperty("model", tempModelFile.getAbsolutePath());
            }
            
            connection = DriverManager.getConnection("jdbc:calcite:", mergedProps);
        } catch (SQLException e) {
            // Re-throw SQL exceptions with better context
            throw new SQLException("Failed to establish Calcite JDBC connection. Check your configuration parameters.", 
                "08001", e);
        } catch (Exception e) {
            throw new SQLException("Unexpected error creating Calcite connection: " + e.getMessage(), 
                "08000", e);
        }
        
        if (connection == null) {
            throw new SQLException("Calcite connection factory returned null - driver configuration issue", "08001");
        }
        
        // Configure file schema
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        
        // Create appropriate schema based on file format
        boolean isMultiLocation = "true".equals(mergedProps.getProperty("isMultiLocation"));
        boolean isHttpUrl = "true".equals(mergedProps.getProperty("isHttpUrl"));
        String schemaName = mergedProps.getProperty("schema", "files");
        
        try {
            // Check if path contains glob characters
            String path = mergedProps.getProperty("path");
            boolean hasGlobChars = path != null && (path.contains("*") || path.contains("?") || 
                                                    path.contains("[") || path.contains("]"));
            
            if (hasGlobChars) {
                // Check if adapter supports single-table globs (new feature)
                String globMode = mergedProps.getProperty("globMode", "multi-table"); // Default to old behavior
                
                if ("single-table".equals(globMode)) {
                    // New adapter feature: create single table from glob pattern
                    Map<String, Object> operand = new HashMap<>();
                    
                    // Get table and column name casing options for later wrapping
                    String tableNameCasing = mergedProps.getProperty("table_name_casing", "UPPER");
                    String columnNameCasing = mergedProps.getProperty("column_name_casing", "UNCHANGED");
                    
                    List<Map<String, Object>> tables = new ArrayList<>();
                    Map<String, Object> tableConfig = new HashMap<>();
                    
                    // Extract table name from path
                    String tableName = mergedProps.getProperty("table");
                    if (tableName == null) {
                        // Generate table name from pattern
                        int lastSlash = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
                        String filePattern = lastSlash >= 0 ? path.substring(lastSlash + 1) : path;
                        tableName = filePattern.replaceAll("[*?\\[\\]]", "").replaceAll("\\W+", "_");
                        if (tableName.isEmpty()) tableName = "glob_table";
                    }
                    
                    tableConfig.put("name", tableName);
                    tableConfig.put("url", path); // Glob pattern as URL
                    
                    // Add format-specific settings
                    String format = mergedProps.getProperty("format", detectFormat(path));
                    if ("csv".equals(format) || "tsv".equals(format)) {
                        tableConfig.put("flavor", "scannable");
                        if (mergedProps.containsKey("header")) {
                            tableConfig.put("header", Boolean.parseBoolean(mergedProps.getProperty("header", "true")));
                        }
                    }
                    
                    tables.add(tableConfig);
                    operand.put("tables", tables);
                    
                    // Add performance parameters
                    if (mergedProps.containsKey("executionEngine")) {
                        operand.put("executionEngine", mergedProps.getProperty("executionEngine"));
                    } else if ("parquet".equalsIgnoreCase(format)) {
                        operand.put("executionEngine", "parquet");
                    }
                    
                    if (mergedProps.containsKey("refreshInterval")) {
                        operand.put("refreshInterval", mergedProps.getProperty("refreshInterval"));
                    }
                    
                    SchemaFactory factory = FileSchemaFactory.INSTANCE;
                    Schema fileSchema = factory.create(rootSchema, schemaName, operand);
                    
                    // Apply casing transformation if needed
                    fileSchema = wrapWithCasingIfNeeded(fileSchema, tableNameCasing, columnNameCasing);
                    
                    rootSchema.add(schemaName, fileSchema);
                    LOGGER.info("Successfully connected with glob pattern as single table: {} -> {}", path, tableName);
                } else {
                    // Old behavior: create separate tables for each file
                    // Split path into base directory and glob pattern
                    int lastSep = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
                    String baseDir;
                    String pattern;
                    
                    if (lastSep > 0) {
                        // Find the deepest directory without glob chars
                        String[] parts = path.split("[\\\\/]");
                        int firstGlobPart = -1;
                        for (int i = 0; i < parts.length; i++) {
                            if (parts[i].contains("*") || parts[i].contains("?") || 
                                parts[i].contains("[") || parts[i].contains("]")) {
                                firstGlobPart = i;
                                break;
                            }
                        }
                        
                        if (firstGlobPart > 0) {
                            // Reconstruct base directory and pattern
                            StringBuilder baseDirBuilder = new StringBuilder();
                            for (int i = 0; i < firstGlobPart; i++) {
                                if (i > 0) baseDirBuilder.append(File.separator);
                                baseDirBuilder.append(parts[i]);
                            }
                            baseDir = baseDirBuilder.toString();
                            
                            StringBuilder patternBuilder = new StringBuilder();
                            for (int i = firstGlobPart; i < parts.length; i++) {
                                if (i > firstGlobPart) patternBuilder.append("/");
                                patternBuilder.append(parts[i]);
                            }
                            pattern = patternBuilder.toString();
                        } else {
                            // Glob is in the root part
                            baseDir = ".";
                            pattern = path;
                        }
                    } else {
                        // No directory separator, use current directory
                        baseDir = ".";
                        pattern = path;
                    }
                    
                    // Update properties for glob handling
                    mergedProps.setProperty("path", baseDir);
                    mergedProps.setProperty("glob", pattern);
                    
                    createGlobSchema(rootSchema, schemaName, mergedProps);
                    LOGGER.info("Successfully connected with implicit glob pattern: {} in directory: {}", pattern, baseDir);
                }
            } else if (isMultiLocation) {
                // Handle multiple locations
                String locations = mergedProps.getProperty("locations");
                if (locations == null || locations.isEmpty()) {
                    throw new SQLException("Multi-location URL requires 'locations' parameter", "42000");
                }
            
                // Parse locations (pipe-delimited)
                String[] locationArray = locations.split("\\|");
                List<Map<String, Object>> tables = new ArrayList<>();
                
                for (String loc : locationArray) {
                    loc = loc.trim();
                    if (!loc.isEmpty()) {
                        // Determine format for each location
                        String format = mergedProps.getProperty("format", detectFormat(loc));
                        
                        // Create table configuration for each location
                        Map<String, Object> tableConfig = createTableConfigForLocation(loc, format, mergedProps);
                        tables.add(tableConfig);
                    }
                }
                
                Map<String, Object> operand = new HashMap<>();
                
                // Get table and column name casing options for later wrapping
                String tableNameCasing = mergedProps.getProperty("table_name_casing", "UPPER");
                String columnNameCasing = mergedProps.getProperty("column_name_casing", "UNCHANGED");
                
                operand.put("tables", tables);
                
                SchemaFactory factory = FileSchemaFactory.INSTANCE;
                Schema fileSchema = factory.create(rootSchema, schemaName, operand);
                
                // Apply casing transformation if needed
                fileSchema = wrapWithCasingIfNeeded(fileSchema, tableNameCasing, columnNameCasing);
                
                rootSchema.add(schemaName, fileSchema);
                
                LOGGER.info("Successfully connected to {} file locations", locationArray.length);
            } else {
            // Single location handling (existing code)
            String location = isHttpUrl ? mergedProps.getProperty("url") : mergedProps.getProperty("path");
            String format = mergedProps.getProperty("format", detectFormat(location));
            
            Map<String, Object> operand;
            if (isHttpUrl) {
                // For HTTP URLs, use the tables configuration
                operand = createHttpOperand(location, format, mergedProps);
            } else {
                operand = createOperand(location, format, mergedProps);
            }
            
            SchemaFactory factory;
            if (isHttpUrl) {
                // Always use FileSchemaFactory for HTTP URLs
                factory = FileSchemaFactory.INSTANCE;
            } else {
                // Check if it's a single file
                File file = new File(location);
                if (!file.isDirectory()) {
                    // For single files, check if it's a binary format that needs ArrowSchemaFactory
                    String detectedFormat = format;
                    if (detectedFormat == null) {
                        detectedFormat = detectFormat(location);
                    }
                    // For single files, check if it's a binary format that needs ArrowSchemaFactory
                    if ("arrow".equalsIgnoreCase(detectedFormat) || "parquet".equalsIgnoreCase(detectedFormat)) {
                        factory = createArrowSchemaFactorySafely();
                    } else {
                        // Other formats use FileSchemaFactory for single files
                        factory = FileSchemaFactory.INSTANCE;
                    }
                } else {
                    // For directories, always use FileSchemaFactory as it supports recursive scanning
                    // and can handle mixed file types within the directory
                    factory = FileSchemaFactory.INSTANCE;
                }
            }
            
            Schema fileSchema = factory.create(rootSchema, schemaName, operand);
            
            // Apply casing transformation if needed
            String tableNameCasing = mergedProps.getProperty("table_name_casing", "UPPER");
            String columnNameCasing = mergedProps.getProperty("column_name_casing", "UNCHANGED");
            fileSchema = wrapWithCasingIfNeeded(fileSchema, tableNameCasing, columnNameCasing);
            
            rootSchema.add(schemaName, fileSchema);
            
                LOGGER.info("Successfully connected to files at {} with format {}", location, format);
            }
            
            // Set default schema
            if (mergedProps.getProperty("currentSchema") == null) {
                mergedProps.setProperty("currentSchema", schemaName);
            }
            
        } catch (SQLException e) {
            ERROR_COUNT.incrementAndGet();
            // Re-throw SQL exceptions with additional context
            throw new SQLException("Schema configuration failed: " + e.getMessage(), e.getSQLState(), e);
        } catch (SecurityException e) {
            ERROR_COUNT.incrementAndGet();
            throw new SQLException("Access denied to file resources: " + e.getMessage(), "42000", e);
        } catch (IllegalArgumentException e) {
            ERROR_COUNT.incrementAndGet();
            throw new SQLException("Invalid configuration parameter: " + e.getMessage(), "42000", e);
        } catch (Exception e) {
            ERROR_COUNT.incrementAndGet();
            throw new SQLException("Unexpected error during schema setup: " + e.getMessage(), "42000", e);
        }
        
        // Add views if specified
        try {
            String viewsFile = mergedProps.getProperty("viewsFile");
            String viewsJson = mergedProps.getProperty("views");
            
            // URL decode the views parameter if present (for inline views via URL)
            if (viewsJson != null) {
                try {
                    viewsJson = java.net.URLDecoder.decode(viewsJson, "UTF-8");
                } catch (Exception e) {
                    LOGGER.warn("Failed to URL decode views parameter, using as-is: {}", e.getMessage());
                }
            }
            
            if (viewsFile != null || viewsJson != null) {
                addViewsToSchema(rootSchema, schemaName, viewsFile, viewsJson);
            }
        } catch (SQLException e) {
            // Views are optional, log but don't fail the connection
            LOGGER.warn("Failed to load views, continuing without them: {}", e.getMessage());
        } catch (Exception e) {
            LOGGER.warn("Unexpected error loading views, continuing without them: {}", e.getMessage());
        }
        
        // Log setup metrics
        long setupTime = System.currentTimeMillis() - startTime;
        SCHEMA_SETUP_COUNT.incrementAndGet();
        logSetupMetrics(schemaName, setupTime, mergedProps);
        
        return connection;
    }
    
    /**
     * Connect using a configuration file
     */
    private Connection connectWithConfig(String url, Properties info) throws SQLException {
        try {
            // Extract config path from URL
            String configPath = null;
            if (url.contains(":config=")) {
                // Format: jdbc:file:config=/path/to/config.json
                int configIndex = url.indexOf(":config=");
                configPath = url.substring(configIndex + 8);
            } else if (url.contains("?config=")) {
                // Format: jdbc:file://dummy?config=/path/to/config.json
                int configIndex = url.indexOf("?config=");
                configPath = url.substring(configIndex + 8);
                // Handle additional parameters
                int nextParam = configPath.indexOf('&');
                if (nextParam > 0) {
                    configPath = configPath.substring(0, nextParam);
                }
            }
            
            if (configPath == null || configPath.isEmpty()) {
                throw new SQLException("Config path is required when using config mode");
            }
            
            // Load and process hybrid config file
            LOGGER.info("Loading config from: {}", configPath);
            FileConfig config = FileConfig.load(configPath);
            
            // Create Calcite model from config
            String tempModelPath = createModelFromConfig(config, info);
            
            // Use standard Calcite connection with generated model
            Properties modelProps = new Properties();
            if (info != null) {
                modelProps.putAll(info);
            }
            modelProps.setProperty("model", tempModelPath);
            
            return DriverManager.getConnection("jdbc:calcite:", modelProps);
            
        } catch (Exception e) {
            throw new SQLException("Error processing config: " + e.getMessage(), e);
        }
    }
    
    /**
     * Create temporary Calcite model.json from hybrid config
     */
    private String createModelFromConfig(FileConfig config, Properties info) throws Exception {
        // Create Calcite model structure
        Map<String, Object> model = new HashMap<>();
        model.put("version", "1.0");
        
        // Set default schema if specified
        if (config.getDefaultSchema() != null) {
            model.put("defaultSchema", config.getDefaultSchema());
        }
        
        // Process schemas and apply global config parameters
        if (config.getSchemas() != null) {
            List<Map<String, Object>> processedSchemas = new ArrayList<>();
            
            for (Map<String, Object> schema : config.getSchemas()) {
                Map<String, Object> processedSchema = new HashMap<>(schema);
                
                // Get or create operand map
                Map<String, Object> operand = (Map<String, Object>) processedSchema.get("operand");
                if (operand == null) {
                    operand = new HashMap<>();
                    processedSchema.put("operand", operand);
                } else {
                    operand = new HashMap<>(operand); // Copy to avoid modifying original
                    processedSchema.put("operand", operand);
                }
                
                // Check if we need casing transformation
                String tableNameCasing = config.getTableNameCasing() != null ? 
                    config.getTableNameCasing() : info.getProperty("table_name_casing", "UPPER");
                String columnNameCasing = config.getColumnNameCasing() != null ? 
                    config.getColumnNameCasing() : info.getProperty("column_name_casing", "UNCHANGED");
                
                // Determine which factory to use
                if (needsCasingTransformation(tableNameCasing, columnNameCasing)) {
                    // Use our wrapper factory that applies casing
                    processedSchema.put("type", "custom");
                    processedSchema.put("factory", "com.hasura.file.CasingSchemaFactory");
                    operand.put("tableNameCasing", tableNameCasing);
                    operand.put("columnNameCasing", columnNameCasing);
                } else {
                    // Use standard FileSchemaFactory
                    processedSchema.put("type", "custom");
                    processedSchema.put("factory", "org.apache.calcite.adapter.file.FileSchemaFactory");
                }
                
                // Apply global parameters that FileSchemaFactory actually supports
                // executionEngine is connection-wide, always apply from global config
                if (config.getExecutionEngine() != null) {
                    operand.put("executionEngine", config.getExecutionEngine());
                }
                
                // These can be overridden at schema/table level
                if (config.getBatchSize() != null && !operand.containsKey("batchSize")) {
                    operand.put("batchSize", config.getBatchSize());
                }
                if (config.getMemoryThreshold() != null && !operand.containsKey("memoryThreshold")) {
                    operand.put("memoryThreshold", config.getMemoryThreshold());
                }
                if (config.getRefreshInterval() != null && !operand.containsKey("refreshInterval")) {
                    operand.put("refreshInterval", config.getRefreshInterval());
                }
                
                // Note: spillDirectory is not supported by FileSchemaFactory
                // Format-specific parameters (charset, header, multiline, skipLines) belong in table definitions
                
                // Process materializations to add default table names
                if (operand.containsKey("materializations")) {
                    List<Map<String, Object>> materializations = (List<Map<String, Object>>) operand.get("materializations");
                    List<Map<String, Object>> processedMaterializations = new ArrayList<>();
                    
                    for (Map<String, Object> materialization : materializations) {
                        Map<String, Object> processed = new HashMap<>(materialization);
                        
                        // Default table name to view + "_materialized" if not specified
                        if (processed.containsKey("view") && !processed.containsKey("table")) {
                            String viewName = (String) processed.get("view");
                            processed.put("table", viewName + "_materialized");
                        }
                        
                        processedMaterializations.add(processed);
                    }
                    
                    operand.put("materializations", processedMaterializations);
                }
                
                processedSchemas.add(processedSchema);
            }
            
            model.put("schemas", processedSchemas);
        }
        
        // Set AWS region if specified (global environment setting)
        if (config.getAwsRegion() != null) {
            System.setProperty("aws.region", config.getAwsRegion());
        }
        
        // Write to temporary file
        ObjectMapper mapper = new ObjectMapper();
        File tempFile = File.createTempFile("calcite-model-", ".json");
        tempFile.deleteOnExit();
        mapper.writeValue(tempFile, model);
        
        return tempFile.getAbsolutePath();
    }
    
    /**
     * Validates and secures file paths to prevent directory traversal attacks.
     */
    private void validatePath(String path) throws SQLException {
        if (path == null || path.trim().isEmpty()) {
            throw new SQLException("Path cannot be null or empty");
        }
        
        try {
            Path normalizedPath = Paths.get(path).normalize();
            
            // Check for directory traversal attempts
            if (path.contains("..") && !normalizedPath.toString().equals(path)) {
                throw new SQLException("Path contains directory traversal sequences: " + path);
            }
            
            // Additional security checks
            String normalizedStr = normalizedPath.toString();
            if (normalizedStr.contains("\0")) {
                throw new SQLException("Path contains null bytes: " + path);
            }
            
            // For local paths, ensure they are absolute (security best practice)
            if (!path.startsWith("http://") && !path.startsWith("https://") && 
                !path.startsWith("s3://") && !normalizedPath.isAbsolute()) {
                LOGGER.warn("Relative path detected, converting to absolute: {}", path);
            }
            
        } catch (InvalidPathException e) {
            throw new SQLException("Invalid path format: " + path, e);
        }
    }
    
    /**
     * Validates configuration parameters for security and correctness.
     */
    private void validateConfiguration(Properties props) throws SQLException {
        // Validate paths
        String path = props.getProperty("path");
        if (path != null) {
            validatePath(path);
        }
        
        String url = props.getProperty("url");
        if (url != null && !url.startsWith("http://") && !url.startsWith("https://") && !url.startsWith("s3://")) {
            validatePath(url);
        }
        
        // Validate spillover directory if present
        String spillDirectory = props.getProperty("spill_directory");
        if (spillDirectory != null) {
            validatePath(spillDirectory);
        }
        
        // Validate numeric parameters
        validateNumericParameter(props, "batchSize", 1, 1000000);
        validateNumericParameter(props, "memory_threshold", 1024, Long.MAX_VALUE);
        validateNumericParameter(props, "max_spill_files", 1, 10000);
        validateNumericParameter(props, "skipLines", 0, 1000000);
        
        // Validate format parameter with latest supported formats
        String format = props.getProperty("format");
        if (format != null) {
            Set<String> validFormats = Set.of("csv", "tsv", "json", "yaml", "yml", 
                "parquet", "arrow", "excel", "html", "htm", "markdown", "md", "docx", "doc");
            if (!validFormats.contains(format.toLowerCase())) {
                throw new SQLException("Unsupported format: " + format + 
                    ". Supported formats: " + String.join(", ", validFormats));
            }
        }
        
        // Validate execution engine
        String executionEngine = props.getProperty("executionEngine");
        if (executionEngine != null) {
            Set<String> validEngines = Set.of("parquet", "vectorized", "arrow", "linq4j");
            if (!validEngines.contains(executionEngine.toLowerCase())) {
                throw new SQLException("Unsupported execution engine: " + executionEngine + 
                    ". Supported engines: " + String.join(", ", validEngines));
            }
        }
        
        // Validate refresh interval format
        String refreshInterval = props.getProperty("refreshInterval");
        if (refreshInterval != null) {
            validateRefreshInterval(refreshInterval);
        }
    }
    
    /**
     * Validates numeric configuration parameters.
     */
    private void validateNumericParameter(Properties props, String key, long min, long max) throws SQLException {
        String value = props.getProperty(key);
        if (value != null) {
            try {
                long numValue = Long.parseLong(value);
                if (numValue < min || numValue > max) {
                    throw new SQLException("Parameter " + key + " must be between " + min + " and " + max + ", got: " + numValue);
                }
            } catch (NumberFormatException e) {
                throw new SQLException("Parameter " + key + " must be a valid number, got: " + value, e);
            }
        }
    }
    
    /**
     * Validates refresh interval format based on file adapter documentation.
     */
    private void validateRefreshInterval(String interval) throws SQLException {
        if (interval == null || interval.trim().isEmpty()) {
            return;
        }
        
        String normalized = interval.toLowerCase().trim();
        
        // Check supported interval patterns
        if (normalized.matches("\\d+\\s*(second|seconds|minute|minutes|hour|hours|day|days)s?")) {
            return; // Valid format
        }
        
        // Check specific supported formats from documentation
        Set<String> validIntervals = Set.of(
            "30 seconds", "1 minute", "5 minutes", "30 minutes",
            "1 hour", "2 hours", "12 hours", "1 day", "7 days"
        );
        
        if (validIntervals.contains(normalized)) {
            return; // Valid predefined interval
        }
        
        throw new SQLException("Invalid refresh interval format: " + interval + 
            ". Supported formats: '30 seconds', '1 minute', '5 minutes', '1 hour', '1 day', etc.");
    }

    private Properties parseAndMergeProperties(String url, Properties info) throws SQLException {
        Properties merged = new Properties();
        if (info != null) {
            merged.putAll(info);
        }
        
        try {
            // Remove jdbc:file: prefix
            String urlWithoutPrefix = url.substring(getConnectStringPrefix().length());
            
            // Check for multi-location syntax (but only parse locations, no glob parameter)
            if (urlWithoutPrefix.startsWith("multi?")) {
                // Handle multi-location URLs
                String queryPart = urlWithoutPrefix.substring(6); // Remove "multi?"
                
                // Parse query parameters
                for (String param : queryPart.split("&")) {
                    String[] keyValue = param.split("=", 2);
                    if (keyValue.length == 2 && !"glob".equals(keyValue[0])) {
                        // Skip any glob parameters - we only support implicit glob detection
                        merged.setProperty(keyValue[0], keyValue[1]);
                    }
                }
                
                merged.setProperty("isMultiLocation", "true");
                
                // Set enhanced spillover defaults for multi-location mode
                setEnhancedSpilloverDefaults(merged);
            }
            // Check if it's an HTTP/HTTPS/S3 URL
            else if (urlWithoutPrefix.startsWith("http://") || urlWithoutPrefix.startsWith("https://") || urlWithoutPrefix.startsWith("s3://")) {
                // Handle HTTP/S3 URLs
                int queryIndex = urlWithoutPrefix.indexOf('?');
                String remoteUrl = queryIndex > 0 ? urlWithoutPrefix.substring(0, queryIndex) : urlWithoutPrefix;
                String queryPart = queryIndex > 0 ? urlWithoutPrefix.substring(queryIndex + 1) : "";
                
                merged.setProperty("url", remoteUrl);
                merged.setProperty("isHttpUrl", "true");
                
                // Parse query parameters
                if (!queryPart.isEmpty()) {
                    for (String param : queryPart.split("&")) {
                        String[] keyValue = param.split("=", 2);
                        if (keyValue.length == 2) {
                            merged.setProperty(keyValue[0], keyValue[1]);
                        }
                    }
                }
            } else {
                // Handle file:// or file:/// protocols
                if (urlWithoutPrefix.startsWith("//")) {
                    urlWithoutPrefix = urlWithoutPrefix.substring(2);
                }
                
                // Split path and query
                int queryIndex = urlWithoutPrefix.indexOf('?');
                String pathPart = queryIndex > 0 ? urlWithoutPrefix.substring(0, queryIndex) : urlWithoutPrefix;
                String queryPart = queryIndex > 0 ? urlWithoutPrefix.substring(queryIndex + 1) : "";
                
                // Clean up path
                if (pathPart.startsWith("/") && pathPart.length() > 2 && pathPart.charAt(2) == ':') {
                    // Windows path like /c:/data
                    pathPart = pathPart.substring(1);
                }
                
                // Resolve relative paths
                Path path = Paths.get(pathPart);
                if (!path.isAbsolute()) {
                    path = Paths.get(System.getProperty("user.dir")).resolve(path);
                }
                
                merged.setProperty("path", path.toString());
                
                // Parse query parameters
                if (!queryPart.isEmpty()) {
                    for (String param : queryPart.split("&")) {
                        String[] keyValue = param.split("=", 2);
                        if (keyValue.length == 2) {
                            merged.setProperty(keyValue[0], keyValue[1]);
                        }
                    }
                }
                
                // Set enhanced spillover defaults
                setEnhancedSpilloverDefaults(merged);
            }
            
            // Normalize parameter names for compatibility
            normalizeParameterNames(merged);
            
        } catch (Exception e) {
            throw new SQLException("Invalid connection URL: " + url, e);
        }
        
        return merged;
    }
    
    /**
     * Normalizes parameter names for compatibility between camelCase and underscore naming.
     */
    private void normalizeParameterNames(Properties props) {
        // Map camelCase parameters to underscore versions for internal consistency
        if (props.containsKey("tableNameCasing")) {
            props.setProperty("table_name_casing", props.getProperty("tableNameCasing"));
        }
        if (props.containsKey("columnNameCasing")) {
            props.setProperty("column_name_casing", props.getProperty("columnNameCasing"));
        }
        if (props.containsKey("executionEngine")) {
            props.setProperty("execution_engine", props.getProperty("executionEngine"));
        }
        if (props.containsKey("memoryThreshold")) {
            props.setProperty("memory_threshold", props.getProperty("memoryThreshold"));
        }
        if (props.containsKey("batchSize")) {
            props.setProperty("batch_size", props.getProperty("batchSize"));
        }
        if (props.containsKey("refreshInterval")) {
            props.setProperty("refresh_interval", props.getProperty("refreshInterval"));
        }
    }
    
    /**
     * Sets enhanced spillover configuration for large dataset handling with latest patterns.
     */
    private void setEnhancedSpilloverDefaults(Properties props) {
        // Memory management (align with latest documentation patterns)
        if (!props.containsKey("memoryThreshold")) {
            props.setProperty("memoryThreshold", "67108864"); // 64MB default
        }
        
        if (!props.containsKey("spillDirectory")) {
            props.setProperty("spillDirectory", 
                System.getProperty("java.io.tmpdir") + "/calcite_spill");
        }
        
        // Batch size tuning based on latest performance guidelines
        if (!props.containsKey("batchSize")) {
            props.setProperty("batchSize", "8192"); // Updated default for better performance
        }
        
        // Execution engine - prioritize PARQUET for unlimited dataset sizes
        if (!props.containsKey("executionEngine")) {
            props.setProperty("executionEngine", "parquet"); // Required for spillover and materialized views
        }
        
        // Table refresh configuration
        if (!props.containsKey("refreshInterval") && 
           (props.getProperty("url", "").startsWith("http") || 
            props.getProperty("url", "").startsWith("s3"))) {
            props.setProperty("refreshInterval", "5 minutes"); // Default for remote files
        }
        
        // Identifier casing defaults (PostgreSQL-compatible)
        if (!props.containsKey("tableNameCasing")) {
            props.setProperty("tableNameCasing", "UPPER"); // SQL standard
        }
        
        if (!props.containsKey("columnNameCasing")) {
            props.setProperty("columnNameCasing", "UNCHANGED"); // Preserve file headers
        }
        
        // Recursive directory scanning (enabled by default)
        if (!props.containsKey("recursive")) {
            props.setProperty("recursive", "true");
        }
    }
    
    private void configurePostgreSQLDialect(Properties props) {
        // Set lexical rules for PostgreSQL-like behavior
        // Use ORACLE lex for double-quoted identifiers (PostgreSQL standard)
        props.setProperty(CalciteConnectionProperty.LEX.camelName(), Lex.ORACLE.name());
        
        // Enable Babel parser for broad SQL compatibility including PostgreSQL features
        props.setProperty(CalciteConnectionProperty.PARSER_FACTORY.camelName(),
            SqlBabelParserImpl.class.getName() + "#FACTORY");
        
        // PostgreSQL identifier handling:
        // - Unquoted identifiers are folded to lowercase
        // - Quoted identifiers preserve case
        // - Identifiers are compared case-sensitively after folding
        
        // Allow these properties to be overridden via connection properties
        if (!props.containsKey(CalciteConnectionProperty.CASE_SENSITIVE.camelName())) {
            props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "true");
        }
        if (!props.containsKey(CalciteConnectionProperty.QUOTED_CASING.camelName())) {
            props.setProperty(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.name());
        }
        if (!props.containsKey(CalciteConnectionProperty.UNQUOTED_CASING.camelName())) {
            props.setProperty(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.TO_LOWER.name());
        }
        
        // Use BABEL conformance for maximum PostgreSQL compatibility
        // This allows PostgreSQL-specific syntax like :: casting
        props.setProperty(CalciteConnectionProperty.CONFORMANCE.camelName(), 
            SqlConformanceEnum.BABEL.name());
        
        // Enable PostgreSQL function library
        // This includes functions like STRING_AGG, ARRAY_AGG, date_part, etc.
        props.setProperty(CalciteConnectionProperty.FUN.camelName(), "postgresql");
        
        // Enable lenient mode for more flexible parsing
        props.setProperty(CalciteConnectionProperty.LENIENT_OPERATOR_LOOKUP.camelName(), "true");
        
        // Set type coercion to match PostgreSQL behavior
        props.setProperty(CalciteConnectionProperty.TYPE_COERCION.camelName(), "true");
    }
    
    private String detectFormat(String path) {
        String lowercasePath = path.toLowerCase();
        if (lowercasePath.endsWith(".csv") || lowercasePath.endsWith(".tsv")) {
            return "csv";
        } else if (lowercasePath.endsWith(".json")) {
            return "json";
        } else if (lowercasePath.endsWith(".yaml") || lowercasePath.endsWith(".yml")) {
            return "json"; // YAML is handled as JSON by FileSchemaFactory
        } else if (lowercasePath.endsWith(".parquet")) {
            return "parquet";
        } else if (lowercasePath.endsWith(".arrow") || lowercasePath.endsWith(".feather")) {
            return "arrow";
        } else if (lowercasePath.endsWith(".xlsx") || lowercasePath.endsWith(".xls")) {
            return "excel";
        } else if (lowercasePath.endsWith(".html") || lowercasePath.endsWith(".htm")) {
            return "html";
        } else if (lowercasePath.endsWith(".md") || lowercasePath.endsWith(".markdown")) {
            return "markdown"; // Markdown table support
        } else if (lowercasePath.endsWith(".docx") || lowercasePath.endsWith(".doc")) {
            return "docx"; // DOCX table support
        }
        // Default to CSV for unknown extensions
        return "csv";
    }
    
    private SchemaFactory getSchemaFactory(String format) {
        switch (format.toLowerCase()) {
            case "csv":
            case "tsv":
                return CsvSchemaFactory.INSTANCE;
            case "json":
            case "excel":
                return FileSchemaFactory.INSTANCE;
            case "arrow":
            case "parquet":
                return createArrowSchemaFactorySafely();
            default:
                // Default to FileSchemaFactory which handles multiple formats
                return FileSchemaFactory.INSTANCE;
        }
    }
    
    /**
     * Creates an ArrowSchemaFactory safely, falling back to FileSchemaFactory 
     * if Gandiva/Z3 native libraries are not available.
     */
    private static SchemaFactory createArrowSchemaFactorySafely() {
        return new SafeArrowSchemaFactory();
    }
    
    private Map<String, Object> createOperand(String path, String format, Properties props) {
        Map<String, Object> operand = new HashMap<>();
        
        File file = new File(path);
        if (file.isDirectory()) {
            operand.put("directory", path);
            // Use the recursive parameter from properties, defaulting to true for backward compatibility
            boolean recursive = Boolean.parseBoolean(props.getProperty("recursive", "true"));
            operand.put("recursive", recursive);
            
            // Note: multiTableExcel parameter removed in latest Calcite - Excel files now always extract all sheets
            
            // Smart defaults for execution engine based on format
            if (!props.containsKey("executionEngine") && "parquet".equalsIgnoreCase(format)) {
                operand.put("executionEngine", "parquet"); // Best for Parquet files
            } else if (props.containsKey("executionEngine")) {
                operand.put("executionEngine", props.getProperty("executionEngine"));
            }
            
            // Performance parameters with enhanced spillover support
            if (props.containsKey("batchSize")) {
                operand.put("batchSize", Integer.parseInt(props.getProperty("batchSize")));
            }
            if (props.containsKey("memoryThreshold")) {
                operand.put("memoryThreshold", Long.parseLong(props.getProperty("memoryThreshold")));
            }
            if (props.containsKey("spillDirectory")) {
                operand.put("spillDirectory", props.getProperty("spillDirectory"));
            }
            
            // Storage provider configuration
            if (props.containsKey("storageType")) {
                operand.put("storageType", props.getProperty("storageType"));
            }
            
            // Remote file authentication
            configureStorageProviderAuth(operand, props);
            
            // Add refresh interval if specified
            if (props.containsKey("refreshInterval")) {
                operand.put("refreshInterval", props.getProperty("refreshInterval"));
            }
            
            // Add partitioned tables configuration if specified
            if (props.containsKey("partitionedTables")) {
                try {
                    // Parse partitionedTables JSON/YAML configuration
                    String partitionedTablesJson = props.getProperty("partitionedTables");
                    ObjectMapper mapper = new ObjectMapper();
                    List<Map<String, Object>> partitionedTables = mapper.readValue(partitionedTablesJson, 
                        mapper.getTypeFactory().constructCollectionType(List.class, Map.class));
                    operand.put("partitionedTables", partitionedTables);
                } catch (Exception e) {
                    LOGGER.warn("Failed to parse partitionedTables configuration: {}", e.getMessage());
                }
            }
            
            // Load views and materialized views from config or viewsFile (legacy)
            if (props.containsKey("configViews")) {
                // Views from config file
                try {
                    String viewsJson = props.getProperty("configViews");
                    ObjectMapper mapper = new ObjectMapper();
                    List<Map<String, Object>> views = mapper.readValue(viewsJson, 
                        mapper.getTypeFactory().constructCollectionType(List.class, Map.class));
                    if (!views.isEmpty()) {
                        operand.put("views", views);
                    }
                } catch (Exception e) {
                    LOGGER.warn("Failed to parse config views: {}", e.getMessage());
                }
            } else if (props.containsKey("viewsFile")) {
                // Legacy viewsFile support
                try {
                    List<Map<String, Object>> views = loadViewsFromFileForOperand(props.getProperty("viewsFile"));
                    if (!views.isEmpty()) {
                        operand.put("views", views);
                    }
                    
                    List<Map<String, Object>> materializations = loadMaterializationsFromFile(props.getProperty("viewsFile"));
                    if (!materializations.isEmpty()) {
                        // Validate that materialized views require PARQUET engine
                        String executionEngine = (String) operand.get("executionEngine");
                        if (executionEngine == null || !"parquet".equalsIgnoreCase(executionEngine)) {
                            LOGGER.warn("Materialized views require PARQUET execution engine. " +
                                "Current engine: {}. Forcing to PARQUET.", executionEngine);
                            operand.put("executionEngine", "parquet");
                        }
                        operand.put("materializations", materializations);
                    }
                } catch (Exception e) {
                    LOGGER.warn("Failed to load views/materializations from viewsFile: {}", e.getMessage());
                }
            }
            
            // Materialized views from config
            if (props.containsKey("configMaterializations")) {
                try {
                    String matJson = props.getProperty("configMaterializations");
                    ObjectMapper mapper = new ObjectMapper();
                    List<Map<String, Object>> materializations = mapper.readValue(matJson, 
                        mapper.getTypeFactory().constructCollectionType(List.class, Map.class));
                    if (!materializations.isEmpty()) {
                        // Validate that materialized views require PARQUET engine
                        String executionEngine = (String) operand.get("executionEngine");
                        if (executionEngine == null || !"parquet".equalsIgnoreCase(executionEngine)) {
                            LOGGER.warn("Materialized views require PARQUET execution engine. " +
                                "Current engine: {}. Forcing to PARQUET.", executionEngine);
                            operand.put("executionEngine", "parquet");
                        }
                        operand.put("materializations", materializations);
                    }
                } catch (Exception e) {
                    LOGGER.warn("Failed to parse config materializations: {}", e.getMessage());
                }
            }
            
        } else {
            // Check if it's an Arrow/Parquet file that needs special handling
            if ("arrow".equalsIgnoreCase(format) || "parquet".equalsIgnoreCase(format)) {
                // ArrowSchemaFactory expects a directory, so use parent directory
                operand.put("directory", file.getParent());
                return operand;
            }
            
            // Check if it's an Excel file that needs special handling
            if ("excel".equalsIgnoreCase(format) || "xlsx".equalsIgnoreCase(format) || 
                path.toLowerCase().endsWith(".xlsx") || path.toLowerCase().endsWith(".xls")) {
                // For Excel files, use directory-based discovery
                // FileSchemaFactory expects Excel files to be in a directory
                operand.put("directory", file.getParent());
                
                // Note: multiTableExcel parameter removed in latest Calcite - Excel files now always extract all sheets
                
                // Performance parameters from README
                if (props.containsKey("executionEngine")) {
                    operand.put("executionEngine", props.getProperty("executionEngine"));
                }
                if (props.containsKey("batchSize")) {
                    operand.put("batchSize", Integer.parseInt(props.getProperty("batchSize")));
                }
                if (props.containsKey("memoryThreshold")) {
                    operand.put("memoryThreshold", Long.parseLong(props.getProperty("memoryThreshold")));
                }
                if (props.containsKey("spillDirectory")) {
                    operand.put("spillDirectory", props.getProperty("spillDirectory"));
                }
                
                return operand;
            }
            
            // Check if it's an HTML file that needs special handling
            if ("html".equalsIgnoreCase(format) || "htm".equalsIgnoreCase(format) || 
                path.toLowerCase().endsWith(".html") || path.toLowerCase().endsWith(".htm")) {
                // For HTML files, use directory-based discovery
                // Latest Calcite requires HTML files to be handled via directory discovery, not explicit table mapping
                operand.put("directory", file.getParent());
                
                // Performance parameters from README
                if (props.containsKey("executionEngine")) {
                    operand.put("executionEngine", props.getProperty("executionEngine"));
                }
                if (props.containsKey("batchSize")) {
                    operand.put("batchSize", Integer.parseInt(props.getProperty("batchSize")));
                }
                if (props.containsKey("memoryThreshold")) {
                    operand.put("memoryThreshold", Long.parseLong(props.getProperty("memoryThreshold")));
                }
                if (props.containsKey("spillDirectory")) {
                    operand.put("spillDirectory", props.getProperty("spillDirectory"));
                }
                
                return operand;
            }
            
            // For other single files, use tables configuration
            {
                List<Map<String, Object>> tables = new ArrayList<>();
                Map<String, Object> tableConfig = new HashMap<>();
                
                // Extract table name from filename
                String filename = file.getName();
                int lastDot = filename.lastIndexOf('.');
                String tableName = lastDot > 0 ? filename.substring(0, lastDot) : filename;
                
                tableConfig.put("name", tableName);
                
                // Note: Local HTML files cannot be used in explicit table definitions per latest Calcite
                // Only HTTP(S) URLs with fragments are allowed for HTML
                tableConfig.put("url", file.getAbsolutePath());
                
                // Add refresh interval if specified (for single file tables)
                if (props.containsKey("refreshInterval")) {
                    tableConfig.put("refreshInterval", props.getProperty("refreshInterval"));
                }
                
                // Format-specific configurations
                switch (format.toLowerCase()) {
                    case "csv":
                    case "tsv":
                        tableConfig.put("flavor", "scannable");
                        if ("tsv".equals(format)) {
                            tableConfig.put("separator", "\t");
                        }
                        if (props.containsKey("header")) {
                            tableConfig.put("header", Boolean.parseBoolean(props.getProperty("header", "true")));
                        }
                        if (props.containsKey("skipLines")) {
                            tableConfig.put("skipLines", Integer.parseInt(props.getProperty("skipLines", "0")));
                        }
                        break;
                        
                    case "json":
                        if (props.containsKey("multiline")) {
                            tableConfig.put("multiline", Boolean.parseBoolean(props.getProperty("multiline", "false")));
                        }
                        break;
                        
                    case "html":
                        // HTML specific settings
                        tableConfig.put("flavor", "scannable");
                        break;
                        
                    case "arrow":
                    case "parquet":
                        // Arrow/Parquet specific settings can be added here
                        break;
                }
                
                // Common settings
                if (props.containsKey("charset")) {
                    tableConfig.put("charset", props.getProperty("charset"));
                }
                
                tables.add(tableConfig);
                operand.put("tables", tables);
                
                // Smart defaults for execution engine based on format
                if (!props.containsKey("executionEngine")) {
                    if ("parquet".equalsIgnoreCase(format)) {
                        operand.put("executionEngine", "parquet"); // Best for Parquet files
                    } else if ("csv".equalsIgnoreCase(format) || "tsv".equalsIgnoreCase(format)) {
                        operand.put("executionEngine", "arrow"); // Good for CSV/TSV
                    }
                } else {
                    operand.put("executionEngine", props.getProperty("executionEngine"));
                }
                
                // Enhanced performance parameters
                if (props.containsKey("batchSize")) {
                    operand.put("batchSize", Integer.parseInt(props.getProperty("batchSize")));
                }
                if (props.containsKey("memoryThreshold")) {
                    operand.put("memoryThreshold", Long.parseLong(props.getProperty("memoryThreshold")));
                }
                if (props.containsKey("spillDirectory")) {
                    operand.put("spillDirectory", props.getProperty("spillDirectory"));
                }
                
                // Storage provider configuration for single files
                configureStorageProviderAuth(operand, props);
            }
        }
        
        return operand;
    }
    
    private Map<String, Object> createHttpOperand(String url, String format, Properties props) {
        Map<String, Object> operand = new HashMap<>();
        
        // Note: Excel files via HTTP are challenging because FileSchemaFactory expects
        // directory-based discovery for multi-sheet handling, but HTTP URLs are single files.
        // For now, we'll use the single-table approach for all HTTP URLs including Excel.
        
        // Extract table name from URL if not provided
        String tableName = props.getProperty("table");
        if (tableName == null) {
            // Extract filename from URL
            int lastSlash = url.lastIndexOf('/');
            if (lastSlash >= 0 && lastSlash < url.length() - 1) {
                String filename = url.substring(lastSlash + 1);
                // Remove extension
                int lastDot = filename.lastIndexOf('.');
                if (lastDot > 0) {
                    tableName = filename.substring(0, lastDot);
                } else {
                    tableName = filename;
                }
            } else {
                tableName = "remote_table";
            }
        }
        
        // Create tables configuration for FileSchemaFactory
        List<Map<String, Object>> tables = new ArrayList<>();
        Map<String, Object> tableConfig = new HashMap<>();
        tableConfig.put("name", tableName);
        tableConfig.put("url", url);
        
        // Format-specific configurations
        switch (format.toLowerCase()) {
            case "csv":
            case "tsv":
                tableConfig.put("flavor", "scannable");
                if ("tsv".equals(format)) {
                    tableConfig.put("separator", "\t");
                }
                if (props.containsKey("header")) {
                    tableConfig.put("header", Boolean.parseBoolean(props.getProperty("header", "true")));
                }
                break;
                
            case "json":
                if (props.containsKey("multiline")) {
                    tableConfig.put("multiline", Boolean.parseBoolean(props.getProperty("multiline", "false")));
                }
                break;
        }
        
        // Common settings
        if (props.containsKey("charset")) {
            tableConfig.put("charset", props.getProperty("charset"));
        }
        
        tables.add(tableConfig);
        operand.put("tables", tables);
        
        // Smart defaults for execution engine based on format
        if (!props.containsKey("executionEngine")) {
            if ("parquet".equalsIgnoreCase(format)) {
                operand.put("executionEngine", "parquet"); // Best for Parquet files
            } else if ("csv".equalsIgnoreCase(format) || "tsv".equalsIgnoreCase(format)) {
                operand.put("executionEngine", "arrow"); // Good for CSV/TSV
            }
        } else {
            operand.put("executionEngine", props.getProperty("executionEngine"));
        }
        
        // Enhanced performance parameters for HTTP URLs
        if (props.containsKey("batchSize")) {
            operand.put("batchSize", Integer.parseInt(props.getProperty("batchSize")));
        }
        if (props.containsKey("memoryThreshold")) {
            operand.put("memoryThreshold", Long.parseLong(props.getProperty("memoryThreshold")));
        }
        if (props.containsKey("spillDirectory")) {
            operand.put("spillDirectory", props.getProperty("spillDirectory"));
        }
        
        // Storage provider configuration for HTTP/remote files
        configureStorageProviderAuth(operand, props);
        
        // Add refresh interval if specified
        if (props.containsKey("refreshInterval")) {
            operand.put("refreshInterval", props.getProperty("refreshInterval"));
        }
        
        return operand;
    }
    
    private Map<String, Object> createTableConfigForLocation(String location, String format, Properties props) {
        Map<String, Object> tableConfig = new HashMap<>();
        
        // Extract table name from location
        String tableName = extractTableName(location);
        tableConfig.put("name", tableName);
        
        // Check if it's a URL or file path
        if (location.startsWith("http://") || location.startsWith("https://") || location.startsWith("s3://")) {
            tableConfig.put("url", location);
        } else {
            // For local files, use absolute path
            Path path = Paths.get(location);
            if (!path.isAbsolute()) {
                path = Paths.get(System.getProperty("user.dir")).resolve(path);
            }
            tableConfig.put("url", path.toUri().toString());
        }
        
        // Format-specific configurations
        switch (format.toLowerCase()) {
            case "csv":
            case "tsv":
                tableConfig.put("flavor", "scannable");
                if ("tsv".equals(format)) {
                    tableConfig.put("separator", "\t");
                }
                if (props.containsKey("header")) {
                    tableConfig.put("header", Boolean.parseBoolean(props.getProperty("header", "true")));
                }
                break;
                
            case "json":
                if (props.containsKey("multiline")) {
                    tableConfig.put("multiline", Boolean.parseBoolean(props.getProperty("multiline", "false")));
                }
                break;
        }
        
        // Common settings
        if (props.containsKey("charset")) {
            tableConfig.put("charset", props.getProperty("charset"));
        }
        
        return tableConfig;
    }
    
    private String extractTableName(String location) {
        // Remove query parameters if any
        int queryIndex = location.indexOf('?');
        String pathPart = queryIndex > 0 ? location.substring(0, queryIndex) : location;
        
        // Extract filename from path
        int lastSlash = pathPart.lastIndexOf('/');
        int lastBackslash = pathPart.lastIndexOf('\\');
        int lastSeparator = Math.max(lastSlash, lastBackslash);
        
        String filename;
        if (lastSeparator >= 0 && lastSeparator < pathPart.length() - 1) {
            filename = pathPart.substring(lastSeparator + 1);
        } else {
            filename = pathPart;
        }
        
        // Remove extension to get table name
        int lastDot = filename.lastIndexOf('.');
        if (lastDot > 0) {
            return filename.substring(0, lastDot);
        }
        
        return filename.isEmpty() ? "table" : filename;
    }
    
    private void addViewsToSchema(SchemaPlus rootSchema, String schemaName, 
                                 String viewsFile, String viewsJson) throws SQLException {
        List<ViewDefinition> views = new ArrayList<>();
        
        try {
            if (viewsFile != null) {
                // Load views from file (JSON or YAML)  
                views = loadViewsFromFile(viewsFile);
            } else if (viewsJson != null) {
                // Parse inline JSON
                ObjectMapper mapper = new ObjectMapper();
                views = mapper.readValue(viewsJson, 
                    mapper.getTypeFactory().constructCollectionType(List.class, ViewDefinition.class));
            }
            
            // Get the schema where we want to add views
            SchemaPlus targetSchema = rootSchema.getSubSchema(schemaName);
            if (targetSchema == null) {
                throw new SQLException("Schema " + schemaName + " not found");
            }
            
            // Add each view as a table to the schema
            for (ViewDefinition view : views) {
                LOGGER.info("Adding view: {}", view.getName());
                
                // Create a ViewTable that will execute the SQL when queried
                String viewSql = view.getSql();
                // The view SQL doesn't need qualification since it runs in the context of the schema
                
                targetSchema.add(view.getName(), 
                    ViewTable.viewMacro(targetSchema, viewSql, 
                        null, Arrays.asList(schemaName), null));
            }
            
            LOGGER.info("Successfully added {} views to schema", views.size());
            
        } catch (Exception e) {
            LOGGER.error("Failed to add views: {}", e.getMessage());
            if (e.getCause() != null) {
                LOGGER.error("Caused by: {}", e.getCause().getMessage());
            }
            throw new SQLException("Failed to add views to schema", e);
        }
    }
    
    private List<ViewDefinition> loadViewsFromFile(String fileLocation) throws Exception {
        ObjectMapper mapper;
        
        // Check if it's a URL or file path
        if (fileLocation.startsWith("http://") || fileLocation.startsWith("https://") || 
            fileLocation.startsWith("s3://")) {
            // Handle URL
            URL url = new URL(fileLocation);
            
            if (fileLocation.endsWith(".yaml") || fileLocation.endsWith(".yml")) {
                mapper = new ObjectMapper(new YAMLFactory());
            } else {
                mapper = new ObjectMapper();
            }
            
            return mapper.readValue(url, 
                mapper.getTypeFactory().constructCollectionType(List.class, ViewDefinition.class));
        } else {
            // Handle local file
            File file = new File(fileLocation);
            if (!file.isAbsolute()) {
                file = new File(System.getProperty("user.dir"), fileLocation);
            }
            
            if (fileLocation.endsWith(".yaml") || fileLocation.endsWith(".yml")) {
                mapper = new ObjectMapper(new YAMLFactory());
            } else {
                mapper = new ObjectMapper();
            }
            
            return mapper.readValue(file, 
                mapper.getTypeFactory().constructCollectionType(List.class, ViewDefinition.class));
        }
    }
    
    private List<Map<String, Object>> loadViewsFromFileForOperand(String fileLocation) throws Exception {
        List<ViewDefinition> viewDefinitions = loadViewsFromFile(fileLocation);
        List<Map<String, Object>> views = new ArrayList<>();
        
        for (ViewDefinition vd : viewDefinitions) {
            Map<String, Object> view = new HashMap<>();
            view.put("name", vd.getName());
            view.put("sql", vd.getSql());
            views.add(view);
        }
        
        return views;
    }
    
    private List<Map<String, Object>> loadMaterializationsFromFile(String fileLocation) throws Exception {
        ObjectMapper mapper;
        List<MaterializedViewDefinition> materializedViews;
        
        // Check if it's a URL or file path
        if (fileLocation.startsWith("http://") || fileLocation.startsWith("https://") || 
            fileLocation.startsWith("s3://")) {
            // Handle URL
            URL url = new URL(fileLocation);
            
            if (fileLocation.endsWith(".yaml") || fileLocation.endsWith(".yml")) {
                mapper = new ObjectMapper(new YAMLFactory());
            } else {
                mapper = new ObjectMapper();
            }
            
            materializedViews = mapper.readValue(url, 
                mapper.getTypeFactory().constructCollectionType(List.class, MaterializedViewDefinition.class));
        } else {
            // Handle local file
            File file = new File(fileLocation);
            if (!file.isAbsolute()) {
                file = new File(System.getProperty("user.dir"), fileLocation);
            }
            
            if (fileLocation.endsWith(".yaml") || fileLocation.endsWith(".yml")) {
                mapper = new ObjectMapper(new YAMLFactory());
            } else {
                mapper = new ObjectMapper();
            }
            
            materializedViews = mapper.readValue(file, 
                mapper.getTypeFactory().constructCollectionType(List.class, MaterializedViewDefinition.class));
        }
        
        // Convert to the format expected by FileSchemaFactory
        List<Map<String, Object>> materializations = new ArrayList<>();
        for (MaterializedViewDefinition mv : materializedViews) {
            Map<String, Object> materialization = new HashMap<>();
            materialization.put("view", mv.getView());
            materialization.put("table", mv.getTable() != null ? mv.getTable() : mv.getView());
            materialization.put("sql", mv.getSql());
            materializations.add(materialization);
        }
        
        return materializations;
    }
    
    // Inner class for materialized view definition
    public static class MaterializedViewDefinition {
        private String view;
        private String table;
        private String sql;
        
        public String getView() { return view; }
        public void setView(String view) { this.view = view; }
        
        public String getTable() { return table; }
        public void setTable(String table) { this.table = table; }
        
        public String getSql() { return sql; }
        public void setSql(String sql) { this.sql = sql; }
    }
    
    // Inner class for view definition
    public static class ViewDefinition {
        private String name;
        private String sql;
        private String description;
        
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        
        public String getSql() { return sql; }
        public void setSql(String sql) { this.sql = sql; }
        
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
    }
    
    private void createGlobSchema(SchemaPlus rootSchema, String schemaName, Properties props) throws SQLException {
        String basePath = props.getProperty("path", props.getProperty("url", "."));
        String globPattern = props.getProperty("glob");
        
        // Generate schema key for deduplication
        String schemaKey = schemaName + ":" + basePath + ":" + globPattern;
        
        // Check if a schema with this name already exists
        if (rootSchema.getSubSchema(schemaName) != null) {
            LOGGER.info("Schema '{}' already exists, skipping creation", schemaName);
            return;
        }
        
        // Avoid duplicate initialization - but only skip if the exact same glob pattern is used
        if (INITIALIZED_SCHEMAS.contains(schemaKey)) {
            LOGGER.info("Glob schema already initialized with same pattern: {}", schemaName);
            return;
        }
        
        GlobFileSchema globSchema = new GlobFileSchema(rootSchema, basePath, globPattern, props);
        
        // Apply casing transformation if needed
        String tableNameCasing = props.getProperty("table_name_casing", "UPPER");
        String columnNameCasing = props.getProperty("column_name_casing", "UNCHANGED");
        Schema wrappedSchema = wrapWithCasingIfNeeded(globSchema, tableNameCasing, columnNameCasing);
        
        rootSchema.add(schemaName, wrappedSchema);
        
        INITIALIZED_SCHEMAS.add(schemaKey);
    }
    
    // Inner class for glob pattern support
    private static class GlobFileSchema extends AbstractSchema {
        private final SchemaPlus rootSchema;
        private final String basePath;
        private final String globPattern;
        private final Properties props;
        private Map<String, Table> tableMap;
        
        GlobFileSchema(SchemaPlus rootSchema, String basePath, String globPattern, Properties props) {
            this.rootSchema = rootSchema;
            this.basePath = basePath;
            this.globPattern = globPattern;
            this.props = props;
        }
        
        @Override
        protected Map<String, Table> getTableMap() {
            if (tableMap == null) {
                tableMap = createTableMap();
            }
            return tableMap;
        }
        
        private Map<String, Table> createTableMap() {
            Map<String, Table> tables = new HashMap<>();
            
            try {
                List<Path> matchingFiles = findMatchingFiles();
                
                // Process each file exactly like a single file (no grouping optimization)
                for (Path file : matchingFiles) {
                    String tableName = deriveTableName(file);
                    String pathStr = file.toString();
                    String format = detectFileFormat(pathStr);
                    
                    // Use the EXACT same logic as createOperand() method for single files
                    Map<String, Object> operand = new HashMap<>();
                    SchemaFactory factory;
                    
                    // Check if it's an Arrow/Parquet file that needs special handling
                    if ("arrow".equalsIgnoreCase(format) || "parquet".equalsIgnoreCase(format)) {
                        // For Arrow/Parquet in glob, use directory
                        operand.put("directory", file.getParent().toString());
                        factory = createArrowSchemaFactorySafely();
                    } else if ("excel".equalsIgnoreCase(format) || "xlsx".equalsIgnoreCase(format) || 
                               pathStr.toLowerCase().endsWith(".xlsx") || pathStr.toLowerCase().endsWith(".xls")) {
                        // For Excel files, use directory-based discovery
                        operand.put("directory", file.getParent().toString());
                        
                        // Note: multiTableExcel parameter removed in latest Calcite - Excel files now always extract all sheets
                        
                        factory = FileSchemaFactory.INSTANCE;
                    } else {
                        // For other single files, use tables configuration
                        List<Map<String, Object>> tableList = new ArrayList<>();
                        Map<String, Object> tableConfig = new HashMap<>();
                        
                        // Extract table name from filename (exact same as createOperand)
                        String filename = file.getFileName().toString();
                        int lastDot = filename.lastIndexOf('.');
                        String baseTableName = lastDot > 0 ? filename.substring(0, lastDot) : filename;
                        
                        tableConfig.put("name", tableName); // Use our nested naming for consistency
                        
                        if (pathStr.startsWith("s3://")) {
                            tableConfig.put("url", pathStr);
                        } else {
                            // Note: Local HTML files should use directory discovery, not explicit tables
                            tableConfig.put("url", file.toFile().getAbsolutePath());
                        }
                        
                        // Format-specific configurations (exact same as createOperand)
                        switch (format.toLowerCase()) {
                            case "csv":
                            case "tsv":
                                tableConfig.put("flavor", "scannable");
                                if ("tsv".equals(format)) {
                                    tableConfig.put("separator", "\t");
                                }
                                // Note: props-based settings would go here if needed
                                break;
                            case "json":
                                // JSON specific settings can be added here
                                break;
                        }
                        
                        tableList.add(tableConfig);
                        operand.put("tables", tableList);
                        factory = FileSchemaFactory.INSTANCE;
                    }
                    
                    // Create schema (reuse for same directory with Arrow/Parquet)
                    try {
                        SchemaPlus schema;
                        
                        if ("arrow".equalsIgnoreCase(format) || "parquet".equalsIgnoreCase(format)) {
                            // For Arrow/Parquet, create individual schema per file
                            // This ensures each file gets its own table discovery
                            String schemaName = "__glob_arrow_" + tableName + "_" + System.currentTimeMillis();
                            schema = rootSchema.add(schemaName, 
                                factory.create(rootSchema, schemaName, operand));
                            // For Arrow/Parquet, we need to find the table by the base filename
                            String filename = file.getFileName().toString();
                            int lastDot = filename.lastIndexOf('.');
                            String baseFileName = lastDot > 0 ? filename.substring(0, lastDot) : filename;
                            
                            
                            // Try both uppercase and original case
                            Table table = schema.getTable(baseFileName.toUpperCase());
                            if (table == null) {
                                table = schema.getTable(baseFileName);
                            }
                            
                            if (table != null) {
                                // Use our derived tableName (with nested path) as the key
                                tables.put(tableName, table);
                            }
                        } else if ("excel".equalsIgnoreCase(format) || "xlsx".equalsIgnoreCase(format) || 
                                   pathStr.toLowerCase().endsWith(".xlsx") || pathStr.toLowerCase().endsWith(".xls")) {
                            // For Excel files, create schema with directory-based discovery
                            String schemaName = "__glob_excel_" + tableName + "_" + System.currentTimeMillis();
                            schema = rootSchema.add(schemaName, 
                                factory.create(rootSchema, schemaName, operand));
                            
                            // Excel files create multiple tables (one per sheet)
                            // Find all tables in the schema
                            for (String sheetTableName : schema.getTableNames()) {
                                Table table = schema.getTable(sheetTableName);
                                if (table != null) {
                                    // For glob discovery, prefix sheet tables with the file's table name
                                    String fullTableName = tableName + "__" + sheetTableName;
                                    tables.put(fullTableName, table);
                                }
                            }
                        } else {
                            // For non-Arrow/Parquet files, create individual schemas
                            String schemaName = "__glob_" + tableName + "_" + System.currentTimeMillis();
                            schema = rootSchema.add(schemaName, 
                                factory.create(rootSchema, schemaName, operand));
                            
                            // For other formats, use the name from tableConfig
                            String registeredTableName = (String) ((Map)((List)operand.get("tables")).get(0)).get("name");
                            Table table = schema.getTable(registeredTableName);
                            if (table != null) {
                                // Use our derived tableName (with nested path) as the key
                                tables.put(tableName, table);
                            }
                        }
                        
                        if (!tables.containsKey(tableName)) {
                            LOGGER.warn("Table {} not found in schema for file {}", tableName, pathStr);
                        }
                    } catch (Exception e) {
                        LOGGER.warn("Failed to create table for {}: {}", pathStr, e.getMessage());
                    }
                }
                
            } catch (Exception e) {
                throw new RuntimeException("Failed to create table map", e);
            }
            
            return tables;
        }
        
        private List<Path> findMatchingFiles() throws IOException {
            List<Path> matchingFiles = new ArrayList<>();
            
            if (basePath.startsWith("s3://")) {
                // S3 glob support
                matchingFiles.addAll(findS3Files(basePath, globPattern));
            } else {
                // Local file glob support
                Path base = Paths.get(basePath);
                if (!Files.exists(base)) {
                    return matchingFiles;
                }
                
                PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + globPattern);
                
                Files.walkFileTree(base, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        Path relativePath = base.relativize(file);
                        if (matcher.matches(relativePath) || matcher.matches(file.getFileName())) {
                            matchingFiles.add(file);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
            
            return matchingFiles;
        }
        
        private List<Path> findS3Files(String s3Location, String pattern) {
            List<Path> files = new ArrayList<>();
            
            try {
                URI uri = new URI(s3Location);
                String bucket = uri.getHost();
                String prefix = uri.getPath().substring(1); // Remove leading /
                
                S3Client s3 = S3Client.builder()
                    .region(Region.of(System.getProperty("aws.region", "us-east-1")))
                    .build();
                
                ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(prefix)
                    .build();
                
                PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
                
                ListObjectsV2Response response;
                do {
                    response = s3.listObjectsV2(request);
                    
                    for (S3Object object : response.contents()) {
                        Path objectPath = Paths.get(object.key());
                        if (matcher.matches(objectPath.getFileName()) || 
                            matcher.matches(Paths.get(prefix).relativize(objectPath))) {
                            // Create a synthetic path for S3 objects
                            files.add(Paths.get("s3://" + bucket + "/" + object.key()));
                        }
                    }
                    
                    request = request.toBuilder()
                        .continuationToken(response.nextContinuationToken())
                        .build();
                        
                } while (response.isTruncated());
                
                s3.close();
                
            } catch (Exception e) {
                LOGGER.warn("Failed to list S3 files: {}", e.getMessage());
            }
            
            return files;
        }
        
        private String deriveTableName(Path file) {
            String tableName;
            
            // For S3 paths, handle specially
            if (file.toString().startsWith("s3://")) {
                // Extract relative path from S3 URI
                try {
                    URI s3Uri = new URI(file.toString());
                    String fullPath = s3Uri.getPath().substring(1); // Remove leading /
                    Path s3BasePath = Paths.get(new URI(basePath).getPath().substring(1));
                    Path relativePath = s3BasePath.relativize(Paths.get(fullPath));
                    tableName = relativePath.toString();
                } catch (URISyntaxException e) {
                    // Fallback to filename only
                    tableName = file.getFileName().toString();
                }
            } else {
                // For local files, calculate relative path from base
                Path base = Paths.get(basePath);
                Path relativePath = base.relativize(file);
                tableName = relativePath.toString();
            }
            
            // Replace path separators with dots to match nested directory convention
            tableName = tableName.replace('/', '.').replace('\\', '.');
            
            // Remove extension
            int lastDot = tableName.lastIndexOf('.');
            if (lastDot > 0) {
                // Check if this is a file extension (not a directory separator)
                String possibleExt = tableName.substring(lastDot);
                if (possibleExt.matches("\\.[a-zA-Z0-9]+$")) {
                    tableName = tableName.substring(0, lastDot);
                }
            }
            
            // Make it a valid SQL identifier
            return tableName.replaceAll("[^a-zA-Z0-9_.]", "_");
        }
        
        
        private String detectFileFormat(String path) {
            String lowercasePath = path.toLowerCase();
            if (lowercasePath.endsWith(".csv") || lowercasePath.endsWith(".tsv")) {
                return "csv";
            } else if (lowercasePath.endsWith(".json")) {
                return "json";
            } else if (lowercasePath.endsWith(".yaml") || lowercasePath.endsWith(".yml")) {
                return "json"; // YAML is handled as JSON by FileSchemaFactory
            } else if (lowercasePath.endsWith(".parquet")) {
                return "parquet";
            } else if (lowercasePath.endsWith(".arrow") || lowercasePath.endsWith(".feather")) {
                return "arrow";
            } else if (lowercasePath.endsWith(".xlsx") || lowercasePath.endsWith(".xls")) {
                return "excel";
            } else if (lowercasePath.endsWith(".html") || lowercasePath.endsWith(".htm")) {
                return "html";
            }
            return "csv"; // default
        }
        
        private SchemaFactory getSchemaFactoryForFormat(String format) {
            switch (format.toLowerCase()) {
                case "csv":
                case "json":
                case "excel":
                case "html":
                    return FileSchemaFactory.INSTANCE;
                case "arrow":
                case "parquet":
                    // Use ArrowSchemaFactory safely with fallback
                    return createArrowSchemaFactorySafely();
                default:
                    return FileSchemaFactory.INSTANCE;
            }
        }
    }
    
    /**
     * Logs setup metrics for monitoring.
     */
    private void logSetupMetrics(String schemaName, long setupTimeMs, Properties props) {
        String format = props.getProperty("format", "auto");
        
        // Track format usage
        FORMAT_USAGE.computeIfAbsent(format, k -> new AtomicLong(0)).incrementAndGet();
        
        LOGGER.info("Schema setup completed: {} in {}ms (format: {})", 
                    schemaName, setupTimeMs, format);
        
        // Log overall statistics periodically
        long connectionCount = CONNECTION_COUNT.get();
        if (connectionCount % 10 == 0) { // Every 10 connections
            LOGGER.info("Driver statistics - Connections: {}, Schemas: {}, Errors: {}", 
                        connectionCount, SCHEMA_SETUP_COUNT.get(), ERROR_COUNT.get());
        }
    }
    
    /**
     * Gets current driver metrics for monitoring.
     */
    public static Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("connection_count", CONNECTION_COUNT.get());
        metrics.put("schema_setup_count", SCHEMA_SETUP_COUNT.get());
        metrics.put("error_count", ERROR_COUNT.get());
        metrics.put("initialized_schemas", INITIALIZED_SCHEMAS.size());
        
        // Format usage statistics
        Map<String, Long> formatStats = new HashMap<>();
        for (Map.Entry<String, AtomicLong> entry : FORMAT_USAGE.entrySet()) {
            formatStats.put(entry.getKey(), entry.getValue().get());
        }
        metrics.put("format_usage", formatStats);
        
        return metrics;
    }
    
    @Override
    protected DriverVersion createDriverVersion() {
        return new DriverVersion(
            DRIVER_NAME,
            DRIVER_VERSION,
            VENDOR_NAME,
            CALCITE_VERSION,
            true,        // JDBC compliant
            1, 0,        // Driver major/minor version
            1, 41        // Calcite major/minor version
        );
    }
    
    /**
     * Wraps a schema with CasingSchema if casing transformations are needed.
     */
    private static Schema wrapWithCasingIfNeeded(Schema schema, String tableNameCasing, String columnNameCasing) {
        if (needsCasingTransformation(tableNameCasing, columnNameCasing)) {
            return new CasingSchema(schema, tableNameCasing, columnNameCasing);
        }
        return schema;
    }
    
    /**
     * Checks if casing transformation is needed.
     */
    private static boolean needsCasingTransformation(String tableNameCasing, String columnNameCasing) {
        // Check if we need to transform from the defaults (UPPER for tables, UNCHANGED for columns)
        if (tableNameCasing != null && !tableNameCasing.equalsIgnoreCase("UPPER")) {
            return true;
        }
        
        if (columnNameCasing != null && !columnNameCasing.equalsIgnoreCase("UNCHANGED")) {
            return true;
        }
        
        return false;
    }
    
    /**
     * Configures storage provider authentication parameters based on latest file adapter patterns.
     */
    private void configureStorageProviderAuth(Map<String, Object> operand, Properties props) {
        // SharePoint OAuth2 configuration
        if (props.containsKey("tenantId") || props.containsKey("clientId")) {
            Map<String, Object> config = new HashMap<>();
            if (props.containsKey("tenantId")) {
                config.put("tenantId", props.getProperty("tenantId"));
            }
            if (props.containsKey("clientId")) {
                config.put("clientId", props.getProperty("clientId"));
            }
            if (props.containsKey("clientSecret")) {
                config.put("clientSecret", props.getProperty("clientSecret"));
            }
            if (props.containsKey("refreshToken")) {
                config.put("refreshToken", props.getProperty("refreshToken"));
            }
            if (props.containsKey("accessToken")) {
                config.put("accessToken", props.getProperty("accessToken"));
            }
            operand.put("config", config);
        }
        
        // SFTP configuration
        if (props.containsKey("username") || props.containsKey("privateKeyPath")) {
            if (props.containsKey("username")) {
                operand.put("username", props.getProperty("username"));
            }
            if (props.containsKey("password")) {
                operand.put("password", props.getProperty("password"));
            }
            if (props.containsKey("privateKeyPath")) {
                operand.put("privateKeyPath", props.getProperty("privateKeyPath"));
            }
            if (props.containsKey("strictHostKeyChecking")) {
                operand.put("strictHostKeyChecking", 
                    Boolean.parseBoolean(props.getProperty("strictHostKeyChecking", "true")));
            }
        }
        
        // AWS region for S3
        if (props.containsKey("awsRegion")) {
            System.setProperty("aws.region", props.getProperty("awsRegion"));
        }
    }
}