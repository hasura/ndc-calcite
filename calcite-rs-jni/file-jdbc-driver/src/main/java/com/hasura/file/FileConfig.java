package com.hasura.file;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.ArrayList;

/**
 * Configuration class for File JDBC Driver.
 * Supports both JSON and YAML formats.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FileConfig {
    
    // Global configuration
    private String version = "1.0";
    private String name;
    private String description;
    private String defaultSchema = "files";
    
    // Calcite schemas array (follows model.json format)
    private List<Map<String, Object>> schemas;
    
    // Global performance settings (applied to FileSchemaFactory operands)
    private String executionEngine; // parquet, arrow, vectorized, linq4j  
    private Integer batchSize = 2048; // Default matches FileSchemaFactory
    private Long memoryThreshold = 67108864L; // 64MB - matches FileSchemaFactory
    private String refreshInterval = "-1"; // Never by default
    
    // AWS settings (for S3)
    private String awsRegion = "us-east-1";
    
    // Table and column name casing options
    private String tableNameCasing; // upper, lower, unchanged - defaults to lower
    private String columnNameCasing; // upper, lower, unchanged - defaults to lower
    
    // Getters and setters
    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }
    
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    
    public String getDefaultSchema() { return defaultSchema; }
    public void setDefaultSchema(String defaultSchema) { this.defaultSchema = defaultSchema; }
    
    public List<Map<String, Object>> getSchemas() { return schemas; }
    public void setSchemas(List<Map<String, Object>> schemas) { this.schemas = schemas; }
    
    // Global performance getters/setters
    public String getExecutionEngine() { return executionEngine; }
    public void setExecutionEngine(String executionEngine) { this.executionEngine = executionEngine; }
    
    public Integer getBatchSize() { return batchSize; }
    public void setBatchSize(Integer batchSize) { this.batchSize = batchSize; }
    
    public Long getMemoryThreshold() { return memoryThreshold; }
    public void setMemoryThreshold(Long memoryThreshold) { this.memoryThreshold = memoryThreshold; }
    
    public String getRefreshInterval() { return refreshInterval; }
    public void setRefreshInterval(String refreshInterval) { this.refreshInterval = refreshInterval; }
    
    // AWS getters/setters
    public String getAwsRegion() { return awsRegion; }
    public void setAwsRegion(String awsRegion) { this.awsRegion = awsRegion; }
    
    // Table and column name casing getters/setters
    public String getTableNameCasing() { return tableNameCasing; }
    public void setTableNameCasing(String tableNameCasing) { this.tableNameCasing = tableNameCasing; }
    
    public String getColumnNameCasing() { return columnNameCasing; }
    public void setColumnNameCasing(String columnNameCasing) { this.columnNameCasing = columnNameCasing; }
    
    /**
     * Load configuration from file or URL
     */
    public static FileConfig load(String location) throws IOException {
        ObjectMapper mapper;
        
        if (location.endsWith(".yaml") || location.endsWith(".yml")) {
            mapper = new ObjectMapper(new YAMLFactory());
        } else {
            mapper = new ObjectMapper();
        }
        
        if (location.startsWith("http://") || location.startsWith("https://") || 
            location.startsWith("s3://")) {
            return mapper.readValue(new URL(location), FileConfig.class);
        } else {
            File file = new File(location);
            if (!file.isAbsolute()) {
                file = new File(System.getProperty("user.dir"), location);
            }
            return mapper.readValue(file, FileConfig.class);
        }
    }
    
}