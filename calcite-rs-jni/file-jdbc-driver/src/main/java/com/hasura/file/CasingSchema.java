package com.hasura.file;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * A schema wrapper that applies table and column name casing transformations.
 */
public class CasingSchema implements Schema {
    private final Schema delegate;
    private final String tableNameCasing;
    private final String columnNameCasing;
    private final Map<String, String> tableNameMapping;
    private final Map<String, String> reverseTableNameMapping;
    
    public CasingSchema(Schema delegate, String tableNameCasing, String columnNameCasing) {
        this.delegate = delegate;
        this.tableNameCasing = tableNameCasing != null ? tableNameCasing : "UPPER";
        this.columnNameCasing = columnNameCasing != null ? columnNameCasing : "UNCHANGED";
        this.tableNameMapping = new HashMap<>();
        this.reverseTableNameMapping = new HashMap<>();
        
        // Build table name mappings
        for (String originalName : delegate.getTableNames()) {
            String transformedName = transformCase(originalName, this.tableNameCasing);
            tableNameMapping.put(transformedName, originalName);
            reverseTableNameMapping.put(originalName, transformedName);
        }
    }
    
    private String transformCase(String name, String casing) {
        if (name == null) return null;
        
        switch (casing.toUpperCase()) {
            case "UPPER":
                return name.toUpperCase();
            case "LOWER":
                return name.toLowerCase();
            case "UNCHANGED":
            default:
                return name;
        }
    }
    
    @Override
    public Table getTable(String name) {
        // Map the requested name back to the original name
        String originalName = tableNameMapping.get(name);
        if (originalName == null) {
            // Try case-insensitive lookup
            for (Map.Entry<String, String> entry : tableNameMapping.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(name)) {
                    originalName = entry.getValue();
                    break;
                }
            }
        }
        
        if (originalName == null) {
            return null;
        }
        
        Table originalTable = delegate.getTable(originalName);
        if (originalTable == null) {
            return null;
        }
        
        // Wrap the table to transform column names
        return new CasingTable(originalTable, columnNameCasing);
    }
    
    @Override
    public Set<String> getTableNames() {
        // Return the transformed table names
        return tableNameMapping.keySet();
    }
    
    @Override
    public Schema getSubSchema(String name) {
        Schema subSchema = delegate.getSubSchema(name);
        if (subSchema == null) {
            return null;
        }
        // Wrap sub-schemas with the same casing rules
        return new CasingSchema(subSchema, tableNameCasing, columnNameCasing);
    }
    
    @Override
    public Set<String> getSubSchemaNames() {
        return delegate.getSubSchemaNames();
    }
    
    @Override
    public Collection<Function> getFunctions(String name) {
        return delegate.getFunctions(name);
    }
    
    @Override
    public Set<String> getFunctionNames() {
        return delegate.getFunctionNames();
    }
    
    @Override
    public Expression getExpression(SchemaPlus parentSchema, String name) {
        return delegate.getExpression(parentSchema, name);
    }
    
    @Override
    public boolean isMutable() {
        return delegate.isMutable();
    }
    
    @Override
    public Schema snapshot(SchemaVersion version) {
        return new CasingSchema(delegate.snapshot(version), tableNameCasing, columnNameCasing);
    }
    
    @Override
    public Set<String> getTypeNames() {
        return delegate.getTypeNames();
    }
    
    @Override
    public org.apache.calcite.rel.type.RelProtoDataType getType(String name) {
        return delegate.getType(name);
    }
    
    /**
     * Table wrapper that transforms column names according to casing rules.
     */
    private static class CasingTable implements Table {
        private final Table delegate;
        private final String columnNameCasing;
        
        public CasingTable(Table delegate, String columnNameCasing) {
            this.delegate = delegate;
            this.columnNameCasing = columnNameCasing;
        }
        
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            RelDataType originalType = delegate.getRowType(typeFactory);
            
            // Transform field names according to casing rules
            RelDataTypeFactory.Builder builder = typeFactory.builder();
            for (RelDataTypeField field : originalType.getFieldList()) {
                String transformedName = transformCase(field.getName(), columnNameCasing);
                builder.add(transformedName, field.getType());
            }
            
            return builder.build();
        }
        
        private String transformCase(String name, String casing) {
            if (name == null) return null;
            
            switch (casing.toUpperCase()) {
                case "UPPER":
                    return name.toUpperCase();
                case "LOWER":
                    return name.toLowerCase();
                case "UNCHANGED":
                default:
                    return name;
            }
        }
        
        @Override
        public Statistic getStatistic() {
            return delegate.getStatistic();
        }
        
        @Override
        public Schema.TableType getJdbcTableType() {
            return delegate.getJdbcTableType();
        }
        
        @Override
        public boolean isRolledUp(String column) {
            return delegate.isRolledUp(column);
        }
        
        @Override
        public boolean rolledUpColumnValidInsideAgg(String column, org.apache.calcite.sql.SqlCall call,
                                                    org.apache.calcite.sql.SqlNode parent, org.apache.calcite.config.CalciteConnectionConfig config) {
            return delegate.rolledUpColumnValidInsideAgg(column, call, parent, config);
        }
    }
}