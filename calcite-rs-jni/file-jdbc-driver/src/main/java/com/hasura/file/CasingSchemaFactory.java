package com.hasura.file;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.adapter.file.FileSchemaFactory;

import java.util.Map;

/**
 * Schema factory that wraps FileSchemaFactory and applies table/column name casing transformations.
 */
public class CasingSchemaFactory implements SchemaFactory {
    
    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        // Extract casing options from operand
        String tableNameCasing = (String) operand.get("table_name_casing");
        String columnNameCasing = (String) operand.get("column_name_casing");
        
        // Remove casing options from operand before passing to FileSchemaFactory
        operand.remove("table_name_casing");
        operand.remove("column_name_casing");
        
        // Create the underlying file schema
        Schema fileSchema = FileSchemaFactory.INSTANCE.create(parentSchema, name, operand);
        
        // Wrap with casing transformation if needed
        if (needsCasingTransformation(tableNameCasing, columnNameCasing)) {
            return new CasingSchema(fileSchema, tableNameCasing, columnNameCasing);
        }
        
        return fileSchema;
    }
    
    private boolean needsCasingTransformation(String tableNameCasing, String columnNameCasing) {
        // Only wrap if we need to transform from the default
        if (tableNameCasing != null && !tableNameCasing.equalsIgnoreCase("UNCHANGED")) {
            return true;
        }
        
        if (columnNameCasing != null && !columnNameCasing.equalsIgnoreCase("UNCHANGED")) {
            return true;
        }
        
        return false;
    }
}