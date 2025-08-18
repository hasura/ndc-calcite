package com.hasura.file;

import org.apache.calcite.adapter.arrow.ArrowSchemaFactory;
import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around ArrowSchemaFactory that gracefully handles Gandiva/Z3 
 * native library dependencies by falling back to FileSchemaFactory.
 */
public class SafeArrowSchemaFactory implements SchemaFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(SafeArrowSchemaFactory.class);
    
    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        // First, check if Z3 library is available by trying to create a test Projector
        if (isGandivaAvailable()) {
            try {
                LOGGER.info("Z3/Gandiva libraries detected, using ArrowSchemaFactory for optimal performance");
                ArrowSchemaFactory arrowFactory = new ArrowSchemaFactory();
                return arrowFactory.create(parentSchema, name, operand);
            } catch (Exception e) {
                LOGGER.warn("ArrowSchemaFactory failed despite Z3 being available, falling back to FileSchemaFactory: {}", e.getMessage());
            }
        } else {
            LOGGER.info("Z3/Gandiva libraries not available, using FileSchemaFactory for basic Arrow support. " +
                "Install Z3 library (brew install z3) for optimal Arrow performance with Gandiva.");
        }
        
        // Fall back to FileSchemaFactory
        FileSchemaFactory fileFactory = FileSchemaFactory.INSTANCE;
        return fileFactory.create(parentSchema, name, operand);
    }
    
    /**
     * Check if Gandiva/Z3 libraries are available by attempting to access
     * Gandiva functionality that would trigger the UnsatisfiedLinkError.
     */
    private boolean isGandivaAvailable() {
        try {
            // Try to access a Gandiva class that would trigger library loading
            Class.forName("org.apache.arrow.gandiva.evaluator.JniLoader");
            
            // Try to get default configuration which triggers native library loading
            Class<?> jniLoaderClass = Class.forName("org.apache.arrow.gandiva.evaluator.JniLoader");
            java.lang.reflect.Method getInstance = jniLoaderClass.getMethod("getInstance");
            getInstance.invoke(null);
            return true;
        } catch (ClassNotFoundException e) {
            LOGGER.debug("Gandiva classes not found: {}", e.getMessage());
            return false;
        } catch (UnsatisfiedLinkError e) {
            if (e.getMessage().contains("gandiva") || e.getMessage().contains("z3")) {
                LOGGER.debug("Gandiva/Z3 native libraries not available: {}", e.getMessage());
                return false;
            } else {
                // Re-throw if it's a different linking issue
                throw e;
            }
        } catch (ExceptionInInitializerError e) {
            if (e.getCause() instanceof UnsatisfiedLinkError) {
                UnsatisfiedLinkError linkError = (UnsatisfiedLinkError) e.getCause();
                if (linkError.getMessage().contains("gandiva") || linkError.getMessage().contains("z3")) {
                    LOGGER.debug("Gandiva/Z3 native libraries not available during initialization: {}", linkError.getMessage());
                    return false;
                } else {
                    throw e;
                }
            } else {
                throw e;
            }
        } catch (Exception e) {
            LOGGER.debug("Gandiva availability check failed: {}", e.getMessage());
            return false;
        }
    }
}