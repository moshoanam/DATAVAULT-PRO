package com.datavault.adapter;

import com.datavault.dto.*;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Database Adapter Interface
 * Supports PostgreSQL, MS SQL, MongoDB, and Iceberg
 */
public interface DatabaseAdapter {
    
    /**
     * Get adapter type (PostgreSQL, MSSQL, MongoDB, Iceberg)
     */
    String getAdapterType();
    
    /**
     * Test connection to database
     */
    boolean testConnection(String connectionString, String username, String password);
    
    /**
     * Extract all schemas from database
     */
    List<String> extractSchemas(Connection connection);
    
    /**
     * Extract all tables from a schema
     */
    List<TableMetadataDTO> extractTables(Connection connection, String schema);
    
    /**
     * Extract all fields from a table
     */
    List<FieldMetadataDTO> extractFields(Connection connection, String schema, String tableName);
    
    /**
     * Extract foreign key relationships
     */
    List<RelationshipDTO> extractRelationships(Connection connection, String schema);
    
    /**
     * Extract table statistics (row count, size)
     */
    Map<String, Object> extractTableStatistics(Connection connection, String schema, String tableName);
    
    /**
     * Analyze field for PII detection
     */
    boolean isPII(String fieldName, String dataType);
    
    /**
     * Build lineage from foreign keys and views
     */
    List<LineageRelationshipDTO> buildLineage(Connection connection, String schema);
    
    /**
     * Calculate data quality metrics
     */
    DataQualityDTO calculateQuality(Connection connection, String schema, String tableName);
    
    /**
     * Get connection from connection string
     */
    Connection getConnection(String connectionString, String username, String password) throws Exception;
}
