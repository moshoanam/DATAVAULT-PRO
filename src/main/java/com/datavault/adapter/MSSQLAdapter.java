package com.datavault.adapter;

import com.datavault.dto.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.*;

@Slf4j
@Component("mssqlAdapter")
public class MSSQLAdapter implements DatabaseAdapter {
    
    private static final Set<String> PII_KEYWORDS = Set.of(
        "email", "phone", "ssn", "social", "passport", "license",
        "credit", "card", "account", "password", "birth", "dob",
        "name", "address", "zip", "postal"
    );

    @Override
    public String getAdapterType() {
        return "MSSQL";
    }

    @Override
    public boolean testConnection(String connectionString, String username, String password) {
        try (Connection conn = DriverManager.getConnection(connectionString, username, password)) {
            return conn.isValid(5);
        } catch (Exception e) {
            log.error("MS SQL connection test failed: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public Connection getConnection(String connectionString, String username, String password) throws Exception {
        return DriverManager.getConnection(connectionString, username, password);
    }

    @Override
    public List<String> extractSchemas(Connection connection) {
        List<String> schemas = new ArrayList<>();
        String query = "SELECT name FROM sys.schemas " +
                      "WHERE name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest', 'db_owner', 'db_accessadmin', " +
                      "'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_denydatareader', 'db_denydatawriter')";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                schemas.add(rs.getString("name"));
            }
        } catch (SQLException e) {
            log.error("Error extracting MS SQL schemas: {}", e.getMessage());
        }
        return schemas;
    }

    @Override
    public List<TableMetadataDTO> extractTables(Connection connection, String schema) {
        List<TableMetadataDTO> tables = new ArrayList<>();
        String query = "SELECT t.name as table_name, " +
                      "ep.value as table_comment " +
                      "FROM sys.tables t " +
                      "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id " +
                      "LEFT JOIN sys.extended_properties ep ON ep.major_id = t.object_id AND ep.minor_id = 0 AND ep.name = 'MS_Description' " +
                      "WHERE s.name = ?";
        
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, schema);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("table_name");
                    TableMetadataDTO table = TableMetadataDTO.builder()
                        .schema(schema)
                        .tableName(tableName)
                        .description(rs.getString("table_comment"))
                        .build();
                    
                    Map<String, Object> stats = extractTableStatistics(connection, schema, tableName);
                    table.setRowCount((Long) stats.get("rowCount"));
                    table.setSizeBytes((Long) stats.get("sizeBytes"));
                    
                    tables.add(table);
                }
            }
        } catch (SQLException e) {
            log.error("Error extracting MS SQL tables: {}", e.getMessage());
        }
        return tables;
    }

    @Override
    public List<FieldMetadataDTO> extractFields(Connection connection, String schema, String tableName) {
        List<FieldMetadataDTO> fields = new ArrayList<>();
        String query = "SELECT " +
                      "c.name as column_name, " +
                      "t.name as data_type, " +
                      "c.max_length, " +
                      "c.is_nullable, " +
                      "dc.definition as default_value, " +
                      "ep.value as column_comment, " +
                      "CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END as is_primary_key, " +
                      "CASE WHEN fk.parent_column_id IS NOT NULL THEN 1 ELSE 0 END as is_foreign_key " +
                      "FROM sys.columns c " +
                      "INNER JOIN sys.types t ON c.user_type_id = t.user_type_id " +
                      "INNER JOIN sys.tables tb ON c.object_id = tb.object_id " +
                      "INNER JOIN sys.schemas s ON tb.schema_id = s.schema_id " +
                      "LEFT JOIN sys.extended_properties ep ON ep.major_id = c.object_id AND ep.minor_id = c.column_id AND ep.name = 'MS_Description' " +
                      "LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id " +
                      "LEFT JOIN (SELECT ic.object_id, ic.column_id FROM sys.index_columns ic " +
                      "  INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id " +
                      "  WHERE i.is_primary_key = 1) pk ON pk.object_id = c.object_id AND pk.column_id = c.column_id " +
                      "LEFT JOIN sys.foreign_key_columns fk ON fk.parent_object_id = c.object_id AND fk.parent_column_id = c.column_id " +
                      "WHERE s.name = ? AND tb.name = ? " +
                      "ORDER BY c.column_id";
        
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, schema);
            stmt.setString(2, tableName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String fieldName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    
                    FieldMetadataDTO field = FieldMetadataDTO.builder()
                        .fieldName(fieldName)
                        .businessName(formatBusinessName(fieldName))
                        .dataType(dataType)
                        .description(rs.getString("column_comment"))
                        .isPrimaryKey(rs.getBoolean("is_primary_key"))
                        .isForeignKey(rs.getBoolean("is_foreign_key"))
                        .isNullable(rs.getBoolean("is_nullable"))
                        .defaultValue(rs.getString("default_value"))
                        .maxLength(rs.getInt("max_length"))
                        .sensitivityLevel(isPII(fieldName, dataType) ? "PII" : "INTERNAL")
                        .build();
                    
                    fields.add(field);
                }
            }
        } catch (SQLException e) {
            log.error("Error extracting MS SQL fields: {}", e.getMessage());
        }
        return fields;
    }

    @Override
    public List<RelationshipDTO> extractRelationships(Connection connection, String schema) {
        List<RelationshipDTO> relationships = new ArrayList<>();
        String query = "SELECT " +
                      "fk.name as constraint_name, " +
                      "OBJECT_NAME(fk.parent_object_id) as source_table, " +
                      "COL_NAME(fkc.parent_object_id, fkc.parent_column_id) as source_column, " +
                      "OBJECT_NAME(fk.referenced_object_id) as target_table, " +
                      "COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) as target_column " +
                      "FROM sys.foreign_keys fk " +
                      "INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id " +
                      "INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id " +
                      "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id " +
                      "WHERE s.name = ?";
        
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, schema);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    RelationshipDTO rel = RelationshipDTO.builder()
                        .sourceTable(rs.getString("source_table"))
                        .sourceColumn(rs.getString("source_column"))
                        .targetTable(rs.getString("target_table"))
                        .targetColumn(rs.getString("target_column"))
                        .relationshipType("FOREIGN_KEY")
                        .constraintName(rs.getString("constraint_name"))
                        .build();
                    relationships.add(rel);
                }
            }
        } catch (SQLException e) {
            log.error("Error extracting MS SQL relationships: {}", e.getMessage());
        }
        return relationships;
    }

    @Override
    public Map<String, Object> extractTableStatistics(Connection connection, String schema, String tableName) {
        Map<String, Object> stats = new HashMap<>();
        
        // Row count
        String countQuery = "SELECT SUM(p.rows) as row_count " +
                           "FROM sys.partitions p " +
                           "INNER JOIN sys.tables t ON p.object_id = t.object_id " +
                           "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id " +
                           "WHERE s.name = ? AND t.name = ? AND p.index_id IN (0,1)";
        try (PreparedStatement stmt = connection.prepareStatement(countQuery)) {
            stmt.setString(1, schema);
            stmt.setString(2, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    stats.put("rowCount", rs.getLong("row_count"));
                }
            }
        } catch (SQLException e) {
            stats.put("rowCount", 0L);
        }
        
        // Table size
        String sizeQuery = "SELECT SUM(a.total_pages) * 8 * 1024 as size " +
                          "FROM sys.tables t " +
                          "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id " +
                          "INNER JOIN sys.indexes i ON t.object_id = i.object_id " +
                          "INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id " +
                          "INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id " +
                          "WHERE s.name = ? AND t.name = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sizeQuery)) {
            stmt.setString(1, schema);
            stmt.setString(2, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    stats.put("sizeBytes", rs.getLong("size"));
                }
            }
        } catch (SQLException e) {
            stats.put("sizeBytes", 0L);
        }
        
        return stats;
    }

    @Override
    public boolean isPII(String fieldName, String dataType) {
        String lowerFieldName = fieldName.toLowerCase();
        return PII_KEYWORDS.stream().anyMatch(lowerFieldName::contains);
    }

    @Override
    public List<LineageRelationshipDTO> buildLineage(Connection connection, String schema) {
        List<LineageRelationshipDTO> lineage = new ArrayList<>();
        List<RelationshipDTO> relationships = extractRelationships(connection, schema);
        
        for (RelationshipDTO rel : relationships) {
            LineageRelationshipDTO lin = LineageRelationshipDTO.builder()
                .sourceFieldName(rel.getSourceTable() + "." + rel.getSourceColumn())
                .targetFieldName(rel.getTargetTable() + "." + rel.getTargetColumn())
                .lineageType("DIRECT")
                .transformationLogic("Foreign Key Relationship")
                .build();
            lineage.add(lin);
        }
        
        return lineage;
    }

    @Override
    public DataQualityDTO calculateQuality(Connection connection, String schema, String tableName) {
        DataQualityDTO quality = DataQualityDTO.builder()
            .tableName(tableName)
            .build();
        
        try {
            List<FieldMetadataDTO> fields = extractFields(connection, schema, tableName);
            
            long fieldsWithDescription = fields.stream()
                .filter(f -> f.getDescription() != null && !f.getDescription().isEmpty())
                .count();
            double completeness = fields.isEmpty() ? 0 : (fieldsWithDescription * 100.0) / fields.size();
            quality.setCompleteness(completeness);
            
            long fieldsWithConstraints = fields.stream()
                .filter(f -> f.getIsPrimaryKey() || f.getIsForeignKey() || !f.getIsNullable())
                .count();
            double validity = fields.isEmpty() ? 0 : (fieldsWithConstraints * 100.0) / fields.size();
            quality.setValidity(validity);
            
            double overall = (completeness + validity) / 2;
            quality.setOverallScore(overall);
            
        } catch (Exception e) {
            log.error("Error calculating MS SQL quality: {}", e.getMessage());
        }
        
        return quality;
    }

    private String formatBusinessName(String fieldName) {
        return Arrays.stream(fieldName.split("_"))
            .map(word -> word.substring(0, 1).toUpperCase() + word.substring(1).toLowerCase())
            .reduce((a, b) -> a + " " + b)
            .orElse(fieldName);
    }
}
