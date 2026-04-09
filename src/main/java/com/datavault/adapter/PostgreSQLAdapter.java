package com.datavault.adapter;

import com.datavault.dto.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.*;

@Slf4j
@Component("postgresqlAdapter")
public class PostgreSQLAdapter implements DatabaseAdapter {
    
    private static final Set<String> PII_KEYWORDS = Set.of(
        "email", "phone", "ssn", "social", "passport", "license",
        "credit", "card", "account", "password", "birth", "dob",
        "name", "address", "zip", "postal"
    );

    @Override
    public String getAdapterType() {
        return "PostgreSQL";
    }

    @Override
    public boolean testConnection(String connectionString, String username, String password) {
        try (Connection conn = DriverManager.getConnection(connectionString, username, password)) {
            return conn.isValid(5);
        } catch (Exception e) {
            log.error("Connection test failed: {}", e.getMessage());
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
        String query = "SELECT schema_name FROM information_schema.schemata " +
                      "WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                schemas.add(rs.getString("schema_name"));
            }
        } catch (SQLException e) {
            log.error("Error extracting schemas: {}", e.getMessage());
        }
        return schemas;
    }

    @Override
    public List<TableMetadataDTO> extractTables(Connection connection, String schema) {
        List<TableMetadataDTO> tables = new ArrayList<>();
        String query = "SELECT table_name, " +
                      "(SELECT obj_description(c.oid) FROM pg_class c " +
                      " JOIN pg_namespace n ON n.oid = c.relnamespace " +
                      " WHERE c.relname = t.table_name AND n.nspname = t.table_schema) as table_comment " +
                      "FROM information_schema.tables t " +
                      "WHERE table_schema = ? AND table_type = 'BASE TABLE'";
        
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, schema);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    TableMetadataDTO table = TableMetadataDTO.builder()
                        .schema(schema)
                        .tableName(rs.getString("table_name"))
                        .description(rs.getString("table_comment"))
                        .build();
                    
                    // Get row count and size
                    Map<String, Object> stats = extractTableStatistics(connection, schema, table.getTableName());
                    table.setRowCount((Long) stats.get("rowCount"));
                    table.setSizeBytes((Long) stats.get("sizeBytes"));
                    
                    tables.add(table);
                }
            }
        } catch (SQLException e) {
            log.error("Error extracting tables: {}", e.getMessage());
        }
        return tables;
    }

    @Override
    public List<FieldMetadataDTO> extractFields(Connection connection, String schema, String tableName) {
        List<FieldMetadataDTO> fields = new ArrayList<>();
        String query = "SELECT " +
                      "c.column_name, c.data_type, c.character_maximum_length, " +
                      "c.is_nullable, c.column_default, " +
                      "pg_catalog.col_description(pgc.oid, c.ordinal_position::int) as column_comment, " +
                      "CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key, " +
                      "CASE WHEN fk.column_name IS NOT NULL THEN true ELSE false END as is_foreign_key " +
                      "FROM information_schema.columns c " +
                      "JOIN pg_class pgc ON pgc.relname = c.table_name " +
                      "JOIN pg_namespace pgn ON pgn.oid = pgc.relnamespace AND pgn.nspname = c.table_schema " +
                      "LEFT JOIN (SELECT ku.column_name FROM information_schema.table_constraints tc " +
                      "  JOIN information_schema.key_column_usage ku ON tc.constraint_name = ku.constraint_name " +
                      "  WHERE tc.table_schema = ? AND tc.table_name = ? AND tc.constraint_type = 'PRIMARY KEY') pk " +
                      "  ON c.column_name = pk.column_name " +
                      "LEFT JOIN (SELECT ku.column_name FROM information_schema.table_constraints tc " +
                      "  JOIN information_schema.key_column_usage ku ON tc.constraint_name = ku.constraint_name " +
                      "  WHERE tc.table_schema = ? AND tc.table_name = ? AND tc.constraint_type = 'FOREIGN KEY') fk " +
                      "  ON c.column_name = fk.column_name " +
                      "WHERE c.table_schema = ? AND c.table_name = ? " +
                      "ORDER BY c.ordinal_position";
        
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, schema);
            stmt.setString(2, tableName);
            stmt.setString(3, schema);
            stmt.setString(4, tableName);
            stmt.setString(5, schema);
            stmt.setString(6, tableName);
            
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
                        .isNullable("YES".equals(rs.getString("is_nullable")))
                        .defaultValue(rs.getString("column_default"))
                        .maxLength(rs.getInt("character_maximum_length"))
                        .sensitivityLevel(isPII(fieldName, dataType) ? "PII" : "INTERNAL")
                        .build();
                    
                    fields.add(field);
                }
            }
        } catch (SQLException e) {
            log.error("Error extracting fields: {}", e.getMessage());
        }
        return fields;
    }

    @Override
    public List<RelationshipDTO> extractRelationships(Connection connection, String schema) {
        List<RelationshipDTO> relationships = new ArrayList<>();
        String query = "SELECT " +
                      "tc.table_name as source_table, " +
                      "kcu.column_name as source_column, " +
                      "ccu.table_name as target_table, " +
                      "ccu.column_name as target_column, " +
                      "tc.constraint_name " +
                      "FROM information_schema.table_constraints tc " +
                      "JOIN information_schema.key_column_usage kcu " +
                      "  ON tc.constraint_name = kcu.constraint_name " +
                      "JOIN information_schema.constraint_column_usage ccu " +
                      "  ON ccu.constraint_name = tc.constraint_name " +
                      "WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = ?";
        
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
            log.error("Error extracting relationships: {}", e.getMessage());
        }
        return relationships;
    }

    @Override
    public Map<String, Object> extractTableStatistics(Connection connection, String schema, String tableName) {
        Map<String, Object> stats = new HashMap<>();
        
        // Get row count — use double-quoted identifiers to prevent SQL injection
        String countQuery = "SELECT COUNT(*) as row_count FROM \"" +
            schema.replace("\"", "\"\"") + "\".\"" +
            tableName.replace("\"", "\"\"") + "\"";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(countQuery)) {
            if (rs.next()) {
                stats.put("rowCount", rs.getLong("row_count"));
            }
        } catch (SQLException e) {
            stats.put("rowCount", 0L);
        }
        
        // Get table size
        String sizeQuery = "SELECT pg_total_relation_size(?) as size";
        try (PreparedStatement stmt = connection.prepareStatement(sizeQuery)) {
            stmt.setString(1, schema + "." + tableName);
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
            
            // Calculate completeness
            long fieldsWithDescription = fields.stream()
                .filter(f -> f.getDescription() != null && !f.getDescription().isEmpty())
                .count();
            double completeness = fields.isEmpty() ? 0 : (fieldsWithDescription * 100.0) / fields.size();
            quality.setCompleteness(completeness);
            
            // Calculate validity (fields with constraints)
            long fieldsWithConstraints = fields.stream()
                .filter(f -> f.getIsPrimaryKey() || f.getIsForeignKey() || !f.getIsNullable())
                .count();
            double validity = fields.isEmpty() ? 0 : (fieldsWithConstraints * 100.0) / fields.size();
            quality.setValidity(validity);
            
            // Overall score
            double overall = (completeness + validity) / 2;
            quality.setOverallScore(overall);
            
        } catch (Exception e) {
            log.error("Error calculating quality: {}", e.getMessage());
        }
        
        return quality;
    }

    private String formatBusinessName(String fieldName) {
        return Arrays.stream(fieldName.split("_"))
            .map(word -> word.substring(0, 1).toUpperCase() + word.substring(1))
            .reduce((a, b) -> a + " " + b)
            .orElse(fieldName);
    }
}
