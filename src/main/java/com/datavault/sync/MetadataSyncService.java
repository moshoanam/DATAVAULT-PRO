package com.datavault.sync;

import com.datavault.entity.*;
import com.datavault.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Automated Metadata Synchronization Service
 * Extracts metadata from AWS PostgreSQL and syncs to catalog
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class MetadataSyncService {

    private final DatabaseRepository databaseRepository;
    private final TableMetadataRepository tableRepository;
    private final FieldMetadataRepository fieldRepository;
    private final LineageRelationshipRepository lineageRepository;
    private final ChangeHistoryRepository changeHistoryRepository;
    private final GovernanceAlertRepository alertRepository;

    @Value("${app.metadata-sync.enabled:true}")
    private boolean syncEnabled;

    // PII Detection Patterns
    private static final Map<String, Pattern> PII_PATTERNS = Map.of(
        "email", Pattern.compile(".*email.*", Pattern.CASE_INSENSITIVE),
        "phone", Pattern.compile(".*(phone|mobile|tel).*", Pattern.CASE_INSENSITIVE),
        "ssn", Pattern.compile(".*(ssn|social.security).*", Pattern.CASE_INSENSITIVE),
        "name", Pattern.compile(".*(first.name|last.name|full.name).*", Pattern.CASE_INSENSITIVE),
        "address", Pattern.compile(".*(address|street|city|zip|postal).*", Pattern.CASE_INSENSITIVE),
        "dob", Pattern.compile(".*(birth.date|dob|date.of.birth).*", Pattern.CASE_INSENSITIVE),
        "credit_card", Pattern.compile(".*(credit.card|cc.number|card.number).*", Pattern.CASE_INSENSITIVE)
    );

    /**
     * Scheduled sync job - runs every 6 hours
     */
    @Scheduled(cron = "${app.metadata-sync.cron:0 0 */6 * * *}")
    @Transactional
    public void scheduledSync() {
        if (!syncEnabled) {
            log.info("Metadata sync is disabled");
            return;
        }

        log.info("Starting scheduled metadata synchronization");
        
        try {
            List<Database> databases = databaseRepository.findAll();
            
            for (Database database : databases) {
                if (database.getConnectionString() != null) {
                    syncDatabase(database);
                }
            }
            
            log.info("Metadata synchronization completed successfully");
        } catch (Exception e) {
            log.error("Error during metadata synchronization", e);
        }
    }

    /**
     * Sync metadata from a specific database
     */
    @Transactional
    public void syncDatabase(Database database) {
        log.info("Syncing metadata for database: {}", database.getName());

        try (Connection conn = getConnection(database)) {
            // Extract tables
            List<TableInfo> tables = extractTables(conn, database);
            
            for (TableInfo tableInfo : tables) {
                syncTable(database, tableInfo, conn);
            }
            
            // Detect relationships
            detectRelationships(conn, database);
            
            // Calculate quality scores
            calculateQualityScores(database);
            
        } catch (SQLException e) {
            log.error("Error syncing database: {}", database.getName(), e);
            createAlert("SYNC_FAILED", database, "Failed to sync metadata: " + e.getMessage());
        }
    }

    @Transactional
    protected void syncTable(Database database, TableInfo tableInfo, Connection conn) {
        log.debug("Syncing table: {}.{}", tableInfo.schema, tableInfo.tableName);

        // Find or create table
        TableMetadata table = tableRepository
            .findByDatabaseAndSchemaAndTable(database.getId(), tableInfo.schema, tableInfo.tableName)
            .orElseGet(() -> createNewTable(database, tableInfo));

        // Update table metadata
        table.setRowCount(tableInfo.rowCount);
        table.setSizeBytes(tableInfo.sizeBytes);
        table.setDescription(tableInfo.comment);
        table.setLastModifiedBy("metadata-sync");
        
        table = tableRepository.save(table);

        // Extract and sync fields
        try {
            List<FieldInfo> fields = extractFields(conn, tableInfo.schema, tableInfo.tableName);
            syncFields(table, fields);
        } catch (SQLException e) {
            log.error("Error syncing fields for table: {}", tableInfo.tableName, e);
        }
    }

    @Transactional
    protected void syncFields(TableMetadata table, List<FieldInfo> fieldInfos) {
        Set<String> currentFields = new HashSet<>();

        for (FieldInfo fieldInfo : fieldInfos) {
            currentFields.add(fieldInfo.fieldName);

            Optional<FieldMetadata> existing = table.getFields().stream()
                .filter(f -> f.getFieldName().equals(fieldInfo.fieldName))
                .findFirst();

            FieldMetadata field;
            if (existing.isPresent()) {
                field = existing.get();
                // Track changes
                if (!field.getDataType().equals(fieldInfo.dataType)) {
                    trackChange(field, "UPDATED",
                        String.format("Data type changed from %s to %s",
                            field.getDataType(), fieldInfo.dataType));
                }
            } else {
                field = createNewField(table, fieldInfo);
                trackChange(field, "FIELD_ADDED", "New field added to table");
            }

            // Update field metadata
            field.setDataType(fieldInfo.dataType);
            field.setIsPrimaryKey(fieldInfo.isPrimaryKey);
            field.setIsForeignKey(fieldInfo.isForeignKey);
            field.setIsNullable(fieldInfo.isNullable);
            field.setDefaultValue(fieldInfo.defaultValue);
            field.setMaxLength(fieldInfo.maxLength);
            field.setDescription(fieldInfo.comment);
            
            // Auto-detect PII
            if (field.getSensitivityLevel() == null) {
                field.setSensitivityLevel(detectPII(fieldInfo.fieldName));
                if (field.getSensitivityLevel() == SensitivityLevel.PII) {
                    createAlert("PII_DETECTED", table, 
                        "PII field detected: " + fieldInfo.fieldName);
                }
            }

            // Auto-generate business name if missing
            if (field.getBusinessName() == null) {
                field.setBusinessName(generateBusinessName(fieldInfo.fieldName));
            }

            field.setLastModifiedBy("metadata-sync");
            fieldRepository.save(field);
        }

        // Detect removed fields
        List<FieldMetadata> removedFields = table.getFields().stream()
            .filter(f -> !currentFields.contains(f.getFieldName()))
            .toList();

        for (FieldMetadata removed : removedFields) {
            trackChange(removed, "FIELD_REMOVED", "Field removed from table");
            // Don't delete - mark as deprecated instead
        }
    }

    /**
     * Extract tables from database
     */
    private List<TableInfo> extractTables(Connection conn, Database database) throws SQLException {
        List<TableInfo> tables = new ArrayList<>();
        
        String query = """
            SELECT 
                t.table_schema,
                t.table_name,
                pg_total_relation_size(quote_ident(t.table_schema) || '.' || quote_ident(t.table_name)) as table_size,
                c.reltuples::bigint as row_count,
                obj_description(c.oid) as table_comment
            FROM information_schema.tables t
            LEFT JOIN pg_class c ON c.relname = t.table_name
            LEFT JOIN pg_namespace n ON n.nspname = t.table_schema AND n.oid = c.relnamespace
            WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema')
            AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_schema, t.table_name
            """;

        try (PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            
            while (rs.next()) {
                tables.add(TableInfo.builder()
                    .schema(rs.getString("table_schema"))
                    .tableName(rs.getString("table_name"))
                    .sizeBytes(rs.getLong("table_size"))
                    .rowCount(rs.getLong("row_count"))
                    .comment(rs.getString("table_comment"))
                    .build());
            }
        }

        return tables;
    }

    /**
     * Extract field metadata
     */
    private List<FieldInfo> extractFields(Connection conn, String schema, String table) 
            throws SQLException {
        List<FieldInfo> fields = new ArrayList<>();
        
        String query = """
            SELECT 
                c.column_name,
                c.data_type,
                c.character_maximum_length,
                c.is_nullable,
                c.column_default,
                col_description((quote_ident(c.table_schema) || '.' || quote_ident(c.table_name))::regclass::oid, c.ordinal_position) as column_comment,
                CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key,
                CASE WHEN fk.column_name IS NOT NULL THEN true ELSE false END as is_foreign_key,
                fk.foreign_table_schema,
                fk.foreign_table_name,
                fk.foreign_column_name
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT ku.column_name, ku.table_schema, ku.table_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku
                    ON tc.constraint_name = ku.constraint_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
            ) pk ON c.column_name = pk.column_name 
                AND c.table_schema = pk.table_schema
                AND c.table_name = pk.table_name
            LEFT JOIN (
                SELECT
                    kcu.column_name,
                    kcu.table_schema,
                    kcu.table_name,
                    ccu.table_schema AS foreign_table_schema,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
            ) fk ON c.column_name = fk.column_name
                AND c.table_schema = fk.table_schema
                AND c.table_name = fk.table_name
            WHERE c.table_schema = ? AND c.table_name = ?
            ORDER BY c.ordinal_position
            """;

        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, schema);
            stmt.setString(2, table);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    fields.add(FieldInfo.builder()
                        .fieldName(rs.getString("column_name"))
                        .dataType(rs.getString("data_type"))
                        .maxLength(rs.getInt("character_maximum_length"))
                        .isNullable("YES".equals(rs.getString("is_nullable")))
                        .defaultValue(rs.getString("column_default"))
                        .comment(rs.getString("column_comment"))
                        .isPrimaryKey(rs.getBoolean("is_primary_key"))
                        .isForeignKey(rs.getBoolean("is_foreign_key"))
                        .foreignTable(rs.getString("foreign_table_name"))
                        .foreignColumn(rs.getString("foreign_column_name"))
                        .build());
                }
            }
        }

        return fields;
    }

    /**
     * Detect foreign key relationships and create lineage
     */
    private void detectRelationships(Connection conn, Database database) throws SQLException {
        String query = """
            SELECT
                tc.table_schema,
                tc.table_name,
                kcu.column_name,
                ccu.table_schema AS foreign_table_schema,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
            """;

        try (PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            
            while (rs.next()) {
                createLineageFromFK(
                    database,
                    rs.getString("table_schema"),
                    rs.getString("table_name"),
                    rs.getString("column_name"),
                    rs.getString("foreign_table_schema"),
                    rs.getString("foreign_table_name"),
                    rs.getString("foreign_column_name")
                );
            }
        }
    }

    /**
     * Create lineage relationship from foreign key
     */
    @Transactional
    protected void createLineageFromFK(Database database, String schema, String table, String column,
                                      String fkSchema, String fkTable, String fkColumn) {
        try {
            TableMetadata sourceTable = tableRepository
                .findByDatabaseAndSchemaAndTable(database.getId(), fkSchema, fkTable)
                .orElse(null);
            TableMetadata targetTable = tableRepository
                .findByDatabaseAndSchemaAndTable(database.getId(), schema, table)
                .orElse(null);

            if (sourceTable != null && targetTable != null) {
                Optional<FieldMetadata> sourceField = sourceTable.getFields().stream()
                    .filter(f -> f.getFieldName().equals(fkColumn))
                    .findFirst();
                Optional<FieldMetadata> targetField = targetTable.getFields().stream()
                    .filter(f -> f.getFieldName().equals(column))
                    .findFirst();

                if (sourceField.isPresent() && targetField.isPresent()) {
                    // Check if lineage already exists
                    List<LineageRelationship> existing = lineageRepository
                        .findBySourceField(sourceField.get());
                    
                    boolean exists = existing.stream()
                        .anyMatch(l -> l.getTargetField().getId().equals(targetField.get().getId()));

                    if (!exists) {
                        LineageRelationship lineage = LineageRelationship.builder()
                            .sourceField(sourceField.get())
                            .targetField(targetField.get())
                            .lineageType(LineageType.DIRECT)
                            .transformationLogic("Foreign Key Reference")
                            .createdBy("metadata-sync")
                            .build();
                        lineageRepository.save(lineage);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Could not create lineage for FK: {}.{} -> {}.{}", 
                fkTable, fkColumn, table, column, e);
        }
    }

    /**
     * Calculate data quality scores
     */
    @Transactional
    protected void calculateQualityScores(Database database) {
        List<TableMetadata> tables = tableRepository.findByDatabase(database);
        
        for (TableMetadata table : tables) {
            double score = 100.0;
            
            // Description exists
            if (table.getDescription() == null || table.getDescription().isEmpty()) {
                score -= 10;
            }
            
            // All fields have descriptions
            long fieldsWithoutDesc = table.getFields().stream()
                .filter(f -> f.getDescription() == null || f.getDescription().isEmpty())
                .count();
            if (table.getFields().size() > 0) {
                score -= (fieldsWithoutDesc * 20.0 / table.getFields().size());
            }
            
            // All fields have owners
            long fieldsWithoutOwner = table.getFields().stream()
                .filter(f -> f.getOwnerEmail() == null)
                .count();
            if (table.getFields().size() > 0) {
                score -= (fieldsWithoutOwner * 20.0 / table.getFields().size());
            }
            
            // PII fields are documented
            long undocumentedPII = table.getFields().stream()
                .filter(f -> f.getSensitivityLevel() == SensitivityLevel.PII)
                .filter(f -> f.getDescription() == null || f.getOwnerEmail() == null)
                .count();
            score -= (undocumentedPII * 10);
            
            table.setDataQualityScore(Math.max(0, score));
            tableRepository.save(table);
        }
    }

    private SensitivityLevel detectPII(String fieldName) {
        for (Map.Entry<String, Pattern> entry : PII_PATTERNS.entrySet()) {
            if (entry.getValue().matcher(fieldName).matches()) {
                return SensitivityLevel.PII;
            }
        }
        return SensitivityLevel.INTERNAL;
    }

    private String generateBusinessName(String fieldName) {
        return Arrays.stream(fieldName.split("_"))
            .map(word -> word.substring(0, 1).toUpperCase() + word.substring(1).toLowerCase())
            .collect(java.util.stream.Collectors.joining(" "));
    }

    private TableMetadata createNewTable(Database database, TableInfo info) {
        return TableMetadata.builder()
            .database(database)
            .schema(info.schema)
            .tableName(info.tableName)
            .description(info.comment)
            .createdBy("metadata-sync")
            .build();
    }

    private FieldMetadata createNewField(TableMetadata table, FieldInfo info) {
        return FieldMetadata.builder()
            .table(table)
            .fieldName(info.fieldName)
            .businessName(generateBusinessName(info.fieldName))
            .dataType(info.dataType)
            .description(info.comment)
            .sensitivityLevel(detectPII(info.fieldName))
            .createdBy("metadata-sync")
            .build();
    }

    private void trackChange(FieldMetadata field, String action, String description) {
        changeHistoryRepository.save(ChangeHistory.builder()
            .entityType("FIELD")
            .entityId(field.getId())
            .entityName(field.getFieldName())
            .action(ChangeAction.valueOf(action))
            .description(description)
            .changedBy("metadata-sync")
            .build());
    }

    private void trackChange(TableMetadata table, String action, String description) {
        changeHistoryRepository.save(ChangeHistory.builder()
            .entityType("TABLE")
            .entityId(table.getId())
            .entityName(table.getTableName())
            .action(ChangeAction.valueOf(action))
            .description(description)
            .changedBy("metadata-sync")
            .build());
    }

    private void createAlert(String alertType, Database database, String message) {
        alertRepository.save(GovernanceAlert.builder()
            .severity(AlertSeverity.HIGH)
            .alertType(AlertType.valueOf(alertType))
            .entityType("DATABASE")
            .entityId(database.getId())
            .entityName(database.getName())
            .message(message)
            .isResolved(false)
            .build());
    }

    private void createAlert(String alertType, TableMetadata table, String message) {
        alertRepository.save(GovernanceAlert.builder()
            .severity(AlertSeverity.MEDIUM)
            .alertType(AlertType.valueOf(alertType))
            .entityType("TABLE")
            .entityId(table.getId())
            .entityName(table.getTableName())
            .message(message)
            .isResolved(false)
            .build());
    }

    private Connection getConnection(Database database) throws SQLException {
        // In production, use connection pooling
        return DriverManager.getConnection(database.getConnectionString());
    }

    @lombok.Data
    @lombok.Builder
    private static class TableInfo {
        String schema;
        String tableName;
        Long sizeBytes;
        Long rowCount;
        String comment;
    }

    @lombok.Data
    @lombok.Builder
    private static class FieldInfo {
        String fieldName;
        String dataType;
        Integer maxLength;
        Boolean isNullable;
        String defaultValue;
        String comment;
        Boolean isPrimaryKey;
        Boolean isForeignKey;
        String foreignTable;
        String foreignColumn;
    }
}
