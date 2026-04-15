package com.datavault.service;

import com.datavault.adapter.*;
import com.datavault.dto.*;
import com.datavault.entity.*;
import com.datavault.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.*;

@Service
@Slf4j
@RequiredArgsConstructor
public class MetadataExtractionService {
    
    private final PostgreSQLAdapter postgresqlAdapter;
    private final MSSQLAdapter mssqlAdapter;
    private final MongoDBAdapter mongodbAdapter;
    private final GlueAdapter glueAdapter;
    
    private final DatabaseRepository databaseRepository;
    private final TableMetadataRepository tableRepository;
    private final FieldMetadataRepository fieldRepository;
    private final LineageRelationshipRepository lineageRepository;
    private final ChangeHistoryRepository changeHistoryRepository;

    @Transactional
    public MetadataExtractionResultDTO extractMetadata(MetadataExtractionRequestDTO request) {
        log.info("Starting metadata extraction for database ID: {}", request.getDatabaseId());
        
        MetadataExtractionResultDTO result = MetadataExtractionResultDTO.builder()
            .success(false)
            .errors(new ArrayList<>())
            .schemasExtracted(0)
            .tablesExtracted(0)
            .fieldsExtracted(0)
            .relationshipsExtracted(0)
            .build();
        
        try {
            DatabaseAdapter adapter = getAdapter(request.getDatabaseType());
            
            if (adapter == null) {
                result.setMessage("Unsupported database type: " + request.getDatabaseType());
                return result;
            }
            
            // Test connection first
            if (!adapter.testConnection(request.getConnectionString(), request.getUsername(), request.getPassword())) {
                result.setMessage("Connection test failed");
                result.getErrors().add("Unable to connect to database");
                return result;
            }
            
            Database database = databaseRepository.findById(request.getDatabaseId())
                .orElseThrow(() -> new RuntimeException("Database not found"));
            
            // Extract based on database type
            if ("MongoDB".equals(request.getDatabaseType())) {
                extractMongoDBMetadata(request, database, result);
            } else if ("AWSGlue".equals(request.getDatabaseType())) {
                extractGlueMetadata(request, database, result);
            } else {
                extractSQLMetadata(request, database, adapter, result);
            }
            
            result.setSuccess(true);
            result.setMessage("Metadata extraction completed successfully");
            
            // Track change
            trackChange(database, ChangeAction.UPDATED, 
                String.format("Extracted %d tables, %d fields", result.getTablesExtracted(), result.getFieldsExtracted()),
                "system");
            
        } catch (Exception e) {
            log.error("Error during metadata extraction: {}", e.getMessage(), e);
            result.setMessage("Extraction failed: " + e.getMessage());
            result.getErrors().add(e.getMessage());
        }
        
        return result;
    }
    
    private void extractSQLMetadata(MetadataExtractionRequestDTO request, Database database, 
                                    DatabaseAdapter adapter, MetadataExtractionResultDTO result) throws Exception {
        
        try (Connection connection = adapter.getConnection(request.getConnectionString(), 
                                                          request.getUsername(), 
                                                          request.getPassword())) {
            
            List<String> schemas = request.getSchemasToExtract();
            if (schemas == null || schemas.isEmpty()) {
                schemas = adapter.extractSchemas(connection);
            }
            
            result.setSchemasExtracted(schemas.size());
            
            for (String schema : schemas) {
                log.info("Extracting schema: {}", schema);
                
                // Extract tables
                List<TableMetadataDTO> tables = adapter.extractTables(connection, schema);
                result.setTablesExtracted(result.getTablesExtracted() + tables.size());
                
                for (TableMetadataDTO tableDTO : tables) {
                    // Save or update table
                    TableMetadata table = saveOrUpdateTable(database, tableDTO);
                    
                    // Extract fields
                    List<FieldMetadataDTO> fields = adapter.extractFields(connection, schema, tableDTO.getTableName());
                    result.setFieldsExtracted(result.getFieldsExtracted() + fields.size());
                    
                    for (FieldMetadataDTO fieldDTO : fields) {
                        saveOrUpdateField(table, fieldDTO);
                    }
                    
                    // Calculate quality if requested
                    if (Boolean.TRUE.equals(request.getCalculateQuality())) {
                        DataQualityDTO quality = adapter.calculateQuality(connection, schema, tableDTO.getTableName());
                        table.setDataQualityScore(quality.getOverallScore());
                        tableRepository.save(table);
                    }
                }
                
                // Extract lineage if requested
                if (Boolean.TRUE.equals(request.getExtractLineage())) {
                    List<LineageRelationshipDTO> lineages = adapter.buildLineage(connection, schema);
                    result.setRelationshipsExtracted(result.getRelationshipsExtracted() + lineages.size());
                    
                    for (LineageRelationshipDTO lineageDTO : lineages) {
                        saveLineageRelationship(database, lineageDTO);
                    }
                }
            }
        }
    }
    
    private void extractMongoDBMetadata(MetadataExtractionRequestDTO request, Database database,
                                       MetadataExtractionResultDTO result) {
        
        MongoDBAdapter mongoAdapter = (MongoDBAdapter) getAdapter("MongoDB");
        
        // If specific schemas (databases) are provided, use them; otherwise extract all
        List<String> databasesToExtract = request.getSchemasToExtract();
        if (databasesToExtract == null || databasesToExtract.isEmpty()) {
            databasesToExtract = mongoAdapter.extractDatabases(request.getConnectionString());
        }
        result.setSchemasExtracted(databasesToExtract.size());
        
        for (String dbName : databasesToExtract) {
            log.info("Extracting MongoDB database: {}", dbName);
            
            // Extract specific collections if provided, otherwise all
            List<TableMetadataDTO> collections = mongoAdapter.extractCollections(
                request.getConnectionString(), 
                dbName,
                request.getCollectionsToExtract() // New field for specific collections
            );
            result.setTablesExtracted(result.getTablesExtracted() + collections.size());
            
            for (TableMetadataDTO collectionDTO : collections) {
                TableMetadata collection = saveOrUpdateTable(database, collectionDTO);
                
                List<FieldMetadataDTO> fields = mongoAdapter.extractFields(
                    request.getConnectionString(), dbName, collectionDTO.getTableName());
                result.setFieldsExtracted(result.getFieldsExtracted() + fields.size());
                
                for (FieldMetadataDTO fieldDTO : fields) {
                    saveOrUpdateField(collection, fieldDTO);
                }
                
                if (Boolean.TRUE.equals(request.getCalculateQuality())) {
                    DataQualityDTO quality = mongoAdapter.calculateQuality(
                        request.getConnectionString(), dbName, collectionDTO.getTableName());
                    collection.setDataQualityScore(quality.getOverallScore());
                    tableRepository.save(collection);
                }
            }
        }
    }
    
    private TableMetadata saveOrUpdateTable(Database database, TableMetadataDTO dto) {
        Optional<TableMetadata> existing = tableRepository.findByDatabaseAndTableName(database, dto.getTableName());
        
        TableMetadata table;
        if (existing.isPresent()) {
            table = existing.get();
            table.setDescription(dto.getDescription());
            table.setRowCount(dto.getRowCount());
            table.setSizeBytes(dto.getSizeBytes());
        } else {
            table = TableMetadata.builder()
                .database(database)
                .schema(dto.getSchema())
                .tableName(dto.getTableName())
                .description(dto.getDescription())
                .rowCount(dto.getRowCount())
                .sizeBytes(dto.getSizeBytes())
                .dataQualityScore(0.0)
                .build();
        }
        
        return tableRepository.save(table);
    }
    
    private FieldMetadata saveOrUpdateField(TableMetadata table, FieldMetadataDTO dto) {
        Optional<FieldMetadata> existing = fieldRepository.findByTableAndFieldName(table, dto.getFieldName());
        
        FieldMetadata field;
        if (existing.isPresent()) {
            field = existing.get();
            field.setBusinessName(dto.getBusinessName());
            field.setDataType(dto.getDataType());
            field.setDescription(dto.getDescription());
        } else {
            field = FieldMetadata.builder()
                .table(table)
                .fieldName(dto.getFieldName())
                .businessName(dto.getBusinessName())
                .dataType(dto.getDataType())
                .description(dto.getDescription())
                .isPrimaryKey(dto.getIsPrimaryKey())
                .isForeignKey(dto.getIsForeignKey())
                .isNullable(dto.getIsNullable())
                .defaultValue(dto.getDefaultValue())
                .maxLength(dto.getMaxLength())
                .sensitivityLevel(dto.getSensitivityLevel() != null ? 
                    SensitivityLevel.valueOf(dto.getSensitivityLevel()) : SensitivityLevel.INTERNAL)
                .build();
        }
        
        return fieldRepository.save(field);
    }
    
    private void saveLineageRelationship(Database database, LineageRelationshipDTO dto) {
        // Parse source and target field names
        String[] sourceParts = dto.getSourceFieldName().split("\\.");
        String[] targetParts = dto.getTargetFieldName().split("\\.");
        
        if (sourceParts.length == 2 && targetParts.length == 2) {
            Optional<TableMetadata> sourceTable = tableRepository.findByDatabaseAndTableName(database, sourceParts[0]);
            Optional<TableMetadata> targetTable = tableRepository.findByDatabaseAndTableName(database, targetParts[0]);
            
            if (sourceTable.isPresent() && targetTable.isPresent()) {
                Optional<FieldMetadata> sourceField = fieldRepository.findByTableAndFieldName(sourceTable.get(), sourceParts[1]);
                Optional<FieldMetadata> targetField = fieldRepository.findByTableAndFieldName(targetTable.get(), targetParts[1]);
                
                if (sourceField.isPresent() && targetField.isPresent()) {
                    LineageRelationship lineage = LineageRelationship.builder()
                        .sourceField(sourceField.get())
                        .targetField(targetField.get())
                        .lineageType(LineageType.valueOf(dto.getLineageType()))
                        .transformationLogic(dto.getTransformationLogic())
                        .confidence(95.0)
                        .build();
                    
                    lineageRepository.save(lineage);
                }
            }
        }
    }
    
    // ── AWS Glue extraction ────────────────────────────────────────────────────

    private void extractGlueMetadata(MetadataExtractionRequestDTO request, Database database,
                                     MetadataExtractionResultDTO result) {
        String connStr  = request.getConnectionString();
        String keyId    = request.getUsername();
        String secret   = request.getPassword();

        // Determine which Glue databases to scan
        List<String> glueDbs = request.getSchemasToExtract();
        if (glueDbs == null || glueDbs.isEmpty()) {
            glueDbs = glueAdapter.extractGlueDatabases(connStr, keyId, secret);
        }
        result.setSchemasExtracted(glueDbs.size());

        for (String glueDb : glueDbs) {
            log.info("Extracting Glue database: {}", glueDb);

            List<TableMetadataDTO> tables = glueAdapter.extractGlueTables(connStr, keyId, secret, glueDb);
            result.setTablesExtracted(result.getTablesExtracted() + tables.size());

            for (TableMetadataDTO tableDTO : tables) {
                TableMetadata table = saveOrUpdateTable(database, tableDTO);

                List<FieldMetadataDTO> fields = glueAdapter.extractGlueFields(
                        connStr, keyId, secret, glueDb, tableDTO.getTableName());
                result.setFieldsExtracted(result.getFieldsExtracted() + fields.size());

                for (FieldMetadataDTO fieldDTO : fields) {
                    saveOrUpdateField(table, fieldDTO);
                }
            }
        }
    }

    // ── Adapter registry ───────────────────────────────────────────────────────

    private DatabaseAdapter getAdapter(String databaseType) {
        switch (databaseType.toUpperCase()) {
            case "POSTGRESQL":
                return postgresqlAdapter;
            case "MSSQL":
            case "SQLSERVER":
                return mssqlAdapter;
            case "MONGODB":
                return mongodbAdapter;
            case "AWSGLUE":
                return glueAdapter;
            default:
                return null;
        }
    }
    
    private void trackChange(Database database, ChangeAction action, String description, String username) {
        ChangeHistory change = ChangeHistory.builder()
            .entityType("DATABASE")
            .entityId(database.getId())
            .entityName(database.getName())
            .action(action)
            .description(description)
            .changedBy(username)
            .changedAt(LocalDateTime.now())
            .build();
        
        changeHistoryRepository.save(change);
    }
}
