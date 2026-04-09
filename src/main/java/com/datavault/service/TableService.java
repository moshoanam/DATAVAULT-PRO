package com.datavault.service;

import com.datavault.dto.*;
import com.datavault.entity.*;
import com.datavault.repository.*;
import com.datavault.exception.ResourceNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class TableService {
    
    private final TableMetadataRepository tableRepository;
    private final DatabaseRepository databaseRepository;
    private final FieldMetadataRepository fieldRepository;
    private final ChangeHistoryRepository changeHistoryRepository;

    public Page<TableMetadataDTO> getAllTables(Pageable pageable) {
        return tableRepository.findAll(pageable).map(this::mapToDTO);
    }

    @Cacheable("tables")
    public Page<TableMetadataDTO> getTablesByDatabase(Long databaseId, Pageable pageable) {
        log.info("Fetching tables for database id: {}", databaseId);
        Page<TableMetadata> tables = tableRepository.findByDatabaseId(databaseId, pageable);
        return tables.map(this::mapToDTO);
    }

    @Cacheable(value = "table", key = "#id")
    public TableMetadataDTO getTableById(Long id) {
        log.info("Fetching table by id: {}", id);
        TableMetadata table = tableRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Table not found with id: " + id));
        return mapToDTO(table);
    }

    @Transactional
    @CacheEvict(value = "tables", allEntries = true)
    public TableMetadataDTO createTable(TableMetadataDTO dto, String username) {
        log.info("Creating table: {}.{} by user: {}", dto.getSchema(), dto.getTableName(), username);
        
        Database database = databaseRepository.findById(dto.getDatabaseId())
                .orElseThrow(() -> new ResourceNotFoundException("Database not found"));
        
        TableMetadata table = TableMetadata.builder()
                .database(database)
                .schema(dto.getSchema())
                .tableName(dto.getTableName())
                .description(dto.getDescription())
                .businessGlossary(dto.getBusinessGlossary())
                .rowCount(dto.getRowCount())
                .sizeBytes(dto.getSizeBytes())
                .dataQualityScore(dto.getDataQualityScore())
                .createdBy(username)
                .lastModifiedBy(username)
                .build();
        
        table = tableRepository.save(table);
        
        // Track change
        trackChange(table, ChangeAction.CREATED, "Table created", username);
        
        return mapToDTO(table);
    }

    @Transactional
    @CacheEvict(value = {"table", "tables"}, allEntries = true)
    public TableMetadataDTO updateTable(Long id, TableMetadataDTO dto, String username) {
        log.info("Updating table id: {} by user: {}", id, username);
        
        TableMetadata table = tableRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Table not found with id: " + id));
        
        table.setTableName(dto.getTableName());
        table.setSchema(dto.getSchema());
        table.setDescription(dto.getDescription());
        table.setBusinessGlossary(dto.getBusinessGlossary());
        table.setRowCount(dto.getRowCount());
        table.setSizeBytes(dto.getSizeBytes());
        table.setDataQualityScore(dto.getDataQualityScore());
        table.setLastModifiedBy(username);
        
        table = tableRepository.save(table);
        
        // Track change
        trackChange(table, ChangeAction.UPDATED, "Table metadata updated", username);
        
        return mapToDTO(table);
    }

    @Cacheable(value = "tableQuality", key = "#id")
    public DataQualityDTO getTableQuality(Long id) {
        log.info("Fetching quality metrics for table id: {}", id);
        
        TableMetadata table = tableRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Table not found with id: " + id));
        
        // Calculate quality metrics
        double completeness = calculateCompleteness(table);
        double uniqueness = calculateUniqueness(table);
        double validity = calculateValidity(table);
        double consistency = calculateConsistency(table);
        
        return DataQualityDTO.builder()
                .tableId(table.getId())
                .tableName(table.getTableName())
                .overallScore(table.getDataQualityScore())
                .completeness(completeness)
                .uniqueness(uniqueness)
                .validity(validity)
                .consistency(consistency)
                .lastMeasured(LocalDateTime.now())
                .issues(new ArrayList<>())
                .build();
    }

    public ERDDataDTO generateERD(Long databaseId) {
        log.info("Generating ERD for database id: {}", databaseId);
        
        Database database = databaseRepository.findById(databaseId)
                .orElseThrow(() -> new ResourceNotFoundException("Database not found"));
        
        List<TableMetadata> tables = tableRepository.findByDatabase(database);
        
        List<ERDTableDTO> erdTables = tables.stream()
                .map(this::mapToERDTable)
                .collect(Collectors.toList());
        
        List<ERDRelationshipDTO> relationships = extractRelationships(tables);
        
        return ERDDataDTO.builder()
                .databaseId(databaseId)
                .databaseName(database.getName())
                .tables(erdTables)
                .relationships(relationships)
                .build();
    }

    private TableMetadataDTO mapToDTO(TableMetadata table) {
        return TableMetadataDTO.builder()
                .id(table.getId())
                .databaseId(table.getDatabase().getId())
                .databaseName(table.getDatabase().getName())
                .schema(table.getSchema())
                .tableName(table.getTableName())
                .description(table.getDescription())
                .businessGlossary(table.getBusinessGlossary())
                .rowCount(table.getRowCount())
                .sizeBytes(table.getSizeBytes())
                .dataQualityScore(table.getDataQualityScore())
                .fieldCount(table.getFields() != null ? table.getFields().size() : 0)
                .createdAt(table.getCreatedAt())
                .updatedAt(table.getUpdatedAt())
                .build();
    }

    private ERDTableDTO mapToERDTable(TableMetadata table) {
        List<ERDFieldDTO> fields = table.getFields().stream()
                .map(f -> ERDFieldDTO.builder()
                        .fieldId(f.getId())
                        .fieldName(f.getFieldName())
                        .dataType(f.getDataType())
                        .isPrimaryKey(f.getIsPrimaryKey())
                        .isForeignKey(f.getIsForeignKey())
                        .build())
                .collect(Collectors.toList());
        
        return ERDTableDTO.builder()
                .tableId(table.getId())
                .tableName(table.getTableName())
                .schema(table.getSchema())
                .rowCount(table.getRowCount())
                .qualityScore(table.getDataQualityScore())
                .fields(fields)
                .build();
    }

    private List<ERDRelationshipDTO> extractRelationships(List<TableMetadata> tables) {
        List<ERDRelationshipDTO> relationships = new ArrayList<>();
        
        for (TableMetadata table : tables) {
            for (FieldMetadata field : table.getFields()) {
                if (field.getIsForeignKey()) {
                    // Extract FK relationships (simplified)
                    relationships.add(ERDRelationshipDTO.builder()
                            .sourceTableId(table.getId())
                            .targetTableId(table.getId()) // Would need FK target
                            .sourceFieldName(field.getFieldName())
                            .targetFieldName(field.getFieldName())
                            .relationshipType("ONE_TO_MANY")
                            .build());
                }
            }
        }
        
        return relationships;
    }

    private double calculateCompleteness(TableMetadata table) {
        if (table.getFields() == null || table.getFields().isEmpty()) {
            return 100.0;
        }
        
        long fieldsWithDescription = table.getFields().stream()
                .filter(f -> f.getDescription() != null && !f.getDescription().isEmpty())
                .count();
        
        return (fieldsWithDescription * 100.0) / table.getFields().size();
    }

    private double calculateUniqueness(TableMetadata table) {
        // Simplified - would need actual data analysis
        return 95.0;
    }

    private double calculateValidity(TableMetadata table) {
        // Simplified - would need actual data validation
        return 98.0;
    }

    private double calculateConsistency(TableMetadata table) {
        // Simplified - would need cross-table validation
        return 97.0;
    }

    private void trackChange(TableMetadata table, ChangeAction action, String description, String username) {
        ChangeHistory change = ChangeHistory.builder()
                .entityType("TABLE")
                .entityId(table.getId())
                .entityName(table.getTableName())
                .action(action)
                .description(description)
                .changedBy(username)
                .changedAt(LocalDateTime.now())
                .build();
        
        changeHistoryRepository.save(change);
    }
}
