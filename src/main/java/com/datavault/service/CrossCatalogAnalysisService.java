package com.datavault.service;

import com.datavault.dto.*;
import com.datavault.entity.*;
import com.datavault.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CrossCatalogAnalysisService {
    
    private final DatabaseRepository databaseRepository;
    private final TableMetadataRepository tableRepository;
    private final FieldMetadataRepository fieldRepository;
    private final LineageRelationshipRepository lineageRepository;

    /**
     * Build relationships across all catalogs based on field names, data types, and patterns
     */
    public List<CrossCatalogRelationshipDTO> buildCrossCatalogRelationships() {
        log.info("Building cross-catalog relationships");
        
        List<CrossCatalogRelationshipDTO> relationships = new ArrayList<>();
        
        // Get all databases
        List<Database> databases = databaseRepository.findAll();
        
        // Build field index
        Map<String, List<FieldWithContext>> fieldIndex = buildFieldIndex();
        
        // Find matching fields across databases
        for (Map.Entry<String, List<FieldWithContext>> entry : fieldIndex.entrySet()) {
            String normalizedFieldName = entry.getKey();
            List<FieldWithContext> fields = entry.getValue();
            
            if (fields.size() > 1) {
                // Multiple databases have this field - potential relationship
                for (int i = 0; i < fields.size(); i++) {
                    for (int j = i + 1; j < fields.size(); j++) {
                        FieldWithContext source = fields.get(i);
                        FieldWithContext target = fields.get(j);
                        
                        // Only create relationships across different databases
                        if (!source.databaseId.equals(target.databaseId)) {
                            CrossCatalogRelationshipDTO relationship = analyzeFieldMatch(source, target, normalizedFieldName);
                            if (relationship != null && relationship.getConfidence() > 70.0) {
                                relationships.add(relationship);
                            }
                        }
                    }
                }
            }
        }
        
        log.info("Found {} cross-catalog relationships", relationships.size());
        return relationships;
    }
    
    /**
     * Find all related fields across catalogs for a specific field
     */
    public List<CrossCatalogRelationshipDTO> findRelatedFields(Long fieldId) {
        FieldMetadata field = fieldRepository.findById(fieldId)
            .orElseThrow(() -> new RuntimeException("Field not found"));
        
        String normalizedName = normalizeFieldName(field.getFieldName());
        Map<String, List<FieldWithContext>> fieldIndex = buildFieldIndex();
        
        List<FieldWithContext> similarFields = fieldIndex.get(normalizedName);
        if (similarFields == null) {
            return new ArrayList<>();
        }
        
        FieldWithContext sourceContext = new FieldWithContext(
            field.getId(),
            field.getTable().getDatabase().getId(),
            field.getTable().getDatabase().getName(),
            field.getTable().getTableName(),
            field.getFieldName(),
            field.getDataType(),
            field.getSensitivityLevel() != null ? field.getSensitivityLevel().name() : "INTERNAL"
        );
        
        return similarFields.stream()
            .filter(target -> !target.databaseId.equals(sourceContext.databaseId))
            .map(target -> analyzeFieldMatch(sourceContext, target, normalizedName))
            .filter(Objects::nonNull)
            .filter(r -> r.getConfidence() > 70.0)
            .collect(Collectors.toList());
    }
    
    /**
     * Build comprehensive lineage across all catalogs
     */
    public Map<String, Object> buildComprehensiveLineage(Long fieldId) {
        Map<String, Object> lineageMap = new HashMap<>();
        
        FieldMetadata field = fieldRepository.findById(fieldId)
            .orElseThrow(() -> new RuntimeException("Field not found"));
        
        // Get direct lineage within same database
        List<LineageRelationship> directUpstream = lineageRepository.findUpstreamLineage(field);
        List<LineageRelationship> directDownstream = lineageRepository.findDownstreamLineage(field);
        
        // Get cross-catalog relationships
        List<CrossCatalogRelationshipDTO> crossCatalog = findRelatedFields(fieldId);
        
        lineageMap.put("field", buildFieldInfo(field));
        lineageMap.put("directUpstream", directUpstream.stream().map(this::mapLineageToDTO).collect(Collectors.toList()));
        lineageMap.put("directDownstream", directDownstream.stream().map(this::mapLineageToDTO).collect(Collectors.toList()));
        lineageMap.put("crossCatalogRelationships", crossCatalog);
        lineageMap.put("totalRelationships", directUpstream.size() + directDownstream.size() + crossCatalog.size());
        
        return lineageMap;
    }
    
    /**
     * Get quality metrics across all catalogs
     */
    public Map<String, Object> getCrossCatalogQualityMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        
        List<Database> databases = databaseRepository.findAll();
        
        long totalTables = 0;
        long totalFields = 0;
        long piiFields = 0;
        double avgQuality = 0.0;
        
        for (Database db : databases) {
            List<TableMetadata> tables = tableRepository.findByDatabase(db);
            totalTables += tables.size();
            
            for (TableMetadata table : tables) {
                List<FieldMetadata> fields = fieldRepository.findByTable(table);
                totalFields += fields.size();
                
                piiFields += fields.stream()
                    .filter(f -> f.getSensitivityLevel() == SensitivityLevel.PII)
                    .count();
                
                if (table.getDataQualityScore() != null) {
                    avgQuality += table.getDataQualityScore();
                }
            }
        }
        
        metrics.put("totalDatabases", databases.size());
        metrics.put("totalTables", totalTables);
        metrics.put("totalFields", totalFields);
        metrics.put("piiFields", piiFields);
        metrics.put("averageQualityScore", totalTables > 0 ? avgQuality / totalTables : 0.0);
        
        return metrics;
    }
    
    /**
     * Audit all PII fields across catalogs
     */
    public List<Map<String, Object>> auditPIIFields() {
        List<Map<String, Object>> piiAudit = new ArrayList<>();
        
        List<FieldMetadata> allFields = fieldRepository.findAll();
        
        for (FieldMetadata field : allFields) {
            if (field.getSensitivityLevel() == SensitivityLevel.PII) {
                Map<String, Object> audit = new HashMap<>();
                audit.put("database", field.getTable().getDatabase().getName());
                audit.put("schema", field.getTable().getSchema());
                audit.put("table", field.getTable().getTableName());
                audit.put("field", field.getFieldName());
                audit.put("dataType", field.getDataType());
                audit.put("owner", field.getOwnerEmail());
                audit.put("encrypted", false); // Would check actual encryption status
                audit.put("masked", false); // Would check masking policy
                
                piiAudit.add(audit);
            }
        }
        
        return piiAudit;
    }
    
    private Map<String, List<FieldWithContext>> buildFieldIndex() {
        Map<String, List<FieldWithContext>> index = new HashMap<>();
        
        List<FieldMetadata> allFields = fieldRepository.findAll();
        
        for (FieldMetadata field : allFields) {
            String normalizedName = normalizeFieldName(field.getFieldName());
            
            FieldWithContext context = new FieldWithContext(
                field.getId(),
                field.getTable().getDatabase().getId(),
                field.getTable().getDatabase().getName(),
                field.getTable().getTableName(),
                field.getFieldName(),
                field.getDataType(),
                field.getSensitivityLevel() != null ? field.getSensitivityLevel().name() : "INTERNAL"
            );
            
            index.computeIfAbsent(normalizedName, k -> new ArrayList<>()).add(context);
        }
        
        return index;
    }
    
    private String normalizeFieldName(String fieldName) {
        // Remove common prefixes/suffixes and underscores
        return fieldName.toLowerCase()
            .replaceAll("^(tbl_|dim_|fact_|stg_)", "")
            .replaceAll("_(id|key|code|nbr)$", "")
            .replaceAll("_", "");
    }
    
    private CrossCatalogRelationshipDTO analyzeFieldMatch(FieldWithContext source, FieldWithContext target, String normalizedName) {
        double confidence = 0.0;
        StringBuilder matchReason = new StringBuilder();
        
        // Field name match
        if (source.fieldName.equalsIgnoreCase(target.fieldName)) {
            confidence += 40.0;
            matchReason.append("Exact name match; ");
        } else {
            confidence += 30.0;
            matchReason.append("Similar name (").append(normalizedName).append("); ");
        }
        
        // Data type match
        if (source.dataType.equalsIgnoreCase(target.dataType)) {
            confidence += 30.0;
            matchReason.append("Data type match; ");
        } else if (areTypesCompatible(source.dataType, target.dataType)) {
            confidence += 15.0;
            matchReason.append("Compatible data types; ");
        }
        
        // Sensitivity level match
        if (source.sensitivityLevel.equals(target.sensitivityLevel)) {
            confidence += 20.0;
            matchReason.append("Same sensitivity level; ");
        }
        
        // Table name similarity
        if (source.tableName.toLowerCase().contains(target.tableName.toLowerCase()) ||
            target.tableName.toLowerCase().contains(source.tableName.toLowerCase())) {
            confidence += 10.0;
            matchReason.append("Related table names; ");
        }
        
        return CrossCatalogRelationshipDTO.builder()
            .sourceDatabaseId(source.databaseId)
            .sourceDatabaseName(source.databaseName)
            .sourceTable(source.tableName)
            .sourceField(source.fieldName)
            .targetDatabaseId(target.databaseId)
            .targetDatabaseName(target.databaseName)
            .targetTable(target.tableName)
            .targetField(target.fieldName)
            .relationshipType("CROSS_CATALOG_REFERENCE")
            .confidence(confidence)
            .matchReason(matchReason.toString())
            .build();
    }
    
    private boolean areTypesCompatible(String type1, String type2) {
        Set<String> numericTypes = Set.of("int", "integer", "bigint", "smallint", "decimal", "numeric", "float", "double", "long");
        Set<String> stringTypes = Set.of("varchar", "char", "text", "string", "nvarchar", "nchar");
        Set<String> dateTypes = Set.of("date", "datetime", "timestamp", "time");
        
        String t1 = type1.toLowerCase();
        String t2 = type2.toLowerCase();
        
        return (numericTypes.stream().anyMatch(t1::contains) && numericTypes.stream().anyMatch(t2::contains)) ||
               (stringTypes.stream().anyMatch(t1::contains) && stringTypes.stream().anyMatch(t2::contains)) ||
               (dateTypes.stream().anyMatch(t1::contains) && dateTypes.stream().anyMatch(t2::contains));
    }
    
    private Map<String, Object> buildFieldInfo(FieldMetadata field) {
        Map<String, Object> info = new HashMap<>();
        info.put("fieldId", field.getId());
        info.put("database", field.getTable().getDatabase().getName());
        info.put("schema", field.getTable().getSchema());
        info.put("table", field.getTable().getTableName());
        info.put("field", field.getFieldName());
        info.put("dataType", field.getDataType());
        info.put("sensitivityLevel", field.getSensitivityLevel() != null ? field.getSensitivityLevel().name() : null);
        return info;
    }
    
    private LineageRelationshipDTO mapLineageToDTO(LineageRelationship lineage) {
        return LineageRelationshipDTO.builder()
            .id(lineage.getId())
            .sourceFieldId(lineage.getSourceField().getId())
            .targetFieldId(lineage.getTargetField().getId())
            .sourceFieldName(lineage.getSourceField().getTable().getTableName() + "." + lineage.getSourceField().getFieldName())
            .targetFieldName(lineage.getTargetField().getTable().getTableName() + "." + lineage.getTargetField().getFieldName())
            .lineageType(lineage.getLineageType() != null ? lineage.getLineageType().name() : null)
            .transformationLogic(lineage.getTransformationLogic())
            .confidence(lineage.getConfidence())
            .build();
    }
    
    private static class FieldWithContext {
        Long fieldId;
        Long databaseId;
        String databaseName;
        String tableName;
        String fieldName;
        String dataType;
        String sensitivityLevel;
        
        FieldWithContext(Long fieldId, Long databaseId, String databaseName, String tableName, 
                        String fieldName, String dataType, String sensitivityLevel) {
            this.fieldId = fieldId;
            this.databaseId = databaseId;
            this.databaseName = databaseName;
            this.tableName = tableName;
            this.fieldName = fieldName;
            this.dataType = dataType;
            this.sensitivityLevel = sensitivityLevel;
        }
    }
}
