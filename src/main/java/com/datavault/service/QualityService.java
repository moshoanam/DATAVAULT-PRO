package com.datavault.service;

import com.datavault.dto.*;
import com.datavault.entity.*;
import com.datavault.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Pattern;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class QualityService {
    
    private final QualityRuleRepository qualityRuleRepository;
    private final TableMetadataRepository tableRepository;
    private final FieldMetadataRepository fieldRepository;

    /**
     * Calculate comprehensive quality score for a table
     */
    public TableQualityReportDTO calculateTableQuality(Long tableId) {
        log.info("Calculating quality for table ID: {}", tableId);
        
        TableMetadata table = tableRepository.findById(tableId)
            .orElseThrow(() -> new RuntimeException("Table not found"));
        
        List<FieldMetadata> fields = fieldRepository.findByTable(table);
        List<QualityRule> rules = qualityRuleRepository.findActiveRulesByTable(tableId);
        
        // Calculate individual metrics
        double completeness = calculateCompleteness(table, fields);
        double validity = calculateValidity(table, fields);
        double consistency = calculateConsistency(table, fields);
        double uniqueness = calculateUniqueness(table, fields);
        
        // Execute quality rules
        List<QualityExecutionResultDTO> results = new ArrayList<>();
        int passed = 0;
        int failed = 0;
        
        for (QualityRule rule : rules) {
            QualityExecutionResultDTO result = executeRule(rule, table, fields);
            results.add(result);
            
            if ("PASSED".equals(result.getResult())) {
                passed++;
            } else if ("FAILED".equals(result.getResult())) {
                failed++;
            }
        }
        
        // Calculate overall score
        double overallScore = (completeness + validity + consistency + uniqueness) / 4.0;
        
        // Update table quality score
        table.setDataQualityScore(overallScore);
        tableRepository.save(table);
        
        return TableQualityReportDTO.builder()
            .tableId(tableId)
            .tableName(table.getTableName())
            .overallScore(overallScore)
            .completenessScore(completeness)
            .validityScore(validity)
            .consistencyScore(consistency)
            .uniquenessScore(uniqueness)
            .totalRules(rules.size())
            .passedRules(passed)
            .failedRules(failed)
            .results(results)
            .lastCalculated(LocalDateTime.now())
            .build();
    }
    
    /**
     * Calculate completeness: % of fields with descriptions and non-null values
     */
    private double calculateCompleteness(TableMetadata table, List<FieldMetadata> fields) {
        if (fields.isEmpty()) return 0.0;
        
        long fieldsWithDescription = fields.stream()
            .filter(f -> f.getDescription() != null && !f.getDescription().trim().isEmpty())
            .count();
        
        long nonNullableFields = fields.stream()
            .filter(f -> Boolean.FALSE.equals(f.getIsNullable()))
            .count();
        
        // Completeness based on documentation and nullability
        double descriptionScore = (fieldsWithDescription * 100.0) / fields.size();
        double nullabilityScore = (nonNullableFields * 100.0) / fields.size();
        
        return (descriptionScore * 0.7 + nullabilityScore * 0.3);
    }
    
    /**
     * Calculate validity: % of fields with constraints (PK, FK, data types)
     */
    private double calculateValidity(TableMetadata table, List<FieldMetadata> fields) {
        if (fields.isEmpty()) return 0.0;
        
        long fieldsWithConstraints = fields.stream()
            .filter(f -> Boolean.TRUE.equals(f.getIsPrimaryKey()) || 
                        Boolean.TRUE.equals(f.getIsForeignKey()) ||
                        Boolean.FALSE.equals(f.getIsNullable()))
            .count();
        
        long fieldsWithValidDataTypes = fields.stream()
            .filter(f -> f.getDataType() != null && !f.getDataType().isEmpty())
            .count();
        
        double constraintScore = (fieldsWithConstraints * 100.0) / fields.size();
        double dataTypeScore = (fieldsWithValidDataTypes * 100.0) / fields.size();
        
        return (constraintScore * 0.6 + dataTypeScore * 0.4);
    }
    
    /**
     * Calculate consistency: naming conventions and standards
     */
    private double calculateConsistency(TableMetadata table, List<FieldMetadata> fields) {
        if (fields.isEmpty()) return 0.0;
        
        // Check naming convention consistency (snake_case vs camelCase)
        Pattern snakeCasePattern = Pattern.compile("^[a-z][a-z0-9_]*$");
        Pattern camelCasePattern = Pattern.compile("^[a-z][a-zA-Z0-9]*$");
        
        long snakeCaseCount = fields.stream()
            .filter(f -> snakeCasePattern.matcher(f.getFieldName()).matches())
            .count();
        
        long camelCaseCount = fields.stream()
            .filter(f -> camelCasePattern.matcher(f.getFieldName()).matches())
            .count();
        
        // Consistency is high if most fields follow the same convention
        long consistentNaming = Math.max(snakeCaseCount, camelCaseCount);
        double namingScore = (consistentNaming * 100.0) / fields.size();
        
        // Check for business names
        long fieldsWithBusinessNames = fields.stream()
            .filter(f -> f.getBusinessName() != null && !f.getBusinessName().trim().isEmpty())
            .count();
        
        double businessNameScore = (fieldsWithBusinessNames * 100.0) / fields.size();
        
        return (namingScore * 0.5 + businessNameScore * 0.5);
    }
    
    /**
     * Calculate uniqueness: % of unique/indexed fields
     */
    private double calculateUniqueness(TableMetadata table, List<FieldMetadata> fields) {
        if (fields.isEmpty()) return 0.0;
        
        long uniqueFields = fields.stream()
            .filter(f -> Boolean.TRUE.equals(f.getIsPrimaryKey()))
            .count();
        
        // Award points for having primary keys
        if (uniqueFields > 0) {
            return 100.0;
        }
        
        // Partial score if no PK but has descriptive unique fields
        long potentialUniqueFields = fields.stream()
            .filter(f -> f.getFieldName().toLowerCase().contains("id") ||
                        f.getFieldName().toLowerCase().contains("key") ||
                        f.getFieldName().toLowerCase().contains("code"))
            .count();
        
        return Math.min((potentialUniqueFields * 50.0) / Math.max(1, fields.size()), 80.0);
    }
    
    /**
     * Execute a single quality rule
     */
    private QualityExecutionResultDTO executeRule(QualityRule rule, TableMetadata table, List<FieldMetadata> fields) {
        log.debug("Executing rule: {} of type: {}", rule.getRuleName(), rule.getRuleType());
        
        try {
            String result = "PASSED";
            Double actualValue = 0.0;
            String message = "Rule passed";
            
            switch (rule.getRuleType()) {
                case "COMPLETENESS":
                    actualValue = calculateCompleteness(table, fields);
                    if (rule.getThresholdValue() != null && actualValue < rule.getThresholdValue()) {
                        result = "FAILED";
                        message = String.format("Completeness %.2f%% below threshold %.2f%%", 
                            actualValue, rule.getThresholdValue());
                    }
                    break;
                    
                case "VALIDITY":
                    actualValue = calculateValidity(table, fields);
                    if (rule.getThresholdValue() != null && actualValue < rule.getThresholdValue()) {
                        result = "FAILED";
                        message = String.format("Validity %.2f%% below threshold %.2f%%", 
                            actualValue, rule.getThresholdValue());
                    }
                    break;
                    
                case "UNIQUENESS":
                    actualValue = calculateUniqueness(table, fields);
                    if (rule.getThresholdValue() != null && actualValue < rule.getThresholdValue()) {
                        result = "FAILED";
                        message = String.format("Uniqueness %.2f%% below threshold %.2f%%", 
                            actualValue, rule.getThresholdValue());
                    }
                    break;
                    
                case "CONSISTENCY":
                    actualValue = calculateConsistency(table, fields);
                    if (rule.getThresholdValue() != null && actualValue < rule.getThresholdValue()) {
                        result = "FAILED";
                        message = String.format("Consistency %.2f%% below threshold %.2f%%", 
                            actualValue, rule.getThresholdValue());
                    }
                    break;
                    
                default:
                    result = "WARNING";
                    message = "Unknown rule type: " + rule.getRuleType();
            }
            
            // Update rule
            rule.setLastExecuted(LocalDateTime.now());
            rule.setLastResult(result);
            qualityRuleRepository.save(rule);
            
            return QualityExecutionResultDTO.builder()
                .ruleId(rule.getId())
                .ruleName(rule.getRuleName())
                .result(result)
                .actualValue(actualValue)
                .thresholdValue(rule.getThresholdValue())
                .message(message)
                .executedAt(LocalDateTime.now())
                .build();
                
        } catch (Exception e) {
            log.error("Error executing rule {}: {}", rule.getRuleName(), e.getMessage());
            return QualityExecutionResultDTO.builder()
                .ruleId(rule.getId())
                .ruleName(rule.getRuleName())
                .result("WARNING")
                .message("Error executing rule: " + e.getMessage())
                .executedAt(LocalDateTime.now())
                .build();
        }
    }
    
    /**
     * Create a new quality rule
     */
    @Transactional
    public QualityRuleDTO createQualityRule(QualityRuleDTO dto, String username) {
        log.info("Creating quality rule: {}", dto.getRuleName());
        
        QualityRule rule = QualityRule.builder()
            .ruleName(dto.getRuleName())
            .ruleType(dto.getRuleType())
            .ruleExpression(dto.getRuleExpression())
            .description(dto.getDescription())
            .thresholdValue(dto.getThresholdValue())
            .isActive(dto.getIsActive() != null ? dto.getIsActive() : true)
            .severity(dto.getSeverity() != null ? dto.getSeverity() : "MEDIUM")
            .createdBy(username)
            .build();
        
        if (dto.getTableId() != null) {
            TableMetadata table = tableRepository.findById(dto.getTableId())
                .orElseThrow(() -> new RuntimeException("Table not found"));
            rule.setTable(table);
        }
        
        if (dto.getFieldId() != null) {
            FieldMetadata field = fieldRepository.findById(dto.getFieldId())
                .orElseThrow(() -> new RuntimeException("Field not found"));
            rule.setField(field);
        }
        
        rule = qualityRuleRepository.save(rule);
        return mapToDTO(rule);
    }
    
    /**
     * Get all quality rules for a table
     */
    public List<QualityRuleDTO> getTableQualityRules(Long tableId) {
        TableMetadata table = tableRepository.findById(tableId)
            .orElseThrow(() -> new RuntimeException("Table not found"));
        
        List<QualityRule> rules = qualityRuleRepository.findByTable(table);
        return rules.stream().map(this::mapToDTO).toList();
    }
    
    private QualityRuleDTO mapToDTO(QualityRule rule) {
        return QualityRuleDTO.builder()
            .id(rule.getId())
            .tableId(rule.getTable() != null ? rule.getTable().getId() : null)
            .fieldId(rule.getField() != null ? rule.getField().getId() : null)
            .ruleName(rule.getRuleName())
            .ruleType(rule.getRuleType())
            .ruleExpression(rule.getRuleExpression())
            .description(rule.getDescription())
            .thresholdValue(rule.getThresholdValue())
            .isActive(rule.getIsActive())
            .severity(rule.getSeverity())
            .lastExecuted(rule.getLastExecuted())
            .lastResult(rule.getLastResult())
            .build();
    }
}
