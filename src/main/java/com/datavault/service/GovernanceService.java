package com.datavault.service;

import com.datavault.dto.*;
import com.datavault.entity.*;
import com.datavault.exception.ResourceNotFoundException;
import com.datavault.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

// ========== Governance Service ==========
@Service
@Slf4j
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class GovernanceService {
    
    private final FieldMetadataRepository fieldRepository;
    private final TableMetadataRepository tableRepository;
    private final QualityRuleRepository qualityRuleRepository;
    private final GovernanceAlertRepository alertRepository;
    private final DataStewardRepository stewardRepository;
    private final DataQualityMetricRepository qualityMetricRepository;

    @Cacheable("complianceStatus")
    public ComplianceStatusDTO getComplianceStatus() {
        long totalFields = fieldRepository.count();
        long piiFields = fieldRepository.countBySensitivityLevel(SensitivityLevel.PII);
        long fieldsWithOwners = fieldRepository.countByOwnerEmailNotNull();
        long fieldsWithDescription = fieldRepository.countByDescriptionNotNull();
        
        if (totalFields == 0) {
            return ComplianceStatusDTO.builder()
                .overallScore(0.0)
                .totalFields(0L)
                .piiFields(piiFields)
                .fieldsWithOwners(0L)
                .fieldsWithDescription(0L)
                .ownershipCompliance(0.0)
                .documentationCompliance(0.0)
                .lastAudit(LocalDateTime.now())
                .build();
        }
        double ownershipCompliance = (fieldsWithOwners * 100.0) / totalFields;
        double documentationCompliance = (fieldsWithDescription * 100.0) / totalFields;
        double overallScore = (ownershipCompliance + documentationCompliance) / 2;
        
        return ComplianceStatusDTO.builder()
            .overallScore(overallScore)
            .totalFields(totalFields)
            .piiFields(piiFields)
            .fieldsWithOwners(fieldsWithOwners)
            .fieldsWithDescription(fieldsWithDescription)
            .ownershipCompliance(ownershipCompliance)
            .documentationCompliance(documentationCompliance)
            .lastAudit(LocalDateTime.now())
            .build();
    }

    @Cacheable("qualitySummary")
    public QualitySummaryDTO getQualitySummary() {
        List<TableMetadata> allTables = tableRepository.findAll();
        
        long highQuality = allTables.stream()
            .filter(t -> t.getDataQualityScore() != null && t.getDataQualityScore() >= 95)
            .count();
        long mediumQuality = allTables.stream()
            .filter(t -> t.getDataQualityScore() != null && 
                        t.getDataQualityScore() >= 85 && t.getDataQualityScore() < 95)
            .count();
        long lowQuality = allTables.stream()
            .filter(t -> t.getDataQualityScore() != null && t.getDataQualityScore() < 85)
            .count();
        
        double avgScore = allTables.stream()
            .filter(t -> t.getDataQualityScore() != null)
            .mapToDouble(TableMetadata::getDataQualityScore)
            .average()
            .orElse(0.0);
        
        return QualitySummaryDTO.builder()
            .averageQualityScore(avgScore)
            .highQualityCount(highQuality)
            .mediumQualityCount(mediumQuality)
            .lowQualityCount(lowQuality)
            .totalTables((long) allTables.size())
            .build();
    }

    public Page<FieldMetadataDTO> getPIIFields(Pageable pageable) {
        Page<FieldMetadata> piiFields = fieldRepository
            .findBySensitivityLevel(SensitivityLevel.PII, pageable);
        return piiFields.map(this::mapToFieldDTO);
    }

    public List<DataStewardDTO> getDataStewards() {
        List<DataSteward> stewards = stewardRepository.findAll();
        return stewards.stream()
            .map(this::mapToStewardDTO)
            .collect(Collectors.toList());
    }

    @Transactional
    public QualityRuleDTO createQualityRule(QualityRuleDTO dto, String username) {
        FieldMetadata field = fieldRepository.findById(dto.getFieldId())
            .orElseThrow(() -> new ResourceNotFoundException("Field not found"));
        
        QualityRule rule = QualityRule.builder()
            .field(field)
            .ruleName(dto.getRuleName())
            .ruleDefinition(dto.getRuleDefinition())
            .ruleType(dto.getRuleType())
            .isActive(true)
            .validationQuery(dto.getValidationQuery())
            .createdBy(username)
            .build();
        
        rule = qualityRuleRepository.save(rule);
        return mapToQualityRuleDTO(rule);
    }

    public List<GovernanceAlertDTO> getAlerts(String severity, Pageable pageable) {
        List<GovernanceAlert> alerts;
        if (severity != null) {
            alerts = alertRepository.findBySeverity(AlertSeverity.valueOf(severity), pageable);
        } else {
            alerts = alertRepository.findByIsResolvedFalse(pageable);
        }
        return alerts.stream()
            .map(this::mapToAlertDTO)
            .collect(Collectors.toList());
    }

    private FieldMetadataDTO mapToFieldDTO(FieldMetadata field) {
        return FieldMetadataDTO.builder()
            .id(field.getId())
            .fieldName(field.getFieldName())
            .businessName(field.getBusinessName())
            .dataType(field.getDataType())
            .sensitivityLevel(field.getSensitivityLevel().name())
            .ownerEmail(field.getOwnerEmail())
            .ownerName(field.getOwnerName())
            .build();
    }

    private DataStewardDTO mapToStewardDTO(DataSteward steward) {
        return DataStewardDTO.builder()
            .id(steward.getId())
            .name(steward.getName())
            .email(steward.getEmail())
            .team(steward.getTeam())
            .department(steward.getDepartment())
            .role(steward.getRole())
            .phoneNumber(steward.getPhoneNumber())
            .isActive(steward.getIsActive())
            .responsibilities(steward.getResponsibilities())
            .databaseCount(steward.getDatabases().size())
            .createdAt(steward.getCreatedAt())
            .build();
    }

    private QualityRuleDTO mapToQualityRuleDTO(QualityRule rule) {
        return QualityRuleDTO.builder()
            .id(rule.getId())
            .fieldId(rule.getField() != null ? rule.getField().getId() : null)
            .tableId(rule.getTable() != null ? rule.getTable().getId() : null)
            .ruleName(rule.getRuleName())
            .ruleDefinition(rule.getRuleDefinition())
            .ruleType(rule.getRuleType())
            .ruleExpression(rule.getRuleExpression())
            .description(rule.getDescription())
            .thresholdValue(rule.getThresholdValue())
            .isActive(rule.getIsActive())
            .validationQuery(rule.getValidationQuery())
            .severity(rule.getSeverity())
            .lastExecuted(rule.getLastExecuted())
            .lastResult(rule.getLastResult())
            .build();
    }

    private GovernanceAlertDTO mapToAlertDTO(GovernanceAlert alert) {
        return GovernanceAlertDTO.builder()
            .id(alert.getId())
            .severity(alert.getSeverity().name())
            .alertType(alert.getAlertType().name())
            .message(alert.getMessage())
            .recommendedAction(alert.getRecommendedAction())
            .isResolved(alert.getIsResolved())
            .createdAt(alert.getCreatedAt())
            .build();
    }
}
