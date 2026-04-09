package com.datavault.service;

import com.datavault.dto.*;
import com.datavault.entity.*;
import com.datavault.repository.*;
import com.datavault.exception.ResourceNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class FieldService {
    
    private final FieldMetadataRepository fieldRepository;
    private final TableMetadataRepository tableRepository;
    private final GlossaryTermRepository glossaryTermRepository;
    private final ChangeHistoryRepository changeHistoryRepository;

    public List<FieldMetadataDTO> getFieldsByTable(Long tableId, String sensitivity) {
        log.info("Fetching fields for table id: {} with sensitivity: {}", tableId, sensitivity);
        
        List<FieldMetadata> fields;
        if (sensitivity != null) {
            SensitivityLevel level = SensitivityLevel.valueOf(sensitivity);
            fields = fieldRepository.findByTableAndSensitivity(tableId, level);
        } else {
            fields = fieldRepository.findByTableId(tableId);
        }
        
        return fields.stream()
                .map(this::mapToDTO)
                .collect(Collectors.toList());
    }

    @Cacheable(value = "field", key = "#id")
    public FieldMetadataDTO getFieldById(Long id) {
        log.info("Fetching field by id: {}", id);
        FieldMetadata field = fieldRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Field not found with id: " + id));
        return mapToDTO(field);
    }

    @Transactional
    @CacheEvict(value = {"field", "fields"}, allEntries = true)
    public FieldMetadataDTO createField(FieldMetadataDTO dto, String username, String email) {
        log.info("Creating field: {} by user: {}", dto.getFieldName(), username);
        
        TableMetadata table = tableRepository.findById(dto.getTableId())
                .orElseThrow(() -> new ResourceNotFoundException("Table not found"));
        
        GlossaryTerm glossaryTerm = null;
        if (dto.getGlossaryTermId() != null) {
            glossaryTerm = glossaryTermRepository.findById(dto.getGlossaryTermId()).orElse(null);
        }
        
        FieldMetadata field = FieldMetadata.builder()
                .table(table)
                .fieldName(dto.getFieldName())
                .businessName(dto.getBusinessName())
                .dataType(dto.getDataType())
                .description(dto.getDescription())
                .businessGlossary(dto.getBusinessGlossary())
                .glossaryTerm(glossaryTerm)
                .sensitivityLevel(dto.getSensitivityLevel() != null ? 
                    SensitivityLevel.valueOf(dto.getSensitivityLevel()) : SensitivityLevel.INTERNAL)
                .ownerEmail(dto.getOwnerEmail() != null ? dto.getOwnerEmail() : email)
                .ownerName(dto.getOwnerName() != null ? dto.getOwnerName() : username)
                .isPrimaryKey(dto.getIsPrimaryKey() != null ? dto.getIsPrimaryKey() : false)
                .isForeignKey(dto.getIsForeignKey() != null ? dto.getIsForeignKey() : false)
                .isNullable(dto.getIsNullable() != null ? dto.getIsNullable() : true)
                .defaultValue(dto.getDefaultValue())
                .maxLength(dto.getMaxLength())
                .qualityRules(dto.getQualityRules())
                .createdBy(username)
                .lastModifiedBy(username)
                .build();
        
        field = fieldRepository.save(field);
        
        // Track change
        trackChange(field, ChangeAction.CREATED, "Field created", username);
        
        return mapToDTO(field);
    }

    @Transactional
    @CacheEvict(value = {"field", "fields"}, allEntries = true)
    public FieldMetadataDTO updateField(Long id, FieldMetadataDTO dto, String username) {
        log.info("Updating field id: {} by user: {}", id, username);
        
        FieldMetadata field = fieldRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Field not found with id: " + id));
        
        String changeDetails = buildChangeDetails(field, dto);
        
        field.setFieldName(dto.getFieldName());
        field.setBusinessName(dto.getBusinessName());
        field.setDataType(dto.getDataType());
        field.setDescription(dto.getDescription());
        field.setBusinessGlossary(dto.getBusinessGlossary());
        
        if (dto.getSensitivityLevel() != null) {
            field.setSensitivityLevel(SensitivityLevel.valueOf(dto.getSensitivityLevel()));
        }
        
        if (dto.getOwnerEmail() != null) {
            field.setOwnerEmail(dto.getOwnerEmail());
        }
        if (dto.getOwnerName() != null) {
            field.setOwnerName(dto.getOwnerName());
        }
        
        field.setIsPrimaryKey(dto.getIsPrimaryKey());
        field.setIsForeignKey(dto.getIsForeignKey());
        field.setIsNullable(dto.getIsNullable());
        field.setDefaultValue(dto.getDefaultValue());
        field.setMaxLength(dto.getMaxLength());
        field.setQualityRules(dto.getQualityRules());
        field.setLastModifiedBy(username);
        
        if (dto.getGlossaryTermId() != null) {
            GlossaryTerm glossaryTerm = glossaryTermRepository.findById(dto.getGlossaryTermId()).orElse(null);
            field.setGlossaryTerm(glossaryTerm);
        }
        
        field = fieldRepository.save(field);
        
        // Track change
        if (!changeDetails.isEmpty()) {
            trackChange(field, ChangeAction.UPDATED, changeDetails, username);
        }
        
        return mapToDTO(field);
    }

    public List<FieldMetadataDTO> getFieldsByOwner(String ownerEmail) {
        log.info("Fetching fields owned by: {}", ownerEmail);
        List<FieldMetadata> fields = fieldRepository.findByOwnerEmail(ownerEmail);
        return fields.stream()
                .map(this::mapToDTO)
                .collect(Collectors.toList());
    }

    private FieldMetadataDTO mapToDTO(FieldMetadata field) {
        return FieldMetadataDTO.builder()
                .id(field.getId())
                .tableId(field.getTable().getId())
                .tableName(field.getTable().getTableName())
                .databaseName(field.getTable().getDatabase().getName())
                .fieldName(field.getFieldName())
                .businessName(field.getBusinessName())
                .dataType(field.getDataType())
                .description(field.getDescription())
                .businessGlossary(field.getBusinessGlossary())
                .glossaryTermId(field.getGlossaryTerm() != null ? field.getGlossaryTerm().getId() : null)
                .glossaryTermName(field.getGlossaryTerm() != null ? field.getGlossaryTerm().getTerm() : null)
                .sensitivityLevel(field.getSensitivityLevel() != null ? field.getSensitivityLevel().name() : null)
                .ownerEmail(field.getOwnerEmail())
                .ownerName(field.getOwnerName())
                .isPrimaryKey(field.getIsPrimaryKey())
                .isForeignKey(field.getIsForeignKey())
                .isNullable(field.getIsNullable())
                .defaultValue(field.getDefaultValue())
                .maxLength(field.getMaxLength())
                .qualityRules(field.getQualityRules())
                .createdAt(field.getCreatedAt())
                .updatedAt(field.getUpdatedAt())
                .build();
    }

    private void trackChange(FieldMetadata field, ChangeAction action, String description, String username) {
        ChangeHistory change = ChangeHistory.builder()
                .entityType("FIELD")
                .entityId(field.getId())
                .entityName(field.getFieldName())
                .action(action)
                .description(description)
                .changedBy(username)
                .changedAt(LocalDateTime.now())
                .build();
        
        changeHistoryRepository.save(change);
    }

    private String buildChangeDetails(FieldMetadata existing, FieldMetadataDTO updated) {
        StringBuilder changes = new StringBuilder();
        
        if (!existing.getFieldName().equals(updated.getFieldName())) {
            changes.append("Name changed from ").append(existing.getFieldName())
                   .append(" to ").append(updated.getFieldName()).append("; ");
        }
        if (!existing.getDataType().equals(updated.getDataType())) {
            changes.append("Type changed from ").append(existing.getDataType())
                   .append(" to ").append(updated.getDataType()).append("; ");
        }
        
        return changes.toString();
    }
}
