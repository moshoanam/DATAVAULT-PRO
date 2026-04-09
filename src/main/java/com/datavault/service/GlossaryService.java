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
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class GlossaryService {
    
    private final GlossaryTermRepository glossaryTermRepository;
    private final FieldMetadataRepository fieldRepository;
    private final ChangeHistoryRepository changeHistoryRepository;

    @Cacheable("glossaryTerms")
    public Page<GlossaryTermDTO> getTerms(String category, Pageable pageable) {
        log.info("Fetching glossary terms for category: {}", category);
        
        Page<GlossaryTerm> terms;
        if (category != null && !category.isEmpty()) {
            terms = glossaryTermRepository.findByCategory(category, pageable);
        } else {
            terms = glossaryTermRepository.findAll(pageable);
        }
        
        return terms.map(this::mapToDTO);
    }

    @Cacheable(value = "glossaryTerm", key = "#id")
    public GlossaryTermDTO getTermById(Long id) {
        log.info("Fetching glossary term by id: {}", id);
        GlossaryTerm term = glossaryTermRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Glossary term not found with id: " + id));
        return mapToDTO(term);
    }

    @Transactional
    @CacheEvict(value = "glossaryTerms", allEntries = true)
    public GlossaryTermDTO createTerm(GlossaryTermDTO dto, String username) {
        log.info("Creating glossary term: {} by user: {}", dto.getTerm(), username);
        
        // Check if term already exists
        glossaryTermRepository.findByTerm(dto.getTerm())
                .ifPresent(existing -> {
                    throw new IllegalArgumentException("Term already exists: " + dto.getTerm());
                });
        
        GlossaryTerm term = GlossaryTerm.builder()
                .term(dto.getTerm())
                .definition(dto.getDefinition())
                .category(dto.getCategory())
                .relatedTerms(dto.getRelatedTerms())
                .synonyms(dto.getSynonyms())
                .createdBy(username)
                .lastModifiedBy(username)
                .build();
        
        term = glossaryTermRepository.save(term);
        
        // Track change
        trackChange(term, ChangeAction.CREATED, "Glossary term created", username);
        
        return mapToDTO(term);
    }

    @Transactional
    @CacheEvict(value = {"glossaryTerm", "glossaryTerms"}, allEntries = true)
    public GlossaryTermDTO updateTerm(Long id, GlossaryTermDTO dto, String username) {
        log.info("Updating glossary term id: {} by user: {}", id, username);
        
        GlossaryTerm term = glossaryTermRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Glossary term not found with id: " + id));
        
        term.setTerm(dto.getTerm());
        term.setDefinition(dto.getDefinition());
        term.setCategory(dto.getCategory());
        term.setRelatedTerms(dto.getRelatedTerms());
        term.setSynonyms(dto.getSynonyms());
        term.setLastModifiedBy(username);
        
        term = glossaryTermRepository.save(term);
        
        // Track change
        trackChange(term, ChangeAction.UPDATED, "Glossary term updated", username);
        
        return mapToDTO(term);
    }

    @Transactional
    @CacheEvict(value = {"glossaryTerm", "glossaryTerms"}, allEntries = true)
    public void deleteTerm(Long id) {
        log.info("Deleting glossary term id: {}", id);
        
        GlossaryTerm term = glossaryTermRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Glossary term not found with id: " + id));
        
        glossaryTermRepository.delete(term);
        
        // Track deletion
        trackChange(term, ChangeAction.DELETED, "Glossary term deleted", "system");
    }

    public List<FieldMetadataDTO> getTermUsage(Long id) {
        log.info("Fetching usage for glossary term id: {}", id);
        
        GlossaryTerm term = glossaryTermRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Glossary term not found with id: " + id));
        
        List<FieldMetadata> fields = fieldRepository.findByGlossaryTerm(term);
        
        return fields.stream()
                .map(this::mapFieldToDTO)
                .collect(Collectors.toList());
    }

    public List<GlossaryTermDTO> searchTerms(String query) {
        log.info("Searching glossary terms for: {}", query);
        
        List<GlossaryTerm> allTerms = glossaryTermRepository.findAll();
        
        return allTerms.stream()
                .filter(term -> matchesQuery(term, query))
                .map(this::mapToDTO)
                .collect(Collectors.toList());
    }

    private boolean matchesQuery(GlossaryTerm term, String query) {
        String lowerQuery = query.toLowerCase();
        return term.getTerm().toLowerCase().contains(lowerQuery) ||
               term.getDefinition().toLowerCase().contains(lowerQuery) ||
               (term.getCategory() != null && term.getCategory().toLowerCase().contains(lowerQuery));
    }

    private GlossaryTermDTO mapToDTO(GlossaryTerm term) {
        return GlossaryTermDTO.builder()
                .id(term.getId())
                .term(term.getTerm())
                .definition(term.getDefinition())
                .category(term.getCategory())
                .relatedTerms(term.getRelatedTerms())
                .synonyms(term.getSynonyms())
                .usageCount(term.getUsedInFields() != null ? term.getUsedInFields().size() : 0)
                .createdAt(term.getCreatedAt())
                .updatedAt(term.getUpdatedAt())
                .build();
    }

    private FieldMetadataDTO mapFieldToDTO(FieldMetadata field) {
        return FieldMetadataDTO.builder()
                .id(field.getId())
                .tableId(field.getTable().getId())
                .tableName(field.getTable().getTableName())
                .databaseName(field.getTable().getDatabase().getName())
                .fieldName(field.getFieldName())
                .businessName(field.getBusinessName())
                .dataType(field.getDataType())
                .description(field.getDescription())
                .sensitivityLevel(field.getSensitivityLevel() != null ? field.getSensitivityLevel().name() : null)
                .build();
    }

    private void trackChange(GlossaryTerm term, ChangeAction action, String description, String username) {
        ChangeHistory change = ChangeHistory.builder()
                .entityType("GLOSSARY_TERM")
                .entityId(term.getId())
                .entityName(term.getTerm())
                .action(action)
                .description(description)
                .changedBy(username)
                .changedAt(LocalDateTime.now())
                .build();
        
        changeHistoryRepository.save(change);
    }
}
