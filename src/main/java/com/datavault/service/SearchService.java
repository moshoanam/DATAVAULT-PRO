package com.datavault.service;

import com.datavault.dto.*;
import com.datavault.entity.*;
import com.datavault.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class SearchService {
    
    private final DatabaseRepository databaseRepository;
    private final TableMetadataRepository tableRepository;
    private final FieldMetadataRepository fieldRepository;
    private final GlossaryTermRepository glossaryTermRepository;

    public SearchResultsDTO search(String query, String type, String sensitivity, Pageable pageable) {
        log.info("Searching for: {} with type: {} and sensitivity: {}", query, type, sensitivity);
        
        SearchResultsDTO results = new SearchResultsDTO();
        results.setQuery(query);
        
        long totalResults = 0;
        
        // Search databases
        if (type == null || "database".equalsIgnoreCase(type)) {
            List<Database> databases = searchDatabases(query);
            results.setDatabases(databases.stream()
                    .map(this::mapDatabaseToDTO)
                    .collect(Collectors.toList()));
            totalResults += databases.size();
        }
        
        // Search tables
        if (type == null || "table".equalsIgnoreCase(type)) {
            List<TableMetadata> tables = searchTables(query);
            results.setTables(tables.stream()
                    .map(this::mapTableToDTO)
                    .collect(Collectors.toList()));
            totalResults += tables.size();
        }
        
        // Search fields
        if (type == null || "field".equalsIgnoreCase(type)) {
            Page<FieldMetadata> fields = fieldRepository.searchFields(query, pageable);
            
            List<FieldMetadataDTO> fieldDTOs = fields.getContent().stream()
                    .filter(f -> sensitivity == null || 
                                f.getSensitivityLevel().name().equalsIgnoreCase(sensitivity))
                    .map(this::mapFieldToDTO)
                    .collect(Collectors.toList());
            
            results.setFields(fieldDTOs);
            totalResults += fields.getTotalElements();
        }
        
        // Search glossary terms
        if (type == null || "glossary".equalsIgnoreCase(type)) {
            Page<GlossaryTerm> terms = glossaryTermRepository.searchTerms(query, pageable);
            results.setGlossaryTerms(terms.getContent().stream()
                    .map(this::mapGlossaryToDTO)
                    .collect(Collectors.toList()));
            totalResults += terms.getTotalElements();
        }
        
        results.setTotalResults(totalResults);
        
        log.info("Search completed. Found {} results", totalResults);
        
        return results;
    }

    private List<Database> searchDatabases(String query) {
        List<Database> allDatabases = databaseRepository.findAll();
        return allDatabases.stream()
                .filter(db -> matchesQuery(db, query))
                .collect(Collectors.toList());
    }

    private List<TableMetadata> searchTables(String query) {
        List<TableMetadata> allTables = tableRepository.findAll();
        return allTables.stream()
                .filter(table -> matchesQuery(table, query))
                .limit(20)
                .collect(Collectors.toList());
    }

    private boolean matchesQuery(Database db, String query) {
        String lowerQuery = query.toLowerCase();
        return db.getName().toLowerCase().contains(lowerQuery) ||
               (db.getDescription() != null && db.getDescription().toLowerCase().contains(lowerQuery)) ||
               (db.getOwner() != null && db.getOwner().toLowerCase().contains(lowerQuery));
    }

    private boolean matchesQuery(TableMetadata table, String query) {
        String lowerQuery = query.toLowerCase();
        return table.getTableName().toLowerCase().contains(lowerQuery) ||
               table.getSchema().toLowerCase().contains(lowerQuery) ||
               (table.getDescription() != null && table.getDescription().toLowerCase().contains(lowerQuery));
    }

    private DatabaseDTO mapDatabaseToDTO(Database database) {
        return DatabaseDTO.builder()
                .id(database.getId())
                .name(database.getName())
                .type(database.getType())
                .environment(database.getEnvironment())
                .description(database.getDescription())
                .owner(database.getOwner())
                .tableCount(database.getTables() != null ? database.getTables().size() : 0)
                .build();
    }

    private TableMetadataDTO mapTableToDTO(TableMetadata table) {
        return TableMetadataDTO.builder()
                .id(table.getId())
                .databaseId(table.getDatabase().getId())
                .databaseName(table.getDatabase().getName())
                .schema(table.getSchema())
                .tableName(table.getTableName())
                .description(table.getDescription())
                .rowCount(table.getRowCount())
                .dataQualityScore(table.getDataQualityScore())
                .fieldCount(table.getFields() != null ? table.getFields().size() : 0)
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
                .ownerEmail(field.getOwnerEmail())
                .ownerName(field.getOwnerName())
                .build();
    }

    private GlossaryTermDTO mapGlossaryToDTO(GlossaryTerm term) {
        return GlossaryTermDTO.builder()
                .id(term.getId())
                .term(term.getTerm())
                .definition(term.getDefinition())
                .category(term.getCategory())
                .relatedTerms(term.getRelatedTerms())
                .synonyms(term.getSynonyms())
                .usageCount(term.getUsedInFields() != null ? term.getUsedInFields().size() : 0)
                .build();
    }
}
