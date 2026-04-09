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
public class DatabaseService {
    
    private final DatabaseRepository databaseRepository;
    private final TableMetadataRepository tableRepository;
    private final ChangeHistoryRepository changeHistoryRepository;

    @Cacheable("databases")
    public List<DatabaseDTO> getAllDatabases(String username) {
        log.info("Fetching all databases for user: {}", username);
        List<Database> databases = databaseRepository.findAll();
        return databases.stream()
                .map(this::mapToDTO)
                .collect(Collectors.toList());
    }

    @Cacheable(value = "database", key = "#id")
    public DatabaseDTO getDatabaseById(Long id) {
        log.info("Fetching database by id: {}", id);
        Database database = databaseRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Database not found with id: " + id));
        return mapToDTO(database);
    }

    @Transactional
    @CacheEvict(value = "databases", allEntries = true)
    public DatabaseDTO createDatabase(DatabaseDTO dto, String username) {
        log.info("Creating database: {} by user: {}", dto.getName(), username);
        
        Database database = Database.builder()
                .name(dto.getName())
                .type(dto.getType())
                .environment(dto.getEnvironment())
                .connectionString(dto.getConnectionString())
                .owner(dto.getOwner())
                .description(dto.getDescription())
                .createdBy(username)
                .lastModifiedBy(username)
                .build();
        
        database = databaseRepository.save(database);
        
        // Track change
        trackChange(database, ChangeAction.CREATED, "Database created", username);
        
        return mapToDTO(database);
    }

    @Transactional
    @CacheEvict(value = {"database", "databases"}, allEntries = true)
    public DatabaseDTO updateDatabase(Long id, DatabaseDTO dto, String username) {
        log.info("Updating database id: {} by user: {}", id, username);
        
        Database database = databaseRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Database not found with id: " + id));
        
        String changeDetails = buildChangeDetails(database, dto);
        
        database.setName(dto.getName());
        database.setType(dto.getType());
        database.setEnvironment(dto.getEnvironment());
        database.setConnectionString(dto.getConnectionString());
        database.setOwner(dto.getOwner());
        database.setDescription(dto.getDescription());
        database.setLastModifiedBy(username);
        
        database = databaseRepository.save(database);
        
        // Track change
        if (!changeDetails.isEmpty()) {
            trackChange(database, ChangeAction.UPDATED, changeDetails, username);
        }
        
        return mapToDTO(database);
    }

    @Transactional
    @CacheEvict(value = {"database", "databases"}, allEntries = true)
    public void deleteDatabase(Long id) {
        log.info("Deleting database id: {}", id);
        
        Database database = databaseRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Database not found with id: " + id));
        
        databaseRepository.delete(database);
        
        // Track deletion
        trackChange(database, ChangeAction.DELETED, "Database deleted", "system");
    }

    public List<DatabaseDTO> getDatabasesByType(String type) {
        log.info("Fetching databases by type: {}", type);
        List<Database> databases = databaseRepository.findByType(type);
        return databases.stream()
                .map(this::mapToDTO)
                .collect(Collectors.toList());
    }

    public List<DatabaseDTO> getDatabasesByEnvironment(String environment) {
        log.info("Fetching databases by environment: {}", environment);
        List<Database> databases = databaseRepository.findByEnvironment(environment);
        return databases.stream()
                .map(this::mapToDTO)
                .collect(Collectors.toList());
    }

    private DatabaseDTO mapToDTO(Database database) {
        return DatabaseDTO.builder()
                .id(database.getId())
                .name(database.getName())
                .type(database.getType())
                .environment(database.getEnvironment())
                .connectionString(database.getConnectionString())
                .owner(database.getOwner())
                .description(database.getDescription())
                .tableCount(database.getTables() != null ? database.getTables().size() : 0)
                .createdAt(database.getCreatedAt())
                .updatedAt(database.getUpdatedAt())
                .createdBy(database.getCreatedBy())
                .build();
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

    private String buildChangeDetails(Database existing, DatabaseDTO updated) {
        StringBuilder changes = new StringBuilder();
        
        if (!existing.getName().equals(updated.getName())) {
            changes.append("Name changed from ").append(existing.getName())
                   .append(" to ").append(updated.getName()).append("; ");
        }
        if (!existing.getType().equals(updated.getType())) {
            changes.append("Type changed from ").append(existing.getType())
                   .append(" to ").append(updated.getType()).append("; ");
        }
        
        return changes.toString();
    }
}
