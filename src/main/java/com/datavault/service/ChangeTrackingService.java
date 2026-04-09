package com.datavault.service;

import com.datavault.dto.*;
import com.datavault.entity.*;
import com.datavault.repository.*;
import com.datavault.exception.ResourceNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
public class ChangeTrackingService {
    
    private final ChangeHistoryRepository changeHistoryRepository;

    @Cacheable("changeHistory")
    public Page<ChangeHistoryDTO> getChangeHistory(String entityType, Long entityId, 
                                                    String action, Pageable pageable) {
        log.info("Fetching change history for entityType: {}, entityId: {}, action: {}", 
                 entityType, entityId, action);
        
        ChangeAction changeAction = action != null ? ChangeAction.valueOf(action) : null;
        
        Page<ChangeHistory> changes = changeHistoryRepository.findByFilters(
                entityType, entityId, changeAction, pageable);
        
        return changes.map(this::mapToDTO);
    }

    @Cacheable(value = "change", key = "#id")
    public ChangeHistoryDTO getChangeById(Long id) {
        log.info("Fetching change by id: {}", id);
        ChangeHistory change = changeHistoryRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Change not found with id: " + id));
        return mapToDTO(change);
    }

    public List<VersionDTO> getVersionHistory(String entityType, Long entityId) {
        log.info("Fetching version history for entityType: {}, entityId: {}", entityType, entityId);
        
        List<ChangeHistory> changes = changeHistoryRepository
                .findByEntityTypeAndEntityId(entityType, entityId);
        
        // Group changes by version tag
        Map<String, List<ChangeHistory>> versionMap = new LinkedHashMap<>();
        
        for (ChangeHistory change : changes) {
            String versionTag = change.getVersionTag() != null ? 
                    change.getVersionTag() : "v1.0." + change.getId();
            
            versionMap.computeIfAbsent(versionTag, k -> new ArrayList<>()).add(change);
        }
        
        // Convert to VersionDTO
        return versionMap.entrySet().stream()
                .map(entry -> VersionDTO.builder()
                        .versionTag(entry.getKey())
                        .versionDate(entry.getValue().get(0).getChangedAt())
                        .changedBy(entry.getValue().get(0).getChangedBy())
                        .changes(entry.getValue().stream()
                                .map(this::mapToDTO)
                                .collect(Collectors.toList()))
                        .build())
                .collect(Collectors.toList());
    }

    public List<ChangeHistoryDTO> getRecentChanges(int limit) {
        log.info("Fetching {} recent changes", limit);
        
        Page<ChangeHistory> changes = changeHistoryRepository.findAll(
                org.springframework.data.domain.PageRequest.of(0, limit,
                        org.springframework.data.domain.Sort.by(
                                org.springframework.data.domain.Sort.Direction.DESC, "changedAt")));
        
        return changes.getContent().stream()
                .map(this::mapToDTO)
                .collect(Collectors.toList());
    }

    public List<ChangeHistoryDTO> getChangesByDateRange(LocalDateTime start, LocalDateTime end) {
        log.info("Fetching changes between {} and {}", start, end);
        
        List<ChangeHistory> changes = changeHistoryRepository.findByDateRange(start, end);
        
        return changes.stream()
                .map(this::mapToDTO)
                .collect(Collectors.toList());
    }

    public List<ChangeHistoryDTO> getChangesByUser(String username) {
        log.info("Fetching changes by user: {}", username);
        
        List<ChangeHistory> allChanges = changeHistoryRepository.findAll();
        
        return allChanges.stream()
                .filter(change -> username.equals(change.getChangedBy()))
                .map(this::mapToDTO)
                .collect(Collectors.toList());
    }

    public Map<String, Long> getChangeSummary() {
        log.info("Calculating change summary");
        
        List<ChangeHistory> allChanges = changeHistoryRepository.findAll();
        
        Map<String, Long> summary = new HashMap<>();
        summary.put("totalChanges", (long) allChanges.size());
        summary.put("created", allChanges.stream().filter(c -> c.getAction() == ChangeAction.CREATED).count());
        summary.put("updated", allChanges.stream().filter(c -> c.getAction() == ChangeAction.UPDATED).count());
        summary.put("deleted", allChanges.stream().filter(c -> c.getAction() == ChangeAction.DELETED).count());
        
        return summary;
    }

    public Map<String, Long> getChangesByEntityType() {
        log.info("Getting changes grouped by entity type");
        
        List<ChangeHistory> allChanges = changeHistoryRepository.findAll();
        
        return allChanges.stream()
                .collect(Collectors.groupingBy(
                        ChangeHistory::getEntityType,
                        Collectors.counting()
                ));
    }

    @Transactional
    public void createChangeEntry(String entityType, Long entityId, String entityName, 
                                   ChangeAction action, String description, String username) {
        log.info("Creating change entry for {} {} by {}", entityType, entityId, username);
        
        ChangeHistory change = ChangeHistory.builder()
                .entityType(entityType)
                .entityId(entityId)
                .entityName(entityName)
                .action(action)
                .description(description)
                .changedBy(username)
                .changedAt(LocalDateTime.now())
                .build();
        
        changeHistoryRepository.save(change);
    }

    @Transactional
    public void createVersionedChange(String entityType, Long entityId, String entityName, 
                                      ChangeAction action, String description, 
                                      String versionTag, String username) {
        log.info("Creating versioned change entry for {} {} version {} by {}", 
                 entityType, entityId, versionTag, username);
        
        ChangeHistory change = ChangeHistory.builder()
                .entityType(entityType)
                .entityId(entityId)
                .entityName(entityName)
                .action(action)
                .description(description)
                .versionTag(versionTag)
                .changedBy(username)
                .changedAt(LocalDateTime.now())
                .build();
        
        changeHistoryRepository.save(change);
    }

    private ChangeHistoryDTO mapToDTO(ChangeHistory change) {
        return ChangeHistoryDTO.builder()
                .id(change.getId())
                .entityType(change.getEntityType())
                .entityId(change.getEntityId())
                .entityName(change.getEntityName())
                .action(change.getAction() != null ? change.getAction().name() : null)
                .description(change.getDescription())
                .changeDetails(change.getChangeDetails())
                .versionTag(change.getVersionTag())
                .changedAt(change.getChangedAt())
                .changedBy(change.getChangedBy())
                .build();
    }
}
