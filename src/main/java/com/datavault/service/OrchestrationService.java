package com.datavault.service;

import com.datavault.dto.*;
import com.datavault.entity.*;
import com.datavault.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Orchestration Service - Coordinates all major workflows
 * Provides resilient, end-to-end operations
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class OrchestrationService {
    
    private final MetadataExtractionService metadataExtractionService;
    private final CrossCatalogAnalysisService crossCatalogAnalysisService;
    private final QualityService qualityService;
    private final LineageService lineageService;
    private final DatabaseRepository databaseRepository;
    private final TableMetadataRepository tableRepository;
    private final ChangeHistoryRepository changeHistoryRepository;

    /**
     * Complete database onboarding workflow
     * 1. Create database entry
     * 2. Extract metadata
     * 3. Calculate quality
     * 4. Build lineage
     * 5. Analyze cross-catalog relationships
     */
    @Transactional
    public CompleteDatabaseOnboardingResultDTO onboardDatabase(DatabaseDTO databaseDTO, 
                                                               MetadataExtractionRequestDTO extractionRequest,
                                                               String username) {
        log.info("Starting complete database onboarding for: {}", databaseDTO.getName());
        
        CompleteDatabaseOnboardingResultDTO result = CompleteDatabaseOnboardingResultDTO.builder()
            .startTime(LocalDateTime.now())
            .errors(new ArrayList<>())
            .warnings(new ArrayList<>())
            .build();
        
        try {
            // Step 1: Create database entry
            log.info("Step 1/5: Creating database entry");
            Database database = createDatabaseEntry(databaseDTO, username);
            result.setDatabaseId(database.getId());
            result.setDatabaseName(database.getName());
            
            // Step 2: Extract metadata
            log.info("Step 2/5: Extracting metadata");
            extractionRequest.setDatabaseId(database.getId());
            MetadataExtractionResultDTO extractionResult = metadataExtractionService.extractMetadata(extractionRequest);
            result.setSchemasExtracted(extractionResult.getSchemasExtracted());
            result.setTablesExtracted(extractionResult.getTablesExtracted());
            result.setFieldsExtracted(extractionResult.getFieldsExtracted());
            result.setRelationshipsExtracted(extractionResult.getRelationshipsExtracted());
            
            if (!extractionResult.getSuccess()) {
                result.getErrors().addAll(extractionResult.getErrors());
                result.setSuccess(false);
                return result;
            }
            
            // Step 3: Calculate quality for all tables
            log.info("Step 3/5: Calculating quality metrics");
            int qualityCalculated = calculateQualityForDatabase(database, result);
            result.setQualityRulesExecuted(qualityCalculated);
            
            // Step 4: Build lineage relationships
            log.info("Step 4/5: Building lineage relationships");
            int lineageBuilt = buildLineageForDatabase(database, result);
            result.setLineageRelationshipsBuilt(lineageBuilt);
            
            // Step 5: Analyze cross-catalog relationships
            log.info("Step 5/5: Analyzing cross-catalog relationships");
            int crossCatalogRelationships = analyzeCrossCatalogRelationships(database, result);
            result.setCrossCatalogRelationshipsFound(crossCatalogRelationships);
            
            result.setSuccess(true);
            result.setEndTime(LocalDateTime.now());
            result.setMessage("Database onboarding completed successfully");
            
            // Track completion
            trackChange(database, ChangeAction.CREATED, 
                String.format("Complete onboarding: %d tables, %d fields extracted", 
                    result.getTablesExtracted(), result.getFieldsExtracted()),
                username);
            
        } catch (Exception e) {
            log.error("Error during database onboarding: {}", e.getMessage(), e);
            result.setSuccess(false);
            result.setMessage("Onboarding failed: " + e.getMessage());
            result.getErrors().add(e.getMessage());
            result.setEndTime(LocalDateTime.now());
        }
        
        return result;
    }
    
    /**
     * Refresh complete database metadata
     * Resilient - continues on errors
     */
    @Async("taskExecutor")
    public CompletableFuture<RefreshResultDTO> refreshDatabaseAsync(Long databaseId, String username) {
        log.info("Starting async database refresh for ID: {}", databaseId);
        
        RefreshResultDTO result = RefreshResultDTO.builder()
            .databaseId(databaseId)
            .startTime(LocalDateTime.now())
            .errors(new ArrayList<>())
            .build();
        
        try {
            Database database = databaseRepository.findById(databaseId)
                .orElseThrow(() -> new RuntimeException("Database not found"));
            
            result.setDatabaseName(database.getName());
            
            // Re-extract metadata
            MetadataExtractionRequestDTO request = buildExtractionRequest(database);
            MetadataExtractionResultDTO extractionResult = metadataExtractionService.extractMetadata(request);
            
            result.setTablesRefreshed(extractionResult.getTablesExtracted());
            result.setFieldsRefreshed(extractionResult.getFieldsExtracted());
            
            // Recalculate quality
            int qualityUpdated = calculateQualityForDatabase(database, result);
            result.setQualityRecalculated(qualityUpdated);
            
            // Rebuild lineage
            int lineageUpdated = buildLineageForDatabase(database, result);
            result.setLineageRebuilt(lineageUpdated);
            
            result.setSuccess(true);
            result.setEndTime(LocalDateTime.now());
            
            trackChange(database, ChangeAction.UPDATED, 
                "Metadata refreshed", username);
            
        } catch (Exception e) {
            log.error("Error refreshing database: {}", e.getMessage(), e);
            result.setSuccess(false);
            result.getErrors().add(e.getMessage());
            result.setEndTime(LocalDateTime.now());
        }
        
        return CompletableFuture.completedFuture(result);
    }
    
    /**
     * Execute quality assessment across all databases
     */
    public QualityAssessmentResultDTO assessQualityAcrossAllDatabases() {
        log.info("Starting quality assessment across all databases");
        
        QualityAssessmentResultDTO result = QualityAssessmentResultDTO.builder()
            .startTime(LocalDateTime.now())
            .databaseResults(new ArrayList<>())
            .build();
        
        List<Database> databases = databaseRepository.findAll();
        int totalTables = 0;
        int tablesAssessed = 0;
        double totalScore = 0.0;
        
        for (Database database : databases) {
            try {
                List<TableMetadata> tables = tableRepository.findByDatabase(database);
                totalTables += tables.size();
                
                for (TableMetadata table : tables) {
                    try {
                        TableQualityReportDTO report = qualityService.calculateTableQuality(table.getId());
                        tablesAssessed++;
                        totalScore += report.getOverallScore();
                    } catch (Exception e) {
                        log.error("Error assessing table {}: {}", table.getTableName(), e.getMessage());
                    }
                }
            } catch (Exception e) {
                log.error("Error assessing database {}: {}", database.getName(), e.getMessage());
            }
        }
        
        result.setTotalDatabases(databases.size());
        result.setTotalTables(totalTables);
        result.setTablesAssessed(tablesAssessed);
        result.setAverageQualityScore(tablesAssessed > 0 ? totalScore / tablesAssessed : 0.0);
        result.setEndTime(LocalDateTime.now());
        
        return result;
    }
    
    /**
     * Build comprehensive data catalog report
     */
    public DataCatalogReportDTO generateCatalogReport() {
        log.info("Generating comprehensive data catalog report");
        
        DataCatalogReportDTO report = DataCatalogReportDTO.builder()
            .generatedAt(LocalDateTime.now())
            .build();
        
        // Gather statistics
        List<Database> databases = databaseRepository.findAll();
        report.setTotalDatabases(databases.size());
        
        int totalTables = 0;
        int totalFields = 0;
        double avgQuality = 0.0;
        
        for (Database database : databases) {
            List<TableMetadata> tables = tableRepository.findByDatabase(database);
            totalTables += tables.size();
            
            for (TableMetadata table : tables) {
                totalFields += fieldRepository.findByTable(table).size();
                if (table.getDataQualityScore() != null) {
                    avgQuality += table.getDataQualityScore();
                }
            }
        }
        
        report.setTotalTables(totalTables);
        report.setTotalFields(totalFields);
        report.setAverageQualityScore(totalTables > 0 ? avgQuality / totalTables : 0.0);
        
        // Cross-catalog relationships
        List<CrossCatalogRelationshipDTO> crossCatalog = crossCatalogAnalysisService.buildCrossCatalogRelationships();
        report.setCrossCatalogRelationships(crossCatalog.size());
        
        // PII audit
        List<Map<String, Object>> piiAudit = crossCatalogAnalysisService.auditPIIFields();
        report.setPiiFieldsFound(piiAudit.size());
        
        return report;
    }
    
    // ==================== HELPER METHODS ====================
    
    private Database createDatabaseEntry(DatabaseDTO dto, String username) {
        // Set defaults
        if (dto.getEnvironment() == null || dto.getEnvironment().trim().isEmpty()) {
            dto.setEnvironment("development");
        }
        if (dto.getOwner() == null || dto.getOwner().trim().isEmpty()) {
            dto.setOwner(username);
        }
        
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
        
        return databaseRepository.save(database);
    }
    
    private int calculateQualityForDatabase(Database database, Object result) {
        int calculated = 0;
        List<TableMetadata> tables = tableRepository.findByDatabase(database);
        
        for (TableMetadata table : tables) {
            try {
                qualityService.calculateTableQuality(table.getId());
                calculated++;
            } catch (Exception e) {
                log.error("Error calculating quality for table {}: {}", table.getTableName(), e.getMessage());
                if (result instanceof CompleteDatabaseOnboardingResultDTO) {
                    ((CompleteDatabaseOnboardingResultDTO) result).getWarnings()
                        .add("Quality calculation failed for table: " + table.getTableName());
                }
            }
        }
        
        return calculated;
    }
    
    private int buildLineageForDatabase(Database database, Object result) {
        int built = 0;
        // Lineage is already built during extraction via foreign keys
        // This would rebuild or validate lineage
        return built;
    }
    
    private int analyzeCrossCatalogRelationships(Database database, Object result) {
        try {
            List<CrossCatalogRelationshipDTO> relationships = 
                crossCatalogAnalysisService.buildCrossCatalogRelationships();
            return (int) relationships.stream()
                .filter(r -> r.getSourceDatabaseId().equals(database.getId()) || 
                           r.getTargetDatabaseId().equals(database.getId()))
                .count();
        } catch (Exception e) {
            log.error("Error analyzing cross-catalog relationships: {}", e.getMessage());
            return 0;
        }
    }
    
    private MetadataExtractionRequestDTO buildExtractionRequest(Database database) {
        return MetadataExtractionRequestDTO.builder()
            .databaseId(database.getId())
            .connectionString(database.getConnectionString())
            .username("admin") // Would come from vault
            .password("password") // Would come from vault
            .databaseType(database.getType())
            .extractLineage(true)
            .calculateQuality(true)
            .build();
    }
    
    private void trackChange(Database database, ChangeAction action, String description, String username) {
        try {
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
        } catch (Exception e) {
            log.error("Error tracking change: {}", e.getMessage());
        }
    }
    
    private final FieldMetadataRepository fieldRepository;
}
