package com.datavault.controller;

import com.datavault.dto.*;
import com.datavault.service.OrchestrationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/v1/orchestration")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Orchestration", description = "End-to-end workflow orchestration")
public class OrchestrationController {
    
    private final OrchestrationService orchestrationService;

    @PostMapping("/onboard-database")
    @Operation(summary = "Complete database onboarding", 
               description = "One-click: create DB, extract metadata, calculate quality, build lineage")
    public ResponseEntity<?> onboardDatabase(@RequestBody CompleteOnboardingRequestDTO request) {
        try {
            log.info("Starting complete database onboarding for: {}", request.getDatabase().getName());
            
            // Validate request
            if (request.getDatabase() == null) {
                return ResponseEntity.badRequest().body(
                    Map.of("error", "Database information is required")
                );
            }
            
            if (request.getExtractionRequest() == null) {
                return ResponseEntity.badRequest().body(
                    Map.of("error", "Extraction request is required")
                );
            }
            
            String username = "admin"; // Would come from auth
            
            CompleteDatabaseOnboardingResultDTO result = orchestrationService.onboardDatabase(
                request.getDatabase(),
                request.getExtractionRequest(),
                username
            );
            
            if (result.getSuccess()) {
                return ResponseEntity.ok(result);
            } else {
                return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT).body(result);
            }
            
        } catch (Exception e) {
            log.error("Error in database onboarding: {}", e.getMessage(), e);
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Onboarding failed");
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }
    
    @PostMapping("/databases/{id}/refresh-complete")
    @Operation(summary = "Complete database refresh", 
               description = "Re-extract metadata, recalculate quality, rebuild lineage")
    public ResponseEntity<?> refreshDatabase(@PathVariable Long id) {
        try {
            log.info("Starting complete database refresh for ID: {}", id);
            
            String username = "admin";
            CompletableFuture<RefreshResultDTO> futureResult = 
                orchestrationService.refreshDatabaseAsync(id, username);
            
            // Return immediately with accepted status
            Map<String, Object> response = new HashMap<>();
            response.put("message", "Database refresh started");
            response.put("databaseId", id);
            response.put("status", "PROCESSING");
            
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
            
        } catch (Exception e) {
            log.error("Error starting database refresh: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(
                Map.of("error", "Failed to start refresh", "message", e.getMessage())
            );
        }
    }
    
    @PostMapping("/assess-quality-all")
    @Operation(summary = "Assess quality across all databases", 
               description = "Calculate quality metrics for every table in every database")
    public ResponseEntity<?> assessQualityAll() {
        try {
            log.info("Starting quality assessment across all databases");
            
            QualityAssessmentResultDTO result = orchestrationService.assessQualityAcrossAllDatabases();
            
            return ResponseEntity.ok(result);
            
        } catch (Exception e) {
            log.error("Error in quality assessment: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(
                Map.of("error", "Quality assessment failed", "message", e.getMessage())
            );
        }
    }
    
    @GetMapping("/catalog-report")
    @Operation(summary = "Generate catalog report", 
               description = "Comprehensive report of all catalog statistics")
    public ResponseEntity<?> getCatalogReport() {
        try {
            log.info("Generating catalog report");
            
            DataCatalogReportDTO report = orchestrationService.generateCatalogReport();
            
            return ResponseEntity.ok(report);
            
        } catch (Exception e) {
            log.error("Error generating catalog report: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(
                Map.of("error", "Report generation failed", "message", e.getMessage())
            );
        }
    }
}

@lombok.Data
@lombok.Builder
@lombok.NoArgsConstructor
@lombok.AllArgsConstructor
class CompleteOnboardingRequestDTO {
    private DatabaseDTO database;
    private MetadataExtractionRequestDTO extractionRequest;
}
