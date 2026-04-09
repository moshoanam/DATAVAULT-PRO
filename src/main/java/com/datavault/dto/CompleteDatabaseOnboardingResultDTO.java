package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CompleteDatabaseOnboardingResultDTO {
    private Boolean success;
    private Long databaseId;
    private String databaseName;
    private String message;
    private Integer schemasExtracted;
    private Integer tablesExtracted;
    private Integer fieldsExtracted;
    private Integer relationshipsExtracted;
    private Integer qualityRulesExecuted;
    private Integer lineageRelationshipsBuilt;
    private Integer crossCatalogRelationshipsFound;
    private List<String> errors;
    private List<String> warnings;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
}
