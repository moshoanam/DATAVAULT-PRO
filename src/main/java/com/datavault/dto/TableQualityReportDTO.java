package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableQualityReportDTO {
    private Long tableId;
    private String tableName;
    private Double overallScore;
    private Double completenessScore;
    private Double validityScore;
    private Double consistencyScore;
    private Double uniquenessScore;
    private Integer totalRules;
    private Integer passedRules;
    private Integer failedRules;
    private List<QualityExecutionResultDTO> results;
    private LocalDateTime lastCalculated;
}
