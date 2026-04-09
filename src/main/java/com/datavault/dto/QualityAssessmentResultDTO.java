package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QualityAssessmentResultDTO {
    private Integer totalDatabases;
    private Integer totalTables;
    private Integer tablesAssessed;
    private Double averageQualityScore;
    private List<Map<String, Object>> databaseResults;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
}
