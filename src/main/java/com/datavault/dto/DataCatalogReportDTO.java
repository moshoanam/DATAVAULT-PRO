package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataCatalogReportDTO {
    private Integer totalDatabases;
    private Integer totalTables;
    private Integer totalFields;
    private Double averageQualityScore;
    private Integer crossCatalogRelationships;
    private Integer piiFieldsFound;
    private LocalDateTime generatedAt;
}
