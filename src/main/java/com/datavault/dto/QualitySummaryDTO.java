package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QualitySummaryDTO {
    private Double averageQualityScore;
    private Long highQualityCount;
    private Long mediumQualityCount;
    private Long lowQualityCount;
    private Long totalTables;
}
