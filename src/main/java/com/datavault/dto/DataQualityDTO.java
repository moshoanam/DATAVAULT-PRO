package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataQualityDTO {
    private Long tableId;
    private String tableName;
    private Double overallScore;
    private Double completeness;
    private Double uniqueness;
    private Double validity;
    private Double consistency;
    private LocalDateTime lastMeasured;
    private List<QualityIssueDTO> issues;
}
