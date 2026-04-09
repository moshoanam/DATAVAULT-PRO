package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QualityExecutionResultDTO {
    private Long ruleId;
    private String ruleName;
    private String result; // PASSED, FAILED, WARNING
    private Double actualValue;
    private Double thresholdValue;
    private String message;
    private LocalDateTime executedAt;
}
