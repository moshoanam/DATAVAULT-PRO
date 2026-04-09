package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QualityRuleDTO {
    private Long id;

    @NotNull(message = "Field ID is required")
    private Long fieldId;

    private Long tableId;

    @NotBlank(message = "Rule name is required")
    private String ruleName;

    @NotBlank(message = "Rule type is required")
    private String ruleType;

    private String ruleDefinition;
    private String ruleExpression;
    private String description;
    private Double thresholdValue;
    private Boolean isActive;
    private String validationQuery;
    private String severity;
    private String lastResult;
    private java.time.LocalDateTime lastExecuted;
}
