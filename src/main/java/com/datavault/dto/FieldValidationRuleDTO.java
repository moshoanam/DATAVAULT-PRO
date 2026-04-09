package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FieldValidationRuleDTO {
    private Long id;
    private Long fieldId;
    private String ruleName;
    private String ruleType; // NOT_NULL, UNIQUE, REGEX, RANGE, CUSTOM
    private String ruleExpression;
    private String description;
    private Boolean enabled;
}
