package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BreakingChangeDTO {
    private Long fieldId;
    private String fieldName;
    private String tableName;
    private String reason;
    private String severity;
    private Integer estimatedImpactHours;
}
