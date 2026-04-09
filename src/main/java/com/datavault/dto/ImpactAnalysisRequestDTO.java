package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ImpactAnalysisRequestDTO {
    @NotNull(message = "Field ID is required")
    private Long fieldId;
    
    @NotBlank(message = "Change type is required")
    private String changeType; // TYPE_CHANGE, DELETE, RENAME, etc.
    
    private String proposedValue;
}
