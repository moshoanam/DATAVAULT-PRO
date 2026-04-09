package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LineageRelationshipDTO {
    private Long id;

    @NotNull(message = "Source field ID is required")
    private Long sourceFieldId;

    @NotNull(message = "Target field ID is required")
    private Long targetFieldId;

    private String sourceFieldName;
    private String targetFieldName;
    private String transformationLogic;

    @NotBlank(message = "Lineage type is required")
    private String lineageType;

    private Double confidence;
    private LocalDateTime createdAt;
    private String createdBy;
}
