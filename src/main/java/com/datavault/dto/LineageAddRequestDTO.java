package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LineageAddRequestDTO {
    private Long sourceFieldId;
    private Long targetFieldId;
    private String lineageType; // DIRECT, DERIVED, AGGREGATED, FILTERED, JOINED
    private String transformationLogic;
    private Double confidence;
}
