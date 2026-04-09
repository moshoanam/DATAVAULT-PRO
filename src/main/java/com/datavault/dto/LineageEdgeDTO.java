package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LineageEdgeDTO {
    private Long sourceFieldId;
    private Long targetFieldId;
    private String lineageType;
    private String transformationLogic;
}
