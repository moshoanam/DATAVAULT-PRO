package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QualityIssueDTO {
    private String fieldName;
    private String issueType;
    private String severity;
    private String message;
    private Double percentageAffected;
}
