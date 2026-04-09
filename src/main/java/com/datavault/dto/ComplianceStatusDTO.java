package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ComplianceStatusDTO {
    private Double overallScore;
    private Long totalFields;
    private Long piiFields;
    private Long fieldsWithOwners;
    private Long fieldsWithDescription;
    private Double ownershipCompliance;
    private Double documentationCompliance;
    private LocalDateTime lastAudit;
}
