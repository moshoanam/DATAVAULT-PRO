package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GovernanceAlertDTO {
    private Long id;
    private String severity;
    private String alertType;
    private String message;
    private String recommendedAction;
    private Boolean isResolved;
    private LocalDateTime createdAt;
}
