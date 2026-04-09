package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChangeHistoryDTO {
    private Long id;
    private String entityType;
    private Long entityId;
    private String entityName;
    private String action;
    private String description;
    private String changeDetails;
    private String versionTag;
    private LocalDateTime changedAt;
    private String changedBy;
}
