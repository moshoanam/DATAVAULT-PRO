package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ImpactAnalysisDTO {
    private Long fieldId;
    private String changeType;
    private Integer totalDownstreamDependencies;
    private List<String> affectedTables;
    private List<String> affectedSystems;
    private List<BreakingChangeDTO> breakingChanges;
    private String severity;
    private List<String> recommendedActions;
}
