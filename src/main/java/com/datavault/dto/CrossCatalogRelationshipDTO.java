package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CrossCatalogRelationshipDTO {
    private Long id;
    private Long sourceDatabaseId;
    private String sourceDatabaseName;
    private String sourceTable;
    private String sourceField;
    private Long targetDatabaseId;
    private String targetDatabaseName;
    private String targetTable;
    private String targetField;
    private String relationshipType;
    private Double confidence;
    private String matchReason;
}
