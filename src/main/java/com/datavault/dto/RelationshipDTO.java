package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RelationshipDTO {
    private Long id;
    private String sourceTable;
    private String sourceColumn;
    private String targetTable;
    private String targetColumn;
    private String relationshipType;
    private String constraintName;
}
