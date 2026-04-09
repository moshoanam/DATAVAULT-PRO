package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ERDRelationshipDTO {
    private Long sourceTableId;
    private Long targetTableId;
    private String sourceFieldName;
    private String targetFieldName;
    private String relationshipType; // ONE_TO_ONE, ONE_TO_MANY, MANY_TO_MANY
}
