package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LineageNodeDTO {
    private Long fieldId;
    private String fieldName;
    private String businessName;
    private String tableName;
    private String databaseName;
    private String dataType;
    private String sensitivityLevel;
    private Integer depth;
}
