package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FieldMetadataDTO {
    private Long id;
    
    @NotNull(message = "Table ID is required")
    private Long tableId;
    
    @NotBlank(message = "Field name is required")
    private String fieldName;
    
    @NotBlank(message = "Business name is required")
    private String businessName;
    
    @NotBlank(message = "Data type is required")
    private String dataType;
    
    private String description;
    private String businessGlossary;
    private Long glossaryTermId;
    private String glossaryTermName;
    private String sensitivityLevel;
    private String ownerEmail;
    private String ownerName;
    private Boolean isPrimaryKey;
    private Boolean isForeignKey;
    private Boolean isNullable;
    private String defaultValue;
    private Integer maxLength;
    private List<String> qualityRules;
    private String tableName;
    private String databaseName;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
