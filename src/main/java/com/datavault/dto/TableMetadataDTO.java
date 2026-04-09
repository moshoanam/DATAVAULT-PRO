package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableMetadataDTO {
    private Long id;
    
    @NotNull(message = "Database ID is required")
    private Long databaseId;
    
    @NotBlank(message = "Schema name is required")
    private String schema;
    
    @NotBlank(message = "Table name is required")
    private String tableName;
    
    private String description;
    private String businessGlossary;
    private Long rowCount;
    private Long sizeBytes;
    private Double dataQualityScore;
    private String databaseName;
    private Integer fieldCount;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
