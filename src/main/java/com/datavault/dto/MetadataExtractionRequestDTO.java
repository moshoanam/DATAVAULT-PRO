package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MetadataExtractionRequestDTO {
    private Long databaseId;
    private String connectionString;
    private String username;
    private String password;
    private String databaseType;
    private List<String> schemasToExtract;
    private List<String> collectionsToExtract; // For MongoDB
    private Boolean extractLineage;
    private Boolean calculateQuality;
}
