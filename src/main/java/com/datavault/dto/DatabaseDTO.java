package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatabaseDTO {
    private Long id;
    
    @NotBlank(message = "Database name is required")
    private String name;
    
    @NotBlank(message = "Database type is required")
    private String type;
    
    @NotBlank(message = "Environment is required")
    private String environment;
    
    private String connectionString;
    private String owner;
    private String description;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private String createdBy;
    private Integer tableCount;
}
