package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GlossaryTermDTO {
    private Long id;
    
    @NotBlank(message = "Term is required")
    private String term;
    
    @NotBlank(message = "Definition is required")
    private String definition;
    
    private String category;
    private List<String> relatedTerms;
    private List<String> synonyms;
    private Integer usageCount;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
