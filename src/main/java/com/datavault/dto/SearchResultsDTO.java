package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SearchResultsDTO {
    private String query;
    private List<DatabaseDTO> databases;
    private List<TableMetadataDTO> tables;
    private List<FieldMetadataDTO> fields;
    private List<GlossaryTermDTO> glossaryTerms;
    private Long totalResults;
}
