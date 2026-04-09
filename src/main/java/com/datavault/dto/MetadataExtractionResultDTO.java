package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MetadataExtractionResultDTO {
    private Boolean success;
    private String message;
    private Integer schemasExtracted;
    private Integer tablesExtracted;
    private Integer fieldsExtracted;
    private Integer relationshipsExtracted;
    private List<String> errors;
}
