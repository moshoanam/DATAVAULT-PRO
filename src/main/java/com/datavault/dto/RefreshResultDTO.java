package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RefreshResultDTO {
    private Boolean success;
    private Long databaseId;
    private String databaseName;
    private Integer tablesRefreshed;
    private Integer fieldsRefreshed;
    private Integer qualityRecalculated;
    private Integer lineageRebuilt;
    private List<String> errors;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
}
