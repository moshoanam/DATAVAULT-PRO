package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ERDTableDTO {
    private Long tableId;
    private String tableName;
    private String schema;
    private Long rowCount;
    private Double qualityScore;
    private List<ERDFieldDTO> fields;
    private Integer x; // For positioning
    private Integer y;
}
