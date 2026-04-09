package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ERDDataDTO {
    private Long databaseId;
    private String databaseName;
    private List<ERDTableDTO> tables;
    private List<ERDRelationshipDTO> relationships;
}
