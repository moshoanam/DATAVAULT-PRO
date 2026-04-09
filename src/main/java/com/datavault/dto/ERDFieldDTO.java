package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ERDFieldDTO {
    private Long fieldId;
    private String fieldName;
    private String dataType;
    private Boolean isPrimaryKey;
    private Boolean isForeignKey;
}
