package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LineageGraphDTO {
    private Long rootFieldId;
    private String rootFieldName;
    private String direction; // UPSTREAM, DOWNSTREAM, COMPLETE
    private List<LineageNodeDTO> nodes;
    private List<LineageEdgeDTO> edges;
    private Integer depth;
}
