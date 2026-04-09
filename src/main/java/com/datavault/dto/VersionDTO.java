package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VersionDTO {
    private String versionTag;
    private LocalDateTime versionDate;
    private String changedBy;
    private List<ChangeHistoryDTO> changes;
}
