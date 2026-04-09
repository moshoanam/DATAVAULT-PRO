package com.datavault.dto;

import lombok.*;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataStewardDTO {
    private Long id;
    private String name;
    private String email;
    private String team;
    private String department;
    private String role;
    private String phoneNumber;
    private Boolean isActive;
    private List<String> responsibilities;
    private Integer databaseCount;
    private LocalDateTime createdAt;
}
