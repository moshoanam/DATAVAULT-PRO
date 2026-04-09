package com.datavault.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import java.time.LocalDateTime;
import java.util.*;

// ========== Data Quality Metric Entity ==========
@Entity
@Table(name = "data_quality_metrics")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DataQualityMetric {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "table_id")
    private TableMetadata table;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "field_id")
    private FieldMetadata field;

    private String metricName; // COMPLETENESS, UNIQUENESS, VALIDITY, CONSISTENCY
    private Double metricValue;
    private String metricUnit;

    @CreationTimestamp
    private LocalDateTime measuredAt;
}
