package com.datavault.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import java.time.LocalDateTime;
import java.util.*;

@Entity
@Table(name = "lineage_relationships", indexes = {
    @Index(name = "idx_source_field", columnList = "source_field_id"),
    @Index(name = "idx_target_field", columnList = "target_field_id")
})
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class LineageRelationship {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "source_field_id", nullable = false)
    private FieldMetadata sourceField;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "target_field_id", nullable = false)
    private FieldMetadata targetField;

    @Column(columnDefinition = "TEXT")
    private String transformationLogic;

    @Enumerated(EnumType.STRING)
    private LineageType lineageType; // DIRECT, DERIVED, AGGREGATED, FILTERED

    private Double confidence;

    @CreationTimestamp
    private LocalDateTime createdAt;

    private String createdBy;

    @Version
    private Long version;
}
