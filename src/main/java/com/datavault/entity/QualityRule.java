package com.datavault.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "quality_rules")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class QualityRule {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "table_id")
    private TableMetadata table;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "field_id")
    private FieldMetadata field;

    @Column(name = "rule_name", nullable = false)
    private String ruleName;

    @Column(name = "rule_type", nullable = false)
    private String ruleType;

    @Column(name = "rule_definition", columnDefinition = "TEXT")
    private String ruleDefinition;

    @Column(name = "rule_expression", columnDefinition = "TEXT")
    private String ruleExpression;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(name = "threshold_value")
    private Double thresholdValue;

    @Column(name = "is_active")
    private Boolean isActive = true;

    @Column(name = "validation_query")
    private String validationQuery;

    @Column(name = "severity")
    private String severity;

    @Column(name = "last_executed")
    private LocalDateTime lastExecuted;

    @Column(name = "last_result")
    private String lastResult;

    @Column(name = "created_by")
    private String createdBy;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
}
