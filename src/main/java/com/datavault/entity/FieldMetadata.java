package com.datavault.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import java.time.LocalDateTime;
import java.util.*;

@Entity
@Table(name = "field_metadata", indexes = {
    @Index(name = "idx_field_table", columnList = "table_id"),
    @Index(name = "idx_field_name", columnList = "field_name"),
    @Index(name = "idx_sensitivity", columnList = "sensitivity_level")
})
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FieldMetadata {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "table_id", nullable = false)
    private TableMetadata table;

    @Column(name = "field_name", nullable = false)
    private String fieldName;

    @Column(nullable = false)
    private String businessName;

    @Column(nullable = false)
    private String dataType;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(columnDefinition = "TEXT")
    private String businessGlossary;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "glossary_term_id")
    private GlossaryTerm glossaryTerm;

    @Column(name = "sensitivity_level")
    @Enumerated(EnumType.STRING)
    private SensitivityLevel sensitivityLevel; // PII, CONFIDENTIAL, INTERNAL, PUBLIC

    @Column(name = "owner_email")
    private String ownerEmail;

    @Column(name = "owner_name")
    private String ownerName;

    private Boolean isPrimaryKey = false;
    private Boolean isForeignKey = false;
    private Boolean isNullable = true;
    private String defaultValue;
    private Integer maxLength;

    @ElementCollection
    @CollectionTable(name = "field_quality_rules", joinColumns = @JoinColumn(name = "field_id"))
    @Column(name = "rule_name")
    private List<String> qualityRules = new ArrayList<>();

    @OneToMany(mappedBy = "sourceField")
    private List<LineageRelationship> downstreamLineage = new ArrayList<>();

    @OneToMany(mappedBy = "targetField")
    private List<LineageRelationship> upstreamLineage = new ArrayList<>();

    @CreationTimestamp
    private LocalDateTime createdAt;

    @UpdateTimestamp
    private LocalDateTime updatedAt;

    private String createdBy;
    private String lastModifiedBy;

    @Version
    private Long version;
}
