package com.datavault.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import java.time.LocalDateTime;
import java.util.*;

@Entity
@Table(name = "table_metadata", indexes = {
    @Index(name = "idx_table_database", columnList = "database_id"),
    @Index(name = "idx_table_name", columnList = "table_name"),
    @Index(name = "idx_table_schema", columnList = "schema_name")
})
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TableMetadata {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "database_id", nullable = false)
    private Database database;

    @Column(name = "schema_name", nullable = false)
    private String schema;

    @Column(name = "table_name", nullable = false)
    private String tableName;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(columnDefinition = "TEXT")
    private String businessGlossary;

    private Long rowCount;
    private Long sizeBytes;
    
    private Double dataQualityScore; // 0-100

    @OneToMany(mappedBy = "table", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<FieldMetadata> fields = new ArrayList<>();

    @OneToMany(mappedBy = "table", cascade = CascadeType.ALL)
    private List<DataQualityMetric> qualityMetrics = new ArrayList<>();

    @CreationTimestamp
    private LocalDateTime createdAt;

    @UpdateTimestamp
    private LocalDateTime updatedAt;

    private String createdBy;
    private String lastModifiedBy;

    @Version
    private Long version;
}
