package com.datavault.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import java.time.LocalDateTime;
import java.util.*;

@Entity
@Table(name = "databases", indexes = {
    @Index(name = "idx_database_name", columnList = "name"),
    @Index(name = "idx_database_env", columnList = "environment")
})
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Database {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String name;

    @Column(name = "database_type", nullable = false)
    private String type; // PostgreSQL, MySQL, Snowflake, etc.

    @Column(nullable = false)
    private String environment; // production, staging, development

    private String connectionString;
    
    private String owner;
    
    @Column(columnDefinition = "TEXT")
    private String description;

    @OneToMany(mappedBy = "database", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<TableMetadata> tables = new ArrayList<>();

    @CreationTimestamp
    private LocalDateTime createdAt;

    @UpdateTimestamp
    private LocalDateTime updatedAt;

    private String createdBy;
    private String lastModifiedBy;

    @Version
    private Long version;
}
