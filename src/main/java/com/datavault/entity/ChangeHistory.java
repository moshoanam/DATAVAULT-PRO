package com.datavault.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import java.time.LocalDateTime;
import java.util.*;

@Entity
@Table(name = "change_history", indexes = {
    @Index(name = "idx_entity_type", columnList = "entity_type"),
    @Index(name = "idx_entity_id", columnList = "entity_id"),
    @Index(name = "idx_changed_at", columnList = "changed_at")
})
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ChangeHistory {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String entityType; // DATABASE, TABLE, FIELD, GLOSSARY_TERM
    private Long entityId;
    private String entityName;

    @Enumerated(EnumType.STRING)
    private ChangeAction action; // CREATED, UPDATED, DELETED, DEPRECATED

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(columnDefinition = "TEXT")
    private String changeDetails; // JSON of before/after

    private String versionTag; // v1.0.0, v2.3.0, etc.

    @CreationTimestamp
    private LocalDateTime changedAt;

    private String changedBy;
}
