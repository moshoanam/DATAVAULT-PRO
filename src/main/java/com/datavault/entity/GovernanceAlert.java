package com.datavault.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import java.time.LocalDateTime;
import java.util.*;

// ========== Governance Alert Entity ==========
@Entity
@Table(name = "governance_alerts")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class GovernanceAlert {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Enumerated(EnumType.STRING)
    private AlertSeverity severity; // CRITICAL, HIGH, MEDIUM, LOW

    @Enumerated(EnumType.STRING)
    private AlertType alertType; // PII_MISSING_ENCRYPTION, QUALITY_BELOW_THRESHOLD, etc.

    private String entityType;
    private Long entityId;
    private String entityName;

    @Column(columnDefinition = "TEXT")
    private String message;

    @Column(columnDefinition = "TEXT")
    private String recommendedAction;

    private Boolean isResolved = false;
    private LocalDateTime resolvedAt;
    private String resolvedBy;

    @CreationTimestamp
    private LocalDateTime createdAt;
}
