package com.datavault.repository;

import com.datavault.entity.*;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface GovernanceAlertRepository extends JpaRepository<GovernanceAlert, Long> {
    List<GovernanceAlert> findByIsResolvedFalse(Pageable pageable);
    List<GovernanceAlert> findBySeverity(AlertSeverity severity, Pageable pageable);
    List<GovernanceAlert> findByAlertType(AlertType alertType);
    
    @Query("SELECT a FROM GovernanceAlert a WHERE a.isResolved = false " +
           "AND a.severity IN ('CRITICAL', 'HIGH') ORDER BY a.createdAt DESC")
    List<GovernanceAlert> findCriticalAlerts();
    
    long countByIsResolvedFalse();
}
