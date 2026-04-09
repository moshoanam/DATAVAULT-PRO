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
public interface ChangeHistoryRepository extends JpaRepository<ChangeHistory, Long> {
    List<ChangeHistory> findByEntityTypeAndEntityId(String entityType, Long entityId);
    Page<ChangeHistory> findByEntityType(String entityType, Pageable pageable);
    Page<ChangeHistory> findByAction(ChangeAction action, Pageable pageable);
    
    @Query("SELECT c FROM ChangeHistory c WHERE " +
           "(:entityType IS NULL OR c.entityType = :entityType) AND " +
           "(:entityId IS NULL OR c.entityId = :entityId) AND " +
           "(:action IS NULL OR c.action = :action) " +
           "ORDER BY c.changedAt DESC")
    Page<ChangeHistory> findByFilters(
        @Param("entityType") String entityType,
        @Param("entityId") Long entityId,
        @Param("action") ChangeAction action,
        Pageable pageable
    );
    
    @Query("SELECT c FROM ChangeHistory c WHERE c.changedAt BETWEEN :start AND :end")
    List<ChangeHistory> findByDateRange(
        @Param("start") LocalDateTime start,
        @Param("end") LocalDateTime end
    );
}
