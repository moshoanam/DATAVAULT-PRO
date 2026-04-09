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
public interface QualityRuleRepository extends JpaRepository<QualityRule, Long> {
    List<QualityRule> findByField(FieldMetadata field);
    List<QualityRule> findByTable(TableMetadata table);
    List<QualityRule> findByIsActiveTrue();
    List<QualityRule> findByRuleType(String ruleType);

    @Query("SELECT r FROM QualityRule r WHERE r.table.id = :tableId AND r.isActive = true")
    List<QualityRule> findActiveRulesByTable(@Param("tableId") Long tableId);
}
